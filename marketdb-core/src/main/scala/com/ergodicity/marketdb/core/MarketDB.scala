package com.ergodicity.marketdb.core

import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import com.ergodicity.marketdb.event.TradeReceived
import com.ergodicity.marketdb.uid.UIDProvider
import org.hbase.async.{PutRequest, HBaseClient}
import com.ergodicity.marketdb.{AsyncHBase, ByteArray, Ooops}
import com.ergodicity.marketdb.model._
import com.twitter.ostrich.admin.{Service, RuntimeEnvironment}
import java.util.concurrent.atomic.AtomicBoolean
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.util.{JavaTimer, Promise, Future, FuturePool}
import com.twitter.finagle.service.Backoff
import com.twitter.ostrich.stats.Stats

sealed trait TradeReaction

case class TradePersisted(payload: TradePayload) extends TradeReaction

case class TradeRejected(cause: NonEmptyList[Ooops]) extends TradeReaction

object MarketDB {
  val log = LoggerFactory.getLogger(getClass.getName)
  val MarketIdWidth: Short = 1
  val CodeIdWidth: Short = 3

  var marketDB: MarketDB = null
  var runtime: RuntimeEnvironment = null
  val stopped = new AtomicBoolean(false)

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      marketDB = runtime.loadRuntimeConfig[MarketDB]()
      marketDB.start()
    } catch {
      case e =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}

class MarketDB(client: HBaseClient, marketIdProvider: UIDProvider, codeIdProvider: UIDProvider,
               val kestrelConfig: Option[KestrelConfig],
               val tradesTable: String) extends Service {

  val log = LoggerFactory.getLogger(classOf[MarketDB])
  log.info("Create marketDB for table: " + tradesTable)
  log.info("marketDB Kestrel configuration: "+kestrelConfig)

  val ColumnFamily = ByteArray("id")
  val UidThreadPoolSize = 1;
  val uidFuturePool = FuturePool(Executors.newFixedThreadPool(UidThreadPoolSize))

  // -- Config Kestrel clients
  val clients: Option[Seq[Client]] = kestrelConfig.map(_.hosts.map {
    host =>
      Client(ClientBuilder()
        .codec(Kestrel())
        .hosts(host)
        .hostConnectionLimit(1) // process at most 1 item per connection concurrently
        .buildFactory())
  })

  val readHandles: Option[Seq[ReadHandle]] = kestrelConfig.flatMap {
    conf =>
      val timer = new JavaTimer(isDaemon = true)
      val retryBackoffs = Backoff.const(10.milliseconds)
      clients map (_.map {
        _.readReliably(conf.tradesQueue, timer, retryBackoffs)
      })
  }

  val readHandle: Option[ReadHandle] = readHandles.map(ReadHandle.merged(_))

  def start() {
    log.info("Start marketDB")
    // Attach an async error handler that prints to stderr
    readHandle.map(_.error foreach { e =>
        if (!MarketDB.stopped.get) System.err.println("zomg! got an error " + e)
    })

    // Attach an async message handler that prints the messages to stdout
    readHandle.map(_.messages foreach { msg =>
        try {
          import sbinary._
          import Operations._
          import TradeProtocol._
          val payload = fromByteArray[TradePayload](msg.bytes.array())
          Stats.incr("trades_received", 1)
          addTrade(payload) onSuccess {reaction =>
            log.info("Trade reaction: " + reaction)
            reaction match {
              case TradePersisted(pl) => Stats.incr("trades_persisted", 1)
              case TradeRejected(cause) => Stats.incr("trades_rejected2", 1)
            }            
          }
        } finally {
          msg.ack() // if we don't do this, no more msgs will come to us
        }
    })
  }

  def shutdown() {
    log.info("Shutdown marketDB")
    client.shutdown()
    MarketDB.stopped.set(true)
    readHandle  map {_.close()}
    clients map {_.foreach {_.close()}}
    log.info("marketDB stopped")
  }

  def addTrade(payload: TradePayload) = {
    log.trace("Add trade: " + payload)

    val draftTrade: DraftTrade = Trade.loadFromHistory(Seq(TradeReceived(payload)))

    // Get Unique Ids for market and code
    val marketUid = uidFuturePool(marketIdProvider.provideId(payload.market.value));
    val codeUid = uidFuturePool(codeIdProvider.provideId(payload.code.value))

    val binaryTradeReaction: Future[ValidationNEL[Ooops, Reaction[BinaryTrade]]] = (marketUid join codeUid) map {
      tuple =>
        (tuple._1 |@| tuple._2) {
          (validMarketUid, validCodeUid) =>
            log.trace("Got marketUid=" + validMarketUid + " and codeUid=" + validCodeUid)
            draftTrade.enrichTrade(validMarketUid.id, validCodeUid.id).flatMap(_.serializeTrade()).reaction
        }
    }

    val binaryTrade = binaryTradeReaction map {
      _.flatMap {
        case Rejected(err) => Ooops(err).failNel[BinaryTrade]
        case Accepted(event, value) => value.successNel[Ooops]
      }
    }

    val futureReaction = binaryTrade flatMap {
      case Failure(err) => Future(TradeRejected(err))
      case Success(binary) => putTradeToHBase(payload, binary)
    }

    // Handle exception into Promise
    val promise = new Promise[TradeReaction]

    futureReaction onSuccess {
      reaction =>
        reaction match {
          case TradeRejected(err) => handleRejectedTrade(payload, err)
          case TradePersisted(payload) => "Trade persisted: " + payload
        }
        promise.setValue(reaction)
    } onFailure {
      e =>
        val ooops = NonEmptyList(Ooops("Unknown error", Some(e)))
        handleRejectedTrade(payload, ooops)
        promise.setValue(TradeRejected(ooops))
    }

    promise
  }

  private def putTradeToHBase(payload: TradePayload, binary: BinaryTrade) = {
    implicit def ba2arr(ba: ByteArray) = ba.toArray
    val putRequest = new PutRequest(ByteArray(tradesTable), binary.row, ColumnFamily, binary.qualifier, binary.payload)

    val promise = new Promise[TradeReaction]
    try {
      import AsyncHBase._
      val deferred = client.put(putRequest)
      deferred.addCallback {
        (_: Any) =>
          promise.setValue(TradePersisted(payload))
      }
      deferred.addErrback {
        (e: Throwable) =>
          promise.setValue(TradeRejected(NonEmptyList(Ooops("PutRequest failed", Some(e)))))
      }
    } catch {
      case e => promise.setValue(TradeRejected(NonEmptyList(Ooops("PutRequest failed", Some(e)))))
    }

    promise
  }

  private def handleRejectedTrade(payload: TradePayload, cause: NonEmptyList[Ooops]) {
    log.info("TradePayload rejected: " + payload + "; Cause: " + cause)
    Stats.incr("trades_rejected", 1)
  }

}