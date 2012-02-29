package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.event.TradeReceived
import com.ergodicity.marketdb.uid.UIDProvider
import org.hbase.async.{PutRequest, HBaseClient}
import com.ergodicity.marketdb.{AsyncHBase, ByteArray}
import com.ergodicity.marketdb.model._
import com.twitter.ostrich.admin.{Service, RuntimeEnvironment}
import java.util.concurrent.atomic.AtomicBoolean
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.util.{JavaTimer, Promise, Future}
import com.twitter.finagle.service.Backoff
import com.twitter.ostrich.stats.Stats

case class TradePersisted(payload: TradePayload)

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
            log.info("Trade persisted")
            Stats.incr("trades_persisted", 1)
          } onFailure {_ =>
            log.info("Trade failed")
            Stats.incr("trades_failed", 1)
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
    val marketUid = marketIdProvider.provideId(payload.market.value)
    val codeUid = codeIdProvider.provideId(payload.code.value)

    val binaryTradeReaction: Future[Reaction[BinaryTrade]] = (marketUid join codeUid) map {
      tuple =>
        log.trace("Got marketUid=" + tuple._1 + " and codeUid=" + tuple._2)
        draftTrade.enrichTrade(tuple._1.id, tuple._2.id).flatMap(_.serializeTrade()).reaction
    }

    val binaryTrade = binaryTradeReaction map {
        case Accepted(event, value) => value
        case Rejected(err) => throw new RuntimeException("Trade rejected: "+err);
    }

    binaryTrade flatMap {putTradeToHBase(payload, _)} onFailure {err =>
      handleFailedTrade(payload, err)
    }
  }

  private def putTradeToHBase(payload: TradePayload, binary: BinaryTrade) = {
    implicit def ba2arr(ba: ByteArray) = ba.toArray
    val putRequest = new PutRequest(ByteArray(tradesTable), binary.row, ColumnFamily, binary.qualifier, binary.payload)

    val promise = new Promise[AnyRef]
    try {
      import AsyncHBase._
      val deferred = client.put(putRequest)
      deferred.addCallback {(_: Any) =>promise.setValue(new AnyRef)}
      deferred.addErrback {(e: Throwable) => promise.setException(e)}
    } catch {
      case e => promise.setException(e)
    }

    promise
  }

  private def handleFailedTrade(payload: TradePayload, cause: Throwable) {
    log.info("Failed to save TradePayload: " + payload + "; Cause: " + cause)
  }

}