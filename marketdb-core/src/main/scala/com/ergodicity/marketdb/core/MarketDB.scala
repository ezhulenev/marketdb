package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.event.TradeReceived
import com.ergodicity.marketdb.uid.UIDProvider
import org.hbase.async.{PutRequest, HBaseClient}
import com.ergodicity.marketdb.{AsyncHBase, ByteArray}
import com.ergodicity.marketdb.model._
import com.twitter.ostrich.admin.{Service, RuntimeEnvironment}
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.util.{JavaTimer, Promise, Future}
import com.twitter.finagle.service.Backoff
import com.twitter.ostrich.stats.Stats
import org.joda.time.format.DateTimeFormat
import org.joda.time.Interval
import com.twitter.finagle.util.Timer
import java.util.TimerTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicBoolean}

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
  val clients: Option[Seq[Client]] = kestrelConfig.map(cfg => cfg.hosts.map {
    host =>
      Client(ClientBuilder()
        .codec(Kestrel())
        .hosts(host)
        .hostConnectionLimit(cfg.hostConnectionLimit)
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
    
    val first = new AtomicBoolean(true)
    val Formatter = DateTimeFormat.forPattern("YYYY MM dd HH:mm:ss:SSS")
    val start = new AtomicLong()

    val task = new AtomicReference[TimerTask]()
    val resetTimer = new java.util.Timer()

    // Attach an async message handler that prints the messages to stdout
    readHandle.map(_.messages foreach { msg =>
        try {
          import sbinary._
          import Operations._
          import TradeProtocol._

          val list = Stats.time("from_byte_array") {fromByteArray[List[TradePayload]](msg.bytes.array())}
          log.trace("Received trades; Size: "+list.size)
          Stats.incr("trades_bunch_received", 1)

          val now = System.currentTimeMillis()
          if (first.get()) {
            Stats.setLabel("first_received", Formatter.print(now))
            start.set(now)
            first.set(false)
          }
          Stats.setLabel("last_received", Formatter.print(now))
          Stats.setLabel("last_received_diff_msec", (now - start.get()).toString)

          val prev = task.getAndSet(new TimerTask {
            def run() {first.set(true)}
          })
          if (prev != null) prev.cancel()
          resetTimer.schedule(task.get(), TimeUnit.SECONDS.toMillis(10))

          for (payload <- list) {
            Stats.incr("trades_received", 1)
            Stats.timeFutureMillis("add_trade") {addTrade(payload)} onSuccess {reaction =>
              val now = System.currentTimeMillis()
              Stats.setLabel("last_persisted", Formatter.print(now))
              Stats.setLabel("last_persisted_diff_msec", (now - start.get()).toString)
              Stats.incr("trades_persisted", 1)
            } onFailure {_ =>
              Stats.incr("trades_failed", 1)
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
    val marketUid = Stats.timeFutureMillis("get_market_uid") {marketIdProvider.provideId(payload.market.value)}
    val codeUid = Stats.timeFutureMillis("get_code_uid") {codeIdProvider.provideId(payload.code.value)}

    val binaryTradeReaction: Future[Reaction[BinaryTrade]] = (marketUid join codeUid) map {
      tuple =>
        draftTrade.enrichTrade(tuple._1.id, tuple._2.id).flatMap(_.serializeTrade()).reaction
    }

    val binaryTrade = binaryTradeReaction map {
        case Accepted(event, value) => value
        case Rejected(err) => throw new RuntimeException("Trade rejected: "+err);
    }

    binaryTrade flatMap {putTradeToHBase(_)} onFailure {err =>
      handleFailedTrade(payload, err)
    }
  }

  private def putTradeToHBase(binary: BinaryTrade) = {
    implicit def ba2arr(ba: ByteArray) = ba.toArray
    val putRequest = new PutRequest(ByteArray(tradesTable), binary.row, ColumnFamily, binary.qualifier, binary.payload)

    val promise = new Promise[Boolean]
    try {
      import AsyncHBase._
      val deferred = client.put(putRequest)
      deferred.addCallback {(_: Any) =>promise.setValue(true)}
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