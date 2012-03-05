package com.ergodicity.marketdb.core

import com.twitter.ostrich.admin.Service
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.util.JavaTimer
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.builder.ClientBuilder
import org.joda.time.format.DateTimeFormat
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicBoolean}
import java.util.TimerTask
import com.twitter.ostrich.stats.Stats
import com.ergodicity.marketdb.model.{TradePayload, TradeProtocol}
import java.util.concurrent.TimeUnit
import org.slf4j.LoggerFactory


trait MarketLoader extends MarketService

/**
 * Reads trade data from Kestrel queue, and push it to marketDB
 * @param marketDb marketDB
 * @param config Kestrel server & queue config
 */
class KestrelLoader(val marketDb: MarketDB, config: KestrelConfig) extends MarketService {
  val log = LoggerFactory.getLogger(classOf[KestrelLoader])

  log.info("Create marketDB Kestrel loader for configuration: " + config)

  // -- Config Kestrel clients
  val clients: Seq[Client] = config.hosts.map {
    host =>
      Client(ClientBuilder()
        .codec(Kestrel())
        .hosts(host)
        .hostConnectionLimit(config.hostConnectionLimit)
        .buildFactory())
  }

  val timer = new JavaTimer(isDaemon = true)
  val retryBackoffs = Backoff.const(10.milliseconds)

  val readHandles: Seq[ReadHandle] = clients map {
    _.readReliably(config.tradesQueue, timer, retryBackoffs)
  }

  val readHandle: ReadHandle = ReadHandle.merged(readHandles)

  def start() {
    log.info("Start marketDB  Kestrel Loader")

    // Attach an async error handler that prints to stderr
    readHandle.error foreach { e =>
      if (!MarketDB.stopped.get) System.err.println("zomg! got an error " + e)
    }

    val first = new AtomicBoolean(true)
    val Formatter = DateTimeFormat.forPattern("YYYY MM dd HH:mm:ss:SSS")
    val start = new AtomicLong()

    val task = new AtomicReference[TimerTask]()
    val resetTimer = new java.util.Timer()

    // Attach an async message handler that prints the messages to stdout
    readHandle.messages foreach { msg =>
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
          Stats.timeFutureMillis("add_trade") {marketDb.addTrade(payload)} onSuccess {reaction =>
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
    }
  }

  def shutdown() {
    log.info("Shutdown marketDB Kestrel Loader")
    readHandle.close()
    clients foreach {_.close()}
    log.info("marketDB Kestrel Loader stopped")
  }
}
