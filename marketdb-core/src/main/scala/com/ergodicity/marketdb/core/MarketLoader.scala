package com.ergodicity.marketdb.core

import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.finagle.service.Backoff
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.builder.ClientBuilder
import org.joda.time.format.DateTimeFormat
import java.util.concurrent.atomic.{AtomicReference, AtomicLong, AtomicBoolean}
import java.util.TimerTask
import com.twitter.ostrich.stats.Stats
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import com.twitter.util.JavaTimer
import com.ergodicity.marketdb.model.{OrderPayload, TradePayload, TradeProtocol}
import java.net.{ConnectException, Socket}


trait MarketLoader extends MarketService

/**
 * Reads trade data from Kestrel queue, and push it to marketDB
 * @param marketDb marketDB
 * @param config Kestrel server & queue config
 */
class KestrelLoader(val marketDb: MarketDB, config: KestrelConfig) extends MarketLoader {
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

  // Read trades
  val tradesReadHandles: Seq[ReadHandle] = clients map {
    _.readReliably(config.tradesQueue, timer, retryBackoffs)
  }
  val tradesReadHandle: ReadHandle = ReadHandle.merged(tradesReadHandles).buffered(1000)

  // Read orders
  val ordersReadHandles: Seq[ReadHandle] = clients map {
    _.readReliably(config.ordersQueue, timer, retryBackoffs)
  }
  val ordersReadHandle: ReadHandle = ReadHandle.merged(ordersReadHandles).buffered(1000)


  def start() {
    log.info("Start marketDB Kestrel Loader")

    assertKestrelRunning()

    val Formatter = DateTimeFormat.forPattern("YYYY MM dd HH:mm:ss:SSS")

    def initializeTradesHandler() {
      log.info("Initialize trades handler")
      // Attach an async error handler that prints to stderr
      tradesReadHandle.error foreach {
        e =>
          if (!MarketDB.stopped.get) System.err.println("zomg! got an error for Trades" + e)
      }

      val first = new AtomicBoolean(true)
      val start = new AtomicLong()

      val task = new AtomicReference[TimerTask]()
      val resetTimer = new java.util.Timer()

      tradesReadHandle.messages foreach {
        msg =>
          try {
            import sbinary._
            import Operations._
            import TradeProtocol._

            val list = fromByteArray[List[TradePayload]](msg.bytes.array())
            log.trace("Received trades; Size: " + list.size)
            Stats.incr("trades_bunch_received", 1)

            val now = System.currentTimeMillis()
            if (first.get()) {
              Stats.setLabel("first_trade_received", Formatter.print(now))
              start.set(now)
              first.set(false)
            }
            Stats.setLabel("last_trade_received", Formatter.print(now))
            Stats.setLabel("last_trade_received_diff_msec", (now - start.get()).toString)

            val prev = task.getAndSet(new TimerTask {
              def run() {
                first.set(true)
              }
            })
            if (prev != null) prev.cancel()
            resetTimer.schedule(task.get(), TimeUnit.SECONDS.toMillis(10))

            for (payload <- list) {
              Stats.incr("trades_received", 1)
              Stats.timeFutureMillis("add_trade") {
                marketDb.addTrade(payload)
              } onSuccess {
                reaction =>
                  val now = System.currentTimeMillis()
                  Stats.setLabel("last_trade_persisted", Formatter.print(now))
                  Stats.setLabel("last_trade_persisted_diff_msec", (now - start.get()).toString)
                  Stats.incr("trades_persisted", 1)
              } onFailure {
                _ =>
                  Stats.incr("trades_failed", 1)
              }
            }
          } finally {
            msg.ack.sync() // if we don't do this, no more msgs will come to us
          }
      }
    }

    def initializeOrdersHandler() {
      log.info("Initialize orders handler")

      // Attach an async error handler that prints to stderr
      ordersReadHandle.error foreach {
        e =>
          if (!MarketDB.stopped.get) System.err.println("zomg! got an error for Orders" + e)
      }

      val first = new AtomicBoolean(true)
      val start = new AtomicLong()

      val task = new AtomicReference[TimerTask]()
      val resetTimer = new java.util.Timer()

      ordersReadHandle.messages foreach {
        msg =>
          try {
            import sbinary._
            import Operations._
            import com.ergodicity.marketdb.model.OrderProtocol._

            val list = fromByteArray[List[OrderPayload]](msg.bytes.array())
            log.trace("Received orders; Size: " + list.size)
            Stats.incr("orders_bunch_received", 1)

            val now = System.currentTimeMillis()
            if (first.get()) {
              Stats.setLabel("first_order_received", Formatter.print(now))
              start.set(now)
              first.set(false)
            }
            Stats.setLabel("last_order_received", Formatter.print(now))
            Stats.setLabel("last_order_received_diff_msec", (now - start.get()).toString)

            val prev = task.getAndSet(new TimerTask {
              def run() {
                first.set(true)
              }
            })
            if (prev != null) prev.cancel()
            resetTimer.schedule(task.get(), TimeUnit.SECONDS.toMillis(10))

            for (payload <- list) {
              Stats.incr("orders_received", 1)
              Stats.timeFutureMillis("add_order") {
                marketDb.addOrder(payload)
              } onSuccess {
                reaction =>
                  val now = System.currentTimeMillis()
                  Stats.setLabel("last_order_persisted", Formatter.print(now))
                  Stats.setLabel("last_order_persisted_diff_msec", (now - start.get()).toString)
                  Stats.incr("orders_persisted", 1)
              } onFailure {
                _ =>
                  Stats.incr("orders_failed", 1)
              }
            }
          } finally {
            msg.ack.sync() // if we don't do this, no more msgs will come to us
          }
      }
    }

    initializeTradesHandler()
    initializeOrdersHandler()
  }

  def shutdown() {
    log.info("Shutdown marketDB Kestrel Loader")
    tradesReadHandle.close()
    ordersReadHandle.close()
    clients foreach {_.close()}
    log.info("marketDB Kestrel Loader stopped")
  }

  protected def assertKestrelRunning() {
    try {
      config.hosts.foreach {
        host =>
          new Socket(host.split(":")(0), host.split(":")(1).toInt)
      }
    } catch {
      case e: ConnectException => 
        log.error("Error: Kestrel must be running on each host: " + config.hosts)
        throw e;
    }
  }

}