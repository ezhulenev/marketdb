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
import com.ergodicity.marketdb.model.{TradePayload, TradeProtocol}
import org.slf4j.LoggerFactory
import com.ergodicity.zeromq.{Bind, Subscribe}
import org.zeromq.ZMQ
import java.util.concurrent.{Executors, TimeUnit}
import com.twitter.util.{FuturePool, JavaTimer}
import org.zeromq.ZMQ.Context


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

  val readHandles: Seq[ReadHandle] = clients map {
    _.readReliably(config.tradesQueue, timer, retryBackoffs)
  }

  val readHandle: ReadHandle = ReadHandle.merged(readHandles).buffered(1000)

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

    readHandle.messages foreach { msg =>
      try {
        import sbinary._
        import Operations._
        import TradeProtocol._

        val list = fromByteArray[List[TradePayload]](msg.bytes.array())
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

class ZMQLoader(val marketDb: MarketDB, endpoint: String) extends MarketLoader {
  import com.ergodicity.zeromq.{Client => ZMQClient, ReadHandle => ZMQReadHandle}
  import com.ergodicity.zeromq.SocketType._
  import TradeProtocol._

  val log = LoggerFactory.getLogger(classOf[ZMQLoader])

  log.info("Create marketDB ZMQ loader for endpoint: " + endpoint)

  implicit val context = ZMQ.context(1)
  implicit val pool = FuturePool(Executors.newSingleThreadExecutor)

  private val Interrupt = new AtomicBoolean(false)

  val client = ZMQClient(Sub, options = Bind(endpoint) :: Subscribe.all :: Nil)
  /*val readHandle: ZMQReadHandle[List[TradePayload]] = client.read[List[TradePayload]]*/

  val first = new AtomicBoolean(true)
  val Formatter = DateTimeFormat.forPattern("YYYY MM dd HH:mm:ss:SSS")
  val startTime = new AtomicLong()

  val task = new AtomicReference[TimerTask]()
  val resetTimer = new java.util.Timer()

  def handleTrades(list: List[TradePayload]) {
      log.trace("Received trades; Size: " + list.size)
      Stats.incr("trades_bunch_received", 1)

      val now = System.currentTimeMillis()
      if (first.get()) {
        Stats.setLabel("first_received", Formatter.print(now))
        startTime.set(now)
        first.set(false)
      }
      Stats.setLabel("last_received", Formatter.print(now))
      Stats.setLabel("last_received_diff_msec", (now - startTime.get()).toString)

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
            Stats.setLabel("last_persisted", Formatter.print(now))
            Stats.setLabel("last_persisted_diff_msec", (now - startTime.get()).toString)
            Stats.incr("trades_persisted", 1)
        } onFailure {
          _ =>
            Stats.incr("trades_failed", 1)
        }
      }

  }

  def start() {
    log.info("Start marketDB ZMQ Loader")

    /*// Attach an async error handler that prints to stderr
    readHandle.error foreach { e =>
      if (!MarketDB.stopped.get) System.err.println("zomg! got an error " + e)
    }
    readHandle.messages foreach handleTrades*/

    pool {
      while (!Interrupt.get()) {
        handleTrades(client.recv[List[TradePayload]])
      }
    }
  }

  def shutdown() {
    log.info("Shutdown marketDB ZMQ Loader")
    Interrupt.set(true)
    /*readHandle.close()*/
    client.close()
    log.info("marketDB ZMQ Loader stopped")
  }
}

