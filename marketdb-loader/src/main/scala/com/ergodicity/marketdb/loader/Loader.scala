package com.ergodicity.marketdb.loader

import org.slf4j.LoggerFactory
import org.joda.time.Interval
import com.ergodicity.marketdb.loader.util.Implicits._
import util.Iteratees
import com.twitter.ostrich.admin.{RuntimeEnvironment, Service}
import com.twitter.ostrich.stats.Stats
import com.ergodicity.marketdb.model.TradePayload
import java.net.{ConnectException, Socket}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.protocol.Kestrel
import java.util.concurrent.TimeUnit

object Loader {
  val log = LoggerFactory.getLogger(getClass.getName)

  var loader: Loader = null
  var runtime: RuntimeEnvironment = null

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      loader = runtime.loadRuntimeConfig[Loader]()
      loader.start()
    } catch {
      case e =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}

class Loader(loader: Option[TradeLoader], interval: Interval, kestrelConfig: Option[KestrelConfig]) extends Service {
  val log = LoggerFactory.getLogger(classOf[Loader])

  if (!loader.isDefined) {
    throw new IllegalStateException("Loader not defined")
  }

  kestrelConfig.map(assertKestrelRunning(_))

  val client = kestrelConfig.map(cfg =>
    Client(ClientBuilder()
    .codec(Kestrel())
    .hosts(cfg.host+":"+cfg.port)
    .hostConnectionLimit(1) // process at most 1 item per connection concurrently
    .buildFactory()))

  lazy val TradePayloadSerializer = {
    payload: TradePayload =>
      import sbinary._
      import Operations._
      import com.ergodicity.marketdb.model.TradeProtocol._
      toByteArray(payload)
  }

  def start() {
    log.info("Start marketDB loader")
    log.info("Loader: " + loader)
    log.info("Date interval: " + interval)

    import Iteratees._

    val i = kestrelConfig.flatMap(cfg => client.map(kestrelLoader[TradePayload](cfg.tradesQueue, _, TradePayloadSerializer))) getOrElse counter[TradePayload]

    for (day <- interval.toDays) {
      log.info("Load data for: " + day)
      val count = Stats.time("trades_enumeration") {
        loader.flatMap(_.enumTrades(day, i))
      }
      log.info("Loader report for day: " + day + "; Report: " + count)
    }
  }

  def shutdown() {
  }

  private[this] def assertKestrelRunning(conf: KestrelConfig) {
    try {
      new Socket(conf.host, conf.port)
    } catch {
      case e: ConnectException =>
        println("Error: Kestrel must be running on host " + conf.host + "; port " + conf.port)
        System.exit(1)
    }
  }
}

case class LoaderReport(count: Int)
