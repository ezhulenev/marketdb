package com.ergodicity.marketdb.loader

import org.slf4j.LoggerFactory
import org.joda.time.Interval
import com.ergodicity.marketdb.loader.util.Implicits._
import com.twitter.ostrich.admin.{RuntimeEnvironment, Service}
import com.twitter.ostrich.stats.Stats
import com.ergodicity.marketdb.model.TradePayload
import scalaz.IterV

object Loader {
  val log = LoggerFactory.getLogger(getClass.getName)

  var loader: Loader[_] = null
  var runtime: RuntimeEnvironment = null

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      loader = runtime.loadRuntimeConfig[Loader[_]]()
      loader.start()
    } catch {
      case e =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}

class Loader[E](interval: Interval, loader: TradeLoader, i: IterV[TradePayload, E]) extends Service {
  val log = LoggerFactory.getLogger(classOf[Loader[_]])

  if (loader == null) {
    throw new IllegalStateException("Loader not defined")
  }

  def start() {
    log.info("Start marketDB loader")
    log.info("Loader: " + loader)
    log.info("I: " + i)
    log.info("Date interval: " + interval)

    for (day <- interval.toDays) {
      log.info("Load data for: " + day)
      val count = Stats.time("trades_enumeration") {
        loader.enumTrades(day, i)
      }
      log.info("Loader report for day: " + day + "; Report: " + count)
    }
  }

  def shutdown() {
  }
}