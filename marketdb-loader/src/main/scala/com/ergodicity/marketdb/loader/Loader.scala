package com.ergodicity.marketdb.loader

import org.slf4j.LoggerFactory
import org.joda.time.Interval
import com.ergodicity.marketdb.loader.util.Implicits._
import util.Iteratees
import com.twitter.ostrich.admin.{RuntimeEnvironment, Service}
import com.twitter.ostrich.stats.Stats

object Loader {
  val log = LoggerFactory.getLogger(getClass.getName)

  var loader: Loader = null
  var runtime: RuntimeEnvironment = null

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      loader = runtime.loadRuntimeConfig[Loader]()
      loader.start()
      loader.execute()
      loader.shutdown()
      System.exit(0)
    } catch {
      case e =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}

class Loader(loader: Option[TradeLoader], interval: Interval) extends Service {
  val log = LoggerFactory.getLogger(classOf[Loader])

  if (!loader.isDefined) {
    throw new IllegalStateException("Loader not defined")
  }

  def start() {
    log.info("Start marketDB loader")
    log.info("Loader: " + loader)
    log.info("Date interval: " + interval)
  }

  def shutdown() {
    log.info("Shutdown marketDB loader")
  }

  private def execute() {
    import Iteratees._
    for (day <- interval.toDays) {
      log.info("Load data for: " + day)
      val count = Stats.time("trades_enumeration") {
        loader.flatMap(_.enumTrades(day, counter))
      }
      log.info("Loader report for day: " + day + "; Report: " + count)
    }
  }
}
