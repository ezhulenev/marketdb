package com.ergodicity.marketdb

import com.ergodicity.marketdb.core.MarketDb
import com.twitter.ostrich.admin.RuntimeEnvironment
import org.slf4j.LoggerFactory

object MarketDbApp {
  val log = LoggerFactory.getLogger(getClass.getName)

  var marketDB: MarketDb = null
  var runtime: RuntimeEnvironment = null

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      marketDB = runtime.loadRuntimeConfig[MarketDb]()
      marketDB.start()
    } catch {
      case e: Throwable =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}
