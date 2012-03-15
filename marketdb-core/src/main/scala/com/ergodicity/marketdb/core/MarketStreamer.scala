package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory

trait MarketStreamer extends MarketService

class TradesStreamer extends MarketStreamer {
  val log = LoggerFactory.getLogger(classOf[TradesStreamer])

  def start() {
    log.info("Start TradesStreamer")
  }

  def shutdown() {
    log.info("Shutdown TradesStreamer")

  }
}

