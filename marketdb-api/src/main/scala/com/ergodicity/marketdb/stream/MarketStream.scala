package com.ergodicity.marketdb.stream

import org.slf4j.LoggerFactory

sealed trait MarketStream {
  val ref: TradesStreamerRef
}

object TradesStream {
  def apply(ref: TradesStreamerRef) = new TradesStream(ref)
}

class TradesStream(val ref: TradesStreamerRef) extends MarketStream {
  val log = LoggerFactory.getLogger(classOf[TradesStream])

  log.info("Open TradesStream for ref: " + ref)
}