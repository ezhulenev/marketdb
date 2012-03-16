package com.ergodicity.marketdb.stream

import com.ergodicity.zeromq.HeartbeatRef

sealed trait MarketStreamerRef {
  val heartbeat: HeartbeatRef
}

case class TradesStreamerRef(req: String, sub: String, heartbeat: HeartbeatRef) extends MarketStreamerRef