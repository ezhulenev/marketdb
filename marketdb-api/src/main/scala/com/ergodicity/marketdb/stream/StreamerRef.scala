package com.ergodicity.marketdb.stream

import com.ergodicity.zeromq.HeartbeatRef

sealed trait StreamerRef {
  val heartbeat: HeartbeatRef
}

case class TradesStreamerRef(req: String, sub: String, heartbeat: HeartbeatRef) extends StreamerRef