package com.ergodicity.marketdb.stream

import java.util.UUID

abstract sealed class Heartbeat {
  def uid: UUID
}
case class Ping(uid: UUID) extends Heartbeat
case class Pong(uid: UUID) extends Heartbeat