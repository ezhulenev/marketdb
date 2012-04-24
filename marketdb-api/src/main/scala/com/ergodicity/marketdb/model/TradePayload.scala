package com.ergodicity.marketdb.model

import sbinary._
import Operations._
import org.joda.time.DateTime

case class TradePayload(market: Market, security: Security, tradeId: Long, price: BigDecimal, amount: Int, time: DateTime, nosystem: Boolean)

object TradeProtocol extends DefaultProtocol {

  import MarketProtocol._

  implicit object TradePayloadFormat extends Format[TradePayload] {
    def reads(in: Input) = TradePayload(
      read[Market](in),
      read[Security](in),
      read[Long](in),
      read[BigDecimal](in),
      read[Int](in),
      read[DateTime](in),
      read[Boolean](in)
    )

    def writes(out: Output, payload: TradePayload) {
      write[Market](out, payload.market)
      write[Security](out, payload.security)
      write[Long](out, payload.tradeId)
      write[BigDecimal](out, payload.price)
      write[Int](out, payload.amount)
      write[DateTime](out, payload.time)
      write[Boolean](out, payload.nosystem)
    }
  }

}