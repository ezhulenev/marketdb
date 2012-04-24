package com.ergodicity.marketdb.model

import org.joda.time.DateTime
import sbinary.Operations._
import sbinary.{Output, Input, Format, DefaultProtocol}

case class OrderPayload(market: Market, security: Security,
                        orderId: Long,
                        time: DateTime,
                        status: Int,
                        action: Short,
                        dir: Short,
                        price: BigDecimal,
                        amount: Int,
                        amount_rest: Int,
                        deal: Option[BigDecimal])

object OrderProtocol extends DefaultProtocol {
  import MarketProtocol._

  implicit object OrderPayloadFormat extends Format[OrderPayload] {
    def reads(in: Input) = OrderPayload(
      read[Market](in),
      read[Security](in),
      read[Long](in),
      read[DateTime](in),
      read[Int](in),
      read[Short](in),
      read[Short](in),
      read[BigDecimal](in),
      read[Int](in),
      read[Int](in),
      read[Option[BigDecimal]](in)
    )

    def writes(out: Output, payload: OrderPayload) {
      write(out, payload.market)
      write(out, payload.security)
      write(out, payload.orderId)
      write(out, payload.time)
      write(out, payload.status)
      write(out, payload.action)
      write(out, payload.dir)
      write(out, payload.price)
      write(out, payload.amount)
      write(out, payload.amount_rest)
      write(out, payload.deal)
    }
  }  
}