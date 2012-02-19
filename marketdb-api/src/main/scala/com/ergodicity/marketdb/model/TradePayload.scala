package com.ergodicity.marketdb.model

import sbinary._
import Operations._
import org.joda.time.DateTime

case class Market(value: String)

case class Code(value: String)

case class Contract(value: String)

case class TradePayload(market: Market, code: Code, contract: Contract,
                        price: BigDecimal, amount: Int, time: DateTime, tradeId: Long, nosystem: Boolean)

object TradeProtocol extends DefaultProtocol {

  implicit object DateTimeFormat extends Format[DateTime] {
    def reads(in: Input) = new DateTime(read[Long](in))

    def writes(out: Output, dateTime: DateTime) {
      write[Long](out, dateTime.getMillis)
    }
  }

  implicit object MarketFormat extends Format[Market] {
    def reads(in: Input) = Market(read[String](in))

    def writes(out: Output, market: Market) {
      write[String](out, market.value)
    }
  }

  implicit object CodeFormat extends Format[Code] {
    def reads(in: Input) = Code(read[String](in))

    def writes(out: Output, code: Code) {
      write[String](out, code.value)
    }
  }

  implicit object ContractFormat extends Format[Contract] {
    def reads(in: Input) = Contract(read[String](in))

    def writes(out: Output, contract: Contract) {
      write[String](out, contract.value)
    }
  }

  implicit object TradePayloadFormat extends Format[TradePayload] {
    def reads(in: Input) = TradePayload(
      read[Market](in),
      read[Code](in),
      read[Contract](in),
      read[BigDecimal](in),
      read[Int](in),
      read[DateTime](in),
      read[Long](in),
      read[Boolean](in)
    )

    def writes(out: Output, payload: TradePayload) {
      write[Market](out, payload.market)
      write[Code](out, payload.code)
      write[Contract](out, payload.contract)
      write[BigDecimal](out, payload.price)
      write[Int](out, payload.amount)
      write[DateTime](out, payload.time)
      write[Long](out, payload.tradeId)
      write[Boolean](out, payload.nosystem)
    }
  }

}