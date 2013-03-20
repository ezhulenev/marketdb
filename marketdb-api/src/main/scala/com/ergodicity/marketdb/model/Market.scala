package com.ergodicity.marketdb.model

import sbinary.Operations._
import sbinary.{DefaultProtocol, Output, Input, Format}
import org.joda.time.DateTime

case class Market(value: String)

case class Security(isin: String)

object MarketProtocol extends DefaultProtocol {

  implicit object MarketFormat extends Format[Market] {
    def reads(in: Input) = Market(read[String](in))

    def writes(out: Output, market: Market) {
      write[String](out, market.value)
    }
  }

  implicit object SecurityFormat extends Format[Security] {
    def reads(in: Input) = Security(read[String](in))

    def writes(out: Output, security: Security) {
      write[String](out, security.isin)
    }
  }

  implicit object DateTimeBinaryFormat extends Format[DateTime] {
    def reads(in: Input) = new DateTime(read[Long](in))

    def writes(out: Output, dateTime: DateTime) {
      write[Long](out, dateTime.getMillis)
    }
  }

}