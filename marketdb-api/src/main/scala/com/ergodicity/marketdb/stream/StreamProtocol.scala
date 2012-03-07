package com.ergodicity.marketdb.stream

import com.ergodicity.marketdb.model.{Market, Code}
import sbinary.Operations._
import sbinary.{Output, Input, Format, DefaultProtocol}
import org.joda.time.{DateTime, Interval}

object StreamProtocol extends DefaultProtocol {

  import com.ergodicity.marketdb.model.TradeProtocol._

  implicit object IntervalFormat extends Format[Interval] {
    def reads(in: Input) = new Interval(read[DateTime](in), read[DateTime](in))

    def writes(out: Output, interval: Interval) {
      write[DateTime](out, interval.getStart)
      write[DateTime](out, interval.getEnd)
    }
  }

  implicit object StreamMessageFormat extends Format[StreamMessage] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => OpenStream(read[Market](in), read[Code](in), read[Interval](in));
      case _ => throw new RuntimeException("Unsupported strem message")
    }

    def writes(out: Output, value: StreamMessage) = value match {
      case o: OpenStream => 
        write[Byte](out, 0)
        write[Market](out, o.market)
        write[Code](out, o.code)
        write[Interval](out, o.interval)
    }
  }

}

sealed abstract class StreamMessage

case class OpenStream(market: Market, code: Code, interval: Interval) extends StreamMessage