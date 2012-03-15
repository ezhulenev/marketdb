package com.ergodicity.marketdb.stream

import sbinary.Operations._
import sbinary.{Output, Input, Format, DefaultProtocol}
import org.joda.time.{DateTime, Interval}
import com.ergodicity.marketdb.model.{TradePayload, Market, Code}

object StreamProtocol extends DefaultProtocol {

  import com.ergodicity.marketdb.model.TradeProtocol._

  implicit object IntervalFormat extends Format[Interval] {
    def reads(in: Input) = new Interval(read[DateTime](in), read[DateTime](in))

    def writes(out: Output, interval: Interval) {
      write[DateTime](out, interval.getStart)
      write[DateTime](out, interval.getEnd)
    }
  }

  implicit object StreamIdentifierFormat extends Format[StreamIdentifier] {
    def reads(in: Input) = StreamIdentifier(read[String](in))

    def writes(out: Output, value: StreamIdentifier) {
      write[String](out, value.id)
    }
  }

  implicit object StreamMessageFormat extends Format[StreamControlMessage] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => OpenStream(read[Market](in), read[Code](in), read[Interval](in))
      case 1 => StreamOpened(read[StreamIdentifier](in))
      case 2 => CloseStream(read[StreamIdentifier](in))
      case _ => throw new RuntimeException("Unsupported strem message")
    }

    def writes(out: Output, value: StreamControlMessage) = value match {
      case open: OpenStream => 
        write[Byte](out, 0)
        write[Market](out, open.market)
        write[Code](out, open.code)
        write[Interval](out, open.interval)
      case opened: StreamOpened =>
        write[Byte](out, 1)
        write[StreamIdentifier](out, opened.stream)
      case close: CloseStream =>
        write[Byte](out, 2)
        write[StreamIdentifier](out, close.stream)
        
    }
  }

}

case class StreamIdentifier(id: String)

sealed abstract class StreamControlMessage

case class OpenStream(market: Market, code: Code, interval: Interval) extends StreamControlMessage
case class StreamOpened(stream: StreamIdentifier) extends StreamControlMessage
case class CloseStream(stream: StreamIdentifier) extends StreamControlMessage

sealed abstract class StreamPayloadMessage

case class Trades(trade: TradePayload)
case class Completed()