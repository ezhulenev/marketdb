package com.ergodicity.marketdb.stream

import sbinary.Operations._
import sbinary.{Output, Input, Format, DefaultProtocol}
import org.joda.time.{DateTime, Interval}
import com.ergodicity.marketdb.model.{TradePayload, Market, Code}

object MarketStreamProtocol extends DefaultProtocol {

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

  implicit object StreamControlMessageFormat extends Format[StreamControlMessage] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => OpenStream(read[Market](in), read[Code](in), read[Interval](in))
      case 1 => StreamOpened(read[StreamIdentifier](in))
      case 2 => CloseStream(read[StreamIdentifier](in))
      case 3 => StreamClosed()
      case _ => throw new RuntimeException("Unsupported strem message")
    }

    def writes(out: Output, value: StreamControlMessage) = value match {
      case OpenStream(market, code, interval) =>
        write[Byte](out, 0)
        write[Market](out, market)
        write[Code](out, code)
        write[Interval](out, interval)
      case StreamOpened(stream) =>
        write[Byte](out, 1)
        write[StreamIdentifier](out, stream)
      case CloseStream(stream) =>
        write[Byte](out, 2)
        write[StreamIdentifier](out, stream)
      case closed: StreamClosed =>
        write[Byte](out, 3)
    }
  }

  implicit object StreamPayloadMessageFormat extends Format[StreamPayloadMessage] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => Trades(read[TradePayload](in))
      case 1 => Broken(read[String](in))
      case 2 => Completed()
      case _ => throw new RuntimeException("Unsupported strem message")
    }

    def writes(out: Output, value: StreamPayloadMessage) = value match {
      case Trades(trade) =>
        write[Byte](out, 0)
        write[TradePayload](out, trade)
      case Broken(err) =>
        write[Byte](out, 1)
        write[String](out, err)
      case Completed() =>
        write[Byte](out, 2)
    }
  }
}

case class StreamIdentifier(id: String)

sealed abstract class StreamControlMessage

case class OpenStream(market: Market, code: Code, interval: Interval) extends StreamControlMessage
case class StreamOpened(stream: StreamIdentifier) extends StreamControlMessage
case class CloseStream(stream: StreamIdentifier) extends StreamControlMessage
case class StreamClosed() extends StreamControlMessage

sealed abstract class StreamPayloadMessage

case class Trades(trade: TradePayload) extends StreamPayloadMessage
case class Broken(err: String) extends StreamPayloadMessage
case class Completed() extends StreamPayloadMessage