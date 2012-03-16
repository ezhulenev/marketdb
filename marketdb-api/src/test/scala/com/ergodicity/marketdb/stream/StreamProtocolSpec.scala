package com.ergodicity.marketdb.stream

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import sbinary._
import Operations._
import MarketStreamProtocol._
import java.util.Arrays
import com.ergodicity.marketdb.model.{Contract, TradePayload, Market, Code}


class StreamProtocolSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[StreamProtocolSpec])

  describe("Stream Payload Protocol") {
    val market = Market("RTS")
    val code = Code("RIH")
    val contract = Contract("RIM 3.12")

    it("should serialize Trades") {
      val mess = Trades(TradePayload(market, code, contract, BigDecimal(1), 1, new DateTime, 1, false))

      val bytes = toByteArray[StreamPayloadMessage](mess)
      val fromBytes = fromByteArray[StreamPayloadMessage](bytes)

      assert(fromBytes match {
        case Trades(trade) => true
        case _ => false
      })
    }
    it ("should searialize Broken") {
      val mess = Broken("Err")

      val bytes = toByteArray[StreamPayloadMessage](mess)
      val fromBytes = fromByteArray[StreamPayloadMessage](bytes)

      assert(fromBytes match {
        case Broken("Err") => true
        case _ => false
      })
    }
    it ("should serialize Completed") {
      val mess = Completed()

      val bytes = toByteArray[StreamPayloadMessage](mess)
      val fromBytes = fromByteArray[StreamPayloadMessage](bytes)

      assert(fromBytes match {
        case Completed() => true
        case _ => false
      })
    }
  }

  describe("Stream Control Protocol") {
    it("should serialize/deserilize OpenStream") {
      val end = new DateTime()
      val start = end - 2.days

      val mess = OpenStream(Market("RTS"), Code("RIH"), start to end)

      val bytes = toByteArray[StreamControlMessage](mess)
      log.info("Bytes lenght=" + bytes.size + "; Bytes = " + Arrays.toString(bytes))

      val fromBytes = fromByteArray[StreamControlMessage](bytes)
      
      assert(fromBytes match {
        case OpenStream(Market("RTS"), Code("RIH"), i) => i.start == start && i.end == end
        case _ => false
      })
    }

    it("should serialize/deserialize StreamOpened") {
      val mess = StreamOpened(StreamIdentifier("Test"))

      val bytes = toByteArray[StreamControlMessage](mess)
      val fromBytes = fromByteArray[StreamControlMessage](bytes)

      assert(fromBytes match {
        case StreamOpened(StreamIdentifier("Test")) => true
        case _ => false
      })
    }

    it("should serialize/deserialize CloseStream") {
      val mess = CloseStream(StreamIdentifier("Test"))

      val bytes = toByteArray[StreamControlMessage](mess)
      val fromBytes = fromByteArray[StreamControlMessage](bytes)

      assert(fromBytes match {
        case CloseStream(StreamIdentifier("Test")) => true
        case _ => false
      })
    }

    it("should serialize/deserialize StreamClosed") {
      val mess = StreamClosed()

      val bytes = toByteArray[StreamControlMessage](mess)
      val fromBytes = fromByteArray[StreamControlMessage](bytes)

      assert(fromBytes match {
        case StreamClosed() => true
        case _ => false
      })
    }
  }

}