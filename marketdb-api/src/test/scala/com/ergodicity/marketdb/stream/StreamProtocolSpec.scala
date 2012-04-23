package com.ergodicity.marketdb.stream

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import sbinary._
import Operations._
import MarketStreamProtocol._
import java.util.Arrays
import com.ergodicity.marketdb.model.{Security, TradePayload, Market}


class StreamProtocolSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[StreamProtocolSpec])

  describe("Stream Payload Protocol") {
    val market = Market("RTS")
    val security = Security("RIM 3.12")

    it("should serialize Payload") {
      val mess = Payload(TradePayload(market, security, BigDecimal(1), 1, new DateTime, 1, false))

      val bytes = toByteArray[MarketStreamPayload](mess)
      val fromBytes = fromByteArray[MarketStreamPayload](bytes)

      assert(fromBytes match {
        case Payload(trade) => true
        case _ => false
      })
    }
    it ("should searialize Broken") {
      val mess = Broken("Err")

      val bytes = toByteArray[MarketStreamPayload](mess)
      val fromBytes = fromByteArray[MarketStreamPayload](bytes)

      assert(fromBytes match {
        case Broken("Err") => true
        case _ => false
      })
    }
    it ("should serialize Completed") {
      val mess = Completed(true)

      val bytes = toByteArray[MarketStreamPayload](mess)
      val fromBytes = fromByteArray[MarketStreamPayload](bytes)

      assert(fromBytes match {
        case Completed(true) => true
        case _ => false
      })
    }
  }

  describe("Stream Control Protocol") {
    it("should serialize/deserilize OpenStream") {
      val end = new DateTime()
      val start = end - 2.days

      val mess = OpenStream(Market("RTS"), Security("RIH"), start to end)

      val bytes = toByteArray[MarketStreamReq](mess)
      log.info("Bytes lenght=" + bytes.size + "; Bytes = " + Arrays.toString(bytes))

      val fromBytes = fromByteArray[MarketStreamReq](bytes)
      
      assert(fromBytes match {
        case OpenStream(Market("RTS"), Security("RIH"), i) => i.start == start && i.end == end
        case _ => false
      })
    }

    it("should serialize/deserialize StreamOpened") {
      val mess = StreamOpened(MarketStream("Test"))

      val bytes = toByteArray[MarketStreamRep](mess)
      val fromBytes = fromByteArray[MarketStreamRep](bytes)

      assert(fromBytes match {
        case StreamOpened(MarketStream("Test")) => true
        case _ => false
      })
    }

    it("should serialize/deserialize CloseStream") {
      val mess = CloseStream(MarketStream("Test"))

      val bytes = toByteArray[MarketStreamReq](mess)
      val fromBytes = fromByteArray[MarketStreamReq](bytes)

      assert(fromBytes match {
        case CloseStream(MarketStream("Test")) => true
        case _ => false
      })
    }

    it("should serialize/deserialize StreamClosed") {
      val mess = StreamClosed()

      val bytes = toByteArray[MarketStreamRep](mess)
      val fromBytes = fromByteArray[MarketStreamRep](bytes)

      assert(fromBytes match {
        case StreamClosed() => true
        case _ => false
      })
    }
  }

}