package com.ergodicity.marketdb.stream

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.model.{Market, Code}
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import sbinary._
import Operations._
import StreamProtocol._
import java.util.Arrays


class StreamProtocolSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[StreamProtocolSpec])

  describe("Stream Protocol") {
    it("should serialize/deserilize OpenStream") {
      val end = new DateTime()
      val start = end - 2.days

      val mess = OpenStream(Market("RTS"), Code("RIH"), start to end)

      val bytes = toByteArray[StreamMessage](mess)
      log.info("Bytes lenght=" + bytes.size + "; Bytes = " + Arrays.toString(bytes))

      val fromBytes = fromByteArray[StreamMessage](bytes)
      
      assert(fromBytes match {
        case OpenStream(Market("RTS"), Code("RIH"), i) => i.start == start && i.end == end
        case _ => false
      })
    }
  }

}