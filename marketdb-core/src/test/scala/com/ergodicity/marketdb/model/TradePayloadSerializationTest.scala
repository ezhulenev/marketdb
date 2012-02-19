package com.ergodicity.marketdb.model

import org.scalatest.Spec
import org.joda.time.DateTime
import sbinary._
import Operations._
import com.ergodicity.marketdb.model.TradeProtocol._
import com.ergodicity.marketdb.ByteArray
import org.slf4j.LoggerFactory


class TradePayloadSerializationTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[TradePayloadSerializationTest])

  describe("TradePayload") {
    it("should serialized and deserialized to/from byte array") {
      val market = Market("RTS")
      val code = Code("Rih")
      val contract = Contract("RTS 3.12")
      val now = new DateTime
      val payload = TradePayload(market, code, contract, BigDecimal("1.111"), 1, now, 11l, false)

      val binary = toByteArray(payload)
      val ba = ByteArray(binary)
      log.info("ByteArray length: " + ba.length)
      log.info("Array: " + showArray(binary))

      val fromBinary = fromByteArray[TradePayload](binary)
      log.info("From binary: " + fromBinary)

      assert(fromBinary match {
        case TradePayload(mrkt, cd, cntrct, prc, amnt, t, id, ns) =>
          mrkt == market && cd == code &&
            cntrct == contract && prc == BigDecimal("1.111") &&
            amnt == 1 && t == now && id == 11l && ns == false
        case _ => false
      })
    }
  }

  // utility methods for printing a byte array
  def showArray(b: Array[Byte]) = b.map(showByte).mkString(" ")

  def showByte(b: Byte) = pad(((b + 256) % 256).toHexString)

  def pad(s: String) = if (s.length == 1) "0" + s else s


}
