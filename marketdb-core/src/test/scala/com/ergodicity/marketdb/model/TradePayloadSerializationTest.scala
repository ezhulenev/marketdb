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
    val market = Market("RTS")
    val code = Code("Rih")
    val contract = Contract("RTS 3.12")
    val now = new DateTime

    it("should serialized and deserialized to/from byte array") {
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

    it("should serialize and deserialize to/from List") {
      val payload1 = TradePayload(market, code, contract, BigDecimal("1.111"), 1, now, 11l, false)
      val payload2 = TradePayload(market, code, contract, BigDecimal("1.111"), 1, now, 12l, false)

      val list = List(payload1, payload2)

      val binary = toByteArray(list)
      val ba = ByteArray(binary)
      log.info("ByteArray length: " + ba.length)
      log.info("Array: " + showArray(binary))

      val fromBinary = fromByteArray[List[TradePayload]](binary)
      log.info("From binary: " + fromBinary)

      assert(fromBinary.size == 2)
    }
  }

  // utility methods for printing a byte array
  def showArray(b: Array[Byte]) = b.map(showByte).mkString(" ")

  def showByte(b: Byte) = pad(((b + 256) % 256).toHexString)

  def pad(s: String) = if (s.length == 1) "0" + s else s


}
