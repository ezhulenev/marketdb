package com.ergodicity.marketdb

import model.{TradePayload, Security, Market}
import org.scalatest.WordSpec
import org.joda.time.DateTime
import sbinary._
import Operations._
import com.ergodicity.marketdb.model.TradeProtocol._
import org.slf4j.LoggerFactory

class TradeProtocolSpec extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[TradeProtocolSpec])

  "TradeProtocol" must {
    val market = Market("RTS")
    val security = Security("RTS 3.12")
    val now = new DateTime

    "serialized and deserialized to/from byte array" in {
      val payload = TradePayload(market, security, 11l, BigDecimal("1.111"), 1, now, false)

      val binary = toByteArray(payload)
      log.info("ByteArray length: " + binary.length)
      log.info("Array: " + showArray(binary))

      val fromBinary = fromByteArray[TradePayload](binary)
      log.info("From binary: " + fromBinary)

      assert(fromBinary match {
        case TradePayload(mrkt, sec, id, prc, amnt, t, ns) =>
          mrkt == market && sec == security && prc == BigDecimal("1.111") && id == 11l && amnt == 1 && t == now && ns == false
        case _ => false
      })
    }

    "serialize and deserialize to/from List" in {
      val payload1 = TradePayload(market, security, 11l, BigDecimal("1.111"), 1, now, false)
      val payload2 = TradePayload(market, security, 12l, BigDecimal("1.111"), 1, now, false)

      val list = List(payload1, payload2)

      val binary = toByteArray(list)
      log.info("ByteArray length: " + binary.length)
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
