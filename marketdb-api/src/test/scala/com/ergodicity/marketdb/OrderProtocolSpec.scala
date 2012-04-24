package com.ergodicity.marketdb

import org.scalatest.WordSpec
import org.joda.time.DateTime
import sbinary.Operations._
import com.ergodicity.marketdb.model.{OrderProtocol, Market, Security, OrderPayload}
import OrderProtocol._

class OrderProtocolSpec extends WordSpec {
  "OrderProtocol" must {
    "serialize and deserialize OrderPayload" in {
      val time = new DateTime()
      val order1 = OrderPayload(Market("RTS"), Security("RIM"), 111l, time, 100, 101, 1, BigDecimal(123), 1000, 1100, None)
      val order2 = OrderPayload(Market("RTS"), Security("RIM"), 111l, time, 100, 101, 1, BigDecimal(123), 1000, 1100, Some(BigDecimal(456)))

      val bytes1 = toByteArray[OrderPayload](order1)
      val fromBytes1 = fromByteArray[OrderPayload](bytes1)
      assert(fromBytes1 == order1)

      val bytes2 = toByteArray[OrderPayload](order2)
      val fromBytes2 = fromByteArray[OrderPayload](bytes2)
      assert(fromBytes2 == order2)
    }
  }
}