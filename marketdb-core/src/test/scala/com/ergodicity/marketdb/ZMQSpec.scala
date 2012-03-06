package com.ergodicity.marketdb

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.zeromq.ZMQ

class ZMQSpec extends WordSpec with MustMatchers {
  "ZMQ" must {
    "support Socket#getType" in {
      val context = ZMQ.context(1)
      val sub = context.socket(ZMQ.SUB)
      sub.getType must equal(ZMQ.SUB)
      sub.close
    }
  }
}
