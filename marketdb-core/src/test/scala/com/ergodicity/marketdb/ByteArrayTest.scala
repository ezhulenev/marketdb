package com.ergodicity.marketdb

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory

class ByteArrayTest extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[ByteArrayTest])

  "ByteArray" must {
    "properly ovverride equals" in {
      val ba1 = new ByteArray(Array[Byte](0, 0, 1))
      val ba2 = new ByteArray(Array[Byte](0, 0, 1))
      val ba3 = new ByteArray(Array[Byte](0, 0, 2))

      assert(ba1 == ba2)
      assert(ba1 != ba3)
    }

    "be proeprly constructed from bytes" in {
      val ba = ByteArray(0, 0, 1)
      log.info("ByteArray = " + ba)
    }
  }

}
