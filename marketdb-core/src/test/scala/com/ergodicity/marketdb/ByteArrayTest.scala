package com.ergodicity.marketdb

import org.scalatest.Spec
import org.slf4j.LoggerFactory

class ByteArrayTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[ByteArrayTest])

  describe("ByteArray") {
    it("should properly ovverride equals") {
      val ba1 = new ByteArray(Array[Byte](0, 0, 1))
      val ba2 = new ByteArray(Array[Byte](0, 0, 1))
      val ba3 = new ByteArray(Array[Byte](0, 0, 2))

      assert(ba1 == ba2)
      assert(ba1 != ba3)
    }

    it("should be proeprly constructed from bytes") {
      val ba = ByteArray(0, 0, 1)
      log.info("ByteArray = " + ba)
    }
  }

}
