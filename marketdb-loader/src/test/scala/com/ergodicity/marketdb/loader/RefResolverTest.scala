package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import java.io.File

class RefResolverTest extends Spec {
  val EmptyPattern = "empty"

  describe("Local Reference Resolver") {
    it("should throw exception on bad directory") {
      intercept[IllegalArgumentException] {
        RefResolver(new File("NoSuchDirecotry"), EmptyPattern)
      }
    }
  }
}