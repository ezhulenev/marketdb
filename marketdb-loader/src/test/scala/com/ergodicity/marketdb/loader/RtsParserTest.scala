package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import scalaz._
import Scalaz._
import scalaz.effects.IO

class RtsParserTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[RtsParserTest])

  describe("RTS History Parser") {
    val parser = DataParser.RtsTradeHistoryParser

    it("should parse history zip") {

      val is = this.getClass.getResourceAsStream("/data/FT120201.zip").pure[IO]
      val iterator = is flatMap parser.handleInputStream _

      val countAll = iterator map {it =>
        var n = 0;
        while (it.hasNext) {
          it.next()
          n+=1
        }
        n
      }
      
      val count = countAll.unsafePerformIO
      log.info("Count: "+count)
      assert(count == 60)
    }
  }
}