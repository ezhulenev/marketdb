package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import DataFetcher._
import java.io.File

class TypeClassTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[TypeClassTest])


  implicit val cache = new Cache {
    def cache = "MyCache"
  }

  describe("Loader model") {
    it("should load local reference with plaint text payload") {


      val rtsTrades = RtsTradeHistory(InputStreamRef(this.getClass.getResourceAsStream("/data/FT120201.zip")))
      log.info("RTS TRADES: "+rtsTrades)
      fetch(rtsTrades)
    }
  }

  private def fetch[R <: DataRef](data: RtsTradeHistory[R])(implicit fetcher: DataFetcher[R]) {
    log.info("FETCH: "+fetcher.fetch(data.ref))
  }



}
