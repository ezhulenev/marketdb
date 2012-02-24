package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import java.io.File
import org.joda.time.{LocalDate, DateTime}
import util.Iteratees

class TradeResolverTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[TradeResolverTest])

  val RtsTradeHistoryPattern = "'ft'YYMMdd'.zip'"
  val res = classOf[TradeResolverTest].getResource("/data").toURI
  val LocalRefResolver = RefResolver(new File(res), RtsTradeHistoryPattern)

  describe("Local RTS History Resolver") {
    val tradeResolver = new TradeResolver(LocalRefResolver, RtsTradeHistory(_: LocalRef))

    it("should return None for non existing trade data") {
      val tradeData = tradeResolver.resolve((new DateTime).toLocalDate)
      assert(tradeData.isEmpty)
    }

    it("should return Some for existing trade data") {
      val date = new LocalDate(2012, 2, 1)
      val tradeData = tradeResolver.resolve(date)
      log.info("Trade data: " + tradeData)
      assert(tradeData.isDefined)
      
      import TradeDataIteratee._
      import Iteratees._

      // Print trade data
      // tradeData.map(_.enumTradeData(printer(log))).map(_.unsafePerformIO).map(_.run)

      // Count Trade Data
      val count = tradeData.map(_.enumTradeData(counter)).map(_.unsafePerformIO).map(_.run).get
      log.info("Count: "+count)
      assert(count == 60)
    }
  }

}