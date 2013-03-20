package com.ergodicity.marketdb.loader

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import java.io.File
import org.joda.time.{LocalDate, DateTime}
import util.Iteratees

class LocalTradeResolverTest extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[LocalTradeResolverTest])

  val RtsTradeHistoryPattern = "'ft'YYMMdd'.zip'"
  val res = classOf[LocalTradeResolverTest].getResource("/data").toURI
  val LocalRefResolver = RefResolver(new File(res), RtsTradeHistoryPattern)

  "Local RTS History Resolver" must {
    val tradeResolver = new TradeResolver(LocalRefResolver, RtsTradeHistory(_: LocalRef))

    "return None for non existing trade data" in {
      val tradeData = tradeResolver.resolve((new DateTime).toLocalDate)
      assert(tradeData.isEmpty)
    }

    "return Some for existing trade data" in {
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