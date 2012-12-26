package com.ergodicity.marketdb.loader

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import util.Iteratees

class TradeDataIterateeTest extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[TradeDataIterateeTest])

  val RtsTrades = () => {
    RtsTradeHistory(InputStreamRef(this.getClass.getResourceAsStream("/data/FT120201.zip")))
  }

  "TradeDataIteratee" must {
    "iterate over RtsTradeHistory" in {
      import TradeDataIteratee._
      import Iteratees._

      val print = RtsTrades().enumTradeData(printer(log)) map (_.run)
      print.unsafePerformIO

      val count = RtsTrades().enumTradeData(counter) map (_.run)
      val countValue = count.unsafePerformIO

      log.info("Trades count: "+countValue)
      assert(countValue == 60)
    }
  }


}
