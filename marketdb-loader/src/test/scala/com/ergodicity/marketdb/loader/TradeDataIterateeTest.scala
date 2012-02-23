package com.ergodicity.marketdb.loader

import scalaz._
import IterV._
import org.scalatest.Spec
import org.slf4j.LoggerFactory

class TradeDataIterateeTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[TradeDataIterateeTest])

  val RtsTrades = () => {
    RtsTradeHistory(InputStreamRef(this.getClass.getResourceAsStream("/data/FT120201.zip")))
  }

  describe("TradeDataIteratee") {
    it("should iterate over RtsTradeHistory") {
      import TradeDataIteratee._

      val print = RtsTrades().enumTradeData(printer) map (_.run)
      print.unsafePerformIO

      val count = RtsTrades().enumTradeData(counter) map (_.run)
      val countValue = count.unsafePerformIO

      log.info("Trades count: "+countValue)
      assert(countValue == 60)
    }
  }

  def printer[E]: IterV[E, Boolean] = {
    def step(is: Boolean, e: E)(s: Input[E]): IterV[E, Boolean] = {
      log.info("STEP: " + e)
      s(el = e2 => Cont(step(is, e2)),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Boolean] = {
      s(el = e1 => Cont(step(true, e1)),
        empty = Cont(first),
        eof = Done(true, EOF[E]))
    }

    Cont(first)
  }


  def counter[E]: IterV[E, Int] = {
    def step(is: Int, e: E)(s: Input[E]): IterV[E, Int] = {
      s(el = e2 => Cont(step(is+1, e2)),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Int] = {
      s(el = e1 => Cont(step(1, e1)),
        empty = Cont(first),
        eof = Done(0, EOF[E]))
    }

    Cont(first)
  }

}
