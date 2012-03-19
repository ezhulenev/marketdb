package com.ergodicity.marketdb.core

import scalaz._
import IterV._
import scalaz.{Input, IterV}
import org.joda.time.Interval
import com.ergodicity.marketdb.model.{TradePayload, Market, Code}
import com.twitter.conversions.time._
import org.hbase.async.Scanner
import collection.JavaConversions._
import sbinary.Operations._
import sbinary.Reads

class ScannerW[T](underlying: Scanner)(implicit reads: Reads[T]) {
  
  var fetched: Option[Iterator[T]] = None

  private def fetchFromUnderlyingScanner: Option[Iterator[T]] = {
    val rows = underlying.nextRows().joinUninterruptibly(MarketIteratee.Timeout.inMillis)
    if (rows != null) {
      val data = asScalaIterator(rows.iterator()) flatMap {
        row =>
          asScalaIterator(row.iterator())
      } map {
        kv =>
          fromByteArray[T](kv.value())
      }
      Some(data)
    } else {
      None
    }
  }

  def readNext(): Option[T] = {
    if (fetched.isEmpty) {
      fetched = fetchFromUnderlyingScanner
    }
    fetched match {
      case None => None
      case Some(iter) => if (iter.hasNext) Some(iter.next()) else {
        fetched = None
        readNext()
      }
    }
  }

  def close() {
    underlying.close()
  }
}

sealed trait MarketTimeSeries[E] {
  def openScanner[E](implicit reads: Reads[E]): ScannerW[E]

  def enumScanner[E, A](scanner: ScannerW[E], it: IterV[E, A]): IterV[E, A] = {
    def loop: IterV[E, A] => IterV[E, A] = {
      case i@Done(_, _) => i
      case i@Cont(k) =>
        val s  = scanner.readNext()
        if (s.isEmpty) i else loop(k(El(s.asInstanceOf[E])))
    }
    loop(it)
  }

  def closeScanner(scanner: ScannerW[E])(implicit reads: Reads[E]) = scanner.close()

  def bracket[A, B, C](init: A, fin: A => B, body: A => C): C = {
    val a = init
    val c = body(a)
    fin(a)
    c
  }

  def enumerate[A](i: IterV[E, A])(implicit reads: Reads[E]) =
    bracket(openScanner,
      closeScanner(_: ScannerW[E]),
      enumScanner(_: ScannerW[E], i))
}

case class TradesTimeSeries(market: Market, code: Code, interval: Interval)(implicit marketDb: MarketDB) extends MarketTimeSeries[TradePayload] {
  def openScanner[E](implicit reads: Reads[E]): ScannerW[E] =
    new ScannerW[E](marketDb.scan(market, code, interval).apply(MarketIteratee.Timeout))
}

object MarketIteratee {
  val Timeout = 5.seconds

  def counter[E]: IterV[E, Int] = {
    def step(is: Int, e: E)(s: Input[E]): IterV[E, Int] = {
      s(el = e2 => Cont(step(is + 1, e2)),
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

  def printer[E](log: org.slf4j.Logger): IterV[E, Boolean] = {
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

}


