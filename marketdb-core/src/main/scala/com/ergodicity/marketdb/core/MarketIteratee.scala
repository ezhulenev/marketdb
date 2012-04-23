package com.ergodicity.marketdb.core

import scalaz._
import IterV._
import scalaz.{Input, IterV}
import org.joda.time.Interval
import collection.JavaConversions._
import sbinary.Operations._
import com.twitter.util.{Promise, Future}
import java.util.ArrayList
import org.hbase.async.{KeyValue, Scanner}
import sbinary.{Writes, Reads}
import com.twitter.ostrich.stats.Stats
import java.util.concurrent.atomic.AtomicBoolean
import com.ergodicity.marketdb.model.{Security, TradePayload, Market}
import com.ergodicity.zeromq.Serializer


sealed trait MarketTimeSeries[E] {
  import com.ergodicity.marketdb.AsyncHBase._
  def openScanner: Future[Scanner]

  def scan[A](scanner: Scanner, it: IterV[E, A])(implicit reads: Reads[E]): Future[IterV[E, A]] = {

    def nextValuesFromUnderlyingScanner: Future[Option[Iterator[E]]] = {
      val promise = new Promise[Option[Iterator[E]]]
      val defered = scanner.nextRows()

      val callback: ArrayList[ArrayList[KeyValue]] => Unit = rows => {
        if (rows != null) {
          val trades = asScalaIterator(rows.iterator()) flatMap {row =>
              asScalaIterator(row.iterator())} map {kv =>
              fromByteArray[E](kv.value())}
          promise.setValue(Some(trades))
        } else {
          promise.setValue(None)
        }
      }
      val errback: Throwable => Unit = e => promise.setException(e)
      defered addCallback callback
      defered addErrback errback

      promise
    }

    def loop(data: Iterator[E], iterv: IterV[E, A]): Future[IterV[E, A]] = {
      val nextValues = if (data.hasNext) Future(Some(data)) else nextValuesFromUnderlyingScanner onFailure {err =>
        scanner.close()
      }

      iterv match {
        case i@Done(_, _) => scanner.close(); Future(i)
        case i@Cont(k) =>
          nextValues.map {
            case None => Future(k(EOF[E])) //Future(i)
            case Some(iterator) =>
              val next = iterator.next()
              loop(iterator, k(El(next)))
          }.flatten
      }
    }

    loop(Iterator.empty, it)
  }

    def closeScanner(scanner: Scanner) = {
      val promise = new Promise[Unit]
      val deferred = scanner.close()
      deferred.addCallback((res: AnyRef) => promise.setValue(()))
      deferred.addErrback((err: Throwable) => promise.setException(err))
      promise
    }

    def bracket[A, B, C](init: Future[A], fin: A => Future[B], body: A => Future[C]): Future[C] =
      for {
        a <- init
        c <- body(a)
        _ <- fin(a)
      } yield c

    def enumerate[A](i: IterV[E, A])(implicit reads: Reads[E]): Future[IterV[E, A]] =
      bracket(openScanner,
        closeScanner(_: Scanner),
        scan(_: Scanner, i))
}


case class TradesTimeSeries(market: Market, security: Security, interval: Interval)
                           (implicit marketDb: MarketDB) extends MarketTimeSeries[TradePayload] {
  def openScanner = marketDb.scan(market, security, interval)
}

object MarketIteratee {
  import com.ergodicity.zeromq.{Client => ZMQClient}

  def zmqStreamer[E, A](client: ZMQClient, serializer: Serializer[E], interrupt: AtomicBoolean = new AtomicBoolean(false))
                     (implicit writes: Writes[A]): IterV[E, Int] = {

    def step(is: Int, e: E)(s: Input[E]): IterV[E, Int] = {
      s(el = e2 => {
        Stats.incr("trades_streamed_zmq", 1);
        client.send(e2)(serializer)
        if (interrupt.get) Done(is + 1, EOF[E]) else Cont(step(is + 1, e2))
      },
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Int] = {
      s(el = e1 => {
        Stats.incr("trades_streamed_zmq", 1);
        client.send(e1)(serializer)
        Cont(step(1, e1))
      },
        empty = Cont(first),
        eof = Done(0, EOF[E]))
    }

    Cont(first)
  }

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


