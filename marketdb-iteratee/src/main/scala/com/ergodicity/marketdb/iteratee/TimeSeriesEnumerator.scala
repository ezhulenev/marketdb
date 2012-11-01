package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.model.MarketPayload
import com.twitter.util.{Promise, Future}
import java.util
import org.hbase.async.{KeyValue, Scanner}
import sbinary.Operations._
import sbinary.Reads
import scala.Some
import scala.collection.JavaConversions._
import scalaz.IterV
import scalaz.IterV.{El, EOF, Cont, Done}
import com.ergodicity.marketdb.{Client, TimeSeries}
import com.stumbleupon.async.Callback

class TimeSeriesEnumerator[E <: MarketPayload](timeSeries: TimeSeries[E]) {

  implicit def f2callback[R, T](f: T => R) = new Callback[R, T] {
    def call(p: T) = f(p)
  }

  def openScanner(implicit client: Client): Future[Scanner] = Future(timeSeries.scan(client))

  def scan[A](scanner: Scanner, it: IterV[E, A])(implicit reads: Reads[E]): Future[IterV[E, A]] = {

    def nextValuesFromUnderlyingScanner: Future[Option[Iterator[E]]] = {
      val promise = new Promise[Option[Iterator[E]]]
      val defered = scanner.nextRows()

      val callback: util.ArrayList[util.ArrayList[KeyValue]] => Unit = rows => {
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

  def enumerate[A](i: IterV[E, A])(implicit reads: Reads[E], client: Client): Future[IterV[E, A]] =
    bracket(openScanner,
      closeScanner(_: Scanner),
      scan(_: Scanner, i))
}