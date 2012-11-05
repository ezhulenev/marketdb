package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.TimeSeries
import com.ergodicity.marketdb.model.MarketPayload
import com.stumbleupon.async.Callback
import com.twitter.util.{Promise, Future}
import java.util
import org.hbase.async.{HBaseClient, KeyValue, Scanner}
import sbinary.Operations._
import sbinary.Reads
import scala.Some
import scala.collection.JavaConversions._
import scalaz.{NonEmptyList, IterV}
import scalaz.IterV.{El, EOF, Cont, Done}

class MultipleTimeSeriesEnumerator[E <: MarketPayload] private[iteratee](timeSeries: NonEmptyList[TimeSeries[E]]) {

  implicit def f2callback[R, T](f: T => R) = new Callback[R, T] {
    def call(p: T) = f(p)
  }

  def openScanners(implicit client: HBaseClient): Future[NonEmptyList[Scanner]] = {
    val scanners = timeSeries.map {
      ts =>
        val scanner = client.newScanner(ts.qualifier.table)
        scanner.setStartKey(ts.qualifier.startKey)
        scanner.setStopKey(ts.qualifier.stopKey)
        scanner
    }

    Future(scanners)
  }

  def scan[A](scanners: NonEmptyList[Scanner], it: IterV[E, A])(implicit reads: Reads[E]): Future[IterV[E, A]] = {

    def nextValuesFromUnderlyingScanner(scanner: Scanner): Future[Option[Stream[E]]] = {
      val promise = new Promise[Option[Stream[E]]]
      val defered = scanner.nextRows()

      val callback: util.ArrayList[util.ArrayList[KeyValue]] => Unit = rows => {
        if (rows != null) {
          val trades = asScalaIterator(rows.iterator()) flatMap (row => asScalaIterator(row.iterator())) map (kv => fromByteArray[E](kv.value()))
          promise.setValue(Some(trades.toStream))
        } else {
          promise.setValue(None)
        }
      }
      val errback: Throwable => Unit = e => promise.setException(e)
      defered addCallback callback
      defered addErrback errback

      promise
    }

    def loop(data: NonEmptyList[(Scanner, Stream[E])], iterv: IterV[E, A]): Future[IterV[E, A]] = {

      // Check that all streams has data
      val fetchedStreams = Future.collect(data.list.map {
        case (scanner, stream) if (stream.isEmpty) => nextValuesFromUnderlyingScanner(scanner).map(_.map((scanner, _)))
        case (scanner, stream) if (!stream.isEmpty) => Future(Some(scanner, stream))
      })

      fetchedStreams onFailure (_ => scanners.map(_.close()))

      // Process iteration step
      iterv match {
        case i@Done(_, _) => scanners.map(_.close()); Future(i)
        case i@Cont(k) =>
          fetchedStreams.map(_.flatten) flatMap {
            fetched =>
            // Find minimum pair of (Scanner, Stream) by head element
              val ord = Ordering.by((_: (Scanner, Stream[E]))._2.head.time.getMillis)
              fetched.reduceOption(ord.min) match {
                case None => Future(k(EOF[E]))
                case Some((scanner, stream)) =>
                  val rest = NonEmptyList((scanner, stream.tail), fetched.filterNot(_._1 == scanner): _*)
                  loop(rest, k(El(stream.head)))
              }
          }
      }
    }

    loop(scanners map ((_, Stream.empty)), it)
  }

  def closeScanner(scanners: NonEmptyList[Scanner]): Future[Unit] = {
    val promises = scanners.list.map {
      scanner =>
        val promise = new Promise[Unit]
        val deferred = scanner.close()
        deferred.addCallback((res: AnyRef) => promise.setValue(()))
        deferred.addErrback((err: Throwable) => promise.setException(err))
        promise
    }

    Future.collect(promises) map (_ => ())
  }

  def bracket[A, B, C](init: Future[A], fin: A => Future[B], body: A => Future[C]): Future[C] =
    for {
      a <- init
      c <- body(a)
      _ <- fin(a)
    } yield c

  def enumerate[A](i: IterV[E, A])(implicit reads: Reads[E], reader: MarketDbReader): Future[IterV[E, A]] =
    bracket(openScanners(reader.client),
      closeScanner(_: NonEmptyList[Scanner]),
      scan(_: NonEmptyList[Scanner], i))
}