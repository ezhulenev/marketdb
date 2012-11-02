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
import scalaz.IterV
import scalaz.IterV.{El, EOF, Cont, Done}

class MultipleTimeSeriesEnumerator[E <: MarketPayload](timeSeries: Seq[TimeSeries[E]]) {

  implicit def f2callback[R, T](f: T => R) = new Callback[R, T] {
    def call(p: T) = f(p)
  }

  def openScanners(implicit client: HBaseClient): Future[Seq[Scanner]] = {
    val scanners = timeSeries.map {
      ts =>
        val scanner = client.newScanner(ts.qualifier.table)
        scanner.setStartKey(ts.qualifier.startKey)
        scanner.setStopKey(ts.qualifier.stopKey)
        scanner
    }

    Future(scanners)
  }

  def scan[A](scanners: Seq[Scanner], it: IterV[E, A])(implicit reads: Reads[E]): Future[IterV[E, A]] = {

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

    def loop(data: Seq[(Scanner, Option[Stream[E]])], iterv: IterV[E, A]): Future[IterV[E, A]] = {

      val prefetchedStreams = Future.collect(data.map {
        case (scanner, Some(stream)) if (stream.isEmpty) =>
          nextValuesFromUnderlyingScanner(scanner)

        case (scanner, Some(stream)) if (!stream.isEmpty) =>
          Future(Some(stream))

        case (scanner, None) => Future(None)
      })

      prefetchedStreams onFailure (_ => scanners.map(_.close()))



      iterv match {
        case i@Done(_, _) => scanners.map(_.close()); Future(i)
        case i@Cont(k) =>
          Future(k(EOF[E]))
          /*prefetchedStreams.map(_.flatten.map(_.head).reduceOption(_ min _)) match {
            case None => Future(k(EOF[E])) //Future(i)
            case Some(payload) =>
              val next = iterator.next()
              loop(payload, k(El(payload)))
          }.flatten*/
      }
    }

    loop(scanners map ((_, Some(Stream.empty))), it)
  }

  def closeScanner(scanners: Seq[Scanner]): Future[Unit] = {
    val promises = scanners.map {
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
      closeScanner(_: Seq[Scanner]),
      scan(_: Seq[Scanner], i))
}