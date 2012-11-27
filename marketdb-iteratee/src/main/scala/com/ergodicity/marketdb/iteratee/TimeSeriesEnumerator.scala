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
import com.ergodicity.marketdb.iteratee.TimeSeriesEnumerator.TimeSeriesW

object TimeSeriesEnumerator {

  case class TimeSeriesW[+P <: MarketPayload](underlying: TimeSeries[P], reads: Reads[_])

  implicit def enrichTimeSeries[P <: MarketPayload](ts: TimeSeries[P])(implicit reads: Reads[P]) = TimeSeriesW(ts, reads)

  implicit def pimp[P <: MarketPayload](ts: TimeSeries[P]) = new {
    def pimp(implicit reads: Reads[P]) = TimeSeriesW(ts, reads)
  }

  def apply[P <: MarketPayload](timeSeries: TimeSeriesW[P]*) = new TimeSeriesEnumerator[P](timeSeries: _*)
}

class TimeSeriesEnumerator[E <: MarketPayload] private[iteratee](timeSeries: TimeSeriesW[E]*) {

  implicit def f2callback[R, T](f: T => R) = new Callback[R, T] {
    def call(p: T) = f(p)
  }

  def openScanners(implicit client: HBaseClient): Future[Seq[(Scanner, Reads[E])]] = {
    val scanners = timeSeries.map {
      ts =>
        val scanner = client.newScanner(ts.underlying.qualifier.table)
        scanner.setStartKey(ts.underlying.qualifier.startKey)
        scanner.setStopKey(ts.underlying.qualifier.stopKey)
        (scanner, ts.reads.asInstanceOf[Reads[E]])
    }

    Future(scanners)
  }

  def scan[A](scanners: Seq[(Scanner, Reads[E])], it: IterV[E, A]): Future[IterV[E, A]] = {

    def nextValuesFromUnderlyingScanner(scanner: Scanner, reads: Reads[E]): Future[Option[Seq[E]]] = {
      val promise = new Promise[Option[Seq[E]]]
      val defered = scanner.nextRows()

      val callback: util.ArrayList[util.ArrayList[KeyValue]] => Unit = rows => {
        if (rows != null) {
          val trades = asScalaIterator(rows.iterator()) flatMap (row => asScalaIterator(row.iterator())) map (kv => fromByteArray[E](kv.value())(reads))
          val sorted = trades.toSeq.sortWith {case (l, r) => l.time isBefore r.time}
          promise.setValue(Some(sorted))
        } else {
          promise.setValue(None)
        }
      }
      val errback: Throwable => Unit = e => promise.setException(e)
      defered addCallback callback
      defered addErrback errback

      promise
    }

    def loop(data: Seq[(Scanner, Reads[E], Seq[E])], iterv: IterV[E, A]): Future[IterV[E, A]] = {

      // Check that all streams has data
      val fetchedStreams = Future.collect(data.map {
        case (scanner, reads, stream) if (stream.isEmpty) => nextValuesFromUnderlyingScanner(scanner, reads).map(_.map((scanner, reads, _)))
        case (scanner, reads, stream) if (!stream.isEmpty) => Future(Some(scanner, reads, stream))
      })

      fetchedStreams onFailure (_ => scanners.map(_._1.close()))

      // Process iteration step
      iterv match {
        case i@Done(_, _) => scanners.map(_._1.close()); Future(i)
        case i@Cont(k) =>
          fetchedStreams.map(_.flatten) flatMap {
            fetched =>
              // Find minimum tuple of (Scanner, Reads, Seq) by head element
              val ord = Ordering.by((_: (Scanner, Reads[E], Seq[E]))._3.head.time.getMillis)
              fetched.reduceOption(ord.min) match {
                case None => Future(k(EOF[E]))
                case Some((scanner, reads, stream)) =>
                  val rest = (scanner, reads, stream.tail) +: fetched.filterNot(_._1 == scanner)
                  loop(rest, k(El(stream.head)))
              }
          }
      }
    }

    loop(scanners map (tuple => (tuple._1, tuple._2, Seq.empty)), it)
  }

  def closeScanner(scanners: Seq[(Scanner, Reads[E])]): Future[Unit] = {
    val promises = scanners.map {
      case (scanner, reads) =>
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

  def enumerate[A](i: IterV[E, A])(implicit reader: MarketDbReader): Future[IterV[E, A]] =
    bracket(openScanners(reader.client),
      closeScanner(_: Seq[(Scanner, Reads[E])]),
      scan(_: Seq[(Scanner, Reads[E])], i))
}