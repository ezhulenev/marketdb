package com.ergodicity.marketdb.loader

import scalaz.effects._
import scalaz._
import Scalaz._
import IterV._
import org.joda.time.format.DateTimeFormat
import java.util.zip.{ZipEntry, ZipInputStream}
import java.io.{Reader, InputStreamReader, InputStream, BufferedReader}
import com.ergodicity.marketdb.model.{Contract, Code, Market, TradePayload}
import com.twitter.ostrich.stats.Stats


sealed trait TradeData[R <: DataRef] {
  val ref: R
}
case class RtsTradeHistory[R <: DataRef](ref: R) extends TradeData[R]


/**
 * Iterate over given TradeData with scalaz.IterV
 */
trait TradeDataIteratee {
  def enumTradeData[A](i: IterV[TradePayload, A]): IO[IterV[TradePayload, A]]
}

object TradeDataIteratee {
  implicit def RtsTradeHistoryIteratee[R <: DataRef](td: RtsTradeHistory[R])
                                                    (implicit fetcher: DataFetcher[R]): TradeDataIteratee = new TradeDataIteratee {

    val RTS = Market("RTS")
    val DateFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    val HeaderPrefix = "code;contract"
    val FuturesEntrySuffix = "ft.csv"
    val OptionsEntrySuffix = "ot.csv"

    private def parseTradePayload(line: String): TradePayload = {
      val split = line.split(";")

      val code = Code(split(0))
      val contract = Contract(split(1))
      val decimal = BigDecimal(split(2))
      val amount = split(3).toInt
      val time = DateFormat.parseDateTime(split(4))
      val tradeId = split(5).toLong
      val nosystem = split(6) == "1" // Nosystem	0 - Рыночная сделка, 1 - Адресная сделка

      TradePayload(RTS, code, contract, decimal, amount, time, tradeId, nosystem)
    }

    def enumReader[A](r: BufferedReader, it: IterV[TradePayload, A]): IO[IterV[TradePayload, A]] = {
      def loop: IterV[TradePayload, A] => IO[IterV[TradePayload, A]] = {
        case i@Done(_, _) => i.pure[IO]
        case i@Cont(k) => for {
          s <- r.readLine.pure[IO]
          a <- if (s == null) i.pure[IO] else loop(k(El(parseTradePayload(s))))
        } yield a
      }
      loop(it)
    }

    def unzipInputStream(iois: IO[InputStream]) = iois map {
      is =>
        val zis = new ZipInputStream(is)
        new BufferedReader(new InputStreamReader(zis)) {
          override def readLine() = {
            val line = super.readLine()
            if (line == null) {
              val nexEntry = switchToNextEntry()
              if (nexEntry == null) null
              else readLine()
            } else if (!validLine(line)) readLine() else line
          }

          private def validLine(line: String) =
            !line.trim().isEmpty && !line.startsWith(HeaderPrefix)

          private def validEntry(name: String) =
            name.endsWith(FuturesEntrySuffix) || name.endsWith(OptionsEntrySuffix)

          private def switchToNextEntry(): ZipEntry = {
            val entry = zis.getNextEntry
            if (entry == null) null
            else if (validEntry(entry.getName)) entry else switchToNextEntry()
          }
        }
    }

    def closeReader(r: Reader) = r.close().pure[IO]

    def bracket[A, B, C](init: IO[A], fin: A => IO[B], body: A => IO[C]): IO[C] =
      for {
        a <- init
        c <- body(a)
        _ <- fin(a)
      } yield c

    def enumTradeData[A](i: IterV[TradePayload, A]): IO[IterV[TradePayload, A]] =
      bracket(unzipInputStream(fetcher.toStream(td.ref)),
        closeReader(_: BufferedReader),
        enumReader(_: BufferedReader, i))
  }


}
