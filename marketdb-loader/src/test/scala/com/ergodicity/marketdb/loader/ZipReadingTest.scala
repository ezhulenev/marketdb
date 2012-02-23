package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.slf4j.LoggerFactory

import scalaz.effects._
import scalaz._
import Scalaz._
import IterV._
import java.io._
import com.ergodicity.marketdb.model.Code._
import com.ergodicity.marketdb.model.Contract._
import com.ergodicity.marketdb.model.TradePayload._
import com.ergodicity.marketdb.model.Market._
import org.joda.time.format.DateTimeFormat
import com.ergodicity.marketdb.model.{Market, Contract, Code, TradePayload}
import java.util.zip.{ZipEntry, ZipInputStream}


class ZipReadingTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[ZipReadingTest])

  val Path = "classpath:FT120201.zip"

  describe("Reading RTS zip file") {
    it("should read zip") {

      val is = this.getClass.getResourceAsStream("/data/FT120201.zip").pure[IO]

      log.info("IS: " + is)

      val zis = is map {
        new ZipInputStream(_)
      }
      val lines = zis map {
        zip =>
          log.info("Entry: " + zip.getNextEntry)
          val reader = new BufferedReader(new InputStreamReader(zip))

          var line = reader.readLine()
          while (line != null) {
            log.info("Line: " + line);
            line = reader.readLine()
          }
      }

      lines.unsafePerformIO
    }

    it("should work with IterV") {
      val s = sortedLogger[String]

      // log.info("RES: "+s(List("1","2","3","0")).run)

      import RtsTradeHistoryIteratee._
      val is = this.getClass.getResourceAsStream("/data/FT120201.zip")

      val tradeData = RtsTradeHistory(InputStreamRef(is))

      val opt = enumTradeData(tradeData, printer) map (_.run)
      val v = opt.unsafePerformIO
      log.info("VALUE: " + v)
    }
  }

  /*implicit val BufferedReaderEnumerator = new Enumerator[BufferedReader] {
    def apply[E, A](e: BufferedReader[E], i: IterV[E, A]) = null
  }*/


  implicit val ListEnumerator = new Enumerator[List] {
    def apply[E, A](e: List[E], i: IterV[E, A]): IterV[E, A] = e match {
      case List() => i
      case x :: xs => i.fold(done = (_, _) => i,
        cont = k => apply(xs, k(El(x))))
    }
  }

  def sortedLogger[E <: String]: IterV[E, Boolean] = {
    def step(is: Boolean, e: E)(s: Input[E]): IterV[E, Boolean] = {
      s(el = e2 => if (is && e < e2)
        Cont(step(is, e2))
      else
        Done(false, EOF[E]),
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


  object RtsTradeHistoryIteratee {

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

    def enumTradeData[A, R <: DataRef](td: RtsTradeHistory[R], i: IterV[TradePayload, A])
                                      (implicit fetcher: DataFetcher[R]): IO[IterV[TradePayload, A]] =
      bracket(unzipInputStream(fetcher.toStream(td.ref)),
        closeReader(_: BufferedReader),
        enumReader(_: BufferedReader, i))
  }


  object InputStreamIteratee {
    def enumReader[A](r: BufferedReader, it: IterV[String, A]): IO[IterV[String, A]] = {
      def loop: IterV[String, A] => IO[IterV[String, A]] = {
        case i@Done(_, _) => i.pure[IO]
        case i@Cont(k) => for {
          s <- r.readLine.pure[IO]
          a <- if (s == null) i.pure[IO] else loop(k(El(s)))
        } yield a
      }
      loop(it)
    }

    def bufferInputStream(is: InputStream) = new BufferedReader(new InputStreamReader(is)).pure[IO]

    def closeReader(r: Reader) = r.close().pure[IO]

    def bracket[A, B, C](init: IO[A], fin: A => IO[B], body: A => IO[C]): IO[C] =
      for {
        a <- init
        c <- body(a)
        _ <- fin(a)
      } yield c

    def enumInputStream[A](is: InputStream, i: IterV[String, A]): IO[IterV[String, A]] =
      bracket(bufferInputStream(is),
        closeReader(_: BufferedReader),
        enumReader(_: BufferedReader, i))
  }

}