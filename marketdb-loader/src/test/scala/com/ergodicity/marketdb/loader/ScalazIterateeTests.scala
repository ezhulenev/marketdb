package com.ergodicity.marketdb.loader

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory

import scalaz.effects._
import scalaz._
import Scalaz._
import IterV._
import java.io._
import java.util.zip.ZipInputStream


class ScalazIterateeTests extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[ScalazIterateeTests])

  val Path = "classpath:FT120201.zip"

  "Reading RTS zip file" must {
    "read zip" in {

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
            log.info("Line: " + line)
            line = reader.readLine()
          }
      }

      lines.unsafePerformIO
    }

    "work with IterV" in {
      // val s = sortedLogger[String]
      // log.info("RES: "+s(List("1","2","3","0")).run)

      val is = this.getClass.getResourceAsStream("/data/FT120201.zip")
      val tradeData = RtsTradeHistory(InputStreamRef(is))

      import TradeDataIteratee._
      val opt = tradeData.enumTradeData(printer) map (_.run)
      val v = opt.unsafePerformIO

      log.info("VALUE: " + v)
    }
  }

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

}