package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import java.util.zip.ZipInputStream

import scalaz.effects._
import scalaz._
import Scalaz._
import IterV._
import java.io._


class ZipReadingTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[ZipReadingTest])

  val Path = "classpath:FT120201.zip"
  
  describe("Reading RTS zip file") {
    it("should read zip") {

      val is = this.getClass.getResourceAsStream("/data/FT120201.zip").pure[IO]

      log.info("IS: "+is)

      val zis = is map {new ZipInputStream(_)}
      val lines = zis map {zip=>
        log.info("Entry: "+zip.getNextEntry)
        val reader = new BufferedReader(new InputStreamReader(zip))

        var line = reader.readLine()
        while (line != null) {
          log.info("Line: "+line);
          line = reader.readLine()
        }
      }

      lines.unsafePerformIO
    }

    it("should work with IterV") {
      val s = sortedLogger[String]

      // log.info("RES: "+s(List("1","2","3","0")).run)

      import InputStreamIteratee._
      val is = this.getClass.getResourceAsStream("/data/FT120201.zip")
      val zis = new ZipInputStream(is)
      zis.getNextEntry
      val opt = enumInputStream(zis, head) map (_.run)
      val v = opt.unsafePerformIO
      log.info("VALUE: "+v)
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

  def sortedLogger[E <: String] : IterV[E, Boolean] = {
    def step(is: Boolean, e: E)(s: Input[E]): IterV[E, Boolean] = {
      
      log.info("Element: "+e)
      
      s(el = e2 => if (is && e < e2)
        Cont(step(is, e2))
      else
        Done(false, EOF[E]),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Boolean] =
      s(el = e1 => Cont(step(true, e1)),
        empty = Cont(first),
        eof = Done(true, EOF[E]))

    Cont(first)
  }

  def sorted[E <: Int] : IterV[E, Boolean] = {
    def step(is: Boolean, e: E)(s: Input[E]): IterV[E, Boolean] =
      s(el = e2 => if (is && e < e2)
        Cont(step(is, e2))
      else
        Done(false, EOF[E]),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))

    def first(s: Input[E]): IterV[E, Boolean] =
      s(el = e1 => Cont(step(true, e1)),
        empty = Cont(first),
        eof = Done(true, EOF[E]))

    Cont(first)
  }

  object InputStreamIteratee {
    def enumReader[A](r: BufferedReader, it: IterV[String,  A]) : IO[IterV[String,  A]] = {
      def loop: IterV[String,  A] => IO[IterV[String,  A]] = {
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

    def bracket[A,B,C](init: IO[A], fin: A => IO[B], body: A => IO[C]): IO[C] =
      for {
        a <- init
        c <- body(a)
        _ <- fin(a)
      } yield c

    def enumInputStream[A](is: InputStream,  i: IterV[String,  A]) : IO[IterV[String, A]] =
      bracket(bufferInputStream(is),
        closeReader(_: BufferedReader),
        enumReader(_: BufferedReader,  i))
  }

}