package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import org.slf4j.LoggerFactory

import scalaz._
import Scalaz._
import IterV._


class ZipReadingTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[ZipReadingTest])

  val Path = "classpath:FT120201.zip"
  
  describe("Reading RTS zip file") {
    it("should read zip") {

      /*val is = this.getClass.getResourceAsStream("/data/FT120201.zip")
      log.info("IS: "+is)

      val zis = new ZipInputStream(is)
      log.info("Entry: "+zis.getNextEntry)

      val reader = new BufferedReader(new InputStreamReader(zis))

      log.info("Line: "+reader.readLine());
      log.info("Line: "+reader.readLine());
      log.info("Line: "+reader.readLine());*/
      
      val list = List(1, 2, 3)

    }
  }

  def drop[E,A](n: Int): IterV[E,Unit] = {
    def step: Input[E] => IterV[E,Unit] = {
      case El(x) => drop(n - 1)
      case Empty => Cont(step)
      case EOF => Done((), EOF)
    }
    if (n == 0) Done((), Empty) else Cont(step)
  }


  def enumerate[E,A]: (List[E], IterV[E,A]) => IterV[E,A] = {
    case (Nil, i) => i
    case (_, i@Done(_, _)) => i
    case (x :: xs, Cont(k)) => enumerate(xs, k(El(x)))
  }


}