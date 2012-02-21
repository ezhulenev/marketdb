package com.ergodicity.marketdb.loader

import org.scalatest.Spec
import tools.nsc.io.File
import org.slf4j.LoggerFactory

class TypeClassTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[TypeClassTest])


  import DataFetcher._

  implicit val cache = new Cache {
    def cache = "MyCache"
  }

  describe("Loader model") {
    it("should load local reference with plaint text payload") {
      val local = Local[PlainText](new File(new java.io.File(".")))
      log.info("LOCAL REF: " + local)
      log.info(local.fetch)

      val remote = Remote[RtsTradeHistory]("http://ftp.rts.ru")
      log.info("REMOTE REF: " + remote)
      log.info(remote.fetch)
      
      /*log.info("LOCAL FETCH: " + fetch(local))

      val remote = Remote[RtsTradeHistory]("http://ftp.rts.ru")

      log.info("REMOTE REF: " + remote)
      log.info("REMOTE FETCH: " + fetch(remote))*/
    }
  }


/*
  private def fetch[R[_], F](ref: R[F])(implicit fetcher: DataFetcher[R], reader: DataParser[F]) = {
    fetcher.fetch(ref) + ":" + reader.read
  }
*/
}
