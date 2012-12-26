package com.ergodicity.marketdb.loader

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import java.io.File
import util.Iteratees

class LocalMirrorCacheTest extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[LocalMirrorCacheTest])

  val RtsFtpUrl = "http://ftp.rts.ru/pub/info/stats/history"

  "LocalMirrorCache" must {
    import com.ergodicity.marketdb.loader.util.RichFile._

    val tempDir = new File(System.getProperty("java.io.tmpdir"))
    val cacheDir = new File(tempDir, "testmirror")
    log.info("Cache dir: " + cacheDir.getAbsolutePath)

    if (cacheDir.exists() && !cacheDir.deleteAll) {
      throw new RuntimeException("Can't delete mirror dir")
    }

    if (!cacheDir.mkdirs()) {
      throw new RuntimeException("Can't prepate mirror dir")
    }

    val cache = new LocalMirrorCache(cacheDir, RtsFtpUrl)

    "cache download data from InptuStream and cache in local directory" in {
      val is = this.getClass.getResourceAsStream("/data/FT120201.zip")
      val ref = new RemoteRef(RtsFtpUrl + "/F/2012/FT120201.zip")

      val io = cache.cache(ref, is)

      val expectedFile = new File(cacheDir, "/F/2012/FT120201.zip")

      log.info("Expected location: " + expectedFile.getAbsolutePath)
      assert(expectedFile.isFile)

      val rtsTradeHistory = RtsTradeHistory(LocalRef(expectedFile))

      // -- Iterate over cached data
      import TradeDataIteratee._
      import Iteratees._
      val count = rtsTradeHistory.enumTradeData(counter).unsafePerformIO.run
      log.info("Count: " + count)
      assert(count == 60)
    }
  }
}
