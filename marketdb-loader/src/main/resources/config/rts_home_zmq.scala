import com.ergodicity.marketdb.loader.util.BatchSettings
import com.ergodicity.marketdb.loader.{ZMQLoaderConfig, RtsTradeLoader}
import java.io.File

new ZMQLoaderConfig("tcp://localhost:30000", BatchSettings(1, Some(300000))) {
  admin.httpPort = 10000

  val dir = new File("D:\\data\\rts")
  if (!dir.isDirectory) {
    throw new IllegalStateException("Directory doesn't exists: " + dir.getAbsolutePath)
  }

  val url = "http://ftp.rts.ru/pub/info/stats/history"
  val pattern = "'/F/'YYYY'/FT'YYMMdd'.zip'"

  val loader = new RtsTradeLoader(dir, url, pattern)
}