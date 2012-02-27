import com.ergodicity.marketdb.loader.{RtsTradeLoader, LoaderConfig}
import java.io.File

new LoaderConfig {
  admin.httpPort = 10000

  val dir = new File("D:\\data\\rts")
  if (!dir.isDirectory) {
    throw new IllegalStateException("Directory doesn't exists: " + dir.getAbsolutePath)
  }

  val url = "http://ftp.rts.ru/pub/info/stats/history"
  val pattern = "'/F/'YYYY'/FT'YYMMdd'.zip'"

  loader = Some(new RtsTradeLoader(dir, url, pattern))
}
