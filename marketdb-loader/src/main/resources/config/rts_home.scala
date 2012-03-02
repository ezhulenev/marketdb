import com.ergodicity.marketdb.loader.util.{BatchSettings, Iteratees}
import com.ergodicity.marketdb.loader.{KestrelConfig, KestrelSettings, RtsTradeLoader, LoaderConfig}
import com.ergodicity.marketdb.model.TradePayload
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.protocol.Kestrel
import java.io.File
import Iteratees._
import org.slf4j.LoggerFactory

new KestrelConfig(KestrelSettings("localhost", 22133, "trades", 10), BatchSettings(100, Some(1000))) {
  admin.httpPort = 10000

  val dir = new File("D:\\data\\rts")
  if (!dir.isDirectory) {
    throw new IllegalStateException("Directory doesn't exists: " + dir.getAbsolutePath)
  }

  val url = "http://ftp.rts.ru/pub/info/stats/history"
  val pattern = "'/F/'YYYY'/FT'YYMMdd'.zip'"

  val loader = new RtsTradeLoader(dir, url, pattern)
}