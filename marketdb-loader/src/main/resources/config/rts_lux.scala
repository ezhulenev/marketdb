import com.ergodicity.marketdb.loader.util.BatchSettings
import com.ergodicity.marketdb.loader.util.Iteratees._
import com.ergodicity.marketdb.loader.{KestrelSettings, RtsTradeLoader, LoaderConfig}
import com.ergodicity.marketdb.model.TradePayload
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.protocol.Kestrel
import java.io.File

new LoaderConfig {
  admin.httpPort = 10000

  val dir = new File("C:\\Dropbox\\Dropbox\\rts")
  if (!dir.isDirectory) {
    throw new IllegalStateException("Directory doesn't exists: " + dir.getAbsolutePath)
  }

  val url = "http://ftp.rts.ru/pub/info/stats/history"
  val pattern = "'/F/'YYYY'/FT'YYMMdd'.zip'"

  // -- Set up Loader
  val loader = new RtsTradeLoader(dir, url, pattern)

  // -- Set up IterV for Kestrel
  val config = KestrelSettings("localhost", 22133, "trades", 10)
  assertKestrelRunning(config)

  implicit val bulkLoaderSettings = BatchSettings(100, Some(1000))

  implicit lazy val TradePayloadSerializer = {
    payload: List[TradePayload] =>
      import sbinary._
      import Operations._
      import com.ergodicity.marketdb.model.TradeProtocol._
      toByteArray(payload)
  }

  val client = Client(ClientBuilder()
    .codec(Kestrel())
    .hosts(config.host + ":" + config.port)
    .hostConnectionLimit(config.hostConnectionLimit) // process at most 1 item per connection concurrently
    .buildFactory())

  val i = kestrelBulkLoader[TradePayload](config.tradesQueue, client)
}
