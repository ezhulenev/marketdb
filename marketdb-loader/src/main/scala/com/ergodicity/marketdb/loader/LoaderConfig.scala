package com.ergodicity.marketdb.loader

import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.admin.RuntimeEnvironment
import org.joda.time.format.DateTimeFormat
import org.scala_tools.time.Implicits._
import com.ergodicity.marketdb.model.TradePayload
import scalaz.IterV
import java.net.{ConnectException, Socket}
import util.Iteratees._
import util.{BatchSettings, LoaderReport}
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.ergodicity.marketdb.model.TradeProtocol._

abstract class LoaderConfig extends ServerConfig[Loader[_]] {
  val Format = DateTimeFormat.forPattern("yyyyMMdd")

  def loader: TradeLoader

  def i: IterV[TradePayload, LoaderReport[TradePayload]]

  def apply(runtime: RuntimeEnvironment) = {
    if (!runtime.arguments.contains("from")) {
      System.err.print("Please provide from argument: -D from=YYYYMMdd")
      System.exit(1)
    }

    if (!runtime.arguments.contains("until")) {
      System.err.print("Please provide until argument: -D until=YYYYMMdd")
      System.exit(1)
    }

    val from = Format.parseDateTime(runtime.arguments("from"))
    val until = Format.parseDateTime(runtime.arguments("until"))

    new Loader(from to until, loader, i)
  }
}

abstract class KestrelLoaderConfig(kestrel: KestrelSettings, batchSettings: BatchSettings) extends LoaderConfig {
  assertKestrelRunning(kestrel)

  implicit def implicitBatchSettings = batchSettings

  val client = Client(ClientBuilder()
    .codec(Kestrel())
    .hosts(kestrel.host + ":" + kestrel.port)
    .hostConnectionLimit(kestrel.hostConnectionLimit) // process at most 1 item per connection concurrently
    .buildFactory())

  val i = kestrelBulkLoader[TradePayload](kestrel.tradesQueue, client)

  protected def assertKestrelRunning(conf: KestrelSettings) {
    try {
      new Socket(conf.host, conf.port)
      conf
    } catch {
      case e: ConnectException =>
        println("Error: Kestrel must be running on host " + conf.host + "; port " + conf.port)
        System.exit(1)
    }
  }
}

case class KestrelSettings(host: String, port: Int, tradesQueue: String, hostConnectionLimit: Int = 1)