package com.ergodicity.marketdb.loader

import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.admin.RuntimeEnvironment
import org.joda.time.format.DateTimeFormat
import org.scala_tools.time.Implicits._
import com.ergodicity.marketdb.model.TradePayload
import scalaz.IterV
import util.BatchSettings._
import java.net.{ConnectException, Socket}
import util.Iteratees._
import util.{BatchSettings, LoaderReport}
import com.twitter.finagle.kestrel.Client._
import com.twitter.finagle.builder.ClientBuilder._
import com.twitter.finagle.kestrel.protocol.Kestrel._
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import org.slf4j.LoggerFactory

abstract class LoaderConfig extends ServerConfig[Loader] {
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

    val from = Format.parseDateTime(runtime.arguments("from"));
    val until = Format.parseDateTime(runtime.arguments("until"));

    new Loader(from to until, loader, i)
  }
}

abstract class KestrelConfig(config: KestrelSettings, batchSettings: BatchSettings) extends LoaderConfig {

  implicit def implicitBatchSettings = batchSettings

  implicit lazy val TradePayloadSerializer = {
    payload: List[TradePayload] =>
      import sbinary._
      import Operations._
      import com.ergodicity.marketdb.model.TradeProtocol._
      toByteArray(payload)
  }

  assertKestrelRunning(config)

  val client = Client(ClientBuilder()
    .codec(Kestrel())
    .hosts(config.host + ":" + config.port)
    .hostConnectionLimit(config.hostConnectionLimit) // process at most 1 item per connection concurrently
    .buildFactory())

  val i = kestrelBulkLoader[TradePayload](config.tradesQueue, client)

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