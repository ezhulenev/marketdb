package com.ergodicity.marketdb.loader

import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.admin.RuntimeEnvironment
import org.joda.time.format.DateTimeFormat
import org.scala_tools.time.Implicits._

class LoaderConfig extends ServerConfig[Loader] {
  val Format = DateTimeFormat.forPattern("yyyyMMdd")

  var limit: Option[Int] = None
  
  var loader: Option[TradeLoader] = None

  var kestrelConfig: Option[KestrelConfig] = None

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

    new Loader(loader, from to until, kestrelConfig, limit)
  }
}

case class KestrelConfig(host: String, port: Int, tradesQueue: String, hostConnectionLimit: Int = 1, bulkSize:Int = 1000)