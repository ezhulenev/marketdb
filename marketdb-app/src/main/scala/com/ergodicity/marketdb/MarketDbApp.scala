package com.ergodicity.marketdb

import com.ergodicity.marketdb.api._
import com.ergodicity.marketdb.core.MarketDb
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.Future
import java.net.SocketAddress
import org.slf4j.LoggerFactory

object MarketDbApp {
  val log = LoggerFactory.getLogger(getClass.getName)

  var app: MarketDbApp = null
  var runtime: RuntimeEnvironment = null

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      app = runtime.loadRuntimeConfig[MarketDbApp]()
      app.start()
    } catch {
      case e: Throwable =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}

class MarketDbApp(socketAddress: Option[SocketAddress], val marketDb: MarketDb) extends com.twitter.ostrich.admin.Service {
  val server = socketAddress.map(address => ServerBuilder()
    .codec(MarketDbCodec)
    .bindTo(address)
    .name("MarketDbApplication")
    .build(new MarketDbApi(marketDb)))

  def start() {
    marketDb.start()
  }

  def shutdown() {
    server.foreach(_.close())
    marketDb.shutdown()
  }

  class MarketDbApi(marketDb: MarketDb) extends Service[MarketDbReq, MarketDbRep] {
    def apply(request: MarketDbReq) = request match {
      case GetMarketDbConfig => Future(MarketDbConfig(marketDb.connection))
      case ScanTrades(market, security, interval) => marketDb.trades(market, security, interval).map(Trades(_))
      case ScanOrders(market, security, interval) => marketDb.orders(market, security, interval).map(Orders(_))
    }
  }
}


