package com.ergodicity.marketdb.api

import com.ergodicity.marketdb.model.{OrderPayload, Security, Market, TradePayload}
import com.ergodicity.marketdb.{TimeSeries, Connection}
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder, Server}
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.TimeSeries.Qualifier


class MarketDbApiProtocolSpec extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[MarketDbApiProtocolSpec])

  val Port = 3333

  val market = Market("FORTS")
  val security = Security("Security")
  val interval = new DateTime to new DateTime

  val qualifier = Qualifier(Array(), Array(), Array())

  "MarketDb Api" must {

    "should reply with MarketDbConfig" in {
      val conn = Connection("test")

      val service = new Service[MarketDbReq, MarketDbRep] {
        def apply(request: MarketDbReq) = request match {
          case GetMarketDbConfig => Future(MarketDbConfig(conn))
          case _ => throw new IllegalArgumentException
        }
      }

      val config = wrap(service) {
        client => client(GetMarketDbConfig).apply().asInstanceOf[MarketDbConfig]
      }

      assert(config.connection == conn)
    }

    "should scan trades" in {
      val timeSeries = new TimeSeries[TradePayload](market, security, interval, qualifier)
      val service = new Service[MarketDbReq, MarketDbRep] {
        def apply(request: MarketDbReq) = request match {
          case ScanTrades(_, _, _) => Future(Trades(timeSeries))
          case _ => throw new IllegalArgumentException
        }
      }

      val trades = wrap(service) {
        client => client(ScanTrades(market, security, interval)).apply().asInstanceOf[Trades]
      }

      assert(trades.timeSeries.market == market)
      assert(trades.timeSeries.security == security)
      assert(trades.timeSeries.interval == interval)
    }

    "should scan orders" in {
      val timeSeries = new TimeSeries[OrderPayload](market, security, interval, qualifier)
      val service = new Service[MarketDbReq, MarketDbRep] {
        def apply(request: MarketDbReq) = request match {
          case ScanOrders(_, _, _) => Future(Orders(timeSeries))
          case _ => throw new IllegalArgumentException
        }
      }

      val orders = wrap(service) {
        client => client(ScanOrders(market, security, interval)).apply().asInstanceOf[Orders]
      }

      assert(orders.timeSeries.market == market)
      assert(orders.timeSeries.security == security)
      assert(orders.timeSeries.interval == interval)
    }

  }

  private def wrap[A](service: Service[MarketDbReq, MarketDbRep])(f: Service[MarketDbReq, MarketDbRep] => A): A = {
    val server: Server = ServerBuilder()
      .codec(MarketDbCodec)
      .bindTo(new InetSocketAddress(Port))
      .name("TestMarketDbApu")
      .build(service)

    val client: Service[MarketDbReq, MarketDbRep] = ClientBuilder()
      .codec(MarketDbCodec)
      .hosts(new InetSocketAddress(Port))
      .hostConnectionLimit(1)
      .build()

    try {
      f(client)
    } finally {
      client.release()
      server.close()
    }
  }
}