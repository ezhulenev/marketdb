package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import org.scalatest.Spec
import org.mockito.Mockito._
import com.ergodicity.zeromq.{Client, Connect}
import org.joda.time.DateTime
import com.ergodicity.marketdb.model.{Contract, Code, Market, TradePayload}
import org.scala_tools.time.Implicits._
import com.ergodicity.zeromq.SocketType._
import com.ergodicity.marketdb.model.TradeProtocol._
import org.zeromq.ZMQ
import com.twitter.util.Future
import org.mockito.Matchers._

class ZMQLoaderSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[ZMQLoaderSpec])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val interval = time.withHourOfDay(0) to time.withHourOfDay(23)

  describe("ZMQLoader") {

    it("should add trades from input socket") {
      val TradesCount = 10

      val marketDb = mock(classOf[MarketDB])
      when(marketDb.addTrade(any())).thenReturn(Future(true))

      val loader = new ZMQLoader(marketDb, "tcp://*:30000")
      loader.start()
      val client = Client(Pub, options = Connect("tcp://localhost:30000") :: Nil)(ZMQ.context(1))

      val payloads = for (i <- 1 to TradesCount) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

      client.send(payloads.toList)

      // Let all trades to be processed
      Thread.sleep(100)

      verify(marketDb, times(TradesCount)).addTrade(any())

      client.close()
      loader.shutdown()
    }
  }

}
