package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import org.junit.Test
import org.joda.time.DateTime
import com.ergodicity.marketdb.model.{TradePayload, Contract, Code, Market}
import MarketIteratee._
import com.ergodicity.marketdb.{ByteArray, ScannerMock}
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.hbase.async.Scanner
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scala_tools.time.Implicits._
import com.twitter.util.Future

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class MarketIterateeTest {
  val log = LoggerFactory.getLogger(classOf[MarketIterateeTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime

  val now = new DateTime
  val interval = now.withHourOfDay(0) to now.withHourOfDay(23)

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val codeId = (_: Code) => ByteArray(1)

  @Test
  def testIterateOverScanner_WithCounter() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);
    val scanner = ScannerMock(payloads)

    implicit val marketDb = mock(classOf[MarketDB])

    when(marketDb.scan(any(), any(), any())).thenReturn(Future(scanner))

    import com.ergodicity.marketdb.model.TradeProtocol._
    val trades = TradesTimeSeries(market, code, interval)
    log.info("Trades TS: "+trades)
    val iterv = trades.enumerate(counter[TradePayload])
    log.info("GOT IterV: "+iterv);
    val count = iterv.run
    log.info("Count: "+count)
    assert(count == 100)
  }

}