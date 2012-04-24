package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import org.junit.Test
import org.joda.time.DateTime
import com.ergodicity.marketdb.model.{TradePayload, Security, Market}
import MarketIteratee._
import com.ergodicity.marketdb.{ByteArray, ScannerMock}
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scala_tools.time.Implicits._
import com.twitter.util.Future
import com.ergodicity.marketdb.model.TradeProtocol._
import org.hbase.async.Scanner

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class MarketIterateeTest {
  val log = LoggerFactory.getLogger(classOf[MarketIterateeTest])

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime

  val now = new DateTime
  val interval = now.withHourOfDay(0) to now.withHourOfDay(23)

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val securityId = (_: Security) => ByteArray(1)

  @Test
  def testOpenScannerFailed() {
    implicit val marketDb = mock(classOf[MarketDB])
    when(marketDb.scan(any(), any(), any())).thenThrow(new IllegalStateException)

    val trades = TradesTimeSeries(market, security, interval)
    import org.scalatest.Assertions._
    intercept[IllegalStateException] {
      trades.enumerate(counter[TradePayload])
    }
  }

  @Test
  def testIterateOverScanner() {
    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, true);
    val scanner = ScannerMock(payloads)

    implicit val marketDb = mock(classOf[MarketDB])
    when(marketDb.scan(any(), any(), any())).thenReturn(Future(scanner))

    val trades = TradesTimeSeries(market, security, interval)
    val iterv = trades.enumerate(counter[TradePayload])
    val count = iterv.map(_.run)()
    log.info("Count: "+count)
    assert(count == Count)

    verify(scanner).close()
  }
  
  @Test
  def testIterationIsBroken() {
    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, true);
    val err = new IllegalStateException
    val scanner = ScannerMock(payloads, failOnBatch = Some(3, err))

    implicit val marketDb = mock(classOf[MarketDB])
    when(marketDb.scan(any(), any(), any())).thenReturn(Future(scanner))

    val trades = TradesTimeSeries(market, security, interval)
    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess {_ =>
      assert(false)
    } onFailure {err =>
      log.info("Error: "+err )
      assert(true)
    }

     verify(scanner).close()
  }

}