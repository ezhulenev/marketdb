package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.ByteArray
import com.ergodicity.marketdb.core.MarketDb
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.twitter.util.Future
import org.hbase.async.{HBaseException, Scanner}
import org.joda.time.DateTime
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.powermock.modules.junit4.PowerMockRunner
import org.scala_tools.time.Implicits._
import org.scalatest.Assertions._
import org.slf4j.LoggerFactory
import scala.Some

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class MarketTradesIterateeTest {
  val log = LoggerFactory.getLogger(classOf[MarketTradesIterateeTest])

  val NoSystem = true

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime

  val now = new DateTime
  val interval = now.withHourOfDay(0) to now.withHourOfDay(23)

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val securityId = (_: Security) => ByteArray(1)

  import MarketIteratees._
  import com.ergodicity.marketdb.model.TradeProtocol._

  @Test
  def testOpenScannerFailed() {
    implicit val marketDb = mock(classOf[MarketDb])
    when(marketDb.scanTrades(any(), any(), any())).thenThrow(new IllegalStateException)

    val trades = TradesTimeSeries(market, security, interval)
    import org.scalatest.Assertions._
    intercept[IllegalStateException] {
      trades.enumerate(counter[TradePayload])
    }
  }

  @Test
  def testIterateOverScanner() {
    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, NoSystem)
    val scanner = ScannerMock(payloads)

    implicit val marketDb = mock(classOf[MarketDb])
    when(marketDb.scanTrades(any(), any(), any())).thenReturn(Future(scanner))

    val trades = TradesTimeSeries(market, security, interval)
    val iterv = trades.enumerate(counter[TradePayload])
    val count = iterv.map(_.run)()
    log.info("Count: " + count)
    assert(count == Count)

    verify(scanner).close()
  }

  @Test
  def testIterationIsBroken() {
    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, NoSystem)
    val err = mock(classOf[HBaseException])
    val scanner = ScannerMock(payloads, failOnBatch = Some(3, err))

    implicit val marketDb = mock(classOf[MarketDb])
    when(marketDb.scanTrades(any(), any(), any())).thenReturn(Future(scanner))

    val trades = TradesTimeSeries(market, security, interval)
    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess {
      _ =>
        assert(false)
    } onFailure {
      err =>
        log.info("Error: " + err)
    }

    verify(scanner).close()
  }

}