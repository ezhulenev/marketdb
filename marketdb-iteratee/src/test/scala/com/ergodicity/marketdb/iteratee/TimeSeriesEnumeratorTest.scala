package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.TimeSeries.Qualifier
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.{TimeSeries, ByteArray}
import java.lang.IllegalStateException
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.hbase.async.{HBaseException, HBaseClient, Scanner}
import org.joda.time.DateTime
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.powermock.modules.junit4.PowerMockRunner
import org.scala_tools.time.Implicits._
import org.scalatest.Assertions._
import org.slf4j.LoggerFactory

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner], classOf[HBaseClient]))
class TimeSeriesEnumeratorTest {
  val log = LoggerFactory.getLogger(classOf[TimeSeriesEnumeratorTest])

  val NoSystem = true

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime

  val now = new DateTime
  val interval = now.withHourOfDay(0) to now.withHourOfDay(23)

  val qualifier = Qualifier(ByteArray("table").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val securityId = (_: Security) => ByteArray(1)

  import MarketIteratees._
  import com.ergodicity.marketdb.model.TradeProtocol._

  @Test
  def testOpenScannerFailed() {
    val ShouldNeverHappen = false
    val latch = new CountDownLatch(1)

    val timeSeries = new TimeSeries[TradePayload](market, security, interval, qualifier)

    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    val scanner = mock(classOf[Scanner])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier.table)).thenReturn(scanner)
    when(scanner.nextRows()).thenThrow(new IllegalStateException)

    val trades = new TimeSeriesEnumerator(timeSeries)
    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess(_ => assert(ShouldNeverHappen)) onFailure {case e =>
      assert(e.isInstanceOf[IllegalStateException])
      latch.countDown()
    }

    assert(latch.await(1, TimeUnit.SECONDS))
  }

  @Test
  def testIterateOverScanner() {
    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, NoSystem)
    val scanner = ScannerMock(payloads)

    val timeSeries = new TimeSeries[TradePayload](market, security, interval, qualifier)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier.table)).thenReturn(scanner)

    val trades = new TimeSeriesEnumerator(timeSeries)
    val iterv = trades.enumerate(counter[TradePayload])
    val count = iterv.map(_.run)()
    log.info("Count: " + count)
    assert(count == Count)

    verify(scanner).close()
  }

  @Test
  def testIterationIsBroken() {
    val ShouldNeverHappen = false
    val latch = new CountDownLatch(1)

    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, NoSystem)
    val err = mock(classOf[HBaseException])
    val scanner = ScannerMock(payloads, failOnBatch = Some(3, err))

    val timeSeries = new TimeSeries[TradePayload](market, security, interval, qualifier)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier.table)).thenReturn(scanner)

    val trades = new TimeSeriesEnumerator(timeSeries)

    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess(_ =>assert(ShouldNeverHappen)) onFailure {e =>
      assert(e.isInstanceOf[HBaseException])
      latch.countDown()
    }

    assert(latch.await(1, TimeUnit.SECONDS))
    verify(scanner).close()
  }

}