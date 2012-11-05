package com.ergodicity.marketdb.iteratee

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
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.TimeSeries.Qualifier
import scala.Some
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import scalaz.NonEmptyList

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner], classOf[HBaseClient]))
class MultipleTimeSeriesEnumeratorTest {
  val log = LoggerFactory.getLogger(classOf[MultipleTimeSeriesEnumeratorTest])

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
  def testScannerFailure() {
    val ShouldNeverHappen = false
    val latch = new CountDownLatch(1)

    val timeSeries = new TimeSeries[TradePayload](market, security, interval, qualifier)

    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    val scanner = mock(classOf[Scanner])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier.table)).thenReturn(scanner)
    when(scanner.nextRows()).thenThrow(new IllegalStateException)

    val trades = new MultipleTimeSeriesEnumerator(NonEmptyList(timeSeries))
    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess (_ => assert(ShouldNeverHappen)) onFailure {
      case e =>
        assert(e.isInstanceOf[IllegalStateException])
        latch.countDown()
    }

    assert(latch.await(1, TimeUnit.SECONDS))
  }


  @Test
  def testIterateOverSingleScanner() {
    val Count = 100
    val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, NoSystem)
    val scanner = ScannerMock(payloads)

    val timeSeries = new TimeSeries[TradePayload](market, security, interval, qualifier)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier.table)).thenReturn(scanner)

    val trades = new MultipleTimeSeriesEnumerator(NonEmptyList(timeSeries))
    val iterv = trades.enumerate(counter[TradePayload])
    val count = iterv.map(_.run)()
    log.info("Count: " + count)
    assert(count == Count)

    verify(scanner).close()
  }

  @Test
  def testSingleScannerIterationIsBroken() {
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

    val trades = new MultipleTimeSeriesEnumerator(NonEmptyList(timeSeries))

    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess (_ => assert(ShouldNeverHappen)) onFailure {
      e =>
        assert(e.isInstanceOf[HBaseException])
        log.info("Iteration error = " + e)
        latch.countDown()
    }

    assert(latch.await(1, TimeUnit.SECONDS))
    verify(scanner).close()
  }

  @Test
  def testIterationOverMultipleScanners() {
    val Count = 100

    val scanner1 = {
      val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time + i.second, NoSystem)
      ScannerMock(payloads)
    }

    val scanner2 = {
      val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("222"), 1, time + 500.millis + i.second, NoSystem)
      ScannerMock(payloads)
    }

    val qualifier1 = Qualifier(ByteArray("table1").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)
    val qualifier2 = Qualifier(ByteArray("table2").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)

    val timeSeries1 = new TimeSeries[TradePayload](market, security, interval, qualifier1)
    val timeSeries2 = new TimeSeries[TradePayload](market, security, interval, qualifier2)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier1.table)).thenReturn(scanner1)
    when(client.newScanner(qualifier2.table)).thenReturn(scanner2)

    // -- Iterate over time series
    val trades = new MultipleTimeSeriesEnumerator(NonEmptyList(timeSeries1, timeSeries2))
    val iterv = trades.enumerate(counter[TradePayload])
    val count = iterv.map(_.run)()
    log.info("Count: " + count)
    assert(count == 2 * Count)

    verify(scanner1).close()
    verify(scanner2).close()
  }

  @Test
  def testIterationOverMultipleScannersWithOneBroken() {
    val ShouldNeverHappen = false
    val latch = new CountDownLatch(1)

    val Count = 100

    val scanner1 = {
      val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time + i.second, NoSystem)
      val err = mock(classOf[HBaseException])
      ScannerMock(payloads, failOnBatch = Some(3, err))
    }

    val scanner2 = {
      val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("222"), 1, time + 500.millis + i.second, NoSystem)
      ScannerMock(payloads)
    }

    val qualifier1 = Qualifier(ByteArray("table1").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)
    val qualifier2 = Qualifier(ByteArray("table2").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)

    val timeSeries1 = new TimeSeries[TradePayload](market, security, interval, qualifier1)
    val timeSeries2 = new TimeSeries[TradePayload](market, security, interval, qualifier2)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier1.table)).thenReturn(scanner1)
    when(client.newScanner(qualifier2.table)).thenReturn(scanner2)

    // -- Iterate over time series
    val trades = new MultipleTimeSeriesEnumerator(NonEmptyList(timeSeries1, timeSeries2))
    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess (_ => assert(ShouldNeverHappen)) onFailure {
      e =>
        assert(e.isInstanceOf[HBaseException])
        log.info("Iteration error = " + e)
        latch.countDown()
    }

    assert(latch.await(1, TimeUnit.SECONDS))

    verify(scanner1).close()
    verify(scanner2).close()
  }
}
