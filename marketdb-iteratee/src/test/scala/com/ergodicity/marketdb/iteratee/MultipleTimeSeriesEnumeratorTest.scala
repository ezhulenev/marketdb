package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.{TimeSeries, ByteArray}
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
import com.ergodicity.marketdb.model._
import TimeSeriesEnumerator._
import com.ergodicity.marketdb.model.TradeProtocol._
import com.ergodicity.marketdb.model.OrderProtocol._
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.TimeSeries.Qualifier
import scala.Some
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.model.OrderPayload
import com.ergodicity.marketdb.mock.ScannerMock

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

  @Test
  def testIterationOverMultipleScanners() {
    val SeriesCount = 10
    val SeriesLength = 10

    val scanners = for (seriesN <- 1 to SeriesCount) yield {
      val payloads = for (i <- 1 to SeriesLength) yield TradePayload(market, security, 1000*seriesN + i, 0, 1, time + (seriesN * 100).millis + i.minute, NoSystem)
      ScannerMock(payloads, name = Some("Scanner#"+seriesN), batchSize = (3 + math.random * 10).toInt)
    }

    val qualifiers = for (seriesN <- 1 to SeriesCount) yield Qualifier(ByteArray("table#" + seriesN).toArray, ByteArray("start").toArray, ByteArray("stop").toArray)

    val timeSeries = qualifiers.map(q => new TimeSeries[TradePayload](market, security, interval, q).pimp)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)

    (qualifiers zip scanners) foreach {
      case (q, s) => when(client.newScanner(q.table)).thenReturn(s)
    }

    // -- Iterate over time series
    val trades = TimeSeriesEnumerator(timeSeries: _*)
    val iterv = trades.enumerate(sequencer[TradePayload])
    val sequence = iterv.map(_.run)()

    log.info("Count: " + sequence.size)
    assert(sequence.size == SeriesCount * SeriesLength)

    sequence.foldLeft(sequence.head) {
      case (prev, current) if (prev.time <= current.time) => current
      case (prev, current) =>
        assert(prev.time <= current.time, "Sequence not ordered by time")
        throw new IllegalArgumentException
    }

    scanners.foreach(scanner => verify(scanner).close())
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
    val trades = TimeSeriesEnumerator(timeSeries1, timeSeries2)
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

  @Test
  def testIterationOverMultipleScannersForTradesAndOrders() {
    val Count = 20

    val scanner1 = {
      val payloads = for (i <- 1 to Count) yield TradePayload(market, security, i, BigDecimal("111"), 1, time + i.second, NoSystem)
      ScannerMock(payloads, batchSize = 10)
    }

    val scanner2 = {
      val payloads = for (i <- 1 to Count) yield OrderPayload(market, security, i, time + 500.millis + i.second, 0, 0, 0, BigDecimal("222"), 1, 1, None)
      ScannerMock(payloads, batchSize = 15)
    }

    val qualifier1 = Qualifier(ByteArray("Trades").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)
    val qualifier2 = Qualifier(ByteArray("Orders").toArray, ByteArray("start").toArray, ByteArray("stop").toArray)

    val timeSeries1 = new TimeSeries[TradePayload](market, security, interval, qualifier1)
    val timeSeries2 = new TimeSeries[OrderPayload](market, security, interval, qualifier2)

    // -- Prepare mocks
    implicit val reader = mock(classOf[MarketDbReader])
    val client = mock(classOf[HBaseClient])
    when(reader.client).thenReturn(client)
    when(client.newScanner(qualifier1.table)).thenReturn(scanner1)
    when(client.newScanner(qualifier2.table)).thenReturn(scanner2)

    // -- Iterate over time series
    val trades: TimeSeriesEnumerator[MarketPayload] = TimeSeriesEnumerator(timeSeries1, timeSeries2)

    val iterv = trades.enumerate(sequencer[MarketPayload])
    val sequence = iterv.map(_.run)()
    log.info("Count: " + sequence.size)
    assert(sequence.size == 2 * Count)

    sequence.foldLeft(sequence.head) {
      case (prev, current) if (prev.time <= current.time) => current
      case (prev, current) =>
        assert(prev.time <= current.time, "Sequence not ordered by time")
        throw new IllegalArgumentException
    }

    sequence.zipWithIndex.foreach {
      case (trade: TradePayload, index) if (index % 2 == 0) => // it's ok
      case (order: OrderPayload, index) if (index % 2 == 1) => // it's ok
      case (payload, index) => assert(false)
    }

    verify(scanner1).close()
    verify(scanner2).close()
  }

}
