package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.model._
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
import com.ergodicity.marketdb.model.OrderPayload

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner], classOf[HBaseClient]))
class SingleTimeSeriesEnumeratorTest {
  val log = LoggerFactory.getLogger(classOf[SingleTimeSeriesEnumeratorTest])

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

    val trades = new SingleTimeSeriesEnumerator(timeSeries)
    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess (_ => assert(ShouldNeverHappen)) onFailure {
      case e =>
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

    val trades = new SingleTimeSeriesEnumerator(timeSeries)
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

    val trades = new SingleTimeSeriesEnumerator(timeSeries)

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
  def testSeq() {
    val NoSystem = true

    val time1 = new DateTime()
    val time2 = time1 + 1.second
    val time3 = time1 + 2.second

    val order1 = OrderPayload(market, security, 0l, time1, 0, 0, 0, 0, 0, 0, None)
    val order2 = OrderPayload(market, security, 0l, time2, 0, 0, 0, 0, 0, 0, None)
    val trade1 = TradePayload(market, security, 0l, 0, 0, time3, NoSystem)

    val seq1: Seq[Option[MarketPayload]] = Seq(Some(order1), None, Option(order2), Some(trade1))
    val seq2: Seq[Option[MarketPayload]] = Seq[Option[MarketPayload]](None, None)

    val ord = Ordering.by((_: MarketPayload).time.getMillis)

    log.info("Ebaka1 = "+seq1.flatten)
    log.info("Min1 = "+seq1.flatten.reduceOption(ord.min))

    log.info("Ebaka2 = "+seq2.flatten)
    log.info("Min2 = "+seq2.flatten.reduceOption(ord.min))
  }
}