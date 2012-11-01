package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
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
import scala.Some
import com.ergodicity.marketdb.{TimeSeries, ByteArray}

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

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val securityId = (_: Security) => ByteArray(1)

  // Implicit parameter for TimeSeriesEnumerator.enumerate
  implicit val client = mock(classOf[HBaseClient])

  import MarketIteratees._
  import com.ergodicity.marketdb.model.TradeProtocol._


  @Test
  def testOpenScannerFailed() {
    val ShouldNeverHappen = false
    val latch = new CountDownLatch(1)

    val timeSeries = mock(classOf[TimeSeries[TradePayload]])
    when(timeSeries.scan(client)).thenThrow(new IllegalStateException)

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

    val timeSeries = mock(classOf[TimeSeries[TradePayload]])
    when(timeSeries.scan(client)).thenReturn(scanner)

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


    val timeSeries = mock(classOf[TimeSeries[TradePayload]])
    when(timeSeries.scan(client)).thenReturn(scanner)

    val trades = new TimeSeriesEnumerator(timeSeries)

    trades.enumerate(counter[TradePayload]).map(_.run) onSuccess(_ =>assert(ShouldNeverHappen)) onFailure {e =>
      assert(e.isInstanceOf[HBaseException])
      latch.countDown()
    }

    assert(latch.await(1, TimeUnit.SECONDS))
    verify(scanner).close()
  }

}