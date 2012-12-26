package com.ergodicity.marketdb.iteratee

import collection.JavaConversions._
import com.ergodicity.marketdb.model._
import java.util
import org.hbase.async._
import org.joda.time.DateTime
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.powermock.modules.junit4.PowerMockRunner
import org.slf4j.LoggerFactory
import scala.Some
import com.ergodicity.marketdb.ByteArray
import sbinary.Operations._
import com.ergodicity.marketdb.model.TradeProtocol._


@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class ScannerMockTest {
  val log = LoggerFactory.getLogger(classOf[ScannerMockTest])

  import ScannerMock._

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val securityId = (_: Security) => ByteArray(1)

  val NoSystem = true

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, security, 11l, BigDecimal("111"), 1, time, NoSystem)

  @Test
  def testSinglePayload() {
    val scanner = fromKeyValues(Seq(TradeToKeyValue.keyValue(payload)))

    val rows = scanner.nextRows().joinUninterruptibly
    log.info("Rows: " + rows)

    assert(rows.size() == 1)
    assert(rows.get(0).size == 1)
  }

  @Test
  def testGroupByKey() {
    val payload1 = TradePayload(market, security, 11l, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 0, 0), NoSystem)
    val payload2 = TradePayload(market, security, 11l, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 1, 0), NoSystem)
    val payload3 = TradePayload(market, security, 11l, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 02, 1, 0), NoSystem)

    val scanner = fromPayloads(Seq(payload1, payload2, payload3))

    val rows = scanner.nextRows().joinUninterruptibly
    log.info("Rows size: " + rows.size())
    log.info("Rows: " + rows)

    assert(rows.size() == 2)
    assert(rows.get(0).size == 2)
    assert(rows.get(1).size == 1)
  }

  @Test
  def testFailOnBatch() {
    val payload1 = TradePayload(market, security, 11l, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 0, 0), NoSystem)
    val payload2 = TradePayload(market, security, 11l, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 1, 0), NoSystem)
    val payload3 = TradePayload(market, security, 11l, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 02, 1, 0), NoSystem)

    val err = mock(classOf[HBaseException])
    val scanner = fromPayloads(Seq(payload1, payload2, payload3), 1, Some(1, err))

    // -- First batch Success
    val rows = scanner.nextRows().joinUninterruptibly
    assert(rows.size() == 1)
    assert(rows.get(0).size == 1)

    // -- Expected failure on second batch
    import org.scalatest.Assertions._
    intercept[HBaseException] {
      scanner.nextRows().joinUninterruptibly()
    }
  }

  @Test
  def testSplitToBatches() {
    var list = List[TradePayload]()
    for (i <- 1 to 95) {
      val payload = TradePayload(market, security, i, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 0, i), NoSystem)
      list = payload :: list
    }

    val scanner = fromPayloads(list.toSeq, batchSize = 10)

    for (i <- 1 to 9) {
      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("#" + i + ": Trades size: " + toTrades(rows).size + "; " + toTrades(rows).toList.map(_.tradeId))

      assert(rows.size() == 1)
      assert(rows.get(0).size == 10)
    }

    val rows = scanner.nextRows().joinUninterruptibly()
    log.info("Last trades size: " + toTrades(rows).size + "; " + toTrades(rows).toList.map(_.tradeId))
    assert(rows.size() == 1)
    assert(rows.get(0).size == 5)

    assert(scanner.nextRows().joinUninterruptibly() == null)
  }

  private def toTrades(rows: util.ArrayList[util.ArrayList[KeyValue]]) = asScalaIterator(rows.iterator()) flatMap {
    row =>
      asScalaIterator(row.iterator())
  } map {
    kv =>
      fromByteArray[TradePayload](kv.value())
  }
}