package com.ergodicity.marketdb

import model._
import model.Contract._
import org.mockito.Mockito._
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import com.stumbleupon.async.Deferred
import org.slf4j.LoggerFactory
import java.util.ArrayList
import collection.JavaConversions
import org.hbase.async._
import org.junit.Test
import org.joda.time.DateTime

object ScannerMock {
  val DefaultRowsCount = 10

  val Family = ByteArray("id").toArray

  def tradesToKeyValues(trades: Seq[TradePayload])(implicit marketId: Market => ByteArray, codeId: Code => ByteArray) = {
    import sbinary.Operations._
    import com.ergodicity.marketdb.model.TradeProtocol._
    trades map {
      payload =>
        val k = TradeRow(marketId(payload.market), codeId(payload.code), payload.time)
        new KeyValue(k.toArray, Family, Bytes.fromLong(payload.tradeId), toByteArray(payload))
    }
  }

  def fromKeyValues(values: Seq[KeyValue], batchSize: Int = DefaultRowsCount) = {
    var pointer = 0

    def nextBatch() = {
      if (pointer >= values.size) {
        null
      } else {
        val slice = values.slice(pointer, pointer + batchSize)
        pointer += batchSize
        slice
      }
    }

    def nextRows = {
      val grouped = nextBatch().groupBy(kv => ByteArray(kv.key()))
      val list = new ArrayList[ArrayList[KeyValue]]()
      grouped.keys.foreach {
        key =>
          val values = grouped.get(key).get
          list.add(new ArrayList[KeyValue](JavaConversions.asJavaCollection(values)))
      }
      list
    }

    val scanner = mock(classOf[Scanner])
    when(scanner.nextRows()).thenReturn(Deferred.fromResult(nextRows))
    scanner
  }
}

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class ScannerMockTest {
  val log = LoggerFactory.getLogger(classOf[ScannerMockTest])

  import ScannerMock._

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val codeId = (_: Code) => ByteArray(1)

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, code, contract, BigDecimal("111"), 1, time, 11l, true)

  @Test
  def testSinglePayload() {
    val scanner = fromKeyValues(tradesToKeyValues(Seq(payload)))

    val rows = scanner.nextRows().joinUninterruptibly
    log.info("Rows: " + rows)

    assert(rows.size() == 1)
    assert(rows.get(0).size == 1)
  }

  @Test
  def testGroupByKey() {
    val payload1 = TradePayload(market, code, contract, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 0, 0), 11l, true)
    val payload2 = TradePayload(market, code, contract, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 01, 1, 0), 11l, true)
    val payload3 = TradePayload(market, code, contract, BigDecimal("111"), 1, new DateTime(2012, 01, 01, 01, 02, 1, 0), 11l, true)

    val scanner = fromKeyValues(tradesToKeyValues(Seq(payload1, payload2, payload3)))

    val rows = scanner.nextRows().joinUninterruptibly
    log.info("Rows size: " + rows.size())
    log.info("Rows: " + rows)

    assert(rows.size() == 2)
    assert(rows.get(0).size == 2)
    assert(rows.get(1).size == 1)
  }
}
