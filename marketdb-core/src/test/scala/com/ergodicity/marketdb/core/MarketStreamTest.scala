package com.ergodicity.marketdb.core

import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import org.slf4j.LoggerFactory
import org.mockito.Mockito._
import org.junit.Test
import org.joda.time.DateTime
import com.ergodicity.marketdb.model.{Contract, TradePayload, Code, Market}
import com.ergodicity.marketdb.{ScannerMock, ByteArray}
import collection.mutable.Stack
import javax.xml.crypto.dsig.keyinfo.KeyValue
import java.util.ArrayList
import org.hbase.async.{HBaseException, Scanner}
import com.stumbleupon.async.Deferred
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import com.twitter.finagle.kestrel.ReadClosedException

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class MarketStreamTest {
  val log = LoggerFactory.getLogger(classOf[MarketStreamTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val codeId = (_: Code) => ByteArray(1)

  @Test
  def testMarketStreamFailed() {
    val scanner = mock(classOf[Scanner])
    
    when(scanner.nextRows()).thenAnswer(new Answer[Deferred[ArrayList[ArrayList[KeyValue]]]] {
      def answer(invocation: InvocationOnMock) = Deferred.fromError[ArrayList[ArrayList[KeyValue]]](mock(classOf[HBaseException]))
    })

    val stream = TradesStream(scanner)
    val handle = stream.read()

    handle.error foreach {
      case e: HBaseException =>
        log.info("Error: "+e)
        assert(true)
      case _ => assert(false)
    }

    handle.trades foreach {_ =>
      assert(false)
    }

    // -- Verify scanner closed
    verify(scanner).close()
  }

  @Test
  def testMarketStreamSuccess() {

    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesStream(scanner)
    val handle = stream.read()

    var exhausted = false
    handle.error foreach {case TradesStreamExhaustedException =>
      exhausted = true
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 100)
    assert(exhausted)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  @Test
  def testMarketStreamWithNoAck() {

    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesStream(scanner)
    val handle = stream.read()

    var exhausted = false
    handle.error foreach {case TradesStreamExhaustedException =>
      exhausted = true
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
    }

    assert(stack.size == 1)
    assert(!exhausted)

    // -- Verify scanner closed
    verify(scanner, never()).close()
  }

  @Test
  def testBufferedStream_SmallBuffer() {
    testBufferedStream(10)
  }

  @Test
  def testBufferedStream_HugeBuffer() {
    testBufferedStream(1000)
  }

  @Test
  def testMarketStreamFailOnBatchNumber() {

    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val err = mock(classOf[HBaseException])
    val scanner = ScannerMock(payloads, 2, Some(10, err))
    val stream = TradesStream(scanner)
    val handle = stream.read()

    handle.error foreach {
      case e: HBaseException => assert(true)
      case _ => assert(false)
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 10*2)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  @Test
  def testBufferedStreamFailOnBatchNumber_SmallBuffer() {
    testBufferedStreamFailOnBatchNumber(10)
  }

  @Test
  def testBufferedStreamFailOnBatchNumber_BigBuffer() {
    testBufferedStreamFailOnBatchNumber(50)
  }

  @Test
  def testCloseMarketStream() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesStream(scanner)
    val handle = stream.read()
    
    var closed = false
    handle.error foreach {
      case ReadClosedException => closed = true
      case _ => assert(false)
    }

    val stack = Stack[TradePayload]()
    var tradeNmr = 0
    handle.trades foreach {
      t =>
        if (tradeNmr == 45) handle.close() else tradeNmr += 1
        log.info("Trade: " + t.payload)
        stack.push(t.payload)        
        t.ack()
    }

    assert(stack.size == 46)
    assert(closed)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  @Test
  def testCloseBufferedMarketStream() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesStream(scanner)
    val handle = stream.read().buffered(10)

    var closed = false
    handle.error foreach {
      case ReadClosedException => closed = true
      case _ => assert(false)
    }

    val stack = Stack[TradePayload]()
    var tradeNmr = 0
    handle.trades foreach {
      t =>
        if (tradeNmr == 45) handle.close() else tradeNmr += 1
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 55)
    assert(closed)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  @Test
  def testCloseBufferedMarketStream_BufferAll() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesStream(scanner)
    val handle = stream.read().buffered(1000)

    var exhausted = false
    handle.error foreach {
      case TradesStreamExhaustedException => exhausted = true
      case _ => assert(false)
    }

    val stack = Stack[TradePayload]()
    var tradeNmr = 0
    handle.trades foreach {
      t =>
        if (tradeNmr == 5) handle.close() else tradeNmr += 1
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 100)
    assert(exhausted)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  private def testBufferedStreamFailOnBatchNumber(size: Int) {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val err = mock(classOf[HBaseException])
    val scanner = ScannerMock(payloads, 2, Some(10, err))
    val stream = TradesStream(scanner)
    val handle = stream.read().buffered(size)

    handle.error foreach {
      case e: HBaseException => assert(true)
      case _ => assert(false)
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 10*2)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  private def testBufferedStream(size: Int) {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads, 2)
    val stream = TradesStream(scanner)
    val handle = stream.read().buffered(size)

    var exhausted = false
    handle.error foreach {case TradesStreamExhaustedException =>
      exhausted = true
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 100)
    assert(exhausted)

    // -- Verify scanner closed
    verify(scanner).close()
  }

}
