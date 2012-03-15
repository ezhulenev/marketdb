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
class TradesScannerTest {
  val log = LoggerFactory.getLogger(classOf[TradesScannerTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val codeId = (_: Code) => ByteArray(1)
  
  @Test
  def testOpenOnce() {
    val scanner = mock(classOf[Scanner])
    when(scanner.nextRows()).thenAnswer(new Answer[Deferred[ArrayList[ArrayList[KeyValue]]]] {
      def answer(invocation: InvocationOnMock) = Deferred.fromResult[ArrayList[ArrayList[KeyValue]]](null)
    })
    val stream = TradesScanner(scanner)

    // -- First success
    stream.open()

    // -- Second should fail
    import org.scalatest.Assertions._
    intercept[IllegalStateException] {
      stream.open()      
    }    
  } 

  @Test
  def testMarketScannerFailed() {
    val scanner = mock(classOf[Scanner])
    
    when(scanner.nextRows()).thenAnswer(new Answer[Deferred[ArrayList[ArrayList[KeyValue]]]] {
      def answer(invocation: InvocationOnMock) = Deferred.fromError[ArrayList[ArrayList[KeyValue]]](mock(classOf[HBaseException]))
    })

    val stream = TradesScanner(scanner)
    val handle = stream.open()

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
  def testMarketScannerSuccess() {

    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesScanner(scanner)
    val handle = stream.open()

    var completed = false
    handle.error foreach {case TradesScanCompleted =>
      completed = true
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 100)
    assert(completed)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  @Test
  def testMarketScannerWithNoAck() {

    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesScanner(scanner)
    val handle = stream.open()

    var completed = false
    handle.error foreach {case TradesScanCompleted =>
      completed = true
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
    }

    assert(stack.size == 1)
    assert(!completed)

    // -- Verify scanner closed
    verify(scanner, never()).close()
  }

  @Test
  def testBufferedScanner_SmallBuffer() {
    testBufferedScanner(10)
  }

  @Test
  def testBufferedScanner_HugeBuffer() {
    testBufferedScanner(1000)
  }

  @Test
  def testMarketScannerFailOnBatchNumber() {

    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val err = mock(classOf[HBaseException])
    val scanner = ScannerMock(payloads, 2, Some(10, err))
    val stream = TradesScanner(scanner)
    val handle = stream.open()

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
  def testBufferedScannerFailOnBatchNumber_SmallBuffer() {
    testBufferedScannerFailOnBatchNumber(10)
  }

  @Test
  def testBufferedScannerFailOnBatchNumber_BigBuffer() {
    testBufferedScannerFailOnBatchNumber(50)
  }

  @Test
  def testCloseMarketScanner() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesScanner(scanner)
    val handle = stream.open()
    
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
  def testCloseBufferedMarketScanner() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesScanner(scanner)
    val handle = stream.open().buffered(10)

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
  def testCloseBufferedMarketScanner_BufferAll() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val stream = TradesScanner(scanner)
    val handle = stream.open().buffered(1000)

    var completed = false
    handle.error foreach {
      case TradesScanCompleted => completed = true
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
    assert(completed)

    // -- Verify scanner closed
    verify(scanner).close()
  }

  private def testBufferedScannerFailOnBatchNumber(size: Int) {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val err = mock(classOf[HBaseException])
    val scanner = ScannerMock(payloads, 2, Some(10, err))
    val stream = TradesScanner(scanner)
    val handle = stream.open().buffered(size)

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

  private def testBufferedScanner(size: Int) {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads, 2)
    val stream = TradesScanner(scanner)
    val handle = stream.open().buffered(size)

    var completed = false
    handle.error foreach {case TradesScanCompleted =>
      completed = true
    }

    val stack = Stack[TradePayload]()
    handle.trades foreach {
      t =>
        log.info("Trade: " + t.payload)
        stack.push(t.payload)
        t.ack()
    }

    assert(stack.size == 100)
    assert(completed)

    // -- Verify scanner closed
    verify(scanner).close()
  }

}
