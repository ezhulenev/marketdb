package com.ergodicity.marketdb.core

import org.powermock.core.classloader.annotations.{PowerMockIgnore, PrepareForTest}
import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.junit.Test
import com.ergodicity.marketdb.model.{TradePayload, Market, Security}
import com.ergodicity.marketdb.{ScannerMock, ByteArray}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.joda.time.{Interval, DateTime}
import org.zeromq.ZMQ
import com.twitter.util.{FuturePool, Future}
import com.ergodicity.zeromq.SocketType._
import org.scala_tools.time.Implicits._
import com.ergodicity.marketdb.stream.MarketStreamProtocol._
import com.ergodicity.marketdb.stream._
import com.ergodicity.zeromq._
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import org.hbase.async.{HBaseException, Scanner}
import java.util.UUID
import com.twitter.finagle.Service
import java.net.InetSocketAddress
import com.twitter.finagle.builder.ClientBuilder

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class ZMQTradesStreamerTest {
  val log = LoggerFactory.getLogger(classOf[ZMQTradesStreamerTest])

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val interval = time.withHourOfDay(0) to time.withHourOfDay(23)

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val securityId = (_: Security) => ByteArray(1)

  val FinaglePort = 3333
  val PublishEndpoint = "inproc://publish-endpoint"
  val Heartbeat = HeartbeatRef("inproc://ping", "inproc://pong")

  implicit val context = ZMQ.context(1)
  implicit val Pool = FuturePool(Executors.newCachedThreadPool())
  
  implicit val deserializer = new Deserializer[MarketStreamPayload] {
    import sbinary._
    import Operations._
    def apply(frames: Seq[Frame]) = {
      frames match {
        case Seq(id, data) => fromByteArray[MarketStreamPayload](data.payload.toArray)
        case err => throw new IllegalStateException("Illegal frames seq = "+err)
      }
    }
  }

  @Test
  def testOpenAndCloseStream() {
    val scanner = mock(classOf[Scanner])
    val marketDb = mock(classOf[MarketDB])
    when(marketDb.scanTrades(any[Market], any[Security], any[Interval])).thenReturn(Future(scanner))

    val tradesStreamer = new ZMQTradesStreamer(marketDb, FinaglePort, PublishEndpoint, Heartbeat)
    tradesStreamer.start()

    val client: Service[MarketStreamReq, MarketStreamRep] = ClientBuilder()
      .codec(MarketStreamCodec)
      .hosts(new InetSocketAddress(FinaglePort))
      .hostConnectionLimit(1)
      .build()

    // Open Stream
    val open: MarketStreamReq = OpenStream(market, security, interval)
    val openReply = client.apply(open)()

    log.info("Open reply: " + openReply)
    assert(openReply match {
      case StreamOpened(s) => true
      case _ => false
    })

    // Close stream
    val close: MarketStreamReq = CloseStream(openReply.asInstanceOf[StreamOpened].stream)
    val closeReply = client.apply(close)()

    log.info("Close reply: " + closeReply)
    assert(closeReply match {
      case StreamClosed() => true
      case _ => false
    })

    // Close all
    tradesStreamer.shutdown()
    client.release()
  }

  @Test
  def testStreamingTradesFailed() {
    val err = mock(classOf[HBaseException])
    when(err.getMessage).thenReturn("Mock Error")

    val failOn = Some(0, err)
    val BatchSize = 30;
    val TradesExpected = 0

    scannerFailed(failOn, BatchSize, TradesExpected)
  }

  @Test
  def testStreamingTradesFailedInProcess() {
    val err = mock(classOf[HBaseException])
    when(err.getMessage).thenReturn("Mock Error")

    val failOn = Some(1, err)
    val BatchSize = 30;
    val TradesExpected = 30
    
    scannerFailed(failOn, BatchSize, TradesExpected)
  }
  
  @Test
  def testStreamingTrades() {
    val TradesCount = 1000
    val payloads = for (i <- 1 to TradesCount) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, true);

    val scanner = ScannerMock(payloads, 100)
    val marketDb = mock(classOf[MarketDB])
    when(marketDb.scanTrades(any[Market], any[Security], any[Interval])).thenReturn(Future(scanner))

    val tradesStreamer = new ZMQTradesStreamer(marketDb, FinaglePort, PublishEndpoint, Heartbeat)
    tradesStreamer.start()

    val client: Service[MarketStreamReq, MarketStreamRep] = ClientBuilder()
      .codec(MarketStreamCodec)
      .hosts(new InetSocketAddress(FinaglePort))
      .hostConnectionLimit(1)
      .build()

    val sub = Client(Sub, options = Connect(PublishEndpoint) :: Nil)

    // Open Stream
    val open: MarketStreamReq = OpenStream(market, security, interval)
    val opened = (client.apply(open)()).asInstanceOf[StreamOpened]

    sub.subscribe(Subscribe(opened.stream.id.getBytes))

    val patient = new Patient(Heartbeat, Identifier(opened.stream.id))
    tradesStreamer.heartbeat.ping(UUID.randomUUID())

    val latch = new CountDownLatch(1)
    var tradesNbr = 0
    val handle = sub.read[MarketStreamPayload]
    handle.messages foreach {
      case ReadMessage(Payload(trade), ack) => tradesNbr += 1; ack()
      case ReadMessage(Completed(interrupted), ack) if !interrupted => log.info("Completed"); latch.countDown(); ack()
      case ReadMessage(_, ack) => ack()
    }

    assert(latch.await(5, TimeUnit.SECONDS))
    assert(tradesNbr == TradesCount, "Expected = "+TradesCount+" actual = "+tradesNbr)

    // Verify
    verify(marketDb, only()).scanTrades(market, security, interval)
    verify(scanner).close();

    // Close all
    tradesStreamer.shutdown()
    patient.close()
    client.release()
    handle.close()
    sub.close()
  }

  private def scannerFailed(failOn: Some[(Int, HBaseException)], BatchSize: Int, TradesExpected: Int) {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, security, i, BigDecimal("111"), 1, time, true);
    val scanner = ScannerMock(payloads, BatchSize, failOn);

    val marketDb = mock(classOf[MarketDB])
    when(marketDb.scanTrades(any[Market], any[Security], any[Interval])).thenReturn(Future(scanner))

    val tradesStreamer = new ZMQTradesStreamer(marketDb, FinaglePort, PublishEndpoint, Heartbeat)
    tradesStreamer.start()

    val client: Service[MarketStreamReq, MarketStreamRep] = ClientBuilder()
      .codec(MarketStreamCodec)
      .hosts(new InetSocketAddress(FinaglePort))
      .hostConnectionLimit(1)
      .build()

    val sub = Client(Sub, options = Connect(PublishEndpoint) :: Subscribe.all :: Nil)

    // Open Stream
    val open: MarketStreamReq = OpenStream(market, security, interval)
    val opened = (client.apply(open)()).asInstanceOf[StreamOpened]

    val latch = new CountDownLatch(1)
    var tradesNbr = 0
    val handle = sub.read[MarketStreamPayload]
    handle.messages foreach {
      case ReadMessage(Payload(trade), ack) => tradesNbr += 1; ack()
      case ReadMessage(Broken(e), ack) => log.info("Broken: " + e); latch.countDown(); ack()
      case ReadMessage(_, ack) => ack()
    }

    val patient = new Patient(Heartbeat, Identifier(opened.stream.id))
    tradesStreamer.heartbeat.ping(UUID.randomUUID())

    assert(latch.await(5, TimeUnit.SECONDS))
    assert(tradesNbr == TradesExpected, "Expected: " + TradesExpected + "; actually got: " + tradesNbr)

    // Verify
    verify(marketDb, only()).scanTrades(market, security, interval)
    verify(scanner).close();

    // Close all
    tradesStreamer.shutdown()
    patient.close()
    client.release()
    handle.close()
    sub.close()
  }

}