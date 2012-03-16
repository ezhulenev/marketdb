package com.ergodicity.marketdb.core

import org.hbase.async.Scanner
import org.powermock.core.classloader.annotations.{PowerMockIgnore, PrepareForTest}
import org.powermock.modules.junit4.PowerMockRunner
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.junit.Test
import com.ergodicity.marketdb.model.{TradePayload, Contract, Code, Market}
import com.ergodicity.marketdb.{ScannerMock, ByteArray}
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.joda.time.{Interval, DateTime}
import org.zeromq.ZMQ
import com.twitter.util.{FuturePool, Future}
import java.util.concurrent.Executors
import com.ergodicity.zeromq.{Connect, Client, HeartbeatRef}
import com.ergodicity.zeromq.SocketType._
import org.scala_tools.time.Implicits._
import com.ergodicity.marketdb.stream.MarketStreamProtocol._
import com.ergodicity.marketdb.stream._

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[Scanner]))
class TradesStreamerTest {
  val log = LoggerFactory.getLogger(classOf[TradesStreamerTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val interval = time.withHourOfDay(0) to time.withHourOfDay(23)

  implicit val marketId = (_: Market) => ByteArray(0)
  implicit val codeId = (_: Code) => ByteArray(1)

  val ControlEndpoint = "inproc://control-endpoint"
  val PublishEndpoint = "inproc://publish-endpoint"
  val Heartbeat = HeartbeatRef("inproc://ping", "inproc://pong")

  implicit val context = ZMQ.context(1)
  implicit val Pool = FuturePool(Executors.newCachedThreadPool())

  @Test
  def testOpenAndCloseStream() {
    val scanner = mock(classOf[Scanner])
    val marketDb = mock(classOf[MarketDB])
    when(marketDb.scan(any[Market], any[Code], any[Interval])).thenReturn(Future(scanner))

    val tradesStreamer = new ConnectedTradesStreamer(marketDb, ControlEndpoint, PublishEndpoint, Heartbeat)

    val client = Client(Req, options = Connect(ControlEndpoint) :: Nil)

    // Open Stream
    val open: StreamControlMessage = OpenStream(market, code, interval)
    val openReply = client ? open

    log.info("Open reply: " + openReply)
    assert(openReply match {
      case StreamOpened(s) => true
      case _ => false
    })

    // Close stream
    val close: StreamControlMessage = CloseStream(openReply.asInstanceOf[StreamOpened].stream)
    val closeReply = client ? close
    log.info("Close reply: " + closeReply)
    assert(closeReply match {
      case StreamClosed() => true
      case _ => false
    })

    // Verify
    verify(marketDb, only()).scan(market, code, interval)

    // Close all
    tradesStreamer.shutdown()
    client.close()
  }
  
  @Test
  def testPublishStream() {
    val payloads = for (i <- 1 to 100) yield TradePayload(market, code, contract, BigDecimal("111"), 1, time, i, true);

    val scanner = ScannerMock(payloads)
    val marketDb = mock(classOf[MarketDB])
    when(marketDb.scan(any[Market], any[Code], any[Interval])).thenReturn(Future(scanner))
    
  }

}