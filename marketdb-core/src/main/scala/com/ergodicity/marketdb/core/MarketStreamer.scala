package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import org.zeromq.ZMQ.Context
import com.twitter.util.FuturePool
import com.ergodicity.zeromq.SocketType._
import org.zeromq.{ZMQForwarder, ZMQ}
import java.util.UUID
import com.ergodicity.marketdb.stream._
import org.joda.time.Interval
import com.ergodicity.marketdb.model.{Market, Code}
import concurrent.stm._
import com.twitter.conversions.time._
import com.ergodicity.zeromq._
import com.twitter.concurrent.{Broker, Offer}

trait MarketStreamer extends MarketService

class TradesStreamer(marketDb: MarketDB, controlEndpoint: String, publishEndpoint: String, heartbeatRef: HeartbeatRef)
                    (implicit context: Context, pool: FuturePool) extends MarketStreamer {
  val log = LoggerFactory.getLogger(classOf[TradesStreamer])

  private var connectedTradeStreamer: ConnectedTradesStreamer = _

  def start() {
    log.info("Start TradesStreamer")
    connectedTradeStreamer = new ConnectedTradesStreamer(marketDb, controlEndpoint, publishEndpoint, heartbeatRef)
  }

  def shutdown() {
    log.info("Shutdown TradesStreamer")
    connectedTradeStreamer.shutdown()
  }
}

private[this] class ConnectedTradesStreamer(marketDb: MarketDB, controlEndpoint: String, publishEndpoint: String, heartbeatRef: HeartbeatRef)
                                           (implicit context: Context, pool: FuturePool) {
  val log = LoggerFactory.getLogger(classOf[ConnectedTradesStreamer])
  
  val FrontendEndpoint = "inproc://trades-stream"

  import MarketStreamProtocol._
  
  private val tradeScanners = Ref(Map[String, (TradesScanner, Option[Offer[Unit]])]())

  // Start beating
  val heartbeat = new Heartbeat(heartbeatRef, duration = 3.second, lossLimit = 3)
  heartbeat.start
  
  // Configure ZMQ.Forwarder
  val frontent = context.socket(ZMQ.SUB)
  frontent.bind(FrontendEndpoint)
  val backend = context.socket(ZMQ.PUB)
  backend.bind(publishEndpoint)
  val forwarder = new ZMQForwarder(context, frontent, backend)

  // Control socket
  val control = Client(Rep, options = Bind(controlEndpoint) :: Nil)
  val controlHandle = control.read[StreamControlMessage]
  controlHandle.messages foreach {
    case OpenStream(market, code, interval) => control.send[StreamControlMessage](openStream(market, code, interval))
    case CloseStream(stream) => control.send[StreamControlMessage](closeStream(stream))
    case msg => log.error("Unknown StreamControllMessage: " + msg)
  }

  private def openStream(market: Market, code: Code, interval: Interval) = {
    log.info("Open stream; Market="+market+", code="+code+", interval="+interval)
    val id = UUID.randomUUID().toString
    val streamIdentifier = StreamIdentifier(id)
    val scanner = marketDb.scan(market, code, interval)()
    val tradesScanner = TradesScanner(scanner)
    
    atomic {implicit txn =>
      tradeScanners.transform(_ + (id -> (tradesScanner, None)))
    }

    heartbeat.track(Identifier(id)) foreach {
      case Connected =>
        log.info("Connected client for stream id=" + id)
        startStreaming(streamIdentifier, tradesScanner)

      case Lost =>
        log.info("Lost client for stream id=" + id);
        closeStream(streamIdentifier)
    }

    StreamOpened(streamIdentifier)
  }

  private def startStreaming(stream: StreamIdentifier, scanner: TradesScanner) {
    log.info("Start streaming: "+stream)
    val client = Client(Pub, options = Connect(FrontendEndpoint) :: Nil)
    val handle = scanner.open()

    val stop = new Broker[Unit]
    val stopOffer = stop.send(())
    stop.recv {_ =>
      log.info("Stop streaming: "+stream)
      handle.close()
      client.close()
    }

    atomic {implicit txt =>
        tradeScanners.transform(_ + (stream.id ->(scanner, Some(stopOffer))))
    }

    handle.trades foreach {r =>
      try {
        client.send[StreamPayloadMessage](Trades(r.payload))
      } finally {
        r.ack
      }
    }

    handle.error foreach {
      case TradesScanCompleted =>
        client.send[StreamPayloadMessage](Completed())
        closeStream(stream)
      case err =>
        client.send[StreamPayloadMessage](Broken(err.getMessage))
        closeStream(stream)
    }
  }
  
  private def closeStream(stream: StreamIdentifier) = {
    log.info("Close stream: "+stream)
    val id = stream.id
    tradeScanners.single() get(id) foreach {_._2 foreach {_()}}    
    atomic {implicit txt =>
      tradeScanners.transform(_ - id)
    }
    StreamClosed()
  }
  
  def shutdown() {
    log.info("Shutdown")
    tradeScanners.single().values.foreach {_._2 map {_()}}
    heartbeat.stop()
    frontent.close()
    backend.close();
    controlHandle.close()
    control.close()
  }
}

