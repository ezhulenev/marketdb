package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import org.zeromq.ZMQ.Context
import com.twitter.util.FuturePool
import com.ergodicity.zeromq.SocketType._
import java.util.UUID
import com.ergodicity.marketdb.stream._
import org.joda.time.Interval
import concurrent.stm._
import com.twitter.conversions.time._
import com.ergodicity.zeromq._
import org.zeromq.{ZMQForwarder, ZMQ}
import com.ergodicity.marketdb.model.{TradePayload, Market, Code}
import MarketIteratee._
import com.ergodicity.marketdb.model.TradeProtocol._
import java.util.concurrent.atomic.AtomicBoolean
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

  val InputEndpoint = "inproc://trades-stream"

  import MarketStreamProtocol._

  private val tradeTimeSeries = Ref(Map[String, Offer[Unit]]())

  // Start beating
  val heartbeat = new Heartbeat(heartbeatRef, duration = 3.second, lossLimit = 3)
  heartbeat.start

  // Configure ZMQ.Forwarder
  val input = context.socket(ZMQ.SUB)
  input.bind(InputEndpoint)
  input.subscribe(Array[Byte]())
  val output = context.socket(ZMQ.PUB)
  output.bind(publishEndpoint)

  val forwarder = new ZMQForwarder(context, input, output)
  val forwarding = pool(forwarder.run()) onSuccess {_=>
    log.error("Forwarder unexpectedly finished")
    shutdown()    
  } onFailure {err =>
    log.error("Forwarder failed: "+err)
    shutdown()
  }

  // Control socket
  val control = Client(Rep, options = Bind(controlEndpoint) :: Nil)
  val controlHandle = control.read[StreamControlMessage]
  controlHandle.messages foreach {
    msg =>
      msg.payload match {
        case OpenStream(market, code, interval) => control.send[StreamControlMessage](openStream(market, code, interval))
        case CloseStream(stream) => control.send[StreamControlMessage](closeStream(stream))
        case unknown => log.error("Unknown StreamControllMessage: " + unknown)
      }
      msg.ack()
  }

  private def openStream(market: Market, code: Code, interval: Interval) = {
    log.info("Open trades stream; Market=" + market + ", code=" + code + ", interval=" + interval)

    val id = UUID.randomUUID().toString
    val streamIdentifier = StreamIdentifier(id)
    
    val timeSeries = TradesTimeSeries(market, code, interval)(marketDb)
    
    def startStreaming() {
      log.info("Start trades streaming: "+id)
      val pub = Client(Pub, options = Connect(InputEndpoint) :: Nil)

      val interrupt = new AtomicBoolean(false)
      val i = zmqStreamer[TradePayload, StreamPayloadMessage](pub, trade => Payload(trade), interrupt)

      val stop = new Broker[Unit]
      Offer.select(
        stop.recv {_ =>
          log.info("Stop streaming: "+id)
          interrupt.set(true)
        }
      )
      val stopOffer = stop.send(())

      atomic {implicit txt =>
        tradeTimeSeries.transform(_ + (id ->stopOffer))
      }

      timeSeries.enumerate(i) onSuccess {
        cnt =>
          log.info("Sent " + cnt + " trades; Interrupted = "+interrupt.get)
          pub.send[StreamPayloadMessage](Completed(interrupt.get))
          atomic {implicit txt =>
            tradeTimeSeries.transform(_ - id)
          }
          pub.close()
      } onFailure {
        err =>
          log.error("Streaming failed for client: " + id + "; Error: " + err)
          pub.send[StreamPayloadMessage](Broken(err.getMessage))
          atomic {implicit txt =>
            tradeTimeSeries.transform(_ - id)
          }
          pub.close()
      }
    }

    heartbeat.track(Identifier(id)) foreach {
      case Connected =>
        log.info("Connected client for trades stream id=" + id)
        startStreaming()

      case Lost =>
        log.info("Lost client for stream id=" + id);
        closeStream(streamIdentifier)
    }

    StreamOpened(streamIdentifier)
  }

  private def closeStream(stream: StreamIdentifier) = {
    log.info("Close trades stream: "+stream)
    tradeTimeSeries.single().get(stream.id) map {_()}
    StreamClosed()
  }

  def shutdown() {
    log.info("Shutdown TradesStreamer")
    // Close all alive streams
    tradeTimeSeries.single() foreach {_._2()}
    // Shutdown heartbeat
    heartbeat.stop()
    // Shutdown forwarder
    forwarding.cancel()
    input.close()
    output.close();
    // Shutdown control
    controlHandle.close()
    control.close()
  }
}

