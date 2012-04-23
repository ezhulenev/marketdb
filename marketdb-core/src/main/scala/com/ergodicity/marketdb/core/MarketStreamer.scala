package com.ergodicity.marketdb.core

import org.slf4j.LoggerFactory
import org.zeromq.ZMQ.Context
import com.ergodicity.zeromq.SocketType._
import java.util.UUID
import com.ergodicity.marketdb.stream._
import org.joda.time.Interval
import concurrent.stm._
import com.twitter.conversions.time._
import com.ergodicity.zeromq._
import org.zeromq.{ZMQForwarder, ZMQ}
import MarketIteratee._
import com.ergodicity.marketdb.model.TradeProtocol._
import java.util.concurrent.atomic.AtomicBoolean
import com.twitter.concurrent.{Broker, Offer}
import com.twitter.finagle.Service
import com.twitter.util.{Future, FuturePool}
import java.net.InetSocketAddress
import com.twitter.finagle.builder.{ServerBuilder, Server}
import MarketStreamProtocol._
import com.ergodicity.marketdb.model.{Security, TradePayload, Market}

trait MarketStreamer extends MarketService

class ZMQTradesStreamer(marketDb: MarketDB, val finaglePort: Int, publishEndpoint: String, heartbeatRef: HeartbeatRef)
                    (implicit context: Context, pool: FuturePool) extends Service[MarketStreamReq, MarketStreamRep] with MarketStreamer {

  val log = LoggerFactory.getLogger(classOf[ZMQTradesStreamer])

  val InputEndpoint = "inproc://trades-stream"

  var server: Server = _

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

  // Finagle Service implementation
  def apply(request: MarketStreamReq) = request match {
    case OpenStream(market, code, interval) => Future(openStream(market, code, interval))
    case CloseStream(stream) => Future(closeStream(stream))
    case unknown =>
      log.error("Unknown MarketStreamReq: " + unknown);
      Future.exception[MarketStreamRep](new IllegalArgumentException("Unknown MarketStreamReq: " + unknown))

  }

  private def openStream(market: Market, security: Security, interval: Interval) = {
    log.info("Open trades stream; Market=" + market + ", security=" + security + ", interval=" + interval)

    val id = UUID.randomUUID().toString
    val streamIdentifier = MarketStream(id)

    val timeSeries = TradesTimeSeries(market, security, interval)(marketDb)

    def startStreaming() {
      log.info("Start trades streaming: "+id)
      val pub = Client(Pub, options = Connect(InputEndpoint) :: Nil)

      val interrupt = new AtomicBoolean(false)
      val i = zmqStreamer[TradePayload, MarketStreamPayload](pub, trade => Payload(trade), interrupt)

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
          pub.send[MarketStreamPayload](Completed(interrupt.get))
          atomic {implicit txt =>
            tradeTimeSeries.transform(_ - id)
          }
          pub.close()
      } onFailure {
        err =>
          log.error("Streaming failed for client: " + id + "; Error: " + err)
          pub.send[MarketStreamPayload](Broken(err.getMessage))
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

  private def closeStream(stream: MarketStream) = {
    log.info("Close trades stream: "+stream)
    tradeTimeSeries.single().get(stream.id) map {_()}
    StreamClosed()
  }

  def start() {
    log.info("Start ZMQTradesStreamer server")
    server = ServerBuilder()
      .codec(MarketStreamCodec)
      .bindTo(new InetSocketAddress(finaglePort))
      .name("ZMQMarketStreamer")
      .build(this)
  }

  def shutdown() {
    log.info("Shutdown ZMQTradesStreamer server")
    // Close all alive streams
    tradeTimeSeries.single() foreach {_._2()}
    // Shutdown heartbeat
    heartbeat.stop()
    // Shutdown forwarder
    forwarding.cancel()
    input.close()
    output.close();
    // Shutdown finagle server
    server.close()
  }
}