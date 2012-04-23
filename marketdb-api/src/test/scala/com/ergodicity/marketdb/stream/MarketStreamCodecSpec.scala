package com.ergodicity.marketdb.stream

import org.scalatest.Spec
import java.net.InetSocketAddress
import com.twitter.finagle.Service
import com.twitter.finagle.builder.{Server, ServerBuilder, ClientBuilder}
import com.twitter.util.Future
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import java.util.concurrent.{TimeUnit, CountDownLatch}
import com.ergodicity.marketdb.model.{Security, Market}

class MarketStreamCodecSpec extends Spec {

  describe("MarketStream Codec") {
    it("should work with finagle") {

      val Count = 100
      val Port = 3333

      val service = new Service[MarketStreamReq, MarketStreamRep] {
        def apply(request: MarketStreamReq) = request match {
          case OpenStream(market, code, interval) => Future(StreamOpened(MarketStream("111")))
          case CloseStream(MarketStream(id)) => Future(StreamClosed())
        }
      }

      val openLatch = new CountDownLatch(Count)
      val closeLatch = new CountDownLatch(Count)

      // Bind the service to port 8080
      val server: Server = ServerBuilder()
        .codec(MarketStreamCodec)
        .bindTo(new InetSocketAddress(Port))
        .name("TestMarketStreamServer")
        .build(service)

      val client: Service[MarketStreamReq, MarketStreamRep] = ClientBuilder()
        .codec(MarketStreamCodec)
        .hosts(new InetSocketAddress(Port))
        .hostConnectionLimit(1)
        .build()

      // Open multiple streams
      for (i <- 1 to Count) {
        client(OpenStream(Market("RTS"), Security("Code"), new DateTime to new DateTime)) onSuccess {
          case StreamOpened(MarketStream("111")) => openLatch.countDown()
          case _ =>
        } onFailure { error => error.printStackTrace() }
      }

      // Close multiple streams
      for (i <- 1 to Count) {
        client(CloseStream(MarketStream("111"))) onSuccess {
          case StreamClosed() => closeLatch.countDown()
          case _ =>
        } onFailure { error => error.printStackTrace() }
      }

      assert(openLatch.await(5, TimeUnit.SECONDS))
      assert(closeLatch.await(5, TimeUnit.SECONDS))

      client.release()
      server.close()
    }
  }
}