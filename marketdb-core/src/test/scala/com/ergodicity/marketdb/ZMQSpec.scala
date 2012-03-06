package com.ergodicity.marketdb

import org.scalatest.WordSpec
import org.scalatest.matchers.MustMatchers
import org.zeromq.ZMQ._
import com.sun.jna.Native
import org.zeromq.{ZeroMQLibrary, ZMQ}
import org.slf4j.LoggerFactory

class ZMQSpec extends WordSpec with MustMatchers {
  val log = LoggerFactory.getLogger(classOf[ZMQSpec])
  
  "ZMQ" must {
    "support Socket#getType" in {
      log.info("HERRS: " + Native.getLibraryOptions(classOf[ZeroMQLibrary]))
      Thread.sleep(1000000);
      /*val context = ZMQ.context(1)
      val sub = context.socket(ZMQ.SUB)
      sub.getType must equal(ZMQ.SUB)
      sub.close*/
    }
    "support pub-sub connection pattern" in {
      val context = ZMQ.context(1)
      val (pub, sub, poller) = (
        context.socket(ZMQ.PUB),
        context.socket(ZMQ.SUB),
        context.poller
        )
      pub.bind("inproc://zmq-spec")
      sub.connect("inproc://zmq-spec")
      sub.subscribe(Array.empty)
      poller.register(sub)
      pub.send(outgoingMessage.getBytes, 0)
      poller.poll must equal(1)
      poller.pollin(0) must equal(true)
      val incomingMessage = sub.recv(0)
      incomingMessage must equal(outgoingMessage.getBytes)
      sub.close
      pub.close
    }
    "support polling of multiple sockets" in {
      val context = ZMQ.context(1)
      val (pub, poller) = (context.socket(ZMQ.PUB), context.poller)
      pub.bind("inproc://zmq-spec")
      val (sub_x, sub_y) = (connectSubscriber(context), connectSubscriber(context))
      poller.register(sub_x)
      poller.register(sub_y)
      pub.send(outgoingMessage.getBytes, 0)
      poller.poll must equal(2)
      poller.pollin(0) must equal(true)
      poller.pollin(1) must equal(true)
      sub_x.close
      sub_y.close
      pub.close
    }
    "support sending of zero-length messages" in {
      val context = ZMQ.context(1)
      val pub = context.socket(ZMQ.PUB)
      pub.send("".getBytes, 0)
      pub.close
    }
  }
  def connectSubscriber(context: Context) = {
    val socket = context.socket(ZMQ.SUB)
    socket.connect("inproc://zmq-spec")
    socket.subscribe(Array.empty)
    socket
  }
  lazy val outgoingMessage = "hello"
}
