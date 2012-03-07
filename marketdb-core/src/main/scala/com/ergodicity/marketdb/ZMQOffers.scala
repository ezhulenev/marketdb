package com.ergodicity.marketdb

import org.zeromq.ZMQ.{Socket, Context}
import com.twitter.concurrent.Offer
import org.zeromq.ZMQ


object ZMQOffers {

  def fromSocket(socket: Socket)(implicit context: Context) = new Offer[ByteArray] {
    val poller = context.poller()
    poller.register(socket, ZMQ.Poller.POLLIN)
    
    def poll() = {
      if (poller.pollin(0)) {
        () => Some(ByteArray(socket.recv(0)))
      } else None
    }

    def enqueue(setter: this.type#Setter) = null

    def objects = Seq()
  }

}