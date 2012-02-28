package com.ergodicity.marketdb.loader.util

import scalaz._
import IterV._
import scalaz.{Input, IterV}
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.loader.LoaderReport
import org.slf4j.LoggerFactory
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.kestrel.Client
import com.twitter.ostrich.stats.Stats
import com.twitter.concurrent.{Offer, ChannelSource}
import java.util.concurrent.atomic.AtomicReference

object Iteratees {
  def printer[E](log: org.slf4j.Logger): IterV[E, Boolean] = {
    def step(is: Boolean, e: E)(s: Input[E]): IterV[E, Boolean] = {
      log.info("STEP: " + e)
      s(el = e2 => Cont(step(is, e2)),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Boolean] = {
      s(el = e1 => Cont(step(true, e1)),
        empty = Cont(first),
        eof = Done(true, EOF[E]))
    }

    Cont(first)
  }

  def kestrelLoader[E](queue: String, client: Client, serializer: E => Array[Byte]): IterV[E, LoaderReport] = {
    def pushToChannel(e: E) {
      val bytes = serializer(e)
      val log = LoggerFactory.getLogger("PUSH_LOGGER")
      log.info("PUSH: " + e)
      Stats.incr("trades_processed", 1)
      client.write(queue, OfferOnce(ChannelBuffers.wrappedBuffer(bytes))) onSuccess {
        failure =>
          log.error("Failed to send trade data: " + failure)
          Stats.incr("trades_failed", 1)
      }
    }

    def step(is: LoaderReport, e: E)(s: Input[E]): IterV[E, LoaderReport] = {
      s(el = e2 => {pushToChannel(e2); if (is.count < 10000) Cont(step(LoaderReport(is.count + 1), e2)) else Done(is, EOF[E])},
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, LoaderReport] = {
      s(el = e1 => {pushToChannel(e1); Cont(step(LoaderReport(1), e1))},
        empty = Cont(first),
        eof = Done(LoaderReport(0), EOF[E]))
    }

    Cont(first)
  }



  def counter[E]: IterV[E, Int] = {
    def step(is: Int, e: E)(s: Input[E]): IterV[E, Int] = {
      s(el = e2 => Cont(step(is + 1, e2)),
        empty = Cont(step(is, e)),
        eof = Done(is, EOF[E]))
    }

    def first(s: Input[E]): IterV[E, Int] = {
      s(el = e1 => Cont(step(1, e1)),
        empty = Cont(first),
        eof = Done(0, EOF[E]))
    }

    Cont(first)
  }
}

object OfferOnce {
  def apply[A](value: A): Offer[A] = new Offer[A] {
    val ref = new AtomicReference[Option[A]](Some(value))

    def objects = Seq()

    def poll() = ref.getAndSet(None).map(() => _)

    def enqueue(setter: this.type#Setter) = () => Unit
  }
  
}