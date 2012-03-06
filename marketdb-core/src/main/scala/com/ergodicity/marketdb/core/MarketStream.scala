package com.ergodicity.marketdb.core

import com.twitter.concurrent.{Broker, Offer}
import com.ergodicity.marketdb.model.TradePayload
import com.twitter.finagle.kestrel.ReadClosedException
import java.util.ArrayList
import collection.JavaConversions
import org.hbase.async.{KeyValue, Scanner}
import com.twitter.util.{Future, Promise, Throw, Return}
import com.ergodicity.marketdb.AsyncHBase._
import sbinary.Operations._
import com.ergodicity.marketdb.model.TradeProtocol._
import JavaConversions._

trait MarketStream

case class ReadTrade(payload: TradePayload, ack: Offer[Unit])

trait TradesHandle {

  val trades: Offer[ReadTrade]

  val error: Offer[Throwable]

  def close()

  def buffered(howmany: Int): TradesHandle = {
    val out = new Broker[ReadTrade]
    val ack = new Broker[Unit]
    val closeReq = new Broker[Unit]

    def loop(nwait: Int, closed: Boolean) {
      // we're done if we're closed, and
      // we're not awaiting any acks.
      if (closed && nwait == 0) {
        close()
        return
      }

      Offer.select(
        if (nwait < howmany && !closed) {
          trades { t =>
            t.ack()
            out ! t.copy(ack = ack.send(()))
            loop(nwait + 1, closed)
          }
        } else {
          Offer.never
        },

        ack.recv {_ => loop(nwait - 1, closed)},

        closeReq.recv {_ => loop(nwait, true)}
      )
    }

    loop(0, false)

    val underlying = this
    new TradesHandle {
      val trades = out.recv
      // todo: should errors be sequenced
      // with respect to messages here, or
      // just (as is now) be propagated
      // immediately.
      val error = underlying.error

      def close() {
        closeReq !()
      }
    }
  }
}

object TradesHandle {
  // A convenience constructor using an offer for closing.
  def apply(
             _trades: Offer[ReadTrade],
             _error: Offer[Throwable],
             closeOf: Offer[Unit]
             ): TradesHandle = new TradesHandle {
    val trades = _trades
    val error = _error
    def close() = closeOf()
  }
}

object TradesStreamExhaustedException extends Exception

trait TradesStream extends MarketStream {
  def read(): TradesHandle
}

object TradesStream {
  def apply(scanner: Scanner) = new TradesStream {

    private def nextTradesFromUnderlyingScanner(): Future[Option[Iterator[TradePayload]]] = {
      val promise = new Promise[Option[Iterator[TradePayload]]]
      val defered = scanner.nextRows()

      defered.addCallback {(rows: ArrayList[ArrayList[KeyValue]]) =>
        if (rows != null) {
          val trades = asScalaIterator(rows.iterator()) flatMap {row =>
              asScalaIterator(row.iterator())
          } map {kv =>
              fromByteArray[TradePayload](kv.value())
          }
          promise.setValue(Some(trades))
        } else {
          promise.setValue(None)
        }
      }
      defered.addErrback {(e: Throwable) => promise.setException(e)}

      promise
    }
    
    def read() = {
      val error = new Broker[Throwable]
      val trades = new Broker[ReadTrade]
      val close = new Broker[Unit]

      def recv(data: Iterator[TradePayload]) {
        val reply = if (data.hasNext) Future(Some(data)) else nextTradesFromUnderlyingScanner()

        Offer.select(
          reply.toOffer {
            case Return(Some(iter)) =>
              val ack = new Broker[Unit]
              trades ! ReadTrade(iter.next(), ack.send())
              
              Offer.select(
                ack.recv { _ => recv(iter) },
                close.recv { t => scanner.close(); error ! ReadClosedException }
              )

            case Return(None) =>
              scanner.close()
              error ! TradesStreamExhaustedException
              
            case Return(_) =>
              scanner.close()
              error ! new IllegalArgumentException("Invalid reply from Scanner")

            case Throw(t) =>
              scanner.close()
              error ! t
          },

          close.recv { _ =>
            scanner.close()
            reply.cancel()
            error ! ReadClosedException
          }
        )
      }

      recv(Iterator.empty)

      TradesHandle(trades.recv, error.recv, close.send(()))
    }
  }
}