package com.ergodicity.marketdb.api

import com.ergodicity.marketdb.Connection
import com.ergodicity.marketdb.TimeSeries
import com.ergodicity.marketdb.TimeSeries.Qualifier
import com.ergodicity.marketdb.model._
import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}
import org.joda.time.{DateTime, Interval}
import sbinary.Operations._
import sbinary.{Output, Input, Format, DefaultProtocol}

// -- Request
sealed trait MarketDbReq

case object GetMarketDbConfig extends MarketDbReq

case class ScanTrades(market: Market, security: Security, interval: Interval) extends MarketDbReq

case class ScanOrders(market: Market, security: Security, interval: Interval) extends MarketDbReq

// -- Reply
sealed trait MarketDbRep

case class MarketDbConfig(connection: Connection) extends MarketDbRep

case class Trades(timeSeries: TimeSeries[TradePayload]) extends MarketDbRep

case class Orders(timeSeries: TimeSeries[OrderPayload]) extends MarketDbRep


object MarketDbProtocol extends DefaultProtocol {

  import com.ergodicity.marketdb.model.MarketProtocol._

  implicit object IntervalFormat extends Format[Interval] {
    def reads(in: Input) = new Interval(read[DateTime](in), read[DateTime](in))

    def writes(out: Output, interval: Interval) {
      write[DateTime](out, interval.getStart)
      write[DateTime](out, interval.getEnd)
    }
  }

  implicit object ConnectionFormat extends Format[Connection] {
    def reads(in: Input) = new Connection(read[String](in))

    def writes(out: Output, connection: Connection) {
      write[String](out, connection.zookeeperQuorum)
    }
  }

  implicit object QualifierFormat extends Format[Qualifier] {
    def reads(in: Input) = Qualifier(read[Array[Byte]](in), read[Array[Byte]](in), read[Array[Byte]](in))

    def writes(out: Output, value: Qualifier) {
      write[Array[Byte]](out, value.table)
      write[Array[Byte]](out, value.startKey)
      write[Array[Byte]](out, value.stopKey)
    }
  }

  implicit def TimeSeriesFormat[P <: MarketPayload] = new Format[TimeSeries[P]] {
    def reads(in: Input) = new TimeSeries[P](read[Market](in), read[Security](in), read[Interval](in), read[Qualifier](in))

    def writes(out: Output, value: TimeSeries[P]) {
      write[Market](out, value.market)
      write[Security](out, value.security)
      write[Interval](out, value.interval)
      write[Qualifier](out, value.qualifier)
    }
  }

  implicit object RequestFormat extends Format[MarketDbReq] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => GetMarketDbConfig
      case 1 => ScanTrades(read[Market](in), read[Security](in), read[Interval](in))
      case 2 => ScanOrders(read[Market](in), read[Security](in), read[Interval](in))
    }

    def writes(out: Output, value: MarketDbReq) {
      value match {
        case GetMarketDbConfig =>
          write[Byte](out, 0)

        case ScanTrades(market, security, interval) =>
          write[Byte](out, 1)
          write[Market](out, market)
          write[Security](out, security)
          write[Interval](out, interval)

        case ScanOrders(market, security, interval) =>
          write[Byte](out, 2)
          write[Market](out, market)
          write[Security](out, security)
          write[Interval](out, interval)

      }
    }
  }

  implicit object ResponseFormat extends Format[MarketDbRep] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => MarketDbConfig(read[Connection](in))
      case 1 => Trades(read[TimeSeries[TradePayload]](in))
      case 2 => Orders(read[TimeSeries[OrderPayload]](in))
    }

    def writes(out: Output, value: MarketDbRep) {
      value match {
        case MarketDbConfig(connection) =>
          write[Byte](out, 0)
          write[Connection](out, connection)

        case Trades(timeSeries) =>
          write[Byte](out, 1)
          write[TimeSeries[TradePayload]](out, timeSeries)

        case Orders(timeSeries) =>
          write[Byte](out, 2)
          write[TimeSeries[OrderPayload]](out, timeSeries)
      }
    }
  }
}


object MarketDbCodec extends MarketDbCodec

class MarketDbCodec extends CodecFactory[MarketDbReq, MarketDbRep] {

  import MarketDbProtocol._

  def server = Function.const {
    new Codec[MarketDbReq, MarketDbRep] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("reqDecoder", new MarketDbReqDecoder)
          pipeline.addLast("repEncoder", new MarketDbRepEncoder)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[MarketDbReq, MarketDbRep] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("reqEncode", new MarketDbReqEncoder)
          pipeline.addLast("repDecode", new MarketDbRepDecoder)
          pipeline
        }
      }
    }
  }

  class MarketDbReqDecoder extends OneToOneDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[ChannelBuffer])) {
        msg
      } else {
        fromByteArray[MarketDbReq](msg.asInstanceOf[ChannelBuffer].toByteBuffer.array)
      }
    }
  }

  class MarketDbRepDecoder extends OneToOneDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[ChannelBuffer])) {
        msg
      } else {
        fromByteArray[MarketDbRep](msg.asInstanceOf[ChannelBuffer].toByteBuffer.array)
      }
    }
  }

  class MarketDbReqEncoder extends OneToOneEncoder {
    def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[MarketDbReq])) {
        msg
      } else {
        ChannelBuffers.copiedBuffer(toByteArray(msg.asInstanceOf[MarketDbReq]))
      }
    }
  }

  class MarketDbRepEncoder extends OneToOneEncoder {
    def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[MarketDbRep])) {
        msg
      } else {
        ChannelBuffers.copiedBuffer(toByteArray(msg.asInstanceOf[MarketDbRep]))
      }
    }
  }

}