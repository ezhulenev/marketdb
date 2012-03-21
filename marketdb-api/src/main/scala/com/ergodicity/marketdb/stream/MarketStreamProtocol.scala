package com.ergodicity.marketdb.stream

import sbinary.Operations._
import org.joda.time.{DateTime, Interval}
import sbinary._
import com.ergodicity.marketdb.model.{TradePayload, Market, Code}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

case class MarketStream(id: String)

sealed trait MarketStreamReq
case class OpenStream(market: Market, code: Code, interval: Interval) extends MarketStreamReq
case class CloseStream(stream: MarketStream) extends MarketStreamReq

sealed trait MarketStreamRep
case class StreamOpened(stream: MarketStream) extends MarketStreamRep
case class StreamClosed() extends MarketStreamRep

sealed trait MarketStreamPayload

case class Payload(data: TradePayload) extends MarketStreamPayload
case class Broken(err: String) extends MarketStreamPayload
case class Completed(interrupted: Boolean) extends MarketStreamPayload


object MarketStreamProtocol extends DefaultProtocol {

  import com.ergodicity.marketdb.model.TradeProtocol._
  
  implicit object IntervalFormat extends Format[Interval] {
    def reads(in: Input) = new Interval(read[DateTime](in), read[DateTime](in))

    def writes(out: Output, interval: Interval) {
      write[DateTime](out, interval.getStart)
      write[DateTime](out, interval.getEnd)
    }
  }

  implicit object StreamIdentifierFormat extends Format[MarketStream] {
    def reads(in: Input) = MarketStream(read[String](in))

    def writes(out: Output, value: MarketStream) {
      write[String](out, value.id)
    }
  }

  implicit object MarketStreamReqFormat extends Format[MarketStreamReq] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => OpenStream(read[Market](in), read[Code](in), read[Interval](in))
      case 1 => CloseStream(read[MarketStream](in))
      case _ => throw new RuntimeException("Unsupported stream message")
    }

    def writes(out: Output, value: MarketStreamReq) = value match {
      case OpenStream(market, code, interval) =>
        write[Byte](out, 0)
        write[Market](out, market)
        write[Code](out, code)
        write[Interval](out, interval)
      case CloseStream(stream) =>
        write[Byte](out, 1)
        write[MarketStream](out, stream)
    }
  }

  implicit object MarketStreamRepFormat extends Format[MarketStreamRep] {
    def reads(in: Input) = read[Byte](in) match {
      case 0 => StreamOpened(read[MarketStream](in))
      case 1 => StreamClosed()
      case _ => throw new RuntimeException("Unsupported stream message")
    }

    def writes(out: Output, value: MarketStreamRep) = value match {
      case StreamOpened(stream) =>
        write[Byte](out, 0)
        write[MarketStream](out, stream)
      case StreamClosed() =>
        write[Byte](out, 1)
    }
  }

  implicit object MarketStreamPayloadFormat extends  Format[MarketStreamPayload] {

    def reads(in: Input) = read[Byte](in) match {
      case 0 => Payload(read[TradePayload](in))
      case 1 => Broken(read[String](in))
      case 2 => Completed(read[Boolean](in))
      case _ => throw new RuntimeException("Unsupported strem message")
    }

    def writes(out: Output, value: MarketStreamPayload) = value match {
      case Payload(data) =>
        write[Byte](out, 0)
        write[TradePayload](out, data)
      case Broken(err) =>
        write[Byte](out, 1)
        write[String](out, err)
      case Completed(interrupted) =>
        write[Byte](out, 2)
        write[Boolean](out, interrupted)
    }
  }
}

import com.twitter.finagle.{Codec, CodecFactory}
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}

object MarketStreamCodec extends MarketStreamCodec

class MarketStreamCodec extends CodecFactory[MarketStreamReq, MarketStreamRep] {
  import MarketStreamProtocol._

  def server = Function.const {
    new Codec[MarketStreamReq, MarketStreamRep] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("reqDecoder", new MarketStreamReqDecoder)
          pipeline.addLast("repEncoder", new MarketStreamRepEncoder)
          pipeline
        }
      }
    }
  }

  def client = Function.const {
    new Codec[MarketStreamReq, MarketStreamRep] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("reqEncode", new MarketStreamReqEncoder)
          pipeline.addLast("repDecode", new MarketStreamRepDecoder)
          pipeline
        }
      }
    }
  }

  class MarketStreamReqDecoder extends OneToOneDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[ChannelBuffer])) {
        msg
      } else {      
        fromByteArray[MarketStreamReq](msg.asInstanceOf[ChannelBuffer].toByteBuffer.array)
      }
    }
  }

  class MarketStreamRepDecoder extends OneToOneDecoder {
    def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[ChannelBuffer])) {
        msg
      } else {
        fromByteArray[MarketStreamRep](msg.asInstanceOf[ChannelBuffer].toByteBuffer.array)
      }
    }
  }
  
  class MarketStreamReqEncoder extends OneToOneEncoder {
    def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[MarketStreamReq])) {
        msg
      } else {
        ChannelBuffers.copiedBuffer(toByteArray(msg.asInstanceOf[MarketStreamReq]))
      }
    }
  }

  class MarketStreamRepEncoder extends OneToOneEncoder {
    def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
      if (!(msg.isInstanceOf[MarketStreamRep])) {
        msg
      } else {
        ChannelBuffers.copiedBuffer(toByteArray(msg.asInstanceOf[MarketStreamRep]))
      }
    }
  }

  
  
}