package com.ergodicity.marketdb.model

import com.ergodicity.marketdb.ByteArray
import com.ergodicity.marketdb.event.{TradeSerialized, TradeEnriched, TradeReceived, TradeEvent}
import Behaviors._
import sbinary._
import Operations._
import com.ergodicity.marketdb.core.MarketDB

object Trade extends AggregateFactory[Trade, TradeEvent] {

  def create(payload: TradePayload) = applyCreated(TradeReceived(payload))

  def applyEvent = applyCreated

  private def applyCreated = handler {
    event: TradeReceived => DraftTrade(event.payload)
  }
}

sealed trait Trade extends AggregateRoot[TradeEvent]

case class DraftTrade(payload: TradePayload) extends Trade {

  def enrichTrade(marketId: ByteArray, codeId: ByteArray): Behavior[EnrichedTrade] = {
    applyTradeEnriched(TradeEnriched(marketId, codeId))
  }

  def applyEvent = applyTradeEnriched

  private def applyTradeEnriched = handler {
    event: TradeEnriched => new EnrichedTrade(event.marketId, event.codeId, payload)
  }
}

case class EnrichedTrade(marketId: ByteArray, codeId: ByteArray, payload: TradePayload) extends Trade {

  def serializeTrade() : Behavior[BinaryTrade] = {
    import MarketDB._
    import TradeProtocol._

    guard(marketId.length == MarketIdWidth, "Market Id width '" + marketId.length + "' not equals to expected: " + MarketIdWidth) flatMap {
      _ =>
        guard(codeId.length == CodeIdWidth, "Code width '" + codeId.length + "' not equals to expected: " + CodeIdWidth) flatMap {
          _ =>

            val year = ByteArray(payload.time.getYear)
            val day = ByteArray(payload.time.getDayOfYear)
            val minute = ByteArray(payload.time.getMinuteOfDay)
            val row = marketId ++ codeId ++ year ++ day ++ minute;

            val qualifier = ByteArray(payload.tradeId)

            applyTradeSerialized(TradeSerialized(row, qualifier, ByteArray(toByteArray(payload))))
        }
    }
  }

  def applyEvent = applyTradeSerialized

  private def applyTradeSerialized = handler {
    event: TradeSerialized => new BinaryTrade(event.row, event.qualifier, event.payload)
  }
}

case class BinaryTrade(row: ByteArray,  qualifier: ByteArray,  payload: ByteArray) extends Trade {
  def applyEvent = unhandled
}