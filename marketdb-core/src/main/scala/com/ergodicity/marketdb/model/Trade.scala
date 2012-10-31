package com.ergodicity.marketdb.model

import com.ergodicity.marketdb.ByteArray
import com.ergodicity.marketdb.event.{TradeSerialized, TradeEnriched, TradeReceived, TradeEvent}
import Behaviors._
import sbinary._
import Operations._
import com.ergodicity.marketdb.core.MarketDb
import org.joda.time.DateTime

object Trade extends AggregateFactory[Trade, TradeEvent] {

  def create(payload: TradePayload) = applyCreated(TradeReceived(payload))

  def applyEvent = applyCreated

  private def applyCreated = handler {
    event: TradeReceived => DraftTrade(event.payload)
  }
}

object TradeRow {
  def apply(marketId: ByteArray, securityId: ByteArray, time: DateTime): ByteArray = {
    val year = ByteArray(time.getYear)
    val day = ByteArray(time.getDayOfYear)
    val minute = ByteArray(time.getMinuteOfDay)

    marketId ++ securityId ++ year ++ day ++ minute;
  }
}

sealed trait Trade extends AggregateRoot[TradeEvent]

case class DraftTrade(payload: TradePayload) extends Trade {

  def enrichTrade(marketId: ByteArray, securityId: ByteArray): Behavior[EnrichedTrade] = {
    applyTradeEnriched(TradeEnriched(marketId, securityId))
  }

  def applyEvent = applyTradeEnriched

  private def applyTradeEnriched = handler {
    event: TradeEnriched => new EnrichedTrade(event.marketId, event.securityId, payload)
  }
}

case class EnrichedTrade(marketId: ByteArray, securityId: ByteArray, payload: TradePayload) extends Trade {

  def serializeTrade(): Behavior[BinaryTrade] = {
    import MarketDb._
    import TradeProtocol._

    guard(marketId.length == MarketIdWidth, "Market Id width '" + marketId.length + "' not equals to expected: " + MarketIdWidth) flatMap {
      _ =>
        guard(securityId.length == SecurityIdWidth, "Code width '" + securityId.length + "' not equals to expected: " + SecurityIdWidth) flatMap {
          _ =>
            val row = TradeRow(marketId, securityId, payload.time)
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

case class BinaryTrade(row: ByteArray, qualifier: ByteArray, payload: ByteArray) extends Trade {
  def applyEvent = unhandled
}