package com.ergodicity.marketdb.event

import com.ergodicity.marketdb.ByteArray
import com.ergodicity.marketdb.model.TradePayload

sealed trait TradeEvent {

}

case class TradeReceived(payload: TradePayload) extends TradeEvent

case class TradeEnriched(marketId: ByteArray, securityId: ByteArray) extends TradeEvent

case class TradeSerialized(row: ByteArray, qualifier: ByteArray, payload: ByteArray) extends TradeEvent