package com.ergodicity.marketdb.event

import com.ergodicity.marketdb.model.OrderPayload
import com.ergodicity.marketdb.ByteArray

sealed trait OrderEvent {

}

case class OrderReceived(payload: OrderPayload) extends OrderEvent

case class OrderEnriched(marketId: ByteArray, securityId: ByteArray) extends OrderEvent

case class OrderSerialized(row: ByteArray, qualifier: ByteArray, payload: ByteArray) extends OrderEvent