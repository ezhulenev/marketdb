package com.ergodicity.marketdb.model

import com.ergodicity.marketdb.ByteArray
import com.ergodicity.marketdb.event.{OrderSerialized, OrderEnriched, OrderReceived, OrderEvent}
import Behaviors._
import sbinary._
import Operations._
import com.ergodicity.marketdb.core.MarketDB
import org.joda.time.DateTime

object Order extends AggregateFactory[Order, OrderEvent] {

  def create(payload: OrderPayload) = applyCreated(OrderReceived(payload))

  def applyEvent = applyCreated

  private def applyCreated = handler {
    event: OrderReceived => DraftOrder(event.payload)
  }
}

object OrderRow {
  def apply(marketId: ByteArray, securityId: ByteArray, time: DateTime): ByteArray = {
    val year = ByteArray(time.getYear)
    val day = ByteArray(time.getDayOfYear)
    val minute = ByteArray(time.getMinuteOfDay)

    marketId ++ securityId ++ year ++ day ++ minute;
  }
}

sealed trait Order extends AggregateRoot[OrderEvent]

case class DraftOrder(payload: OrderPayload) extends Order {

  def enrichOrder(marketId: ByteArray, securityId: ByteArray): Behavior[EnrichedOrder] = {
    applyOrderEnriched(OrderEnriched(marketId, securityId))
  }

  def applyEvent = applyOrderEnriched

  private def applyOrderEnriched = handler {
    event: OrderEnriched => new EnrichedOrder(event.marketId, event.securityId, payload)
  }
}

case class EnrichedOrder(marketId: ByteArray, securityId: ByteArray, payload: OrderPayload) extends Order {

  def serializeOrder(): Behavior[BinaryOrder] = {
    import MarketDB._
    import OrderProtocol._

    guard(marketId.length == MarketIdWidth, "Market Id width '" + marketId.length + "' not equals to expected: " + MarketIdWidth) flatMap {
      _ =>
        guard(securityId.length == SecurityIdWidth, "Code width '" + securityId.length + "' not equals to expected: " + SecurityIdWidth) flatMap {
          _ =>
            val row = OrderRow(marketId, securityId, payload.time)
            val qualifier = ByteArray(payload.orderId)

            applyOrderSerialized(OrderSerialized(row, qualifier, ByteArray(toByteArray(payload))))
        }
    }
  }

  def applyEvent = applyOrderSerialized

  private def applyOrderSerialized = handler {
    event: OrderSerialized => new BinaryOrder(event.row, event.qualifier, event.payload)
  }
}

case class BinaryOrder(row: ByteArray, qualifier: ByteArray, payload: ByteArray) extends Order {
  def applyEvent = unhandled
}