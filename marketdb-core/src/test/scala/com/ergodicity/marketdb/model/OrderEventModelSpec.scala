package com.ergodicity.marketdb.model

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.event._
import org.joda.time.DateTime
import com.ergodicity.marketdb.ByteArray

class OrderEventModelSpec extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[OrderEventModelSpec])

  val market = Market("Test")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = OrderPayload(market, security, 11111l, time, 100, 101, 1, BigDecimal("150000.50"), 1, 1, Some(100, BigDecimal("150000.55")))

  "Order event sourced model" must {
    "create DraftOrder" in {
      val draftOrder: DraftOrder = Order.loadFromHistory(Seq(OrderReceived(payload)))
      val enriched = draftOrder.enrichOrder(ByteArray("b"), ByteArray("d"))

      log.info("Enriched: " + enriched.reaction)

      enriched.reaction match {
        case Accepted(events, result) => log.info("Enriched order: "+result+"; Events: "+events)
      }
    }
  }

  "EnrichedOrder" must {

    "fail to serialize with too long MarketId" in {
      val enrichedOrder = EnrichedOrder(ByteArray("Too long"), ByteArray("0"), payload)
      val binary = enrichedOrder.serializeOrder().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    "fail to serialize with too short CodeId" in {
      val enrichedOrder = EnrichedOrder(ByteArray("0"), ByteArray("0"), payload)
      val binary = enrichedOrder.serializeOrder().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    "be serialized to BinaryOrder" in {
      val enrichedOrder = EnrichedOrder(ByteArray("0"), ByteArray("001"), payload)
      val binary = enrichedOrder.serializeOrder().reaction

      assert(binary match {
        case Accepted(events, result) =>
          log.info("Binary order: "+result)
          log.info("Row width: "+result.row.length)
          log.info("Qualifier width: "+result.qualifier.length)
          log.info("Payload width: "+result.payload.length)
          true
        case _ => false
      })
    }
  }
}