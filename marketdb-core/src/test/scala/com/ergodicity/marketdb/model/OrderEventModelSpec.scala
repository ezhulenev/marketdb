package com.ergodicity.marketdb.model

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.event._
import org.joda.time.DateTime
import com.ergodicity.marketdb.ByteArray

class OrderEventModelSpec extends Spec {
  val log = LoggerFactory.getLogger(classOf[OrderEventModelSpec])

  val market = Market("Test")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = OrderPayload(market, security, 11111l, time, 100, 101, 1, BigDecimal("150000.50"), 1, 1, Some(100, BigDecimal("150000.55")))

  describe("Order event sourced model") {
    it("should create DraftOrder") {
      val draftOrder: DraftOrder = Order.loadFromHistory(Seq(OrderReceived(payload)))
      val enriched = draftOrder.enrichOrder(ByteArray("b"), ByteArray("d"))

      log.info("Enriched: " + enriched.reaction)

      enriched.reaction match {
        case Accepted(events, result) => log.info("Enriched order: "+result+"; Events: "+events)
      }
    }
  }

  describe("EnrichedOrder") {

    it("should fail to serialize with too long MarketId") {
      val enrichedOrder = EnrichedOrder(ByteArray("Too long"), ByteArray("0"), payload)
      val binary = enrichedOrder.serializeOrder().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    it("should fail to serialize with too short CodeId") {
      val enrichedOrder = EnrichedOrder(ByteArray("0"), ByteArray("0"), payload)
      val binary = enrichedOrder.serializeOrder().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    it("should be serialized to BinaryOrder") {
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