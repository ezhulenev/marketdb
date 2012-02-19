package com.ergodicity.marketdb.model

import org.scalatest.Spec
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.event._
import org.joda.time.DateTime
import com.ergodicity.marketdb.ByteArray

class TradeEventModelTest extends Spec {
  val log = LoggerFactory.getLogger(classOf[TradeEventModelTest])

  val market = Market("Test")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val payload = TradePayload(market, code, contract, BigDecimal("150000.50"), 1, new DateTime(), 11111l, false)

  describe("Trade event sourced model") {
    it("should create DraftTrade") {
      val draftTrade: DraftTrade = Trade.loadFromHistory(Seq(TradeReceived(payload)))
      val enriched = draftTrade.enrichTrade(ByteArray("b"), ByteArray("d"))

      log.info("Enriched: " + enriched.reaction)

      enriched.reaction match {
        case Accepted(events, result) => log.info("Enriched trade: "+result+"; Events: "+events)
      }
    }
  }

  describe("EnrichedTrade") {

    it("should fail to serialize with too long MarketId") {
      val enrichedTrade = EnrichedTrade(ByteArray("Too long"), ByteArray("0"), payload)
      val binary = enrichedTrade.serializeTrade().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    it("should fail to serialize with too short CodeId") {
      val enrichedTrade = EnrichedTrade(ByteArray("0"), ByteArray("0"), payload)
      val binary = enrichedTrade.serializeTrade().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    it("should be serialized to BinaryTrade") {
      val enrichedTrade = EnrichedTrade(ByteArray("0"), ByteArray("001"), payload)
      val binary = enrichedTrade.serializeTrade().reaction

      assert(binary match {
        case Accepted(events, result) =>
          log.info("Binary trade: "+result)
          log.info("Row width: "+result.row.length)
          log.info("Qualifier width: "+result.qualifier.length)
          log.info("Payload width: "+result.payload.length)
          true
        case _ => false
      })
    }


  }
}