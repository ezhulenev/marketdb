package com.ergodicity.marketdb.model

import org.scalatest.WordSpec
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.event._
import org.joda.time.DateTime
import com.ergodicity.marketdb.ByteArray

class TradeEventModelSpec extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[TradeEventModelSpec])

  val market = Market("Test")
  val security = Security("RTS 3.12")
  val payload = TradePayload(market, security, 11111l, BigDecimal("150000.50"), 1, new DateTime(), false)

  "Trade event sourced model" must {
    "create DraftTrade" in {
      val draftTrade: DraftTrade = Trade.loadFromHistory(Seq(TradeReceived(payload)))
      val enriched = draftTrade.enrichTrade(ByteArray("b"), ByteArray("d"))

      log.info("Enriched: " + enriched.reaction)

      enriched.reaction match {
        case Accepted(events, result) => log.info("Enriched trade: "+result+"; Events: "+events)
      }
    }
  }

  "EnrichedTrade" must {

    "fail to serialize with too long MarketId" in {
      val enrichedTrade = EnrichedTrade(ByteArray("Too long"), ByteArray("0"), payload)
      val binary = enrichedTrade.serializeTrade().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    "fail to serialize with too short CodeId" in {
      val enrichedTrade = EnrichedTrade(ByteArray("0"), ByteArray("0"), payload)
      val binary = enrichedTrade.serializeTrade().reaction

      assert(binary match {
        case Rejected(mess) => log.info("Serialization rejected: "+mess); true
        case _ => false
      })
    }

    "be serialized to BinaryTrade" in {
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