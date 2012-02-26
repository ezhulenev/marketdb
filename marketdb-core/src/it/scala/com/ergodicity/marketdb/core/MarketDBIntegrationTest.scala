package com.ergodicity.marketdb.core

import org.scalatest.{GivenWhenThen, Spec}
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.joda.time.DateTime
import com.ergodicity.marketdb.{TimeRecording, IntegrationTestConfiguration}
import com.ergodicity.marketdb.model._

class MarketDBIntegrationTest extends Spec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[MarketDBIntegrationTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, code, contract, BigDecimal("111"), 1, time, 11l, true)

  val config = new AnnotationConfigApplicationContext(classOf[IntegrationTestConfiguration])
    .getBean(classOf[IntegrationTestConfiguration])

  describe("MarketDB") {

    val marketDB = new MarketDB(config.hbaseClient(), config.marketUidProvider(), config.codeUidProvider(), config.tradesTableName)

    it("should persist new trade") {

      // Execute
      val futureReaction = recordTime("Add trade", () => marketDB.addTrade(payload))
      val reaction = recordTime("Reaction", () => futureReaction.apply())

      log.info("Trade reaction: " + reaction)
      assert(reaction match {
        case TradePersisted(pl) => true
        case _ => false
      })

    }
  }
}
