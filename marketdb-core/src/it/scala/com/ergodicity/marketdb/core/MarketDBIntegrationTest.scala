package com.ergodicity.marketdb.core

import org.scalatest.{GivenWhenThen, Spec}
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import com.ergodicity.marketdb.TimeRecording
import com.ergodicity.marketdb.model._
import java.io.File
import com.twitter.ostrich.admin.RuntimeEnvironment

class MarketDBIntegrationTest extends Spec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[MarketDBIntegrationTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, code, contract, BigDecimal("111"), 1, time, 11l, true)

  describe("MarketDB") {

    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File(this.getClass.getResource("/config/it.scala").toURI)
    val marketDB = runtime.loadRuntimeConfig[MarketDB]()

    it("should persist new trade") {

      // Execute
      val futureReaction = recordTime("Add trade", () => marketDB.addTrade(payload))
      val reaction = recordTime("Reaction", () => futureReaction.apply())

      log.info("Trade reaction: " + reaction)
    }
  }
}
