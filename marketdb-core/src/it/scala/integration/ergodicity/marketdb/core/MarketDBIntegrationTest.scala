package integration.ergodicity.marketdb.core

import org.scalatest.{GivenWhenThen, Spec}
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import integration.ergodicity.marketdb.TimeRecording
import com.ergodicity.marketdb.model._
import java.io.File
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.Future
import org.scala_tools.time.Implicits._
import collection.JavaConversions
import scala.Predef._
import com.ergodicity.marketdb.core.{TradesTimeSeries, MarketIteratee, MarketDB}

class MarketDBIntegrationTest extends Spec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[MarketDBIntegrationTest])

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, security, 11l, BigDecimal("111"), 1, time, true)

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

    it("should persist new trades and scan them later") {
      val time1 = new DateTime(1970, 01, 01, 1, 0, 0, 0)
      val time2 = new DateTime(1970, 01, 01, 1, 0, 1, 0)

      val payload1 = TradePayload(market, security, 111l, BigDecimal("111"), 1, time1, true)
      val payload2 = TradePayload(market, security, 112l, BigDecimal("112"), 1, time2, true)

      val f1 = marketDB.addTrade(payload1)
      val f2 = marketDB.addTrade(payload2)

      // Wait for trades persisted
      Future.join(List(f1, f2))()

      // -- Verify two rows for 1970 Jan 1
      val interval = new DateTime(1970, 01, 01, 0, 0, 0, 0) to new DateTime(1970, 01, 01, 23, 0, 0, 0)
      val scanner = marketDB.scan(market, security, interval)()

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Jan 1: " + rows)

      import sbinary.Operations._
      import TradeProtocol._
      val trades = for (list <- JavaConversions.asScalaIterator(rows.iterator());
                        kv <- JavaConversions.asScalaIterator(list.iterator())) yield fromByteArray[TradePayload](kv.value());

      trades foreach {
        trade => log.info("Trade: " + trade)
      }

      assert(rows.size() == 1)
      assert(rows.get(0).size() == 2)

      assert(scanner.nextRows().joinUninterruptibly() == null)
    }

    it("should persist new trades iterate over them with MarketIteratee") {
      val time1 = new DateTime(1970, 01, 05, 1, 0, 0, 0)
      val time2 = new DateTime(1970, 01, 05, 1, 0, 1, 0)

      val payload1 = TradePayload(market, security, 111l, BigDecimal("111"), 1, time1, true)
      val payload2 = TradePayload(market, security, 112l, BigDecimal("112"), 1, time2, true)

      val f1 = marketDB.addTrade(payload1)
      val f2 = marketDB.addTrade(payload2)

      // Wait for trades persisted
      Future.join(List(f1, f2))()

      // -- Verify two rows for 1970 Jan 5
      val interval = new DateTime(1970, 01, 05, 0, 0, 0, 0) to new DateTime(1970, 01, 05, 23, 0, 0, 0)

      import MarketIteratee._
      import TradeProtocol._
      val tradeSeries = TradesTimeSeries(market, security, interval)(marketDB)
      val cnt = counter[TradePayload]

      val iter = tradeSeries.enumerate(cnt).map(_.run)
      val count = iter()

      assert(count == 2)
    }

    it("should return null if no trades exists") {
      // -- Verify two rows for 1970 Feb 1
      val interval = new DateTime(1970, 02, 01, 0, 0, 0, 0) to new DateTime(1970, 02, 01, 23, 0, 0, 0)
      val scanner = marketDB.scan(market, security, interval)()

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Feb1 1: " + rows)

      assert(rows == null)
    }
  }
}
