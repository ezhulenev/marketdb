package integration.ergodicity.marketdb.core

import collection.JavaConversions
import com.ergodicity.marketdb.core.MarketDb
import com.ergodicity.marketdb.model._
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.Future
import integration.ergodicity.marketdb.TimeRecording
import java.io.File
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.{WordSpec, GivenWhenThen}
import org.slf4j.LoggerFactory
import scala.Predef._
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.iteratee.MarketDbReader
import org.mockito.Mockito

class TradesIntegrationSpec extends WordSpec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[OrdersIntegrationSpec])

  val NoSystem = true

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime

  "MarketDb" must {

    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File("./config/it.scala")
    val marketDB = runtime.loadRuntimeConfig[MarketDb]()

    implicit val reader = Mockito.mock(classOf[MarketDbReader])
    Mockito.when(reader.client).thenReturn(marketDB.client)

    "should persist new trade" in {
      val payload = TradePayload(market, security, 11l, BigDecimal("111"), 1, time, NoSystem)

      // Execute
      val futureReaction = recordTime("Add trade", () => marketDB.addTrade(payload))
      val reaction = recordTime("Reaction", () => futureReaction.apply())

      log.info("Trade reaction: " + reaction)
    }

    "should persist new trades and scan them later" in {
      val time1 = new DateTime(1970, 01, 01, 1, 0, 0, 0)
      val time2 = new DateTime(1970, 01, 01, 1, 0, 1, 0)

      val payload1 = TradePayload(market, security, 111l, BigDecimal("111"), 1, time1, NoSystem)
      val payload2 = TradePayload(market, security, 112l, BigDecimal("112"), 1, time2, NoSystem)

      val f1 = marketDB.addTrade(payload1)
      val f2 = marketDB.addTrade(payload2)

      // Wait for trades persisted
      Future.join(List(f1, f2))()

      // -- Verify two rows for 1970 Jan 1
      val interval = new DateTime(1970, 01, 01, 0, 0, 0, 0) to new DateTime(1970, 01, 01, 23, 0, 0, 0)
      val timeSeries = marketDB.trades(market, security, interval).apply()

      val scanner = {
        val scanner = marketDB.client.newScanner(timeSeries.qualifier.table)
        scanner.setStartKey(timeSeries.qualifier.startKey)
        scanner.setStopKey(timeSeries.qualifier.stopKey)
        scanner
      }

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Jan 1: " + rows)

      import TradeProtocol._
      import sbinary.Operations._
      val trades = for (list <- JavaConversions.asScalaIterator(rows.iterator());
                        kv <- JavaConversions.asScalaIterator(list.iterator())) yield fromByteArray[TradePayload](kv.value())

      trades foreach {
        trade => log.info("Trade: " + trade)
      }

      assert(rows.size() == 1)
      assert(rows.get(0).size() == 2)

      assert(scanner.nextRows().joinUninterruptibly() == null)
    }

    "should return null if no trades exists" in {
      // -- Verify two rows for 1970 Feb 1
      val interval = new DateTime(1970, 02, 01, 0, 0, 0, 0) to new DateTime(1970, 02, 01, 23, 0, 0, 0)
      val timeSeries = marketDB.trades(market, security, interval).apply()

      val scanner = {
        val scanner = marketDB.client.newScanner(timeSeries.qualifier.table)
        scanner.setStartKey(timeSeries.qualifier.startKey)
        scanner.setStopKey(timeSeries.qualifier.stopKey)
        scanner
      }

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Feb1 1: " + rows)

      assert(rows == null)
    }
  }
}
