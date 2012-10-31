package integration.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.core.MarketDb
import com.ergodicity.marketdb.iteratee.{TradesTimeSeries, MarketIteratees}
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.model.TradeProtocol._
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.Future
import java.io.File
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.{WordSpec, GivenWhenThen}

class TradesIterateeSpec extends WordSpec with GivenWhenThen {
  val NoSystem = true

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime

  "MarketDb" must {

    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File("./config/it.scala")
    val marketDB = runtime.loadRuntimeConfig[MarketDb]()

    "should persist new trades iterate over them with MarketIteratee" in {
      val time1 = new DateTime(1970, 01, 05, 1, 0, 0, 0)
      val time2 = new DateTime(1970, 01, 05, 1, 0, 1, 0)

      val payload1 = TradePayload(market, security, 111l, BigDecimal("111"), 1, time1, NoSystem)
      val payload2 = TradePayload(market, security, 112l, BigDecimal("112"), 1, time2, NoSystem)

      val f1 = marketDB.addTrade(payload1)
      val f2 = marketDB.addTrade(payload2)

      // Wait for trades persisted
      Future.join(List(f1, f2))()

      // -- Verify two rows for 1970 Jan 5
      val interval = new DateTime(1970, 01, 05, 0, 0, 0, 0) to new DateTime(1970, 01, 05, 23, 0, 0, 0)

      import MarketIteratees._
      val tradeSeries = TradesTimeSeries(market, security, interval)(marketDB)
      val cnt = counter[TradePayload]

      val iter = tradeSeries.enumerate(cnt).map(_.run)
      val count = iter()

      assert(count == 2)
    }
  }
}
