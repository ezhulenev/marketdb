package integration.ergodicity.marketdb.core

import org.scalatest.{GivenWhenThen, Spec}
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import com.ergodicity.marketdb.model._
import java.io.File
import com.twitter.ostrich.admin.RuntimeEnvironment
import org.scala_tools.time.Implicits._
import scala.Predef._
import integration.ergodicity.marketdb.TimeRecording
import com.ergodicity.marketdb.core.{TradesTimeSeries, MarketIteratee, MarketDB}


class MarketDbTradesPerformanceTest extends Spec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[MarketDbOrdersIntegrationTest])

  val market = Market("RTS")
  val security = Security("RTS 3.12")

  describe("MarketDB") {
    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File(this.getClass.getResource("/config/development.scala").toURI)
    val marketDB = runtime.loadRuntimeConfig[MarketDB]()

    it("should persist new trades iterate over them with MarketIteratee") {
      // Expect about 500000 records
      val interval = new DateTime(2012, 02, 03, 0, 0, 0, 0) to new DateTime(2012, 02, 03, 23, 0, 0, 0)

      import MarketIteratee._
      import TradeProtocol._
      val tradeSeries = TradesTimeSeries(market, security, interval)(marketDB)
      val cnt = counter[TradePayload]

      val iter = tradeSeries.enumerate(cnt).map(_.run)
      val count = iter()

      log.info("Count = " + count)
    }
  }
}
