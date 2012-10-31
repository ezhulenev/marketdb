package integration.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.model._
import com.twitter.ostrich.admin.RuntimeEnvironment
import integration.ergodicity.marketdb.TimeRecording
import integration.ergodicity.marketdb.core.OrdersIntegrationSpec
import java.io.File
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.{WordSpec, GivenWhenThen}
import org.slf4j.LoggerFactory
import scala.Predef._
import com.ergodicity.marketdb.core.MarketDB
import com.ergodicity.marketdb.iteratee.TradesTimeSeries


class IterateePerformanceSpec extends WordSpec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[OrdersIntegrationSpec])

  val market = Market("RTS")
  val security = Security("RTS 3.12")

  import TradeProtocol._

  "MarketDB" must {
    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File("./config/dev.scala")
    val marketDB = runtime.loadRuntimeConfig[MarketDB]()

    "should persist new trades iterate over them with MarketIteratee" in {
      // Expect about 500000 records
      val interval = new DateTime(2012, 02, 03, 0, 0, 0, 0) to new DateTime(2012, 02, 03, 23, 0, 0, 0)

      import com.ergodicity.marketdb.iteratee.MarketIteratees._
      val tradeSeries = TradesTimeSeries(market, security, interval)(marketDB)
      val cnt = counter[TradePayload]

      val iter = tradeSeries.enumerate(cnt).map(_.run)
      val count = iter()

      log.info("Count = " + count)
    }
  }
}
