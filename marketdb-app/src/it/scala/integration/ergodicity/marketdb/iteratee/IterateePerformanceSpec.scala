package integration.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.core.MarketDb
import com.ergodicity.marketdb.iteratee.TimeSeriesEnumerator
import com.ergodicity.marketdb.model.{TradeProtocol, Market, Security, TradePayload}
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.Duration
import java.io.File
import java.util.concurrent.TimeUnit
import org.joda.time.DateTime
import org.scala_tools.time.Implicits._
import org.scalatest.{WordSpec, GivenWhenThen}
import org.slf4j.LoggerFactory
import scala.Predef._

class IterateePerformanceSpec extends WordSpec with GivenWhenThen {
  val log = LoggerFactory.getLogger(classOf[IterateePerformanceSpec])

  val market = Market("RTS")
  val security = Security("RTS 3.12")

  import TradeProtocol._

  "MarketDb" must {
    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File("./config/it.scala")
    val marketDB = runtime.loadRuntimeConfig[MarketDb]()

    implicit val client = marketDB.client

    "should persist new trades iterate over them with MarketIteratee" in {
      // Expect about 500000 records
      val interval = new DateTime(2012, 02, 03, 0, 0, 0, 0) to new DateTime(2012, 02, 03, 23, 0, 0, 0)

      import com.ergodicity.marketdb.iteratee.MarketIteratees._
      val tradeSeries = marketDB.trades(market, security, interval).apply(Duration.fromTimeUnit(3, TimeUnit.SECONDS))
      val enumerator = new TimeSeriesEnumerator(tradeSeries)
      val cnt = counter[TradePayload]

      val iter = enumerator.enumerate(cnt).map(_.run)
      val count = iter()

      log.info("Count = " + count)
    }
  }
}
