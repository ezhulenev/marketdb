package integration.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.MarketDbApp
import com.ergodicity.marketdb.iteratee.{TimeSeriesEnumerator, MarketDbReader, MarketIteratees}
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.model.TradeProtocol._
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.{Duration, Future}
import java.io.File
import java.util.concurrent.TimeUnit
import org.joda.time.DateTime
import org.mockito.Mockito
import org.scala_tools.time.Implicits._
import org.scalatest.{WordSpec, GivenWhenThen}
import org.slf4j.LoggerFactory
import scalaz.NonEmptyList

class TradesIterateeSpec extends WordSpec with GivenWhenThen {
  val log = LoggerFactory.getLogger(classOf[TradesIterateeSpec])

  val NoSystem = true

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime

  "MarketDb" must {

    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File("./config/it.scala")
    val marketDBApp = runtime.loadRuntimeConfig[MarketDbApp]()

    implicit val reader = Mockito.mock(classOf[MarketDbReader])
    Mockito.when(reader.client).thenReturn(marketDBApp.marketDb.client)

    "should persist new trades iterate over them with MarketIteratee" in {
      val time1 = new DateTime(1972, 01, 05, 1, 0, 0, 0)
      val time2 = new DateTime(1972, 01, 05, 1, 0, 1, 0)

      val payload1 = TradePayload(market, security, 111l, BigDecimal("111"), 1, time1, NoSystem)
      val payload2 = TradePayload(market, security, 112l, BigDecimal("112"), 1, time2, NoSystem)

      val f1 = marketDBApp.marketDb.addTrade(payload1)
      val f2 = marketDBApp.marketDb.addTrade(payload2)

      // Wait for trades persisted
      Future.join(List(f1, f2))()

      // -- Verify two rows for 1972 Jan 5
      val interval = new DateTime(1972, 01, 05, 0, 0, 0, 0) to new DateTime(1972, 01, 05, 23, 0, 0, 0)

      import MarketIteratees._

      val tradeSeries = marketDBApp.marketDb.trades(market, security, interval).apply(Duration.fromTimeUnit(3, TimeUnit.SECONDS))
      val enumerator = TimeSeriesEnumerator(tradeSeries)

      val cnt = counter[TradePayload]

      val iter = enumerator.enumerate(cnt).map(_.run)
      val count = iter()

      assert(count == 2)
    }

    "should persist trades and iterate over them for multiple securities" in {
      val interval = new DateTime(1972, 01, 05, 0, 0, 0, 0) to new DateTime(1972, 01, 05, 23, 0, 0, 0)
      val baseTime = new DateTime(1972, 01, 05, 1, 0, 0, 0)

      val SecuritiesCount = 10
      val TradeCount = 10

      val securities = (1 to SecuritiesCount) map (i => (i, Security("Security#" + i)))

      val payloads = securities.flatMap {
        case (secIdx, sec) =>
          (1 to TradeCount) map (i => TradePayload(market, sec, 100l + i, 100l + i, 1, baseTime + i.seconds + secIdx.millis, NoSystem))
      }

      // Wait for trades persisted
      Future.join(payloads.map(marketDBApp.marketDb.addTrade(_)))()

      import MarketIteratees._
      val tradeSeries = Future.collect(securities.map(sec => marketDBApp.marketDb.trades(market, sec._2, interval))).apply()
      log.info("Time series = " + tradeSeries)

      val enumerator = TimeSeriesEnumerator(NonEmptyList(tradeSeries.head, tradeSeries.tail: _*))
      val iterv = sequencer[TradePayload]

      val iter = enumerator.enumerate(iterv).map(_.run)
      val result = iter()

      assert(result.size == payloads.size, "actual size = " + result.size + ", expected = " + payloads.size)
    }
  }
}
