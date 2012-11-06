package integration.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.core.MarketDb
import com.ergodicity.marketdb.iteratee.{TimeSeriesEnumerator, MarketDbReader}
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
import org.mockito.Mockito
import integration.ergodicity.marketdb.TimeRecording

class IterateePerformanceSpec extends WordSpec with GivenWhenThen with TimeRecording {
  val log = LoggerFactory.getLogger(classOf[IterateePerformanceSpec])

  val market = Market("RTS")
  val security = Security("RTS-3.12")

  import TradeProtocol._

  "MarketDb" must {

    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File("./config/it.scala")
    val marketDB = runtime.loadRuntimeConfig[MarketDb]()

    implicit val reader = Mockito.mock(classOf[MarketDbReader])
    Mockito.when(reader.client).thenReturn(marketDB.client)

    "iterate over existing trades in HBase with Iteratees" in {
      val interval = new DateTime(2012, 02, 01, 0, 0, 0, 0) to new DateTime(2012, 02, 28, 23, 0, 0, 0)

      import com.ergodicity.marketdb.iteratee.MarketIteratees._
      val tradeSeries = marketDB.trades(market, security, interval).apply(Duration.fromTimeUnit(3, TimeUnit.SECONDS))
      val enumerator = TimeSeriesEnumerator(tradeSeries)
      val cnt = counter[TradePayload]

      val iter = enumerator.enumerate(cnt).map(_.run)
      val count = recordTime("Iteratee based iteration", () => {iter apply Duration.fromTimeUnit(5, TimeUnit.MINUTES)})

      log.info("Count = " + count)
    }

    "iterate over existing trades in HBase with Scanner" in {
      val interval = new DateTime(2012, 02, 01, 0, 0, 0, 0) to new DateTime(2012, 02, 28, 23, 0, 0, 0)

      val tradeSeries = marketDB.trades(market, security, interval).apply(Duration.fromTimeUnit(3, TimeUnit.SECONDS))

      val scanner = {
        val scanner = marketDB.client.newScanner(tradeSeries.qualifier.table)
        scanner.setStartKey(tradeSeries.qualifier.startKey)
        scanner.setStopKey(tradeSeries.qualifier.stopKey)
        scanner
      }

      val count = recordTime("Scanner based iteration", () => {
        var count = 0
        var rows = scanner.nextRows().joinUninterruptibly()

        while(rows != null) {
          val rowsIterator = rows.iterator()
          while(rowsIterator.hasNext) {
            val bucket = rowsIterator.next()
            val bucketIterator = bucket.iterator()
            while(bucketIterator.hasNext) {
              val row = bucketIterator.next()
              count = count + 1
            }
          }
          rows = scanner.nextRows().joinUninterruptibly()
        }
        count
      })

      log.info("Count = " + count)
    }

  }
}
