package integration.ergodicity.marketdb.core

import org.scalatest.{GivenWhenThen, Spec}
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import integration.ergodicity.marketdb.TimeRecording
import com.ergodicity.marketdb.model._
import java.io.File
import com.twitter.ostrich.admin.RuntimeEnvironment
import scala.Predef._
import com.ergodicity.marketdb.core.MarketDB
import com.twitter.util.Future
import collection.JavaConversions
import org.scala_tools.time.Implicits._

class MarketDbOrdersIntegrationTest extends Spec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[MarketDbOrdersIntegrationTest])

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = OrderPayload(market, security, 11l, time, 100, 101, 1, BigDecimal("111"), 1, 1, None)

  describe("MarketDB") {

    val runtime = RuntimeEnvironment(this, Array[String]())
    runtime.configFile = new File(this.getClass.getResource("/config/it.scala").toURI)
    val marketDB = runtime.loadRuntimeConfig[MarketDB]()

    it("should persist new order") {

      // Execute
      val futureReaction = recordTime("Add order", () => marketDB.addOrder(payload))
      val reaction = recordTime("Reaction", () => futureReaction.apply())

      log.info("order reaction: " + reaction)
    }

    it("should persist new orders and scan them later") {
      val time1 = new DateTime(1970, 01, 01, 1, 0, 0, 0)
      val time2 = new DateTime(1970, 01, 01, 1, 0, 1, 0)

      val payload1 = OrderPayload(market, security, 111l, time1, 100, 101, 1, BigDecimal("111"), 1, 1, None)
      val payload2 = OrderPayload(market, security, 112l, time2, 100, 101, 1, BigDecimal("111"), 1, 1, None)

      val f1 = marketDB.addOrder(payload1)
      val f2 = marketDB.addOrder(payload2)

      // Wait for orders persisted
      Future.join(List(f1, f2))()

      // -- Verify two rows for 1970 Jan 1
      val interval = new DateTime(1970, 01, 01, 0, 0, 0, 0) to new DateTime(1970, 01, 01, 23, 0, 0, 0)
      val scanner = marketDB.scanOrders(market, security, interval)()

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Jan 1: " + rows)

      import sbinary.Operations._
      import OrderProtocol._
      val orders = for (list <- JavaConversions.asScalaIterator(rows.iterator());
                        kv <- JavaConversions.asScalaIterator(list.iterator())) yield fromByteArray[OrderPayload](kv.value());

      orders foreach {
        order => log.info("Order: " + order)
      }

      assert(rows.size() == 1)
      assert(rows.get(0).size() == 2)

      assert(scanner.nextRows().joinUninterruptibly() == null)
    }
  }
}