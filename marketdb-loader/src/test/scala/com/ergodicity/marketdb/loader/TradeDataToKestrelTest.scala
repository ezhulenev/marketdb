package com.ergodicity.marketdb.loader

import org.slf4j.LoggerFactory
import org.scalatest.WordSpec
import com.twitter.finagle.kestrel.Client
import org.mockito.Mockito._
import org.mockito.Matchers._
import util.{BatchSettings, Iteratees}
import com.ergodicity.marketdb.model.TradeProtocol._

class TradeDataToKestrelTest extends WordSpec {
  val log = LoggerFactory.getLogger(classOf[TradeDataIterateeTest])

  val Queue = "Queue"

  val RtsTrades = () => {
    RtsTradeHistory(InputStreamRef(this.getClass.getResourceAsStream("/data/FT120201.zip")))
  }

  "Kestrel Iteratee" must {
    "push messages to Kestrel" in {
      import TradeDataIteratee._
      import Iteratees._

      val client = mock(classOf[Client])

      implicit val settings = BatchSettings(1000, None)
      val reportIo = RtsTrades().enumTradeData(kestrelBulkLoader(Queue, client)) map (_.run)
      val report = reportIo.unsafePerformIO

      log.info("Report: " + report)

      assert(report.count == 60)
      assert(report.list.size == 0)

      verify(client, only()).write(anyString(), any())
    }

    "push messages to Kestrel with specified bulk size" in {
      import TradeDataIteratee._
      import Iteratees._

      val client = mock(classOf[Client])

      implicit val settings = BatchSettings(40, None)
      val reportIo = RtsTrades().enumTradeData(kestrelBulkLoader(Queue, client)) map (_.run)
      val report = reportIo.unsafePerformIO

      log.info("Report: " + report)

      assert(report.count == 60)
      assert(report.list.size == 0)

      verify(client, times(2)).write(anyString(), any())
    }

    "push messages to Kestrel with specified bulk size and limit" in {
      import TradeDataIteratee._
      import Iteratees._

      val client = mock(classOf[Client])

      implicit val settings = BatchSettings(40, Some(50))
      val reportIo = RtsTrades().enumTradeData(kestrelBulkLoader(Queue, client)) map (_.run)
      val report = reportIo.unsafePerformIO

      log.info("Report: " + report)

      assert(report.count == 50)
      assert(report.list.size == 0)

      verify(client, times(2)).write(anyString(), any())
    }

    "push messages to Kestrel with specified bulk size and limit below bulk size" in {
      import TradeDataIteratee._
      import Iteratees._

      val client = mock(classOf[Client])

      implicit val settings = BatchSettings(40, Some(20))
      val reportIo = RtsTrades().enumTradeData(kestrelBulkLoader(Queue, client)) map (_.run)
      val report = reportIo.unsafePerformIO

      log.info("Report: " + report)

      assert(report.count == 20)
      assert(report.list.size == 0)

      verify(client, times(1)).write(anyString(), any())
    }

    "push any uniqie payload" in {
      import TradeDataIteratee._
      import Iteratees._

      val client = mock(classOf[Client])

      implicit val settings = BatchSettings(1, None)
      val reportIo = RtsTrades().enumTradeData(kestrelBulkLoader(Queue, client)) map (_.run)
      val report = reportIo.unsafePerformIO

      log.info("Report: " + report)

      assert(report.count == 60)
      assert(report.list.size == 0)

      verify(client, times(60)).write(anyString(), any())
    }
  }
}