package com.ergodicity.marketdb.core

import org.scalatest.{GivenWhenThen, Spec}
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import com.ergodicity.marketdb.TimeRecording
import com.ergodicity.marketdb.model._
import java.io.File
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.Future
import org.scala_tools.time.Implicits._
import collection.JavaConversions
import scala.Predef._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class MarketDBIntegrationTest extends Spec with GivenWhenThen with TimeRecording {
  override val log = LoggerFactory.getLogger(classOf[MarketDBIntegrationTest])

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, code, contract, BigDecimal("111"), 1, time, 11l, true)

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
      
      val payload1 = TradePayload(market, code, contract, BigDecimal("111"), 1, time1, 111l, true)
      val payload2 = TradePayload(market, code, contract, BigDecimal("112"), 1, time2, 112l, true)
      
      val f1 = marketDB.addTrade(payload1)
      val f2 = marketDB.addTrade(payload2)
      
      // Wait for trades persisted
      Future.join(List(f1, f2))()
      
      // -- Verify two rows for 1970 Jan 1
      val interval = new DateTime(1970, 01, 01, 0, 0, 0, 0) to new DateTime(1970, 01, 01, 23, 0, 0, 0)
      val scanner = marketDB.scan(market, code, interval)()

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Jan 1: "+rows)

      import sbinary.Operations._
      import TradeProtocol._
      val trades = for (list <- JavaConversions.asScalaIterator(rows.iterator());
           kv <- JavaConversions.asScalaIterator(list.iterator())) yield fromByteArray[TradePayload](kv.value());
      
      trades foreach {trade => log.info("Trade: "+trade)}

      assert(rows.size() == 1)
      assert(rows.get(0).size() == 2)

      assert(scanner.nextRows().joinUninterruptibly() == null)
    }

    it("should return null if no trades exists") {
      // -- Verify two rows for 1970 Feb 1
      val interval = new DateTime(1970, 02, 01, 0, 0, 0, 0) to new DateTime(1970, 02, 01, 23, 0, 0, 0)
      val scanner = marketDB.scan(market, code, interval)()

      val rows = scanner.nextRows().joinUninterruptibly()
      log.info("ROWS Feb1 1: "+rows)

      assert(rows == null)
    }

    it("should persist new trades and read them with stream") {
      readWithTradeStream(marketDB) {_.read()}
    }

    it("should persist new trades and read them with buffered stream (size=10)") {
      readWithTradeStream(marketDB) {_.read().buffered(10)}
    }

    it("should persist new trades and read them with buffered stream (size=100)") {
      readWithTradeStream(marketDB) {_.read().buffered(100)}
    }
  }

  private def readWithTradeStream(marketDB: MarketDB)(f: TradesStream => TradesHandle) {
    val payloads = for (m <- 0 to 59;
                        s <- 0 to 59)
    yield TradePayload(market, code, contract, BigDecimal("111"), 1, new DateTime(1971, 01, 01, 1, m, s, 0), s + m * 60, true)

    log.info("Payloads size: " + payloads.size)

    val fs = payloads.map(marketDB.addTrade(_))

    // Wait for trades persisted
    Future.join(fs)()

    val interval = new DateTime(1971, 01, 01, 0, 0, 0, 0) to new DateTime(1971, 01, 01, 23, 0, 0, 0)
    val scanner = marketDB.scan(market, code, interval)()

    // -- Read trades with stream
    val stream = TradesStream(scanner)
    val handle = f(stream)

    val exhaustedLatch = new java.util.concurrent.CountDownLatch(1)
    handle.error foreach {
      case TradesStreamExhaustedException => exhaustedLatch.countDown()
    }

    val counter = new AtomicInteger()
    handle.trades foreach {
      t =>
        counter.incrementAndGet()
        t.ack()
    }

    exhaustedLatch.await(10, TimeUnit.SECONDS)

    log.info("Counter: " + counter.get())
    assert(scanner.nextRows().joinUninterruptibly() == null)
  }
}
