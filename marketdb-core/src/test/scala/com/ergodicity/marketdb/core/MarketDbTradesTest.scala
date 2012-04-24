package com.ergodicity.marketdb.core

import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import com.stumbleupon.async.Deferred
import org.slf4j.LoggerFactory

import org.mockito.Mockito._
import org.mockito.Matchers._
import org.junit.Test
import org.joda.time.DateTime
import com.ergodicity.marketdb.uid.{UniqueId, UIDProvider}
import org.hbase.async._
import com.ergodicity.marketdb.{HBaseMatchers, ByteArray}
import com.ergodicity.marketdb.model._
import com.twitter.util.Future
import org.scalatest.Assertions._

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[HBaseClient], classOf[RowLock], classOf[Deferred[_]]))
class MarketDbTradesTest extends HBaseMatchers {
  val log = LoggerFactory.getLogger(classOf[MarketDbOrdersTest]);

  val tradesTable = "TRADES"
  val ordersTable = "ORDERS"

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, security, 11l, BigDecimal("111"), 1, time, true)


  // Prepare mocks for testing

  val client = mock(classOf[HBaseClient])
  val marketUidProvider = mock(classOf[UIDProvider])
  val securityUidProvider = mock(classOf[UIDProvider])

  val marketDb = new MarketDB(client, marketUidProvider, securityUidProvider, tradesTable, ordersTable)


  @Test
  def testTradeRejectedForUidValidationError() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenThrow(mock(classOf[RuntimeException]))
    when(securityUidProvider.provideId("RTS 3.12")).thenThrow(mock(classOf[RuntimeException]))

    // Execute
    intercept[RuntimeException] {
        marketDb.addTrade(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
  }

  @Test
  def testTradeRejectedForUidException() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenThrow(new RuntimeException("Test UID exception"))

    // Execute
    intercept[RuntimeException] {
      marketDb.addTrade(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testTradeRejectedForInvalidUidWidth() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray("TooLong"))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})

    // Execute
    intercept[RuntimeException] {
      marketDb.addTrade(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testTradeRejectedForHBaseFailure() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})
    when(client.put(any(classOf[PutRequest]))).thenThrow(mock(classOf[HBaseException]))

    // Execute
    intercept[HBaseException] {
      marketDb.addTrade(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testTradeRejectedForHBaseDeferredFailure() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})
    when(client.put(any(classOf[PutRequest]))).thenReturn(Deferred.fromError[AnyRef](mock(classOf[HBaseException])))

    // Execute
    intercept[HBaseException] {
      marketDb.addTrade(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testTradePersisted() {

    val year = ByteArray(payload.time.getYear)
    val day = ByteArray(payload.time.getDayOfYear)
    val minute = ByteArray(payload.time.getMinuteOfDay)
    val row = ByteArray('0') ++ ByteArray(0, 0, 1) ++ year ++ day ++ minute;

    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})
    when(client.put(putForRow(row))).thenReturn(Deferred.fromResult(new AnyRef()))

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
    verify(client).put(putForRow(row))
  }

}
