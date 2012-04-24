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
class MarketDbOrdersTest extends HBaseMatchers {
  val log = LoggerFactory.getLogger(classOf[MarketDbOrdersTest]);

  val tradesTable = "TRADES"
  val ordersTable = "ORDERS"

  val market = Market("RTS")
  val security = Security("RTS 3.12")
  val time = new DateTime
  val payload = OrderPayload(market, security, 11l, time, 100, 101, 1, BigDecimal("111"), 1, 1, Some(BigDecimal("112")))


  // Prepare mocks for testing

  val client = mock(classOf[HBaseClient])
  val marketUidProvider = mock(classOf[UIDProvider])
  val securityUidProvider = mock(classOf[UIDProvider])

  val marketDb = new MarketDB(client, marketUidProvider, securityUidProvider, tradesTable, ordersTable)


  @Test
  def testOrderRejectedForUidValidationError() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenThrow(mock(classOf[RuntimeException]))
    when(securityUidProvider.provideId("RTS 3.12")).thenThrow(mock(classOf[RuntimeException]))

    // Execute
    intercept[RuntimeException] {
      marketDb.addOrder(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
  }

  @Test
  def testOrderRejectedForUidException() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenThrow(new RuntimeException("Test UID exception"))

    // Execute
    intercept[RuntimeException] {
      marketDb.addOrder(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testOrderRejectedForInvalidUidWidth() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray("TooLong"))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})

    // Execute
    intercept[RuntimeException] {
      marketDb.addOrder(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testOrderRejectedForHBaseFailure() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})
    when(client.put(any(classOf[PutRequest]))).thenThrow(mock(classOf[HBaseException]))

    // Execute
    intercept[HBaseException] {
      marketDb.addOrder(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testOrderRejectedForHBaseDeferredFailure() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})
    when(client.put(any(classOf[PutRequest]))).thenReturn(Deferred.fromError[AnyRef](mock(classOf[HBaseException])))

    // Execute
    intercept[HBaseException] {
      marketDb.addOrder(payload).apply()
    }

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
  }

  @Test
  def testOrderPersisted() {

    val year = ByteArray(payload.time.getYear)
    val day = ByteArray(payload.time.getDayOfYear)
    val minute = ByteArray(payload.time.getMinuteOfDay)
    val row = ByteArray('0') ++ ByteArray(0, 0, 1) ++ year ++ day ++ minute;

    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Future {UniqueId("RTS", ByteArray('0'))})
    when(securityUidProvider.provideId("RTS 3.12")).thenReturn(Future {UniqueId("RTS 3.12", ByteArray(0, 0, 1))})
    when(client.put(putForRow(row))).thenReturn(Deferred.fromResult(new AnyRef()))

    // Execute
    val reaction = marketDb.addOrder(payload).apply()

    log.info("Order reaction: "+reaction)

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(securityUidProvider).provideId("RTS 3.12")
    verify(client).put(putForRow(row))
  }
}
