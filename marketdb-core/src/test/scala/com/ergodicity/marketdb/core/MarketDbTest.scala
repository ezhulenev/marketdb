package com.ergodicity.marketdb.core

import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.core.classloader.annotations.{PrepareForTest, PowerMockIgnore}
import com.stumbleupon.async.Deferred
import org.slf4j.LoggerFactory

import scalaz._
import Scalaz._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.junit.Test
import org.joda.time.DateTime
import com.ergodicity.marketdb.uid.{UniqueId, UIDProvider}
import org.hbase.async._
import com.ergodicity.marketdb.{HBaseMatchers, ByteArray, Ooops}
import com.ergodicity.marketdb.model._

@RunWith(classOf[PowerMockRunner])
@PowerMockIgnore(Array("javax.management.*", "javax.xml.parsers.*",
  "com.sun.org.apache.xerces.internal.jaxp.*", "ch.qos.logback.*", "org.slf4j.*"))
@PrepareForTest(Array(classOf[HBaseClient], classOf[RowLock], classOf[Deferred[_]]))
class MarketDbTest extends HBaseMatchers {
  val log = LoggerFactory.getLogger(classOf[MarketDbTest]);

  val tradesTable = "TRADES"

  val market = Market("RTS")
  val code = Code("RIH")
  val contract = Contract("RTS 3.12")
  val time = new DateTime
  val payload = TradePayload(market, code, contract, BigDecimal("111"), 1, time, 11l, true)


  // Prepare mocks for testing

  val client = mock(classOf[HBaseClient])
  val marketUidProvider = mock(classOf[UIDProvider])
  val codeUidProvider = mock(classOf[UIDProvider])

  val marketDb = new MarketDB(client, tradesTable)
  marketDb.marketIdProvider = marketUidProvider
  marketDb.codeIdProvider = codeUidProvider


  @Test
  def testTradeRejectedForUidValidationError() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(Ooops("Market UID error").failNel[UniqueId])
    when(codeUidProvider.provideId("RIH")).thenReturn(Ooops("Code UID error").failNel[UniqueId])

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)

    assert(reaction match {
      case TradeRejected(err) => true
      case _ => false
    })

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(codeUidProvider).provideId("RIH")
  }

  @Test
  def testTradeRejectedForUidException() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(UniqueId("RTS", ByteArray('0')).successNel[Ooops])
    when(codeUidProvider.provideId("RIH")).thenThrow(new RuntimeException("Test UID exception"))

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)
    assert(reaction match {
      case TradeRejected(err) => true
      case _ => false
    })

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(codeUidProvider).provideId("RIH")
  }

  @Test
  def testTradeRejectedForInvalidUidWidth() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(UniqueId("RTS", ByteArray("TooLong")).successNel[Ooops])
    when(codeUidProvider.provideId("RIH")).thenReturn(UniqueId("RIH", ByteArray(0, 0, 1)).successNel[Ooops])

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)
    assert(reaction match {
      case TradeRejected(err) => true
      case _ => false
    })

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(codeUidProvider).provideId("RIH")
  }

  @Test
  def testTradeRejectedForHBaseFailure() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(UniqueId("RTS", ByteArray('0')).successNel[Ooops])
    when(codeUidProvider.provideId("RIH")).thenReturn(UniqueId("RIH", ByteArray(0, 0, 1)).successNel[Ooops])
    when(client.put(any(classOf[PutRequest]))).thenThrow(mock(classOf[HBaseException]))

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)
    assert(reaction match {
      case TradeRejected(err) => true
      case _ => false
    })

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(codeUidProvider).provideId("RIH")
  }

  @Test
  def testTradeRejectedForHBaseDeferredFailure() {
    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(UniqueId("RTS", ByteArray('0')).successNel[Ooops])
    when(codeUidProvider.provideId("RIH")).thenReturn(UniqueId("RIH", ByteArray(0, 0, 1)).successNel[Ooops])
    when(client.put(any(classOf[PutRequest]))).thenReturn(Deferred.fromError[AnyRef](mock(classOf[HBaseException])))

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)
    assert(reaction match {
      case TradeRejected(err) => true
      case _ => false
    })

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(codeUidProvider).provideId("RIH")
  }

  @Test
  def testTradePersisted() {

    val year = ByteArray(payload.time.getYear)
    val day = ByteArray(payload.time.getDayOfYear)
    val minute = ByteArray(payload.time.getMinuteOfDay)
    val row = ByteArray('0') ++ ByteArray(0, 0, 1) ++ year ++ day ++ minute;

    // Init mocks
    when(marketUidProvider.provideId("RTS")).thenReturn(UniqueId("RTS", ByteArray('0')).successNel[Ooops])
    when(codeUidProvider.provideId("RIH")).thenReturn(UniqueId("RIH", ByteArray(0, 0, 1)).successNel[Ooops])
    when(client.put(putForRow(row))).thenReturn(Deferred.fromResult(new AnyRef()))

    // Execute
    val reaction = marketDb.addTrade(payload).apply()

    log.info("Trade reaction: "+reaction)
    assert(reaction match {
      case TradePersisted(pl) => true
      case _ => false
    })

    // Verify
    verify(marketUidProvider).provideId("RTS")
    verify(codeUidProvider).provideId("RIH")
    verify(client).put(putForRow(row))
  }

}
