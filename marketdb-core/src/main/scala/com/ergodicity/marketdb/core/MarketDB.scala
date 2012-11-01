package com.ergodicity.marketdb.core

import com.ergodicity.marketdb.event.{OrderReceived, TradeReceived}
import com.ergodicity.marketdb.model._
import com.ergodicity.marketdb.uid.UIDProvider
import com.ergodicity.marketdb.{TimeSeries, AsyncHBase, ByteArray}
import com.twitter.ostrich.admin.Service
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Promise, Future}
import org.hbase.async.{HBaseClient, PutRequest}
import org.joda.time.Interval
import org.slf4j.LoggerFactory

trait MarketService extends Service

case class TradePersisted(payload: TradePayload)

case class OrderPersisted(payload: OrderPayload)

object MarketDb {
  val MarketIdWidth: Short = 1
  val SecurityIdWidth: Short = 3
}

class MarketDb(val client: HBaseClient, marketIdProvider: UIDProvider, securityIdProvider: UIDProvider,
               val tradesTable: String, val ordersTable: String, serviceBuilders: Seq[MarketDb => MarketService] = Seq()) extends Service {

  val log = LoggerFactory.getLogger(classOf[MarketDb])
  val ColumnFamily = ByteArray("id")

  log.info("Create marketDB for table: " + tradesTable)
  val services = serviceBuilders map {_(this)}

  def start() {
    log.info("Start marketDB")
    services foreach {_.start()}
  }

  def shutdown() {
    log.info("Shutdown marketDB")
    client.shutdown()
    services foreach {_.shutdown()}
    log.info("marketDB stopped")
  }
  
  def trades(market: Market, security: Security, interval: Interval): Future[TimeSeries[TradePayload]] = {
    log.info("Scan marketDB Trades for market="+market.value+"; Security="+security.isin+"; Interval="+interval)

    // Get Unique Ids for market and security
    val marketUid = Stats.timeFutureMillis("get_market_uid") {marketIdProvider.provideId(market.value)}
    val securityUid = Stats.timeFutureMillis("get_security_uid") {securityIdProvider.provideId(security.isin)}

    (marketUid join securityUid) map {
      tuple =>
        val startKey = TradeRow(tuple._1.id, tuple._2.id, interval.getStart)
        val stopKey = TradeRow(tuple._1.id, tuple._2.id, interval.getEnd) ++ ByteArray(0)

        new TimeSeries[TradePayload](market, security, interval)(ByteArray(tradesTable).toArray, startKey.toArray, stopKey.toArray)
    }
  }

  def orders(market: Market, security: Security, interval: Interval): Future[TimeSeries[OrderPayload]] = {
    log.info("Scan marketDB Orders for market="+market.value+"; Security="+security.isin+"; Interval="+interval)

    // Get Unique Ids for market and security
    val marketUid = Stats.timeFutureMillis("get_market_uid") {marketIdProvider.provideId(market.value)}
    val securityUid = Stats.timeFutureMillis("get_security_uid") {securityIdProvider.provideId(security.isin)}

    (marketUid join securityUid) map {
      tuple =>
        val startKey = OrderRow(tuple._1.id, tuple._2.id, interval.getStart)
        val stopKey = OrderRow(tuple._1.id, tuple._2.id, interval.getEnd) ++ ByteArray(0)

        new TimeSeries[OrderPayload](market, security, interval)(ByteArray(ordersTable).toArray, startKey.toArray, stopKey.toArray)
    }
  }

  def addOrder(payload: OrderPayload) = {
    log.trace("Add order: " + payload)

    val draftOrder: DraftOrder = Order.loadFromHistory(Seq(OrderReceived(payload)))

    // Get Unique Ids for market and security
    val marketUid = Stats.timeFutureMillis("get_market_uid") {marketIdProvider.provideId(payload.market.value)}
    val securityUid = Stats.timeFutureMillis("get_security_uid") {securityIdProvider.provideId(payload.security.isin)}

    val binaryOrderReaction: Future[Reaction[BinaryOrder]] = (marketUid join securityUid) map {
      tuple =>
        draftOrder.enrichOrder(tuple._1.id, tuple._2.id).flatMap(_.serializeOrder()).reaction
    }

    val binaryOrder = binaryOrderReaction map {
      case Accepted(event, value) => value
      case Rejected(err) => throw new RuntimeException("order rejected: "+err)
    }

    binaryOrder flatMap {putOrderToHBase(_)} onFailure {err =>
      handleFailedOrder(payload, err)
    }
  }

  def addTrade(payload: TradePayload) = {
    log.trace("Add trade: " + payload)

    val draftTrade: DraftTrade = Trade.loadFromHistory(Seq(TradeReceived(payload)))

    // Get Unique Ids for market and security
    val marketUid = Stats.timeFutureMillis("get_market_uid") {marketIdProvider.provideId(payload.market.value)}
    val securityUid = Stats.timeFutureMillis("get_security_uid") {securityIdProvider.provideId(payload.security.isin)}

    val binaryTradeReaction: Future[Reaction[BinaryTrade]] = (marketUid join securityUid) map {
      tuple =>
        draftTrade.enrichTrade(tuple._1.id, tuple._2.id).flatMap(_.serializeTrade()).reaction
    }

    val binaryTrade = binaryTradeReaction map {
        case Accepted(event, value) => value
        case Rejected(err) => throw new RuntimeException("Trade rejected: "+err)
    }

    binaryTrade flatMap {putTradeToHBase(_)} onFailure {err =>
      handleFailedTrade(payload, err)
    }
  }

  private def putOrderToHBase(binary: BinaryOrder) = {
    implicit def ba2arr(ba: ByteArray) = ba.toArray
    val putRequest = new PutRequest(ByteArray(ordersTable), binary.row, ColumnFamily, binary.qualifier, binary.payload)

    val promise = new Promise[Boolean]
    try {
      import AsyncHBase._
      val deferred = client.put(putRequest)
      deferred.addCallback {(_: Any) =>promise.setValue(true)}
      deferred.addErrback {(e: Throwable) => promise.setException(e)}
    } catch {
      case e: Throwable => promise.setException(e)
    }

    promise
  }

  private def putTradeToHBase(binary: BinaryTrade) = {
    implicit def ba2arr(ba: ByteArray) = ba.toArray
    val putRequest = new PutRequest(ByteArray(tradesTable), binary.row, ColumnFamily, binary.qualifier, binary.payload)

    val promise = new Promise[Boolean]
    try {
      import AsyncHBase._
      val deferred = client.put(putRequest)
      deferred.addCallback {(_: Any) =>promise.setValue(true)}
      deferred.addErrback {(e: Throwable) => promise.setException(e)}
    } catch {
      case e: Throwable => promise.setException(e)
    }

    promise
  }

  private def handleFailedTrade(payload: TradePayload, cause: Throwable) {
    log.info("Failed to save TradePayload: " + payload + "; Cause: " + cause)
  }

  private def handleFailedOrder(payload: OrderPayload, cause: Throwable) {
    log.info("Failed to save OrderPayload: " + payload + "; Cause: " + cause)
  }


}