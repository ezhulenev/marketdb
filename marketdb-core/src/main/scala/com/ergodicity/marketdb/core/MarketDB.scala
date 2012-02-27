package com.ergodicity.marketdb.core

import scalaz._
import Scalaz._
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import com.ergodicity.marketdb.event.TradeReceived
import com.ergodicity.marketdb.uid.UIDProvider
import org.hbase.async.{PutRequest, HBaseClient}
import com.ergodicity.marketdb.{AsyncHBase, ByteArray, Ooops}
import com.ergodicity.marketdb.model._
import com.twitter.util.{Promise, Future, FuturePool}
import com.twitter.ostrich.admin.{Service, RuntimeEnvironment}

sealed trait TradeReaction

case class TradePersisted(payload: TradePayload) extends TradeReaction

case class TradeRejected(cause: NonEmptyList[Ooops]) extends TradeReaction

object MarketDB {
  val log = LoggerFactory.getLogger(getClass.getName)
  val MarketIdWidth: Short = 1
  val CodeIdWidth: Short = 3

  var marketDB: MarketDB = null
  var runtime: RuntimeEnvironment = null

  def main(args: Array[String]) {
    try {
      runtime = RuntimeEnvironment(this, args)
      marketDB = runtime.loadRuntimeConfig[MarketDB]()
      marketDB.start()
    } catch {
      case e =>
        log.error("Exception during startup; exiting!", e)
        System.exit(1)
    }
  }
}

class MarketDB(client: HBaseClient, marketIdProvider: UIDProvider, codeIdProvider: UIDProvider,
               val tradesTable: String) extends Service {

  val log = LoggerFactory.getLogger(classOf[MarketDB])
  log.info("Create marketDB for table: " + tradesTable)

  val ColumnFamily = ByteArray("id")
  val UidThreadPoolSize = 50;
  val uidFuturePool = FuturePool(Executors.newFixedThreadPool(UidThreadPoolSize))


  def start() {
    log.info("Start marketDB")
  }

  def shutdown() {
    log.info("Shutdown marketDB")
    client.shutdown()
  }

  def addTrade(payload: TradePayload) = {
    log.trace("Add trade: " + payload)

    val draftTrade: DraftTrade = Trade.loadFromHistory(Seq(TradeReceived(payload)))

    // Get Unique Ids for market and code
    val marketUid = uidFuturePool(marketIdProvider.provideId(payload.market.value));
    val codeUid = uidFuturePool(codeIdProvider.provideId(payload.code.value))

    val binaryTradeReaction: Future[ValidationNEL[Ooops, Reaction[BinaryTrade]]] = (marketUid join codeUid) map {
      tuple =>
        (tuple._1 |@| tuple._2) {
          (validMarketUid, validCodeUid) =>
            log.trace("Got marketUid=" + validMarketUid + " and codeUid=" + validCodeUid)
            draftTrade.enrichTrade(validMarketUid.id, validCodeUid.id).flatMap(_.serializeTrade()).reaction
        }
    }

    val binaryTrade = binaryTradeReaction map {
      _.flatMap {
        case Rejected(err) => Ooops(err).failNel[BinaryTrade]
        case Accepted(event, value) => value.successNel[Ooops]
      }
    }

    val futureReaction = binaryTrade flatMap {
      case Failure(err) => Future(TradeRejected(err))
      case Success(binary) => putTradeToHBase(payload, binary)
    }

    // Handle exception into Promise
    val promise = new Promise[TradeReaction]

    futureReaction onSuccess {
      reaction =>
        reaction match {
          case TradeRejected(err) => handleRejectedTrade(payload, err)
          case TradePersisted(payload) => "Trade persisted: " + payload
        }
        promise.setValue(reaction)
    } onFailure {
      e =>
        val ooops = NonEmptyList(Ooops("Unknown error", Some(e)))
        handleRejectedTrade(payload, ooops)
        promise.setValue(TradeRejected(ooops))
    }

    promise
  }

  private def putTradeToHBase(payload: TradePayload, binary: BinaryTrade) = {
    implicit def ba2arr(ba: ByteArray) = ba.toArray
    val putRequest = new PutRequest(ByteArray(tradesTable), binary.row, ColumnFamily, binary.qualifier, binary.payload)

    val promise = new Promise[TradeReaction]
    try {
      import AsyncHBase._
      val deferred = client.put(putRequest)
      deferred.addCallback {
        (_: Any) =>
          promise.setValue(TradePersisted(payload))
      }
      deferred.addErrback {
        (e: Throwable) =>
          promise.setValue(TradeRejected(NonEmptyList(Ooops("PutRequest failed", Some(e)))))
      }
    } catch {
      case e => promise.setValue(TradeRejected(NonEmptyList(Ooops("PutRequest failed", Some(e)))))
    }

    promise
  }

  private def handleRejectedTrade(payload: TradePayload, cause: NonEmptyList[Ooops]) {
    log.info("TradePayload rejected: " + payload + "; Cause: " + cause)
  }

}