package com.ergodicity.marketdb.iteratee

import collection.JavaConversions
import com.ergodicity.marketdb.model._
import com.stumbleupon.async.Deferred
import java.util
import org.hbase.async._
import org.slf4j.LoggerFactory
import com.ergodicity.marketdb.ByteArray
import com.ergodicity.marketdb.model.OrderProtocol._
import sbinary.Operations._
import com.ergodicity.marketdb.model.TradeProtocol._
import com.ergodicity.marketdb.model.Market
import com.ergodicity.marketdb.model.Security
import com.ergodicity.marketdb.model.TradePayload
import com.ergodicity.marketdb.model.OrderPayload
import org.mockito.Mockito._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

object ScannerMock {

  trait ToKeyValue[P <: MarketPayload] {
    def keyValue(payload: P)(implicit marketId: Market => ByteArray, securityId: Security => ByteArray): KeyValue
  }

  implicit object TradeToKeyValue extends ToKeyValue[TradePayload] {
    def keyValue(payload: TradePayload)(implicit marketId: Market => ByteArray, securityId: Security => ByteArray) = {
      val k = TradeRow(marketId(payload.market), securityId(payload.security), payload.time)
      new KeyValue(k.toArray, Family, Bytes.fromLong(payload.tradeId), toByteArray(payload))
    }
  }

  implicit object OrderToKeyValue extends ToKeyValue[OrderPayload] {
    def keyValue(payload: OrderPayload)(implicit marketId: Market => ByteArray, securityId: Security => ByteArray) = {
      val k = OrderRow(marketId(payload.market), securityId(payload.security), payload.time)
      new KeyValue(k.toArray, Family, Bytes.fromLong(payload.orderId), toByteArray(payload))
    }
  }

  val DefaultRowsCount = 10
  val log = LoggerFactory.getLogger("ScannerMock")

  def apply[P <: MarketPayload](payload: Seq[P], batchSize: Int = DefaultRowsCount, failOnBatch: Option[(Int, Exception)] = None, name: Option[String] = None)
                               (implicit marketId: Market => ByteArray, securityId: Security => ByteArray, toKeyValue: ToKeyValue[P]) = {
    fromPayloads(payload, batchSize, failOnBatch)
  }

  val Family = ByteArray("id").toArray

  def fromPayloads[P <: MarketPayload](values: Seq[P], batchSize: Int = DefaultRowsCount, failOnBatch: Option[(Int, Exception)] = None, name: Option[String] = None)
                                      (implicit marketId: Market => ByteArray, securityId: Security => ByteArray, toKeyValue: ToKeyValue[P]) = {
    fromKeyValues(values.map(v => toKeyValue.keyValue(v)), batchSize, failOnBatch)
  }


  def fromKeyValues(values: Seq[KeyValue], batchSize: Int = DefaultRowsCount, failOnBatch: Option[(Int, Exception)] = None, name: Option[String] = None) = {
    var pointer = 0

    def nextBatch() = {
      log.info("Fetch next rows batch")
      if (pointer >= values.size) {
        null
      } else {
        val slice = values.slice(pointer, pointer + batchSize)
        pointer += batchSize
        slice
      }
    }

    def nextRows: util.ArrayList[util.ArrayList[KeyValue]] = {
      val batch = nextBatch()
      if (batch == null) return null

      val grouped = batch.groupBy(kv => ByteArray(kv.key()))
      val list = new util.ArrayList[util.ArrayList[KeyValue]]()
      grouped.keys.foreach {
        key =>
          val values = grouped.get(key).get
          list.add(new util.ArrayList[KeyValue](JavaConversions.asJavaCollection(values)))
      }
      list
    }

    val scanner = name.map(n => mock(classOf[Scanner], n)) getOrElse mock(classOf[Scanner])
    when(scanner.nextRows()).thenAnswer(new Answer[Deferred[util.ArrayList[util.ArrayList[KeyValue]]]] {
      var batch = 0

      def answer(invocation: InvocationOnMock) = {
        // -- Check if wee need to fail
        if (failOnBatch.isDefined && failOnBatch.get._1 <= batch) {
          log.info("Fail on batch #" + batch + "; With err=" + failOnBatch.get._2)
          Deferred.fromError[util.ArrayList[util.ArrayList[KeyValue]]](failOnBatch.get._2)
        } else {
          batch = batch + 1
          Deferred.fromResult(nextRows)
        }
      }
    })

    when(scanner.close()).thenReturn(Deferred.fromResult[AnyRef](new AnyRef()))
    scanner
  }
}

