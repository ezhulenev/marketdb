package com.ergodicity.marketdb

import com.ergodicity.marketdb.model.{Market, Security, MarketPayload}
import org.joda.time.Interval
import com.ergodicity.marketdb.TimeSeries.Qualifier

object TimeSeries {

  case class Qualifier(table: Array[Byte], startKey: Array[Byte], stopKey: Array[Byte])

}

class TimeSeries[+P <: MarketPayload](val market: Market, val security: Security, val interval: Interval, val qualifier: Qualifier) {
  override def toString = "TimeSeries(market='" + market + "', security='" + security + "', interval='" + interval + "')"
}