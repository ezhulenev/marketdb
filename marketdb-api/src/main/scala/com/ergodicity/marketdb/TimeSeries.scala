package com.ergodicity.marketdb

import com.ergodicity.marketdb.model.{Market, Security, MarketPayload}
import org.joda.time.Interval
import org.hbase.async.HBaseClient

class TimeSeries[P <: MarketPayload](val market: Market, val security: Security, val interval: Interval)
                                    (private[marketdb] val table: Array[Byte], private[marketdb] val startKey: Array[Byte], private[marketdb] val stopKey: Array[Byte]) {
  def scan(client: HBaseClient) = {
    val scanner = client.newScanner(table)
    scanner.setStartKey(startKey)
    scanner.setStopKey(stopKey)
    scanner
  }
}
