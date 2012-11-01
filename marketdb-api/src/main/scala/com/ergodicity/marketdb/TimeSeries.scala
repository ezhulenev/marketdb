package com.ergodicity.marketdb

import com.ergodicity.marketdb.model.{Market, Security, MarketPayload}
import org.joda.time.Interval

class TimeSeries[P <: MarketPayload](val market: Market, val security: Security, val interval: Interval)
                                    (private[marketdb] val table: Array[Byte], private[marketdb] val startKey: Array[Byte], private[marketdb] val stopKey: Array[Byte]) {
  def scan(client: Client) = {
    val scanner = client.hbaseClient.newScanner(table)
    scanner.setStartKey(startKey)
    scanner.setStopKey(stopKey)
    scanner
  }
}
