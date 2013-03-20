package com.ergodicity.marketdb.iteratee

import com.ergodicity.marketdb.Connection
import org.hbase.async.HBaseClient

class MarketDbReader(connection: Connection) {
  lazy val client = new HBaseClient(connection.zookeeperQuorum)

  def shutdown() = client.shutdown().joinUninterruptibly()
}