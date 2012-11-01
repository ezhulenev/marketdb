package com.ergodicity.marketdb

import org.hbase.async.HBaseClient

class Client(private[marketdb] val zookeeperQuorum: String) {
  lazy val hbaseClient = new HBaseClient(zookeeperQuorum)

  def apply() = hbaseClient

  def shutdown() {
    hbaseClient.shutdown().joinUninterruptibly()
  }
}