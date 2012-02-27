package com.ergodicity.marketdb.core

import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.admin.RuntimeEnvironment
import org.hbase.async.HBaseClient
import com.ergodicity.marketdb.uid.{UIDCache, UIDProvider}
import com.ergodicity.marketdb.ByteArray
import org.slf4j.LoggerFactory

class MarketDBConfig extends ServerConfig[MarketDB] {
  val log = LoggerFactory.getLogger(classOf[MarketDBConfig])

  var zookeeperQuorum = "127.0.0.1"
  var tradesTable = "market-trades"
  var uidTable = "market-uid"
  
  def apply(runtime: RuntimeEnvironment) = {
    log.info("Build new marketDB configuration")

    val client = new HBaseClient(zookeeperQuorum)

    import MarketDB._
    val marketUIDProvider = new UIDProvider(client, new UIDCache, ByteArray(uidTable), ByteArray("Market"), MarketIdWidth)
    val codeUIDProvider =  new UIDProvider(client, new UIDCache, ByteArray(uidTable), ByteArray("Code"), CodeIdWidth)

    new MarketDB(client, marketUIDProvider, codeUIDProvider, tradesTable)
  }
}