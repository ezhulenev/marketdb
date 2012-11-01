package com.ergodicity.marketdb

import com.ergodicity.marketdb.core.{MarketService, MarketDb}
import com.ergodicity.marketdb.uid.{UIDCache, UIDProvider}
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.stats.Stats
import org.hbase.async.HBaseClient
import org.slf4j.LoggerFactory

class MarketDbConfig extends ServerConfig[MarketDb] {
  val log = LoggerFactory.getLogger(classOf[MarketDbConfig])

  var zookeeperQuorum = "127.0.0.1"
  var tradesTable = "market-trades"
  var ordersTable = "market-orders"
  var uidTable = "market-uid"

  var services: Seq[MarketDb => MarketService] = Seq()

  def apply(runtime: RuntimeEnvironment) = {
    log.info("Build new marketDB configuration")
    log.debug("ZooKeeper quorum = " + zookeeperQuorum + "; Trades table = " + tradesTable + "; Orders table = " + ordersTable + "; UID table = " + uidTable)

    val client = new Client(zookeeperQuorum)

    import MarketDb._
    val marketUIDProvider = new UIDProvider(client, new UIDCache, ByteArray(uidTable), ByteArray("Market"), MarketIdWidth)
    val securityUIDProvider =  new UIDProvider(client, new UIDCache, ByteArray(uidTable), ByteArray("Security"), SecurityIdWidth)

    // Add statistics for UID caching
    Stats.addGauge("marketUid_cache_hits") {marketUIDProvider.cacheHits}
    Stats.addGauge("marketUid_cache_misses") {marketUIDProvider.cacheMisses}
    Stats.addGauge("securityUid_cache_hits") {securityUIDProvider .cacheHits}
    Stats.addGauge("securityUid_cache_misses") {securityUIDProvider .cacheMisses}

    new MarketDb(client, marketUIDProvider, securityUIDProvider , tradesTable, ordersTable, services)
  }
}

case class KestrelConfig(hosts: Seq[String], tradesQueue: String, ordersQueue: String, hostConnectionLimit: Int)