package com.ergodicity.marketdb

import com.ergodicity.marketdb.core.MarketDb.ClientBuilder
import com.ergodicity.marketdb.core.{MarketService, MarketDb}
import com.ergodicity.marketdb.uid.{UIDCache, UIDProvider}
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.ostrich.admin.config.ServerConfig
import com.twitter.ostrich.stats.Stats
import java.net.SocketAddress
import org.hbase.async.HBaseClient
import org.slf4j.LoggerFactory
import scalaz.Scalaz._

class MarketDbConfig extends ServerConfig[MarketDbApp] {
  val log = LoggerFactory.getLogger(classOf[MarketDbConfig])

  var socketAddress: Option[SocketAddress] = None

  var connection = Connection("127.0.0.1")
  var tradesTable = "market-trades"
  var ordersTable = "market-orders"
  var uidTable = "market-uid"

  var services: Seq[MarketDb => MarketService] = Seq()

  implicit object HBaseClientBuilder extends ClientBuilder {
    private val memo = mutableHashMapMemo[Connection, HBaseClient].apply(conn => new HBaseClient(conn.zookeeperQuorum))

    def apply(connection: Connection) = memo(connection)
  }

  def apply(runtime: RuntimeEnvironment) = {
    log.info("Build new marketDB configuration")
    log.debug("Connection = " + connection + "; Trades table = " + tradesTable + "; Orders table = " + ordersTable + "; UID table = " + uidTable)


    import MarketDb._
    val marketUIDProvider = new UIDProvider(connection, new UIDCache, ByteArray(uidTable), ByteArray("Market"), MarketIdWidth)
    val securityUIDProvider = new UIDProvider(connection, new UIDCache, ByteArray(uidTable), ByteArray("Security"), SecurityIdWidth)

    // Add statistics for UID caching
    Stats.addGauge("marketUid_cache_hits") {
      marketUIDProvider.cacheHits
    }
    Stats.addGauge("marketUid_cache_misses") {
      marketUIDProvider.cacheMisses
    }
    Stats.addGauge("securityUid_cache_hits") {
      securityUIDProvider.cacheHits
    }
    Stats.addGauge("securityUid_cache_misses") {
      securityUIDProvider.cacheMisses
    }

    val providers = UIDProviders(marketUIDProvider, securityUIDProvider)
    val tables = Tables(tradesTable, ordersTable)

    new MarketDbApp(socketAddress, new MarketDb(connection, providers, tables, services))
  }
}

case class KestrelConfig(hosts: Seq[String], tradesQueue: String, ordersQueue: String, hostConnectionLimit: Int)