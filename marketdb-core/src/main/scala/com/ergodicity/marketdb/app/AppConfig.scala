package com.ergodicity.marketdb.app

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.{Bean, Configuration}
import org.hbase.async.HBaseClient
import com.ergodicity.marketdb.core.MarketDB
import com.ergodicity.marketdb.uid.{UIDCache, UIDProvider}
import com.ergodicity.marketdb.ByteArray
import org.slf4j.LoggerFactory

@Configuration
class AppConfig {
  val log = LoggerFactory.getLogger(classOf[AppConfig])

  log.info("Instantiate MarketDB App Configuration")

  @Value("${hbase.zookeeper.quorum}")
  var quorumUrl: String = _

  @Value("${marketdb.table.trades}")
  var tradesTableName: String = _

  @Value("${marketdb.table.uid}")
  var uidTableName: String = _


  @Bean(destroyMethod = "disconnectEverything")
  def hbaseClient(): HBaseClient = {
    new HBaseClient(quorumUrl)
  }

  @Bean(name = Array[String]("marketUidProvider"))
  def marketUidProvider(): UIDProvider = {
    import MarketDB._
    new UIDProvider(hbaseClient(), new UIDCache, ByteArray(uidTableName), ByteArray("Market"), MarketIdWidth)
  }

  @Bean(name = Array[String]("codeUidProvider"))
  def codeUidProvider(): UIDProvider = {
    import MarketDB._
    new UIDProvider(hbaseClient(), new UIDCache, ByteArray(uidTableName), ByteArray("Code"), CodeIdWidth)
  }

  @Bean(name = Array[String]("marketDB"))
  def marketDB(): MarketDB = {
    new MarketDB(hbaseClient(), marketUidProvider(), codeUidProvider(), tradesTableName)
  }
}
