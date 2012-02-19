package com.ergodicity.marketdb

import core.MarketDB
import model.Trade
import org.hbase.async.HBaseClient
import org.springframework.context.annotation.{ImportResource, Bean, Configuration}
import org.springframework.beans.factory.annotation.Value
import uid.{UIDCache, UIDProvider}


@Configuration
@ImportResource(Array[String] {
  "classpath:test-properties-config.xml"
})
class IntegrationTestConfiguration {

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
}