import com.ergodicity.marketdb.core.{KestrelLoader, MarketDB, KestrelConfig, MarketDBConfig}
import com.twitter.ostrich.admin.config.{StatsConfig, JsonStatsLoggerConfig, TimeSeriesCollectorConfig}

new MarketDBConfig {
  admin.httpPort = 9000

  val kestrelLoaderService = (marketDB: MarketDB) => {
    new KestrelLoader(marketDB, KestrelConfig(Seq("localhost:22133"), "trades", 30))
  }

  services = Seq(kestrelLoaderService)

  admin.statsNodes = new StatsConfig {
    reporters = new JsonStatsLoggerConfig {
      loggerName = "stats"
      serviceName = "marketDB"
    } :: new TimeSeriesCollectorConfig
  }
}
