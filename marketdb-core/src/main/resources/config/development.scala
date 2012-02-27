import com.ergodicity.marketdb.core.MarketDBConfig
import com.twitter.ostrich.admin.config.{StatsConfig, JsonStatsLoggerConfig, TimeSeriesCollectorConfig}

new MarketDBConfig {
  admin.httpPort = 9000

  admin.statsNodes = new StatsConfig {
    reporters = new JsonStatsLoggerConfig {
      loggerName = "stats"
      serviceName = "kestrel"
    } :: new TimeSeriesCollectorConfig
  }
}
