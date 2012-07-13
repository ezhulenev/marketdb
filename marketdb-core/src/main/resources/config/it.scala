import com.ergodicity.marketdb.core.MarketDBConfig

new MarketDBConfig {
  zookeeperQuorum = "marketdb-host"
  tradesTable = "test-market-trades"
  ordersTable = "test-market-orders"
  uidTable = "test-market-uid"
}
