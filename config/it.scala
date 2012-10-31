import com.ergodicity.marketdb.MarketDbConfig

new MarketDbConfig {
  zookeeperQuorum = "marketdb-host"
  tradesTable = "test-market-trades"
  ordersTable = "test-market-orders"
  uidTable = "test-market-uid"
}
