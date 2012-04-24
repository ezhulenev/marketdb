import com.ergodicity.marketdb.core.MarketDBConfig

new MarketDBConfig {
  zookeeperQuorum = "127.0.0.1"
  tradesTable = "test-market-trades"
  ordersTable = "test-market-orders"
  uidTable = "test-market-uid"
}
