import com.ergodicity.marketdb.{Connection, MarketDbConfig}

new MarketDbConfig {
  connection = Connection("marketdb-host")
  tradesTable = "test-market-trades"
  ordersTable = "test-market-orders"
  uidTable = "test-market-uid"
}
