MarketDb is built using [sbt](http://code.google.com/p/simple-build-tool/wiki/RunningSbt). To build:

    $ ./sbt package


# MarketDb

MarketDb is a distributed, scalable Time Series Database written on top of HBase, inspired by [OpenTSDB](https://github.com/OpenTSDB/opentsdb), focused on Financial Market time series. MarketDb was written to address a common need: store, index and serve market time series (trades and orders) collected from market data providers and make this data conveniently accessible for backtesting and strategies simulation.

MarketDb written in scala and provides functional [Iteratee](http://jsuereth.com/scala/2012/02/29/iteratees.html) style timeseries processing.

## Quick Start
1. Install & Run [HBase](http://hbase.apache.org/). (How to install HBase on Windows could be found [here](http://hbase.apache.org/))
2. Install & Run [Kestrel](https://github.com/robey/kestrel) - distributed message queue which is used to deliver market data to MarketDb
3. Setup environment variables:

        HBASE_HOME=[path to HBase installation directory]        
        TRADES_TABLE=[table name for trades time series, default='market-trades']
        ORDERS_TABLE=[table name for orders time series, default='market-orders']
        UID_TABLE=[table name for generated UID, default='market-uid']
        COMPRESSION=[HBase compression, default='NONE']        
   More about compression levels could be found [here](http://wiki.apache.org/hadoop/UsingLzoCompression)
   

4. Clone MarketDb repository
5. Execute commands from MarketDb root directory:

        $ ./install/create_tables.sh - create tables
        $ ./install/create_test_tables.sh - create tables for integration tests (with prefix 'test-')
6. Build MarketDb

        $ ./sbt package
    
    
7. Create MarketDb configuration.  
   MarketDb uses [Ostrich](https://github.com/twitter/ostrich) for configuration, hence you need to create Scala configuration before running. 

   Configuration for HBase & Kestrel installed locally:

        import com.ergodicity.marketdb.core.MarketDb
        import com.ergodicity.marketdb.{MarketDbConfig, KestrelLoader, KestrelConfig}
        import com.twitter.ostrich.admin.config._
        import java.net.InetSocketAddress
        
        new MarketDbConfig {
          // HBase connection: ZookeeperQuorumUrl
          connection = Connection("localhost")
        
          // HBase tables
          tradesTable = "market-trades"
          ordersTable = "market-orders"
          uidTable = "market-uid"
        
          // Socked address used for exposing MarketDbService
          socketAddress = Some(new InetSocketAddress(10333))
          
          
          // Connection to running Kestrel (possibly multiple applications)
          // And queue names to consume Trades & Orders
          val kestrelLoaderService = (marketDB: MarketDb) => {
            new KestrelLoader(marketDB, KestrelConfig(Seq("localhost:22133"), "trades", "orders", hostConnectionLimit = 30))
          }
    
          // Services to start right after MarketDb itself
          services = Seq(kestrelLoaderService)
        
          // HTTP port for Ostrich admin console
          admin.httpPort = 9000
          
          // Ostrich statistics
          admin.statsNodes = new StatsConfig {
            reporters = new JsonStatsLoggerConfig {
              loggerName = "stats"
              serviceName = "marketDB"
            } :: new TimeSeriesCollectorConfig
          }
        }



8. Run MarketDb


            java -jar marketdb-0.1-SNAPSHOT.jar -f config.scala


        
