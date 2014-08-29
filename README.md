# MarketDb

## Where to get it

To get the latest version of the library, add the following to your SBT build:

``` scala
resolvers += "Scalafi Bintray Repo" at "http://dl.bintray.com/ezhulenev/releases"
```

And use following library dependency:

```
libraryDependencies +=  "com.scalafi.marketdb" %% "marketdb-api" % "0.0.1"

## What is MarketDb

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
   

4. Checkout & build MarketDb

        $ git clone git://github.com/Ergodicity/marketdb.git
        $ cd ./marketdb
        $ sbt package

5. Execute commands from MarketDb root directory:

        $ ./install/create_tables.sh - create tables
        $ ./install/create_test_tables.sh - create tables for integration tests (with prefix 'test-')
    
    
6. Create MarketDb configuration.  
   MarketDb uses [Ostrich](https://github.com/twitter/ostrich) for configuration, hence you need to create Scala configuration before running. 

   ##### Configuration for local HBase & Kestrel

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
            new KestrelLoader(marketDB, KestrelConfig(Seq("localhost:22133"), 
                              "trades", "orders", hostConnectionLimit = 30))
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



7. Run MarketDb

            $ java -jar marketdb-0.1-SNAPSHOT.jar -f config.scala
            
   At this point you can access the MarketDb's admin page through 127.0.0.1:9000 (if it's running on your local machine).


## Filling MarketDb

MarketDb consumes market data from Kestrel queues used in configuration. How to write data into Kestrel queues could be found in [finagle-kestrel api documentation](http://twitter.github.com/finagle/api/finagle-kestrel/index.html) and [Finagle examples](https://github.com/twitter/finagle/blob/master/README.md#api-reference-documentation)

For particular code examples you may also take a look into marketdb-loader project which provides convinient way to load historical data into MarketDb from "Russian Trading System" Stock Exchange [trades result archive](http://www.rts.ru/s638).


## Querying MarketDb

MarketDb exposes binary API using [Twitter's Finagle](https://github.com/twitter/finagle) library to get Timeseries for further processing.

Timeseries processing relies on Enumeration based I/O with Iteratees, based on [Scalaz 6.0.4 Iteratees](https://github.com/scalaz/scalaz/blob/release/6.0.4/example/src/main/scala/scalaz/example/ExampleIteratee.scala). Rúnar Óli wrote a good [article](http://apocalisp.wordpress.com/2010/10/17/scalaz-tutorial-enumeration-based-io-with-iteratees/) about this approach for composable & functional input processing.

##### Scala MarketDb Client Implementation
    import com.ergodicity.marketdb.api._
    import com.ergodicity.marketdb.model._
    import com.ergodicity.marketdb.iteratee.TimeSeriesEnumerator._
    import java.net.InetSocketAddress
    import com.twitter.finagle.builder.ClientBuilder
    import com.twitter.util.Future
    import org.joda.time.DateTime
    import org.scala_tools.time.Implicits._
    
    
    object MarketDbClient {
      val market = Market("FORTS")
      val security = Security("RTS-3.13")
      val interval = new DateTime(2013, 1, 8, 10, 0) to new DateTime(2013, 1, 8, 19, 0)
    
      def main(args: Array[String]) {
        // Wrap the raw MarketDb service in a Client decorator. The client provides
        // a convenient procedural interface for accessing the MarketDb server.
        val marketdb = ClientBuilder()
          .codec(MarketDbCodec)
          .hosts(new InetSocketAddress(10033))
          .hostConnectionLimit(1)
          .build()
    
        val config: Future[MarketDbConfig] = marketdb(GetMarketDbConfig).map(_.asInstanceOf[MarketDbConfig])
        val trades: Future[Trades] = marketdb(ScanTrades(market, security, interval)).map(_.asInstanceOf[Trades])
        
        (config join trades) onSuccess {
          case (MarketDbConfig(connection), Trades(timeSeries)) =>
            implicit val marketDbReader = new MarketDbReader(connection)
    
            val enumerator = TimeSeriesEnumerator(timeSeries)
            val iterv = enumerator.enumerate(MarketIteratees.counter[TradePayload])
            val count = iterv.map(_.run)()
            
            System.out.println("Trades count = " + count + ", for given interval = " + interval)
    
        } onFailure {
          case cause => System.out.println("Failed with cause: " + cause)
        } ensure {
          marketdb.release()
        }
      }
    }
    
    
## Monitoring MarketDb
You can monitor MarketDb status and statistics using Ostrich admin console, which is configured using MarketDbConfig (admin.httpPort). See <a href="#configuration-for-local-hbase--kestrel">example cofniguration</a>

