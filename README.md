MarketDb is built using [sbt](http://code.google.com/p/simple-build-tool/wiki/RunningSbt). To build:

    $ ./sbt package


# MarketDb

MarketDb is a distributed, scalable Time Series Database written on top of HBase, inspired by [OpenTSDB](https://github.com/OpenTSDB/opentsdb), focused on Financial Market time series. MarketDb was written to address a common need: store, index and serve market time series (trades and orders) collected from market data providers and make this data conveniently accessible for backtesting and strategies simulation.

MarketDb written in scala and provides functional [Iteratee](http://jsuereth.com/scala/2012/02/29/iteratees.html) style timeseries processing.

## Quick Start
1. Install [HBase](http://hbase.apache.org/). (How to install HBase on Windows could be found [here](http://hbase.apache.org/))
2. Setup environment variables:

        HBASE_HOME=[path to HBase installation directory]        
        TRADES_TABLE=[table name for trades time series, default='market-trades']
        ORDERS_TABLE=[table name for orders time series, default='market-orders']
        UID_TABLE=[table name for generated UID, default='market-uid']
        COMPRESSION=[HBase compression, default='NONE']        
   More about compression levels could be found [here](http://wiki.apache.org/hadoop/UsingLzoCompression)
   

3. Clone MarketDb repository
4. Execute commands from MarketDb root directory:

        ./install/create_tables.sh - create tables
        ./install/create_test_tables.sh - create tables for integration tests (with prefix 'test-')
