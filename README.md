MarketDb is built using [sbt](http://code.google.com/p/simple-build-tool/wiki/RunningSbt). To build:

    $ ./sbt package


# MarketDb

MarketDb is a distributed, scalable Time Series Database written on top of HBase, inspired by [OpenTSDB](https://github.com/OpenTSDB/opentsdb), focused on Financial Market time series. MarketDb was written to address a common need: store, index and serve market time series (trades and orders) collected from market data providers and make this data conveniently accessible for backtesting and strategies simulation.

MarketDb written in scala and provides functional [Iteratee](http://jsuereth.com/scala/2012/02/29/iteratees.html) based timeseries processing.

## Install
