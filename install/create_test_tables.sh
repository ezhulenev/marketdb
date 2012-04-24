#!/bin/sh
# Small script to setup the HBase tables used by MarketDB for tests execution

export TRADES_TABLE='test-market-trades'
export ORDERS_TABLE='test-market-orders'
export UID_TABLE='test-market-uid'
export COMPRESSION=NONE

# Execute original script
source $(dirname $0)/../install/create_tables.sh