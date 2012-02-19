#!/bin/sh
# Small script to drop the HBase tables used by MarketDB for tests execution

export TRADES_TABLE='test-market-trades'
export UID_TABLE='test-market-uid'
export COMPRESSION=NONE

# Execute original script
source $(dirname $0)/../install/drop_tables.sh