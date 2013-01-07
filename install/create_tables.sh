#!/bin/sh
# Small script to setup the HBase tables used by MarketDb.

test -n "$HBASE_HOME" || {
  echo >&2 'The environment variable HBASE_HOME must be set'
  exit 1
}
test -d "$HBASE_HOME" || {
  echo >&2 "No such directory: HBASE_HOME=$HBASE_HOME"
  exit 1
}


TRADES_TABLE=${TRADES_TABLE-'market-trades'}
ORDERS_TABLE=${ORDERS_TABLE-'market-orders'}
UID_TABLE=${UID_TABLE-'market-uid'}
BLOOMFILTER=${BLOOMFILTER-'ROW'}
# LZO requires lzo2 64bit to be installed + the hadoop-gpl-compression jar.
COMPRESSION=${COMPRESSION-'None'}

echo "Going to create tables: "
echo " - Trades table: $TRADES_TABLE"
echo " - Orders table: $ORDERS_TABLE"
echo " - UID table:    $UID_TABLE"

# HBase scripts also use a variable named `HBASE_HOME', and having this
# variable in the environment with a value somewhat different from what
# they expect can confuse them in some cases.  So rename the variable.
hbh=$HBASE_HOME
unset HBASE_HOME
exec "$hbh/bin/hbase" shell <<EOF
create '$UID_TABLE',
  {NAME => 'id', COMPRESSION => '$COMPRESSION'},
  {NAME => 'name', COMPRESSION => '$COMPRESSION'}

create '$TRADES_TABLE',
  {NAME => 'id', VERSIONS => 1, COMPRESSION => '$COMPRESSION', BLOOMFILTER => '$BLOOMFILTER'}

create '$ORDERS_TABLE',
    {NAME => 'id', VERSIONS => 1, COMPRESSION => '$COMPRESSION', BLOOMFILTER => '$BLOOMFILTER'}
EOF
