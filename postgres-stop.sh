#!/bin/bash

WD=$(pwd)

echo ".. Stopping Postgres Server"
. ./postgres-load-env.sh "$1"

cd $PG_INSTALL_DIR
pg_ctl -D $PG_INSTALL_DIR/data stop
export PATH="${PATH//$PG_BIN_PATH:}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH//$PG_INSTALL_DIR/lib:}"
export C_INCLUDE_PATH="${C_INCLUDE_PATH//$PG_INSTALL_DIR/include/server:}"

if [ -z "$PG_INSTALL_DIR" ] ; then
    unset PG_INSTALL_DIR
fi
if [ -z "$PG_BIN_PATH" ] ; then
	unset PG_BIN_PATH
fi
if [ -z "$PGPORT" ] ; then
	unset PGPORT
fi
if [ -z "$PGDATA" ] ; then
    unset PGDATA
fi

cd $WD
