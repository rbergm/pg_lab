#!/bin/bash

WD=$(pwd)

. ./postgres-load-env.sh "$1"

cd $PG_INSTALL_DIR
pg_ctl -D $PG_INSTALL_DIR/data -l pg.log start
cd $WD
