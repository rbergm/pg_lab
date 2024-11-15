#!/bin/bash

WD=$PWD
NCORES=$(($(nproc --all) / 2))
PG_PATH="../postgres-server"

if [ ! -z "$1" ]; then
    PG_PATH=$1
fi


cd $PG_PATH
make -j $NCORES && make install
if [ $? -ne 0 ]; then
    echo "Could not build patch - changes contain compile errors"
    exit 1
fi

PG_VERSION=$(grep '#define PG_MAJORVERSION_NUM' src/include/pg_config.h | awk '{print $3}')

git diff > "$WD/pg_lab-pg$PG_VERSION.patch"

cd $WD
