#!/bin/bash

WD=$PWD
NCORES=$(($(nproc --all) / 2))
PG_PATH="../postgres-server"

if [ "$1" = "--help" ] ; then
    echo "Usage: $0 [path-to-postgres-source]"
    echo " Generates a new patch file for the current changes in the postgres source"
    exit 0
fi

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
