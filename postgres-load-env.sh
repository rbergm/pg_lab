#!/bin/bash

WD=$(pwd)

if [ -z "$1" ] ; then
	PG_INSTALL_DIR=$(ls -d $WD/servers/*/ | grep "pg-" | tail -n 1)
elif [[ "$1" = /* ]] ; then
	PG_INSTALL_DIR="$1"
elif [ $(ls -d $WD/servers/*/ | grep "$1") ] ; then
	PG_INSTALL_DIR="$WD/servers/$1"
else
    PG_INSTALL_DIR="$WD/servers/$1"
fi

PG_INSTALL_DIR=${PG_INSTALL_DIR%/}
cd $PG_INSTALL_DIR

PG_BIN_PATH="$PG_INSTALL_DIR/bin"
INIT=$(echo "$PATH" | grep "$PG_BIN_PATH")
PGPORT=$(grep "port =" $PG_INSTALL_DIR/data/postgresql.conf | awk '{print $3}')

if [ -z "$INIT" ] ; then
    export PG_INSTALL_DIR
	export PG_BIN_PATH
	export PGPORT
	export PG_CTL_PATH="$WD"
	export PGDATA="$PG_INSTALL_DIR/data"
	export PATH="$PG_BIN_PATH:$PATH"
	export LD_LIBRARY_PATH="$PG_INSTALL_DIR/lib:$LD_LIBRARY_PATH"
	export C_INCLUDE_PATH="$PG_INSTALL_DIR/include/server:$C_INCLUDE_PATH"
fi

cd $WD
