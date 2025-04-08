#!/bin/bash

if [ "$1" == "--help" ] || [ "$1" == "-h" ] ; then
    echo "Usage: . ./postgres-load-env.sh [directory]"
    echo "  Initialize the shell environment for a specific Postgres installation."
    echo "  Initialization includes setting the general PATH, LD_LIBRARY_PATH, C_INCLUDE_PATH variables as well as the PG-specific PGDATA and PGPORT."
    echo "  This script must be executed from the pg_lab root directory, unless a path is provided (see below)."
    echo ""
    echo "Available options:"
    echo -e "  directory\tThe directory where the Postgres installation is located."
    echo -e "\t\tIf not provided, the script will look for the latest installation in the pg-build directory."
    echo -e "\t\tThe directory can either be supplied as an absolute path, as a relative path, or as the name of the installation directory in pg-build."
    echo -e "\t\tNotice that directories in pg-build overwrite relative paths in case of ambiguity."
    exit 0
fi

# Assert that we are sourcing
if [ -n "$BASH_VERSION" -a "$BASH_SOURCE" == "$0" ] || [ -n "$ZSH_VERSION" -a "$ZSH_EVAL_CONTEX" == "toplevel" ] ; then
    echo "This script must be sourced. Please run it as . ./postgres-load-env.sh" 1>&2
    exit 1
fi

WD=$(pwd)

if [ -z "$1" ] ; then
	PG_INSTALL_DIR=$(ls -d $WD/pg-build/*/ | grep "pg-" | sort | tail -n 1)
elif [[ "$1" = /* ]] ; then
	PG_INSTALL_DIR="$1"
else
    if [ -z "$(ls -F $WD/pg-build/ | grep -E '/$')" ] ; then
        echo "No PG installations found" 1>&2
        exit 1
    fi

    if [ $(ls -d $WD/pg-build/*/ | grep "$1") ] ; then
        PG_INSTALL_DIR="$WD/pg-build/$1"
    else
        PG_INSTALL_DIR="$WD/$1"
    fi
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
