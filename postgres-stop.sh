#!/bin/bash

if [ "$1" = "--help" ] || [ "$1" = "-h" ] ; then
    echo "Usage: . ./postgres-start.sh [directory]"
    echo "  Stops a Postgres server and removes it from the environment."
    echo "  Removal includes the general PATH, LD_LIBRARY_PATH, C_INCLUDE_PATH variables as well as the PG-specific PGDATA and PGPORT."
    echo "  This script must be executed from the pg_lab root directory, unless a path is provided (see below)."
    echo ""
    echo "Available options:"
    echo -e "  directory\tThe directory where the Postgres installation is located."
    echo -e "\t\tIf not provided, the script will look for the latest installation in the pg-build directory."
    echo -e "\t\tThe directory can either be supplied as an absolute path, as a relative path, or as the name of the installation directory in pg-build."
    echo -e "\t\tNotice that directories in pg-build overwrite relative paths in case of ambiguity."
    exit 0
fi

if [ -n "$BASH_VERSION" -a "$BASH_SOURCE" = "$0" ] || [ -n "$ZSH_VERSION" -a "$ZSH_EVAL_CONTEXT" = "toplevel" ] ; then
    echo "WARNING: This script should be sourced to remove the Postgres binaries from PATH. Please run it as . ./postgres-start.sh" 1>&2
fi

WD=$(pwd)

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
