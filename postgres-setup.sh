#!/bin/bash

sed_wrapper() {
    REPLACEMENT="$1"
    FILE="$2"

    if [ "$OS_TYPE" = "Darwin" ]; then
        sed -i '' -e "$REPLACEMENT" "$FILE"
    else
        sed -i -e "$REPLACEMENT" "$FILE"
    fi
}

# Assert that we are not sourcing
if [ -n "$BASH_VERSION" -a "$BASH_SOURCE" != "$0" ] || [ -n "$ZSH_VERSION" -a "$ZSH_EVAL_CONTEXT" != "toplevel" ] ; then
    echo "This script should not be sourced. Please run it as ./postgres-setup.sh" 1>&2
    return 1
fi

set -e  # exit on error

WD=$(pwd)
USER=$(whoami)
PG_VER_PRETTY=17
PG_VERSION=REL_17_STABLE
PG_SRC_DIR="$WD/pg-source"
PG_TARGET_DIR="$WD/pg-build"
FORCE_TARGET_DIR="false"
PG_DEFAULT_PORT=5432
PG_PORT=$PG_DEFAULT_PORT
ENABLE_REMOTE_ACCESS="false"
USER_PASSWORD=""
STOP_AFTER="false"
DEBUG_BUILD="false"

OS_TYPE=$(uname)
if [[ "$OS_TYPE" = "Darwin" ]]; then
    MAKE_CORES=$(sysctl -n hw.logicalcpu)
    UUID_FLAGS="--with-uuid=e2fs"
elif [[ "$OS_TYPE" = "Linux" ]]; then
    MAKE_CORES=$(nproc)
    UUID_FLAGS="--with-uuid=ossp"
else
    echo "Unsupported operating system: $OS_TYPE"
    exit 1
fi

show_help() {
    RET=$1
    NEWLINE="\n\t\t\t\t"
    echo -e "Usage: $0 <options>"
    echo -e "Setup a local Postgres server with the given options. The default user is the current UNIX username.\n"
    echo -e "Allowed options:"
    echo -e "--pg-ver <version>\t\tSetup Postgres with the given version.${NEWLINE}Currently allowed values: 16, 17 (default))"
    echo -e "-d | --dir <directory>\t\tInstall Postgres server to the designated directory (pg-build by default)."
    echo -e "-p | --port <port number>\tConfigure the Postgres server to listen on the given port (5432 by default)."
    echo -e "--remote-password <password>\tEnable remote access for the current user, based on the given password.${NEWLINE}Remote access is disabled if no password is provided."
    echo -e "--debug\t\t\t\tProduce a debug build of the Postgres server"
    echo -e "--stop\t\t\t\tStop the Postgres server process after installation and setup finished"
    exit $RET
}

while [ $# -gt 0 ] ; do
    case $1 in
        --pg-ver)
            case $2 in
                16)
                    PG_VER_PRETTY="16"
                    PG_VERSION=REL_16_STABLE
                    ;;
                17)
                    PG_VER_PRETTY="17"
                    PG_VERSION=REL_17_STABLE
                    ;;
                *)
                    show_help
                    ;;
            esac
            shift
            shift
            ;;
        -d|--dir)
            FORCE_TARGET_DIR="true"
            if [[ "$2" = /* ]] ; then
                PG_TARGET_DIR=$2
            else
                PG_TARGET_DIR="$WD/$2"
                echo "... Normalizing relative target directory to $PG_TARGET_DIR"
            fi
            shift
            shift
            ;;
        -p|--port)
            PG_PORT=$2
            shift
            shift
            ;;
        --remote-password)
            ENABLE_REMOTE_ACCESS="true"
            USER_PASSWORD=$2
            shift
            shift
            ;;
        --debug)
            DEBUG_BUILD="true"
            shift
            ;;
        --stop)
            STOP_AFTER="true"
            shift
            ;;
        --help)
            show_help 0
            ;;
        *)
            show_help 1
            ;;
    esac
done

if [ "$FORCE_TARGET_DIR" = "false" ] ; then
    PG_TARGET_DIR="$PG_TARGET_DIR/pg-$PG_VER_PRETTY"
fi
export PGDATA="$PG_TARGET_DIR/data"

echo ".. Building Postgres $PG_VER_PRETTY"
cd $PG_SRC_DIR
git submodule update --init
git switch $PG_VERSION && git pull

if [ "$DEBUG_BUILD" = "true" ] ; then
    ./configure --prefix=$PG_TARGET_DIR \
        --with-ssl=openssl \
        --with-python \
        --with-llvm \
        --with-lz4 \
        --with-zstd \
        --enable-debug \
        --enable-cassert \
        $UUID_FLAGS CFLAGS='-Og'
else
    ./configure --prefix=$PG_TARGET_DIR \
        --with-ssl=openssl \
        --with-python \
        --with-llvm \
        --with-lz4 \
        --with-zstd \
        $UUID_FLAGS
fi

make clean && make -j $MAKE_CORES && make install
export PATH="$PG_TARGET_DIR/bin:$PATH"
export LD_LIBRARY_PATH="$PG_TARGET_DIR/lib:$LD_LIBRARY_PATH"
export C_INCLUDE_PATH="$PG_TARGET_DIR/include/server:$C_INCLUDE_PATH"

echo ".. Installing pg_prewarm extension"
cd $PG_SRC_DIR/contrib/pg_prewarm
make -j $MAKE_CORES && make install

echo ".. Installing pg_buffercache extension"
cd $PG_SRC_DIR/contrib/pg_buffercache
make -j $MAKE_CORES && make install


echo ".. Installing pg_lab extension"
PGLAB_DIR=$WD/extensions/pg_lab
mkdir -p $PGLAB_DIR/build && cd $PGLAB_DIR/build

echo ".. Installing pg_temperature extension"
cd $WD/extensions/pg_temperature
make && make install

if [ "$DEBUG_BUILD" = "true" ] ; then
    cmake -DCMAKE_BUILD_TYPE=Debug -DPG_INSTALL_DIR="$PG_TARGET_DIR" ..
else
    cmake -DCMAKE_BUILD_TYPE=Release -DPG_INSTALL_DIR="$PG_TARGET_DIR" ..
fi

make -j $MAKE_CORES


echo ".. Initializing Postgres Server environment"
cd $PG_TARGET_DIR

SERVER_STARTED="true"
if [ -d "$PGDATA" ] ; then
    SERVER_STARTED="false"
    echo "... Data directory exists, leaving as-is"
else
    echo "... Creating cluster"
    initdb -D $PGDATA

    if [ "$PG_PORT" != "$PG_DEFAULT_PORT" ] ; then
        echo "... Updating Postgres port to $PG_PORT"
        sed_wrapper "s/#\{0,1\}port = 5432/port = $PG_PORT/" $PGDATA/postgresql.conf
    fi

    echo "... Adding pg_buffercache, pg_lab and pg_prewarm to preload libraries"
    sed_wrapper "s/#\{0,1\}shared_preload_libraries.*/shared_preload_libraries = 'pg_buffercache,pg_prewarm,pg_temperature,pg_lab'/" $PGDATA/postgresql.conf
    echo "pg_prewarm.autoprewarm = false" >>  $PGDATA/postgresql.conf

    echo "... Starting Postgres (log file is $PGDATA/pg.log)"
    pg_ctl -D $PGDATA -l "$PGDATA/pg.log" start

    echo "... Creating user database for $USER"
    createdb -p $PG_PORT $USER

    if [ "$ENABLE_REMOTE_ACCESS" = "true" ] ; then
        echo "... Enabling remote access for $USER"
        echo -e "#customization\nhost all $USER 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
        sed_wrapper "s/#\{0,1\}listen_addresses = 'localhost'/listen_addresses = '*'/" $PGDATA/postgresql.conf
        psql -c "ALTER USER $USER WITH PASSWORD '$USER_PASSWORD';"
    fi
fi

if [ "$STOP_AFTER" = "true" -a "$SERVER_STARTED" = "true" ] ; then
    pg_ctl -D $PGDATA stop
    echo ".. Setup done"
else
    echo ".. Setup done, ready to connect"
fi

cd $WD
