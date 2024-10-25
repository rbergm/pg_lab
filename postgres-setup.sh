#!/bin/bash

set -e  # exit on error

WD=$(pwd)
USER=$(whoami)
PG_VER_PRETTY=17
PG_VERSION=REL_17_STABLE
PG_PATCH=$WD/pg-patches/pg_lab-pg17.patch
PG_PATCH_DIR=$WD/pg-patches
PG_TARGET_DIR="$WD/postgres-server"
PG_DEFAULT_PORT=5432
PG_PORT=5432
MAKE_CORES=$(($(nproc --all) / 2))
ENABLE_REMOTE_ACCESS="false"
USER_PASSWORD=""
STOP_AFTER="false"
PG_BUILDOPTS=""

show_help() {
    RET=$1
    NEWLINE="\n\t\t\t\t"
    echo -e "Usage: $0 <options>"
    echo -e "Setup a local Postgres server with the given options. The default user is the current UNIX username.\n"
    echo -e "Allowed options:"
    echo -e "--pg-ver <version>\t\tSetup Postgres with the given version.${NEWLINE}Currently allowed values: 16, 17 (default))"
    echo -e "-d | --dir <directory>\t\tInstall Postgres server to the designated directory (postgres-server by default)."
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
                    PG_PATCH=$PG_PATCH_DIR/pg_lab-pg16.patch
                    ;;
                17)
                    PG_VER_PRETTY="17"
                    PG_VERSION=REL_17_STABLE
                    PG_PATCH=$PG_PATCH_DIR/pg_lab-pg17.patch
                    ;;
                *)
                    show_help
                    ;;
            esac
            shift
            shift
            ;;
        -d|--dir)
            if [[ "$2" = /* ]] ; then
                PG_TARGET_DIR=$2
            else
                PG_TARGET_DIR=$WD/$2
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
            PG_BUILDOPTS="--enable-debug --enable-cassert CFLAGS=\"-O0 -DOPTIMIZER_DEBUG\""
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

PG_BUILD_DIR=$PG_TARGET_DIR/dist
export PGDATA=$PG_BUILD_DIR/data


if [ -d $PG_TARGET_DIR ] ; then
    echo ".. Directory $PG_TARGET_DIR already exists, assuming this is a (patched) Postgres installation and re-using"
else
    echo ".. Cloning Postgres $PG_VER_PRETTY"
    git clone --depth 1 --branch $PG_VERSION https://github.com/postgres/postgres.git $PG_TARGET_DIR

    cd $PG_TARGET_DIR
    echo ".. Applying pg_lab patches for Postgres $PG_VER_PRETTY"
    git apply $PG_PATCH
fi

cd $PG_TARGET_DIR
echo ".. Building Postgres $PG_VER_PRETTY"
./configure --prefix=$PG_TARGET_DIR/dist $PG_BUILDOPTS
make clean && make -j $MAKE_CORES && make install
export PATH="$PG_BUILD_DIR/bin:$PATH"
export LD_LIBRARY_PATH="$PG_BUILD_DIR/lib:$LD_LIBRARY_PATH"
export C_INCLUDE_PATH="$PG_BUILD_DIR/include/server:$C_INCLUDE_PATH"

echo ".. Installing pg_prewarm extension"
cd $PG_TARGET_DIR/contrib/pg_prewarm
make -j $MAKE_CORES && make install
cd $PG_TARGET_DIR

echo ".. Installing pg_buffercache extension"
cd $PG_TARGET_DIR/contrib/pg_buffercache
make -j $MAKE_CORES && make install
cd $PG_TARGET_DIR


echo ".. Installing pg_lab extension"
mkdir -p $WD/pg_lab/build
cd $WD/pg_lab/build
cmake ..
make -j $MAKE_CORES
ln -s $PWD/libpg_lab.so $PG_BUILD_DIR/lib/pg_lab.so


echo ".. Initializing Postgres Server environment"
cd $PG_TARGET_DIR

echo "... Creating cluster"
initdb -D $PG_BUILD_DIR/data

if [ "$PG_PORT" != "$PG_DEFAULT_PORT" ] ; then
    echo "... Updating Postgres port to $PG_PORT"
    sed -i "s/#\{0,1\}port = 5432/port = $PG_PORT/" $PGDATA/postgresql.conf
fi

echo "... Adding pg_buffercache, pg_hint_plan and pg_prewarm to preload libraries"
sed -i "s/#\{0,1\}shared_preload_libraries.*/shared_preload_libraries = 'pg_buffercache,pg_lab,pg_prewarm'/" $PGDATA/postgresql.conf
echo "pg_prewarm.autoprewarm = false" >>  $PGDATA/postgresql.conf

echo "... Starting Postgres (log file is pg.log)"
pg_ctl -D $PGDATA -l pg.log start

echo "... Creating user database for $USER"
createdb -p $PG_PORT $USER

if [ "$ENABLE_REMOTE_ACCESS" == "true" ] ; then
    echo "... Enabling remote access for $USER"
    echo -e "#customization\nhost all $USER 0.0.0.0/0 md5" >> $PGDATA/pg_hba.conf
    sed -i "s/#\{0,1\}listen_addresses = 'localhost'/listen_addresses = '*'/" $PGDATA/postgresql.conf
    psql -c "ALTER USER $USER WITH PASSWORD '$USER_PASSWORD';"
fi

if [ "$STOP_AFTER" == "true" ] ; then
    pg_ctl -D $PGDATA stop
    echo ".. Setup done"
else
    echo ".. Setup done, ready to connect"
fi

cd $WD
