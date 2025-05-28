#!/bin/bash

cd /pg_lab/
. ./postgres-load-env.sh
postgres -D $PGDATA
