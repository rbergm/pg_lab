#!/bin/bash

if [ -z "$PGVER" ] ;  then
    PGVER="18"
fi

if [ -z "$(ls /pg_lab)" ] ; then
    echo "[setup] No files found in /pg_lab. Starting setup"

    sudo chown -R $USERNAME:$USERNAME /pg_lab
    sudo chmod -R 755 /pg_lab

    git clone --depth 1 --branch=main https://github.com/Optimizer-Playground/pg_lab /pg_lab
    if [ "$DEBUG" = "true" ]; then
        ./postgres-setup.sh --stop --pg-ver "$PGVER" --debug --remote-password "$USERNAME"
    else
        ./postgres-setup.sh --stop --pg-ver "$PGVER" --remote-password "$USERNAME"
    fi

    cd /pg_lab/
    source ./postgres-start.sh

    if [ "$SETUP_JOB" = "true" ] | [ "$SETUP_IMDB" = "true" ] ; then
        echo "[setup] Setting up JOB/IMDB database"
        wget https://raw.githubusercontent.com/Optimizer-Playground/PostBOUND/refs/heads/main/db-support/postgres/workload-job-setup.sh -O /pg_lab/workload-job-setup.sh
        chmod +x /pg_lab/workload-job-setup.sh
        mkdir -p /pg_lab/datasets/
        /pg_lab/workload-job-setup.sh -d /pg_lab/datasets/imdb
    fi

    if [ "$SETUP_STATS" = "true" ] ; then
        echo "[setup] Setting up Stats database"
        wget https://raw.githubusercontent.com/Optimizer-Playground/PostBOUND/refs/heads/main/db-support/postgres/workload-stats-setup.sh -O /pg_lab/workload-stats-setup.sh
        chmod +x /pg_lab/workload-stats-setup.sh
        mkdir -p /pg_lab/datasets/
        /pg_lab/workload-stats-setup.sh -d /pg_lab/datasets/stats
    fi

    if [ "$SETUP_STACK" = "true" ] ; then
        echo "[setup] Setting up Stack database"
        wget https://raw.githubusercontent.com/Optimizer-Playground/PostBOUND/refs/heads/main/db-support/postgres/workload-stack-setup.sh -O /pg_lab/workload-stack-setup.sh
        chmod +x /pg_lab/workload-stack-setup.sh
        mkdir -p /pg_lab/datasets/
        /pg_lab/workload-stack-setup.sh -d /pg_lab/datasets/stack
    fi

    source ./postgres-stop.sh

    echo "cd /pg_lab/ && source ./postgres-load-env.sh" >> /home/$USERNAME/.bashrc

    echo "[setup] Installation complete. You can now connect to your database"
fi

cd /pg_lab/
source ./postgres-start.sh

tail -f /dev/null
