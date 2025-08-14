#!/bin/bash

if [ -z "$PGVER" ] ;  then
    PGVER="17"
fi

if [ -z "$(ls /pg_lab)" ] ; then
    echo "[setup] No files found in /pg_lab. Starting setup"

    sudo chown -R $USERNAME:$USERNAME /pg_lab
    sudo chmod -R 755 /pg_lab

    git clone --depth 1 --branch=main https://github.com/rbergm/pg_lab /pg_lab
    if [ "$DEBUG" = "true" ]; then
        ./postgres-setup.sh --stop --pg-ver "$PGVER" --debug --remote-password "$USERNAME"
    else
        ./postgres-setup.sh --stop --pg-ver "$PGVER" --remote-password "$USERNAME"
    fi

    echo "cd /pg_lab/ && source ./postgres-load-env.sh" >> /home/$USERNAME/.bashrc

    echo "[setup] Installation complete. You can now connect to your database"
fi

cd /pg_lab/
source ./postgres-start.sh

tail -f /dev/null
