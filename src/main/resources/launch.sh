#!/usr/bin/env bash

export DIR="/root/ozonebulkdelete/"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via hadoop jar command"

    hadoop jar ozone-bulk-delete.jar $@

    sleep 2

echo "*** Finished program ***"