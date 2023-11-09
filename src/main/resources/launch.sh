#!/usr/bin/env bash

export DIR="/root/ozonebulkdelete/"

echo "*** Starting to launch program ***"

    cd $DIR

echo "Launching jar via hadoop jar command"

    hadoop jar ozone-bulk-delete.jar /home/francois/francois.keytab francois majorque s3v 1 true

    sleep 2

echo "*** Finished program ***"