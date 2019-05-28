#!/bin/bash

. /common.sh

if ! getent hosts spark-master; then
  sleep 5
  exit 0
fi

if [ -z $MEMORY_LIMIT ]; then
  memory='1G'
else
  memory=$MEMORY_LIMIT
fi

/opt/spark/sbin/start-slave.sh spark://spark-master:7077 --webui-port 8081 --memory $memory --properties-file /opt/spark/conf/spark-defaults.conf

while :
do
  sleep 3
done