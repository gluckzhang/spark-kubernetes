#!/bin/bash

. /common.sh

if ! getent hosts spark-master; then
  sleep 5
  exit 0
fi

start-slave.sh spark://spark-master:7077 --webui-port 8081 --properties-file /opt/spark/conf/spark-defaults.conf