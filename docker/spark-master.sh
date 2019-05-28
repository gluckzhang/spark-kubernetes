#!/bin/bash

. /common.sh

echo "$(hostname -i) spark-master" >> /etc/hosts

/opt/spark/sbin/start-master.sh --host spark-master --port 7077 --webui-port 8080 --properties-file /opt/spark/conf/spark-defaults.conf

tail -f /opt/spark/logs/$(ls /opt/spark/logs/)