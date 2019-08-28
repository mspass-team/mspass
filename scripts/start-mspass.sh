#!/bin/bash

if [ "$MSPASS_ROLE" = "master" ]; then
  $SPARK_HOME/sbin/start-master.sh
  mongod --dbpath $MONGO_DATA --logpath $MONGO_LOG
fi
if [ "$MSPASS_ROLE" = "worker" ]; then
  $SPARK_HOME/sbin/start-slave.sh spark://$SPARK_MASTER:$SPARK_MASTER_PORT
fi
