#!/bin/bash

if grep docker /proc/1/cgroup -qa; then
  export SPARK_LOG_DIR=/home
  MONGO_DATA=/home/data
  MONGO_LOG=/home/mongo_log
else
  export SPARK_LOG_DIR=$PWD
  MONGO_DATA=${PWD%/}/data
  MONGO_LOG=${PWD%/}/mongo_log
fi

if [ $# -eq 0 ]; then
  if [ "$MSPASS_ROLE" = "master" ]; then
    $SPARK_HOME/sbin/start-master.sh
    [[ -d $MONGO_DATA ]] || mkdir $MONGO_DATA
    mongod --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all
  elif [ "$MSPASS_ROLE" = "worker" ]; then
    $SPARK_HOME/sbin/start-slave.sh spark://$SPARK_MASTER:$SPARK_MASTER_PORT
    tail -f /dev/null
  fi
else
  docker-entrypoint.sh $@
fi

