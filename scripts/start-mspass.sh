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

  function start_mspass_frontend {
    if [ "$MSPASS_SCHEDULER" = "spark" ]; then
      export PYSPARK_DRIVER_PYTHON=jupyter
      export PYSPARK_DRIVER_PYTHON_OPTS="notebook --notebook-dir=${SPARK_LOG_DIR} --port=${JUPYTER_PORT} --no-browser --ip=0.0.0.0 --allow-root"
      /usr/sbin/tini -g -- pyspark \
        --conf "spark.mongodb.input.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
        --conf "spark.mongodb.output.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
        --conf "spark.master=spark://${MSPASS_SCHEDULER_ADDRESS}:${SPARK_MASTER_PORT}" \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0
    else # if [ "$MSPASS_SCHEDULER" = "dask" ]
      export DASK_SCHEDULER_ADDRESS=${MSPASS_SCHEDULER_ADDRESS}:${DASK_SCHEDULER_PORT}
      /usr/sbin/tini -g -- notebook --notebook-dir=${SPARK_LOG_DIR} --port=${JUPYTER_PORT} --no-browser --ip=0.0.0.0 --allow-root
    fi
  }

  MY_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
  if [ "$MSPASS_SCHEDULER" = "spark" ]; then
    MSPASS_SCHEDULER_CMD='$SPARK_HOME/sbin/start-master.sh'
    MSPASS_WORKER_CMD='$SPARK_HOME/sbin/start-slave.sh spark://$MSPASS_SCHEDULER_ADDRESS:$SPARK_MASTER_PORT'
  else # if [ "$MSPASS_SCHEDULER" = "dask" ]
    MSPASS_SCHEDULER_CMD='dask-scheduler --port $DASK_SCHEDULER_PORT > ${SPARK_LOG_DIR}/dask-scheduler_log_${MY_ID} 2>&1 & sleep 5'
    MSPASS_WORKER_CMD='dask-worker --local-directory $SPARK_LOG_DIR tcp://$MSPASS_SCHEDULER_ADDRESS:$DASK_SCHEDULER_PORT > ${SPARK_LOG_DIR}/dask-worker_log_${MY_ID} 2>&1 &'
  fi

  if [ "$MSPASS_ROLE" = "db" ]; then
    [[ -d $MONGO_DATA ]] || mkdir $MONGO_DATA
    mongod --port $MONGODB_PORT --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all
  elif [ "$MSPASS_ROLE" = "dbmanager" ]; then
    MONGODB_CONFIG_PORT=$(($MONGODB_PORT+1))
    [[ -d ${MONGO_DATA}_config ]] || mkdir ${MONGO_DATA}_config
    mongod --port $MONGODB_CONFIG_PORT --configsvr --replSet configserver --dbpath ${MONGO_DATA}_config --logpath ${MONGO_LOG}_config --bind_ip_all &
    sleep 5
    mongo --port $MONGODB_CONFIG_PORT --eval \
      "rs.initiate({_id: \"configserver\", configsvr: true, version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_CONFIG_PORT\" }]})"
    mongos --port $MONGODB_PORT --configdb configserver/$HOSTNAME:$MONGODB_CONFIG_PORT --logpath ${MONGO_LOG}_router --bind_ip_all &
    sleep 5
    for i in ${MSPASS_SHARD_LIST[@]}; do 
      mongo --port $MONGODB_PORT --eval "sh.addShard(\"${i}\")"
    done
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "shard" ]; then
    [[ -n $MSPASS_SHARD_ID ]] || MSPASS_SHARD_ID=$MY_ID
    [[ -d ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} ]] || mkdir ${MONGO_DATA}_shard_${MSPASS_SHARD_ID}
    mongod --port $MONGODB_PORT --shardsvr --dbpath ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} --logpath ${MONGO_LOG}_shard_${MSPASS_SHARD_ID}  --bind_ip_all
  elif [ "$MSPASS_ROLE" = "scheduler" ]; then
    eval $MSPASS_SCHEDULER_CMD
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "worker" ]; then
    eval $MSPASS_WORKER_CMD
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "frontend" ]; then
    start_mspass_frontend
  else # if [ "$MSPASS_ROLE" = "all" ]
    MSPASS_SCHEDULER_ADDRESS=127.0.0.1
    eval $MSPASS_SCHEDULER_CMD
    eval $MSPASS_WORKER_CMD
    [[ -d $MONGO_DATA ]] || mkdir $MONGO_DATA
    mongod --port $MONGODB_PORT --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all &
    start_mspass_frontend
else
  docker-entrypoint.sh $@
fi

