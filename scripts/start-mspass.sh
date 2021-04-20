#!/bin/bash

# If running with docker use /home, else use pwd to store all data and logs
if grep docker /proc/1/cgroup -qa; then
  _MY_HOME=/home
else
  _MY_HOME=${PWD%/}
fi

MSPASS_DB_DIR=${_MY_HOME}/db
MSPASS_LOG_DIR=${_MY_HOME}/logs
MSPASS_WORKER_DIR=${_MY_HOME}/work
# Note that only log is required for all roles. Other dirs will be created later when needed.
[[ -d $MSPASS_LOG_DIR ]] || mkdir -p $MSPASS_LOG_DIR

MONGO_DATA=${MSPASS_DB_DIR}/data
MONGO_LOG=${MSPASS_LOG_DIR}/mongo_log
export SPARK_WORKER_DIR=${MSPASS_WORKER_DIR}
export SPARK_LOG_DIR=${MSPASS_LOG_DIR}

if [ $# -eq 0 ]; then

  function start_mspass_frontend {
    NOTEBOOK_ARGS="--notebook-dir=${_MY_HOME} --port=${JUPYTER_PORT} --no-browser --ip=0.0.0.0 --allow-root"
    # if MSPASS_JUPYTER_PWD is not set, notebook will generate a default token
    if [[ ! -z ${MSPASS_JUPYTER_PWD+x} ]]; then
      # we rely on jupyter's python function to hash the password
      MSPASS_JUPYTER_PWD_HASHED=$(python3 -c "from notebook.auth import passwd; print(passwd('${MSPASS_JUPYTER_PWD}'))")
      NOTEBOOK_ARGS="${NOTEBOOK_ARGS} --NotebookApp.password=${MSPASS_JUPYTER_PWD_HASHED}" 
    fi
    if [ "$MSPASS_SCHEDULER" = "spark" ]; then
      export PYSPARK_DRIVER_PYTHON=jupyter
      export PYSPARK_DRIVER_PYTHON_OPTS="notebook ${NOTEBOOK_ARGS}"
      pyspark \
        --conf "spark.mongodb.input.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
        --conf "spark.mongodb.output.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
        --conf "spark.master=spark://${MSPASS_SCHEDULER_ADDRESS}:${SPARK_MASTER_PORT}" \
        --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0
    else # if [ "$MSPASS_SCHEDULER" = "dask" ]
      export DASK_SCHEDULER_ADDRESS=${MSPASS_SCHEDULER_ADDRESS}:${DASK_SCHEDULER_PORT}
      jupyter notebook ${NOTEBOOK_ARGS}
    fi
  }

  MY_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
  if [ "$MSPASS_SCHEDULER" = "spark" ]; then
    MSPASS_SCHEDULER_CMD='$SPARK_HOME/sbin/start-master.sh'
    MSPASS_WORKER_CMD='$SPARK_HOME/sbin/start-slave.sh spark://$MSPASS_SCHEDULER_ADDRESS:$SPARK_MASTER_PORT'
  else # if [ "$MSPASS_SCHEDULER" = "dask" ]
    MSPASS_SCHEDULER_CMD='dask-scheduler --port $DASK_SCHEDULER_PORT > ${MSPASS_LOG_DIR}/dask-scheduler_log_${MY_ID} 2>&1 & sleep 5'
    MSPASS_WORKER_CMD='dask-worker --local-directory $MSPASS_WORKER_DIR tcp://$MSPASS_SCHEDULER_ADDRESS:$DASK_SCHEDULER_PORT > ${MSPASS_LOG_DIR}/dask-worker_log_${MY_ID} 2>&1 &'
  fi

  if [ "$MSPASS_ROLE" = "db" ]; then
    [[ -d $MONGO_DATA ]] || mkdir -p $MONGO_DATA
    mongod --port $MONGODB_PORT --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all
  elif [ "$MSPASS_ROLE" = "dbmanager" ]; then
    # config server configuration
    MONGODB_CONFIG_PORT=$(($MONGODB_PORT+1))
    [[ -d ${MONGO_DATA}_config ]] || mkdir -p ${MONGO_DATA}_config
    mongod --port $MONGODB_CONFIG_PORT --configsvr --replSet configserver --dbpath ${MONGO_DATA}_config --logpath ${MONGO_LOG}_config --bind_ip_all &
    sleep $SLEEP_TIME
    rs_init_status=$(mongo --port $MONGODB_CONFIG_PORT --quiet --eval "rs.status().code")
    if [ "${rs_init_status}" = "94" ]; then
      echo "dbmanager config server replicaSet is initialized"
      mongo --port $MONGODB_CONFIG_PORT --eval \
        "rs.initiate({_id: \"configserver\", configsvr: true, version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_CONFIG_PORT\" }]})"
    else
      echo "dbmanager config server replicaSet is reconfig"
      mongo --port $MONGODB_CONFIG_PORT --eval \
        "rsconf=rs.conf();rsconf.members=[{ _id: 0, host : \"$HOSTNAME:$MONGODB_CONFIG_PORT\" }];rs.reconfig(rsconf, {force: true})"
    fi

    # mongos server configuration
    mongos --port $MONGODB_PORT --configdb configserver/$HOSTNAME:$MONGODB_CONFIG_PORT --logpath ${MONGO_LOG}_router --bind_ip_all &
    sleep $SLEEP_TIME
    for i in ${MSPASS_SHARD_LIST[@]}; do
      echo ${i}
      mongo --host $HOSTNAME --port $MONGODB_PORT --eval "sh.addShard(\"${i}\")"
      sleep $SLEEP_TIME
    done
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "shard" ]; then
    [[ -n $MSPASS_SHARD_ID ]] || MSPASS_SHARD_ID=$MY_ID
    [[ -d ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} ]] || mkdir -p ${MONGO_DATA}_shard_${MSPASS_SHARD_ID}
    # Note that we have to create a one-member replica set here 
    # because certain pymongo API will use "retryWrites=true" 
    # and thus trigger an error.
    if [ "$SHARD_DB_PATH" = "tmp" ]; then
      # backup from the dump bson file if exists using mongorestore
      mongod --port $MONGODB_PORT --shardsvr --replSet ${HOSTNAME} --dbpath /tmp/db --logpath /tmp/logs  --bind_ip_all &
      # restore the backup if exists
      if [[ -d "${MONGO_DATA}_shard_${MSPASS_SHARD_ID}/backup" ]]; then
        mongorestore --host=${HOSTNAME}:$MONGODB_PORT --dir=${MONGO_DATA}_shard_${MSPASS_SHARD_ID}/backup --oplogReplay
      fi
    else
      mongod --port $MONGODB_PORT --shardsvr --replSet "rs${MSPASS_SHARD_ID}" --dbpath ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} --logpath ${MONGO_LOG}_shard_${MSPASS_SHARD_ID}  --bind_ip_all &
    fi
    sleep $SLEEP_TIME

    # shard server configuration
    rs_init_status=$(mongo --port $MONGODB_PORT --quiet --eval "rs.status().code")
    if [ "${rs_init_status}" = "94" ]; then
      echo "shard server replicaSet is initialized"
      mongo --port $MONGODB_PORT --eval \
        "rs.initiate({_id: \"rs${MSPASS_SHARD_ID}\", version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_PORT\" }]})"
    else
      echo "shard server replicaSet is reconfig"
      mongo --port $MONGODB_PORT --eval "rsconf=rs.conf();rsconf.members=[{ _id: 0, host : \"$HOSTNAME:$MONGODB_PORT\" }];rs.reconfig(rsconf, {force: true})"
    fi
    
    # if specify as tmp, we need to back up the shard database into our scratch file system using mongodump
    if [ "$SHARD_DB_PATH" = "tmp" ]; then
      mongodump --host=${HOSTNAME}:$MONGODB_PORT --out=${MONGO_DATA}_shard_${MSPASS_SHARD_ID}/backup --oplog
    fi
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "scheduler" ]; then
    eval $MSPASS_SCHEDULER_CMD
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "worker" ]; then
    [[ -d $MSPASS_WORKER_DIR ]] || mkdir -p $MSPASS_WORKER_DIR
    eval $MSPASS_WORKER_CMD
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "frontend" ]; then
    start_mspass_frontend
  else # if [ "$MSPASS_ROLE" = "all" ]
    MSPASS_DB_ADDRESS=$HOSTNAME
    MSPASS_SCHEDULER_ADDRESS=$HOSTNAME
    eval $MSPASS_SCHEDULER_CMD
    eval $MSPASS_WORKER_CMD
    [[ -d $MONGO_DATA ]] || mkdir -p $MONGO_DATA
    mongod --port $MONGODB_PORT --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all &
    start_mspass_frontend
  fi
else
  docker-entrypoint.sh $@
fi