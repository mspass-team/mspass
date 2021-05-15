#!/bin/bash

# If running with docker use /home, else use pwd to store all data and logs
if grep docker /proc/1/cgroup -qa; then
  MSPASS_WORKDIR=/home
else
  MSPASS_WORKDIR=$SCRATCH/mspass/workdir
fi

MSPASS_DB_DIR=${MSPASS_WORKDIR}/db
MSPASS_LOG_DIR=${MSPASS_WORKDIR}/logs
MSPASS_WORKER_DIR=${MSPASS_WORKDIR}/work
# Note that only log is required for all roles. Other dirs will be created later when needed.
[[ -d $MSPASS_LOG_DIR ]] || mkdir -p $MSPASS_LOG_DIR

MONGO_DATA=${MSPASS_DB_DIR}/data
MONGO_LOG=${MSPASS_LOG_DIR}/mongo_log
export SPARK_WORKER_DIR=${MSPASS_WORKER_DIR}
export SPARK_LOG_DIR=${MSPASS_LOG_DIR}

if [ $# -eq 0 ]; then

  function start_mspass_frontend {
    NOTEBOOK_ARGS="--notebook-dir=${MSPASS_WORKDIR} --port=${JUPYTER_PORT} --no-browser --ip=0.0.0.0 --allow-root"
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
    # ---------------------- clean up workflow -------------------------
    # stop mongos routers
    mongo --port $MONGODB_PORT admin --eval "db.shutdownServer({force:true})"
    sleep 5
    # stop each shard replica set
    for i in ${MSPASS_SHARD_ADDRESS[@]}; do
        ssh -o "StrictHostKeyChecking no" ${i} "kill -2 \$(pgrep mongo)"
        sleep 5
    done
    # stop config servers
    mongo --port $(($MONGODB_PORT+1)) admin --eval "db.shutdownServer({force:true})"

    # copy the shard data to scratch if the shards are deployed in /tmp
    if [ "$MSPASS_SHARD_MODE" = "tmp" ]; then
      echo "copy shard data to scratch"
      # copy data
      for i in ${MSPASS_SHARD_DB_PATH[@]}; do
        scp -r -o StrictHostKeyChecking=no ${i} ${MSPASS_DB_DIR}
        #rsync -e "ssh -o StrictHostKeyChecking=no" -avtr ${i} ${MSPASS_DB_DIR}
      done
      # copy log
      for i in ${MSPASS_SHARD_LOGS_PATH[@]}; do
        scp -r -o StrictHostKeyChecking=no ${i} ${MSPASS_LOG_DIR}
        #rsync -e "ssh -o StrictHostKeyChecking=no" -avtr ${i} ${MSPASS_LOG_DIR}
      done
    fi
    sleep 30
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
    if [ -d ${MONGO_DATA}_config ]; then
      echo "restore config server $HOSTNAME cluster"
      # start a mongod instance
      mongod --port $MONGODB_CONFIG_PORT --dbpath ${MONGO_DATA}_config --logpath ${MONGO_LOG}_config --bind_ip_all &
      sleep ${MSPASS_SLEEP_TIME}
      # drop the local database
      echo "drop local database for config server $HOSTNAME"
      mongo --port $MONGODB_CONFIG_PORT local --eval "db.dropDatabase()"
      sleep ${MSPASS_SLEEP_TIME}
      # update config.shards collections
      echo "update shard host names for config server $HOSTNAME"
      # if using ${!MSPASS_SHARD_LIST[@]} style for loop, it doesn't work. Not sure why it doesn't work.
      ITER=0
      for i in ${MSPASS_SHARD_LIST[@]}; do
        echo "update rs${ITER} with host ${i}"
        mongo --port $MONGODB_CONFIG_PORT config --eval "db.shards.updateOne({\"_id\": \"rs${ITER}\"}, {\$set: {\"host\": \"${i}\"}})"
        ((ITER++))
        sleep ${MSPASS_SLEEP_TIME}
      done
      echo "restart the config server $HOSTNAME as a replica set"
      # restart the mongod as a new single-node replica set
      mongo --port $MONGODB_CONFIG_PORT admin --eval "db.shutdownServer()"
      sleep ${MSPASS_SLEEP_TIME}
      mongod --port $MONGODB_CONFIG_PORT --configsvr --replSet configserver --dbpath ${MONGO_DATA}_config --logpath ${MONGO_LOG}_config --bind_ip_all &
      sleep ${MSPASS_SLEEP_TIME}
      # initiate the new replica set
      mongo --port $MONGODB_CONFIG_PORT --eval \
        "rs.initiate({_id: \"configserver\", configsvr: true, version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_CONFIG_PORT\" }]})"
      sleep ${MSPASS_SLEEP_TIME}

      # start a mongos router server
      mongos --port $MONGODB_PORT --configdb configserver/$HOSTNAME:$MONGODB_CONFIG_PORT --logpath ${MONGO_LOG}_router --bind_ip_all &
      sleep ${MSPASS_SLEEP_TIME}
    else
      # create a config dir
      mkdir -p ${MONGO_DATA}_config
      echo "dbmanager config server $HOSTNAME replicaSet is initialized"
      # start a config server
      mongod --port $MONGODB_CONFIG_PORT --configsvr --replSet configserver --dbpath ${MONGO_DATA}_config --logpath ${MONGO_LOG}_config --bind_ip_all &
      sleep ${MSPASS_SLEEP_TIME}
      mongo --port $MONGODB_CONFIG_PORT --eval \
        "rs.initiate({_id: \"configserver\", configsvr: true, version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_CONFIG_PORT\" }]})"
      sleep ${MSPASS_SLEEP_TIME}
      
      # start a mongos router server
      mongos --port $MONGODB_PORT --configdb configserver/$HOSTNAME:$MONGODB_CONFIG_PORT --logpath ${MONGO_LOG}_router --bind_ip_all &
      # add shard clusters
      for i in ${MSPASS_SHARD_LIST[@]}; do
        echo "add shard with host ${i}"
        sleep ${MSPASS_SLEEP_TIME}
        mongo --host $HOSTNAME --port $MONGODB_PORT --eval "sh.addShard(\"${i}\")"
      done
    fi

    # enable database sharding
    echo "enable database $MSPASS_SHARD_DATABASE sharding"
    mongo --host $HOSTNAME --port $MONGODB_PORT --eval "sh.enableSharding(\"${MSPASS_SHARD_DATABASE}\")"
    sleep ${MSPASS_SLEEP_TIME}
    # shard collection(using hashed)
    for i in ${MSPASS_SHARD_COLLECTIONS[@]}; do
      echo "shard collection $MSPASS_SHARD_DATABASE.${i%%:*} and shard key is ${i##*:}"
      mongo --host $HOSTNAME --port $MONGODB_PORT --eval "sh.shardCollection(\"$MSPASS_SHARD_DATABASE.${i%%:*}\", {${i##*:}: \"hashed\"})"
      sleep ${MSPASS_SLEEP_TIME}
    done
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "shard" ]; then
    [[ -n $MSPASS_SHARD_ID ]] || MSPASS_SHARD_ID=$MY_ID
    # Note that we have to create a one-member replica set here 
    # because certain pymongo API will use "retryWrites=true" 
    # and thus trigger an error.
    if [ "$MSPASS_SHARD_MODE" = "tmp" ]; then
      echo "store shard data in tmp for shard server $HOSTNAME"
      # create db and log dirs if not exists
      [[ -d /tmp/db/data_shard_${MSPASS_SHARD_ID} ]] || mkdir -p /tmp/db/data_shard_${MSPASS_SHARD_ID}
      [[ -d /tmp/logs ]] || mkdir -p /tmp/logs && touch /tmp/logs/mongo_log_shard_${MSPASS_SHARD_ID}
      # copy all the shard data to the local tmp folder
      scp -r -o StrictHostKeyChecking=no ${MSPASS_DB_DIR}/data_shard_${MSPASS_SHARD_ID} /tmp/db
      # reconfig the shard replica set
      if [ -d ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} ]; then
        # restore the shard replica set
        mongod --port $MONGODB_PORT --dbpath /tmp/db/data_shard_${MSPASS_SHARD_ID} --logpath /tmp/logs/mongo_log_shard_${MSPASS_SHARD_ID} --bind_ip_all &
        sleep ${MSPASS_SLEEP_TIME}
        # drop local database
        echo "drop local database for shard server $HOSTNAME"
        mongo --port $MONGODB_PORT local --eval "db.dropDatabase()"
        sleep ${MSPASS_SLEEP_TIME}
        # update shard metadata in each shard's identity document
        echo "update config server host names for shard server $HOSTNAME"
        mongo --port $MONGODB_PORT admin --eval "db.system.version.updateOne({\"_id\": \"shardIdentity\"}, {\$set: {\"configsvrConnectionString\": \"${MSPASS_CONFIG_SERVER_ADDR}\"}})"
        sleep ${MSPASS_SLEEP_TIME}
        # restart the mongod as a new single-node replica set
        echo "restart the shard server $HOSTNAME as a replica set"
        mongo --port $MONGODB_PORT admin --eval "db.shutdownServer()"
        sleep ${MSPASS_SLEEP_TIME}
        mongod --port $MONGODB_PORT --shardsvr --replSet "rs${MSPASS_SHARD_ID}" --dbpath /tmp/db/data_shard_${MSPASS_SHARD_ID} --logpath /tmp/logs/mongo_log_shard_${MSPASS_SHARD_ID} --bind_ip_all &
      else
        # store the shard data in the /tmp folder in local machine
        mongod --port $MONGODB_PORT --shardsvr --replSet "rs${MSPASS_SHARD_ID}" --dbpath /tmp/db/data_shard_${MSPASS_SHARD_ID} --logpath /tmp/logs/mongo_log_shard_${MSPASS_SHARD_ID} --bind_ip_all &
      fi
    else
      echo "store shard data in scratch for shard server $HOSTNAME"
      if [ -d ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} ]; then
        # restore the shard replica set
        mongod --port $MONGODB_PORT --dbpath ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} --logpath ${MONGO_LOG}_shard_${MSPASS_SHARD_ID} --bind_ip_all &
        sleep ${MSPASS_SLEEP_TIME}
        # drop local database
        echo "drop local database for shard server $HOSTNAME"
        mongo --port $MONGODB_PORT local --eval "db.dropDatabase()"
        sleep ${MSPASS_SLEEP_TIME}
        # update shard metadata in each shard's identity document
        echo "update config server host names for shard server $HOSTNAME"
        mongo --port $MONGODB_PORT admin --eval "db.system.version.updateOne({\"_id\": \"shardIdentity\"}, {\$set: {\"configsvrConnectionString\": \"${MSPASS_CONFIG_SERVER_ADDR}\"}})"
        sleep ${MSPASS_SLEEP_TIME}
        # restart the mongod as a new single-node replica set
        echo "restart the shard server $HOSTNAME as a replica set"
        mongo --port $MONGODB_PORT admin --eval "db.shutdownServer()"
        sleep ${MSPASS_SLEEP_TIME}
        mongod --port $MONGODB_PORT --shardsvr --replSet "rs${MSPASS_SHARD_ID}" --dbpath ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} --logpath ${MONGO_LOG}_shard_${MSPASS_SHARD_ID} --bind_ip_all &
      else
        # initialize the shard replica set
        mkdir -p ${MONGO_DATA}_shard_${MSPASS_SHARD_ID}
        mongod --port $MONGODB_PORT --shardsvr --replSet "rs${MSPASS_SHARD_ID}" --dbpath ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} --logpath ${MONGO_LOG}_shard_${MSPASS_SHARD_ID} --bind_ip_all &
      fi
    fi
    sleep ${MSPASS_SLEEP_TIME}

    # shard server configuration
    echo "shard server $HOSTNAME replicaSet is initialized"
    mongo --port $MONGODB_PORT --eval \
      "rs.initiate({_id: \"rs${MSPASS_SHARD_ID}\", version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_PORT\" }]})"
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
