#!/bin/bash

# If enabled for Kubernetes deployments, use HOME as the PVC mount point.
# Otherwise keep the historical Docker /home default.
if [[ "${MSPASS_USE_HOME_WORKDIR:-false}" = "true" && -n "${KUBERNETES_SERVICE_HOST}" ]]; then
  MSPASS_WORKDIR=${MSPASS_WORK_DIR:-${HOME}}
  mkdir -p "${MSPASS_WORKDIR}" 2>/dev/null || true
elif grep "docker/containers" /proc/self/mountinfo -qa; then
  MSPASS_WORKDIR=/home
elif [[ -z ${MSPASS_WORK_DIR} ]]; then
  MSPASS_WORKDIR=`pwd`
else
  MSPASS_WORKDIR=$MSPASS_WORK_DIR
fi

function terminal_columns {
  local columns=${COLUMNS:-}
  if [[ -z "$columns" ]] && command -v tput >/dev/null 2>&1; then
    columns=$(tput cols 2>/dev/null || true)
  fi
  echo "${columns:-80}"
}

function print_centered_line {
  local columns
  columns=$(terminal_columns)
  echo "$1" | sed -e :a -e "s/^.\{1,${columns}\}$/ & /;ta" | tr -d '\n' | head -c "$columns"
  echo -e "\n"
}

function print_notebook_link {
  local title="$1"
  local url="$2"
  local columns
  columns=$(terminal_columns)
  printf '%*s\n' "$columns" '' | tr ' ' -
  print_centered_line "$title"
  print_centered_line "The link below will open a MsPASS Jupyter Notebook"
  print_centered_line "$url"
  printf '%*s\n' "$columns" '' | tr ' ' -
}

function configure_tacc_interactive_access {
  local tapis_exec_system
  local node_hostname
  local login_port
  local status_port

  if [ "$_tapisExecSystemId" == "Stampede3" ]; then
    tapis_exec_system="stampede3"
  elif [ "$_tapisExecSystemId" == "Frontera.exec" ]; then
    tapis_exec_system="frontera"
  elif [ "$_tapisExecSystemId" == "Stampede.exec" ]; then
    tapis_exec_system="stampede2"
  else
    tapis_exec_system="frontera"
  fi

  node_hostname=$(hostname -s)
  login_port=$(echo "$node_hostname" | perl -ne 'print (($2+1).$3.$1) if /c\d(\d\d)-(\d)(\d\d)/;')
  status_port=$(($login_port + 1))
  echo "got login node port $login_port"
  echo "got status node port $status_port"

  for i in `seq 4`; do
    ssh -q -f -g -N -R $login_port:$node_hostname:8888 login$i > /dev/null 2>&1
    ssh -q -f -g -N -R $status_port:$node_hostname:8787 login$i > /dev/null 2>&1
  done

  MSPASS_JUPYTER_TOKEN=${MSPASS_JUPYTER_TOKEN:-$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 20 | head -n 1)}
  echo "Created reverse ports on $tapis_exec_system logins"
  print_notebook_link "Welcome to Tapis interactive" "http://$tapis_exec_system.tacc.utexas.edu:$login_port/lab?token=$MSPASS_JUPYTER_TOKEN"
}

function configure_tacc_interactive_access_if_needed {
  if [[ "${MSPASS_TACC_MODE:-false}" = "true" ]]; then
    configure_tacc_interactive_access
  fi
}

function is_jupyterhub_singleuser_command {
  local arg
  for arg in "$@"; do
    if [[ "$(basename "$arg")" = "jupyterhub-singleuser" ]]; then
      return 0
    fi
  done
  return 1
}

function scheduler_address_has_port {
  local address="$1"
  local without_scheme="${address#*://}"

  if [[ "$without_scheme" =~ ^\[.*\]:[0-9]+$ ]]; then
    return 0
  fi

  if [[ "$without_scheme" =~ ^[^:]+:[0-9]+$ ]]; then
    return 0
  fi

  return 1
}

function build_dask_scheduler_uri {
  local address="$1"
  local port="${2:-8786}"

  if scheduler_address_has_port "$address"; then
    if [[ "$address" = *"://"* ]]; then
      printf '%s' "$address"
    else
      printf 'tcp://%s' "$address"
    fi
  else
    if [[ "$address" = *"://"* ]]; then
      printf '%s:%s' "$address" "$port"
    else
      printf 'tcp://%s:%s' "$address" "$port"
    fi
  fi
}

# define SLEEP_TIME
if [[ -z $MSPASS_SLEEP_TIME ]]; then
  MSPASS_SLEEP_TIME=15
fi

# This sets defaults for this set of env variables
if [[ -z ${MSPASS_DB_DIR} ]]; then
  MSPASS_DB_DIR=${MSPASS_WORKDIR}/db
fi

if [[ -z ${MSPASS_LOG_DIR} ]]; then
  MSPASS_LOG_DIR=${MSPASS_WORKDIR}/logs
fi
if [[ -z ${MSPASS_WORKER_DIR} ]]; then
  MSPASS_WORKER_DIR=${MSPASS_WORKDIR}/work
fi
# Note that only log is required for all roles. Other dirs will be created later when needed.
[[ -d $MSPASS_LOG_DIR ]] || mkdir -p $MSPASS_LOG_DIR

MONGO_DATA=${MSPASS_DB_DIR}/data
MONGO_LOG=${MSPASS_LOG_DIR}/mongo_log
export SPARK_WORKER_DIR=${MSPASS_WORKER_DIR}
export SPARK_LOG_DIR=${MSPASS_LOG_DIR}

MSPASS_START_LOCAL_SERVICES=false
if [ $# -eq 0 ] || [ "$1" = "--batch" ]; then
  MSPASS_START_LOCAL_SERVICES=true
elif is_jupyterhub_singleuser_command "$@" && [ "${MSPASS_ROLE:-all}" = "all" ]; then
  MSPASS_START_LOCAL_SERVICES=true
fi

if [ "$MSPASS_START_LOCAL_SERVICES" = "true" ]; then

  function start_mspass_frontend {
      RUN_JUPYTER_AS_NB_USER=false
      if [[ "${MSPASS_RUN_JUPYTER_AS_NB_USER:-false}" = "true" ]]; then
          RUN_JUPYTER_AS_NB_USER=true
      fi

      NOTEBOOK_ARGS=(
          "--notebook-dir=${MSPASS_WORKDIR}"
          "--port=${JUPYTER_PORT}"
          "--no-browser"
          "--ip=0.0.0.0"
      )
      if [[ "${MSPASS_ALLOW_ROOT:-auto}" = "true" || ( "${MSPASS_ALLOW_ROOT:-auto}" = "auto" && "$RUN_JUPYTER_AS_NB_USER" != "true" ) ]]; then
          NOTEBOOK_ARGS+=("--allow-root")
      fi

      function quote_command {
          local quoted_command
          printf -v quoted_command '%q ' "$@"
          printf '%s' "${quoted_command% }"
      }

      function run_frontend_as_nb_user {
          NB_USER=${NB_USER:-mspass}
          chown -R ${NB_USER}:100 "${MSPASS_WORKDIR}" 2>/dev/null || true
          local quoted_command
          quoted_command=$(quote_command "$@")
          su --preserve-environment -c "export PATH=${CONDA_DIR:-/opt/conda}/bin:\$PATH; ${quoted_command}" "${NB_USER}"
      }

      # Handle Jupyter password hashing if set
      if [[ ! -z ${MSPASS_JUPYTER_PWD+x} ]]; then
          MSPASS_JUPYTER_PWD_HASHED=$(MSPASS_JUPYTER_PWD="${MSPASS_JUPYTER_PWD}" python3 -c 'import os; from notebook.auth import passwd; print(passwd(os.environ["MSPASS_JUPYTER_PWD"]))')
          NOTEBOOK_ARGS+=("--NotebookApp.password=${MSPASS_JUPYTER_PWD_HASHED}")
      fi
      if [[ -n ${MSPASS_JUPYTER_TOKEN:-} ]]; then
          NOTEBOOK_ARGS+=("--NotebookApp.token=${MSPASS_JUPYTER_TOKEN}")
      fi

      if [ "$MSPASS_SCHEDULER" = "spark" ]; then
          if [ -z "$1" ]; then
              # Interactive Jupyter Lab mode for Spark
              export PYSPARK_DRIVER_PYTHON=jupyter
              NOTEBOOK_ARGS_STRING=$(quote_command "${NOTEBOOK_ARGS[@]}")
              export PYSPARK_DRIVER_PYTHON_OPTS="lab ${NOTEBOOK_ARGS_STRING}"
              if [[ "$RUN_JUPYTER_AS_NB_USER" = "true" ]]; then
                  run_frontend_as_nb_user pyspark \
                      --conf "spark.mongodb.input.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
                      --conf "spark.mongodb.output.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
                      --conf "spark.master=spark://${MSPASS_SCHEDULER_ADDRESS}:${SPARK_MASTER_PORT}" \
                      --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0
              else
                  pyspark \
                      --conf "spark.mongodb.input.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
                      --conf "spark.mongodb.output.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
                      --conf "spark.master=spark://${MSPASS_SCHEDULER_ADDRESS}:${SPARK_MASTER_PORT}" \
                      --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0
              fi
          else
              # Batch mode processing for Spark
              input_file="$1"
              if [[ "$input_file" == *.ipynb ]]; then
                  echo "Converting notebook to Python script: $input_file"
                  jupyter nbconvert --to script "$input_file"
                  script_file="${input_file%.*}.py"
              else
                  script_file="$input_file"
              fi
              spark-submit \
                  --conf "spark.mongodb.input.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
                  --conf "spark.mongodb.output.uri=mongodb://${MSPASS_DB_ADDRESS}:${MONGODB_PORT}/test.misc" \
                  --conf "spark.master=spark://${MSPASS_SCHEDULER_ADDRESS}:${SPARK_MASTER_PORT}" \
                  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 \
                  "$script_file"
          fi
      else
          # Dask scheduler configuration
          export DASK_SCHEDULER_ADDRESS="$(build_dask_scheduler_uri "${MSPASS_SCHEDULER_ADDRESS}" "${DASK_SCHEDULER_PORT}")"
          if [ -z "$1" ]; then
              # Interactive Jupyter Lab mode for Dask
              if [[ "$RUN_JUPYTER_AS_NB_USER" = "true" ]]; then
                  run_frontend_as_nb_user jupyter lab "${NOTEBOOK_ARGS[@]}"
              else
                  jupyter lab "${NOTEBOOK_ARGS[@]}"
              fi
          else
              # Batch mode processing for Dask
              input_file="$1"
              if [[ "$input_file" == *.ipynb ]]; then
                  echo "Converting notebook to Python script: $input_file"
                  jupyter nbconvert --to script "$input_file"
                  script_file="${input_file%.*}.py"
              else
                  script_file="$input_file"
              fi
              python "$script_file"
          fi
      fi
  }

  function clean_up_single_node {
    # ---------------------- clean up workflow -------------------------
    # stop mongodb
    mongosh --port $MONGODB_PORT admin --eval "db.shutdownServer({force:true})"
    sleep 5
    # copy shard data to scratch
    if [ "$MSPASS_DB_PATH" = "tmp" ]; then
      echo "standalone: copy shard data to scratch"
      # copy data
      scp -r /tmp/db/data ${MSPASS_DB_DIR}
      # copy log
      scp -r /tmp/logs/mongo_log ${MSPASS_LOG_DIR}
    fi
    sleep ${MSPASS_SLEEP_TIME}
  }

  function clean_up_multiple_nodes {
    # ---------------------- clean up workflow -------------------------
    # stop mongos routers
    mongosh --port $MONGODB_PORT admin --eval "db.shutdownServer({force:true})"
    sleep 5
    # stop each shard replica set
    for i in ${MSPASS_SHARD_ADDRESS[@]}; do
        ssh -o "StrictHostKeyChecking no" ${i} "kill -2 \$(pgrep mongo)"
        sleep 5
    done
    # stop config servers
    mongosh --port $(($MONGODB_PORT+1)) admin --eval "db.shutdownServer({force:true})"

    # copy the shard data to scratch if the shards are deployed in /tmp
    if [ "$MSPASS_DB_PATH" = "tmp" ]; then
      echo "distributed: copy shard data to scratch"
      # copy data
      for i in ${MSPASS_SHARD_DB_PATH[@]}; do
        scp -r -o StrictHostKeyChecking=no ${i} ${MSPASS_DB_DIR}
      done
      # copy log
      for i in ${MSPASS_SHARD_LOGS_PATH[@]}; do
        scp -r -o StrictHostKeyChecking=no ${i} ${MSPASS_LOG_DIR}
      done
    fi
    sleep ${MSPASS_SLEEP_TIME}
  }

  FRONTEND_CLEANUP_DONE=false
  FRONTEND_CLEANUP_MODE=single

  function run_frontend_cleanup {
    local status=${1:-$?}
    trap - EXIT INT TERM
    if [ "$FRONTEND_CLEANUP_DONE" = "true" ]; then
      return "$status"
    fi

    FRONTEND_CLEANUP_DONE=true
    if [ "$FRONTEND_CLEANUP_MODE" = "multiple" ]; then
      clean_up_multiple_nodes
    else
      clean_up_single_node
    fi

    pkill -TERM -P $$ 2>/dev/null || true
    return "$status"
  }

  function enable_frontend_cleanup_trap {
    trap 'run_frontend_cleanup $?' EXIT
    trap 'run_frontend_cleanup 130; exit 130' INT
    trap 'run_frontend_cleanup 143; exit 143' TERM
  }

  function start_db_scratch {
    [[ -d $MONGO_DATA ]] || mkdir -p $MONGO_DATA
    mongod --port $MONGODB_PORT --dbpath $MONGO_DATA --logpath $MONGO_LOG --bind_ip_all &
  }

  function start_db_tmp {
    # create db and log dirs if not exists
    [[ -d /tmp/db ]] || mkdir -p /tmp/db
    [[ -d /tmp/logs ]] || mkdir -p /tmp/logs && touch /tmp/logs/mongo_log
    # copy all data on scratch to the local tmp folder
    if [[ -d ${MSPASS_DB_DIR}/data ]]; then
      cp -r ${MSPASS_DB_DIR}/data /tmp/db
    else
      mkdir -p /tmp/db/data
    fi
    # copy dfiles to /tmp
    if [[ -d $MSPASS_SCRATCH_DATA_DIR ]]; then
      cp -r $MSPASS_SCRATCH_DATA_DIR /tmp
    fi
    # start mongodb on /tmp
    mongod --port $MONGODB_PORT --dbpath /tmp/db/data --logpath /tmp/logs/mongo_log --bind_ip_all &
  }

  function start_local_mspass_services {
    export MSPASS_DB_ADDRESS=$HOSTNAME
    export MSPASS_SCHEDULER_ADDRESS=$HOSTNAME
    eval $MSPASS_SCHEDULER_CMD
    eval $MSPASS_WORKER_CMD
    if [ "$MSPASS_DB_PATH" = "tmp" ]; then
      start_db_tmp
    else
      start_db_scratch
    fi
  }

  MY_ID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
  if [ "$MSPASS_SCHEDULER" = "spark" ]; then
    MSPASS_SCHEDULER_CMD='$SPARK_HOME/sbin/start-master.sh'
    MSPASS_WORKER_CMD='$SPARK_HOME/sbin/start-slave.sh spark://$MSPASS_SCHEDULER_ADDRESS:$SPARK_MASTER_PORT'
  else # if [ "$MSPASS_SCHEDULER" = "dask" ]
    MSPASS_SCHEDULER_CMD='dask scheduler --port $DASK_SCHEDULER_PORT > ${MSPASS_LOG_DIR}/dask-scheduler_log_${MY_ID} 2>&1 & sleep 5'
    MSPASS_WORKER_CMD='dask worker ${MSPASS_WORKER_ARG} --memory-limit=${MSPASS_DASK_WORKER_MEMORY_LIMIT:-0} --local-directory $MSPASS_WORKER_DIR "$(build_dask_scheduler_uri "$MSPASS_SCHEDULER_ADDRESS" "$DASK_SCHEDULER_PORT")" > ${MSPASS_LOG_DIR}/dask-worker_log_${MY_ID} 2>&1 &'
  fi

  if [ "$MSPASS_ROLE" = "db" ]; then
    if [ "$MSPASS_DB_PATH" = "tmp" ]; then
      start_db_tmp
    else
      start_db_scratch
    fi
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
      mongosh --port $MONGODB_CONFIG_PORT local --eval "db.dropDatabase()"
      sleep ${MSPASS_SLEEP_TIME}
      # update config.shards collections
      echo "update shard host names for config server $HOSTNAME"
      # if using ${!MSPASS_SHARD_LIST[@]} style for loop, it doesn't work. Not sure why it doesn't work.
      ITER=0
      for i in ${MSPASS_SHARD_LIST[@]}; do
        echo "update rs${ITER} with host ${i}"
        mongosh --port $MONGODB_CONFIG_PORT config --eval "db.shards.updateOne({\"_id\": \"rs${ITER}\"}, {\$set: {\"host\": \"${i}\"}})"
        ((ITER++))
        sleep ${MSPASS_SLEEP_TIME}
      done
      echo "restart the config server $HOSTNAME as a replica set"
      # restart the mongod as a new single-node replica set
      mongosh --port $MONGODB_CONFIG_PORT admin --eval "db.shutdownServer()"
      sleep ${MSPASS_SLEEP_TIME}
      mongod --port $MONGODB_CONFIG_PORT --configsvr --replSet configserver --dbpath ${MONGO_DATA}_config --logpath ${MONGO_LOG}_config --bind_ip_all &
      sleep ${MSPASS_SLEEP_TIME}
      # initiate the new replica set
      mongosh --port $MONGODB_CONFIG_PORT --eval \
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
      mongosh --port $MONGODB_CONFIG_PORT --eval \
        "rs.initiate({_id: \"configserver\", configsvr: true, version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_CONFIG_PORT\" }]})"
      sleep ${MSPASS_SLEEP_TIME}

      # start a mongos router server
      mongos --port $MONGODB_PORT --configdb configserver/$HOSTNAME:$MONGODB_CONFIG_PORT --logpath ${MONGO_LOG}_router --bind_ip_all &
      # add shard clusters
      for i in ${MSPASS_SHARD_LIST[@]}; do
        echo "add shard with host ${i}"
        sleep ${MSPASS_SLEEP_TIME}
        mongosh --host $HOSTNAME --port $MONGODB_PORT --eval "sh.addShard(\"${i}\")"
      done
    fi

    # enable database sharding
    echo "enable database $MSPASS_SHARD_DATABASE sharding"
    mongosh --host $HOSTNAME --port $MONGODB_PORT --eval "sh.enableSharding(\"${MSPASS_SHARD_DATABASE}\")"
    sleep ${MSPASS_SLEEP_TIME}
    # shard collection(using hashed)
    for i in ${MSPASS_SHARD_COLLECTIONS[@]}; do
      echo "shard collection $MSPASS_SHARD_DATABASE.${i%%:*} and shard key is ${i##*:}"
      mongosh --host $HOSTNAME --port $MONGODB_PORT --eval "sh.shardCollection(\"$MSPASS_SHARD_DATABASE.${i%%:*}\", {${i##*:}: \"hashed\"})"
      sleep ${MSPASS_SLEEP_TIME}
    done
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "shard" ]; then
    [[ -n $MSPASS_SHARD_ID ]] || MSPASS_SHARD_ID=$MY_ID
    # Note that we have to create a one-member replica set here
    # because certain pymongo API will use "retryWrites=true"
    # and thus trigger an error.
    if [ "$MSPASS_DB_PATH" = "tmp" ]; then
      echo "store shard data in tmp for shard server $HOSTNAME"
      # create db and log dirs if not exists
      [[ -d /tmp/db ]] || mkdir -p /tmp/db
      [[ -d /tmp/logs ]] || mkdir -p /tmp/logs && touch /tmp/logs/mongo_log_shard_${MSPASS_SHARD_ID}
      # copy all the shard data to the local tmp folder
      if [[ -d ${MSPASS_DB_DIR}/data_shard_${MSPASS_SHARD_ID} ]]; then
        scp -r -o StrictHostKeyChecking=no ${MSPASS_DB_DIR}/data_shard_${MSPASS_SHARD_ID} /tmp/db
      else
        mkdir -p /tmp/db/data_shard_${MSPASS_SHARD_ID}
      fi
      # reconfig the shard replica set
      if [ -d ${MONGO_DATA}_shard_${MSPASS_SHARD_ID} ]; then
        # restore the shard replica set
        mongod --port $MONGODB_PORT --dbpath /tmp/db/data_shard_${MSPASS_SHARD_ID} --logpath /tmp/logs/mongo_log_shard_${MSPASS_SHARD_ID} --bind_ip_all &
        sleep ${MSPASS_SLEEP_TIME}
        # drop local database
        echo "drop local database for shard server $HOSTNAME"
        mongosh --port $MONGODB_PORT local --eval "db.dropDatabase()"
        sleep ${MSPASS_SLEEP_TIME}
        # update shard metadata in each shard's identity document
        echo "update config server host names for shard server $HOSTNAME"
        mongosh --port $MONGODB_PORT admin --eval "db.system.version.updateOne({\"_id\": \"shardIdentity\"}, {\$set: {\"configsvrConnectionString\": \"${MSPASS_CONFIG_SERVER_ADDR}\"}})"
        sleep ${MSPASS_SLEEP_TIME}
        # restart the mongod as a new single-node replica set
        echo "restart the shard server $HOSTNAME as a replica set"
        mongosh --port $MONGODB_PORT admin --eval "db.shutdownServer()"
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
        mongosh --port $MONGODB_PORT local --eval "db.dropDatabase()"
        sleep ${MSPASS_SLEEP_TIME}
        # update shard metadata in each shard's identity document
        echo "update config server host names for shard server $HOSTNAME"
        mongosh --port $MONGODB_PORT admin --eval "db.system.version.updateOne({\"_id\": \"shardIdentity\"}, {\$set: {\"configsvrConnectionString\": \"${MSPASS_CONFIG_SERVER_ADDR}\"}})"
        sleep ${MSPASS_SLEEP_TIME}
        # restart the mongod as a new single-node replica set
        echo "restart the shard server $HOSTNAME as a replica set"
        mongosh --port $MONGODB_PORT admin --eval "db.shutdownServer()"
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
    mongosh --port $MONGODB_PORT --eval \
      "rs.initiate({_id: \"rs${MSPASS_SHARD_ID}\", version: 1, members: [{ _id: 0, host : \"$HOSTNAME:$MONGODB_PORT\" }]})"
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "scheduler" ]; then
    eval $MSPASS_SCHEDULER_CMD
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "worker" ]; then
    [[ -d $MSPASS_WORKER_DIR ]] || mkdir -p $MSPASS_WORKER_DIR
    eval $MSPASS_WORKER_CMD
    # copy dfiles to /tmp
    if [[ -d $MSPASS_SCRATCH_DATA_DIR ]]; then
      cp -r $MSPASS_SCRATCH_DATA_DIR /tmp
    fi
    tail -f /dev/null
  elif [ "$MSPASS_ROLE" = "frontend" ]; then
    if [ "$MSPASS_DB_MODE" = "shard" ]; then
      FRONTEND_CLEANUP_MODE=multiple
    else
      FRONTEND_CLEANUP_MODE=single
    fi
    enable_frontend_cleanup_trap
    if [ "$1" != "--batch" ]; then
      configure_tacc_interactive_access_if_needed
    fi
    start_mspass_frontend "$2"
    frontend_status=$?
    run_frontend_cleanup "$frontend_status"
    if [ "$1" = "--batch" ]
    then
      exit $frontend_status
    fi
  else # if [ "$MSPASS_ROLE" = "all" ]
    FRONTEND_CLEANUP_MODE=single
    enable_frontend_cleanup_trap
    start_local_mspass_services
    if is_jupyterhub_singleuser_command "$@"; then
      docker-entrypoint.sh "$@"
      frontend_status=$?
    else
      if [ "$1" != "--batch" ]; then
        configure_tacc_interactive_access_if_needed
      fi
      start_mspass_frontend "$2"
      frontend_status=$?
    fi
    run_frontend_cleanup "$frontend_status"
    if [ "$1" = "--batch" ]
    then
      exit $frontend_status
    fi
  fi
  wait
else
  docker-entrypoint.sh "$@"
fi
