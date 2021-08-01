#!/bin/bash

#SBATCH -J mspass           # Job name
#SBATCH -o mspass.o%j       # Name of stdout output file
#SBATCH -p normal          # Queue (partition) name
#SBATCH -N 3               # Total # of nodes (must be 1 for serial)
#SBATCH -n 3               # Total # of mpi tasks (should be 1 for serial)
#SBATCH -t 02:00:00        # Run time (hh:mm:ss)
#SBATCH -A MsPASS       # Allocation name (req'd if you have more than 1)

# working directory
WORK_DIR=$SCRATCH/mspass/cluster_workdir
# directory where contains docker image
MSPASS_CONTAINER=$WORK2/mspass/mspass_master.sif

# command that start the container
SING_COM="singularity run $MSPASS_CONTAINER"

ml unload xalt
ml tacc-singularity

module list
pwd
date

NODE_HOSTNAME=`hostname -s`
echo "primary node $NODE_HOSTNAME"
LOGIN_PORT=`echo $NODE_HOSTNAME | perl -ne 'print (($2+1).$3.$1) if /c\d(\d\d)-(\d)(\d\d)/;'`
echo "got login node port $LOGIN_PORT"

# create reverse tunnel port to login nodes.  Make one tunnel for each login so the user can just
# connect to stampede.tacc
for i in `seq 4`; do
    ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 login$i
done
echo "Created reverse ports on Stampede2 logins"

mkdir -p $WORK_DIR
cd $WORK_DIR

# start a distributed scheduler container in the primary node
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_ROLE=scheduler $SING_COM &

# get the all the hostnames of worker nodes
WORKER_LIST=`scontrol show hostname ${SLURM_NODELIST} | \
             awk -vORS=, -v hostvar="$NODE_HOSTNAME" '{ if ($0!=hostvar) print $0 }' | \
             sed 's/,$/\n/'`
echo $WORKER_LIST

# start worker container in each worker node
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_ROLE=worker \
mpiexec.hydra -n $((SLURM_NNODES-1)) -ppn 1 -hosts $WORKER_LIST $SING_COM &

# specify the location where user wants to store the data
# should be in either tmp or scratch, default is scratch
DB_PATH='scratch'

# extract the hostname of each worker node
OLD_IFS=$IFS
IFS=","
WORKER_LIST_ARR=($WORKER_LIST)
IFS=$OLD_IFS

# control the interval between mongo instance and mongo shell execution
SLEEP_TIME=15

# start a dbmanager container in the primary node
username=`whoami`
for i in ${!WORKER_LIST_ARR[@]}; do
    SHARD_LIST[$i]="rs$i/${WORKER_LIST_ARR[$i]}.stampede2.tacc.utexas.edu:27017"
    SHARD_ADDRESS[$i]="$username@${WORKER_LIST_ARR[$i]}.stampede2.tacc.utexas.edu"
    SHARD_DB_PATH[$i]="$username@${WORKER_LIST_ARR[$i]}.stampede2.tacc.utexas.edu:/tmp/db/data_shard_$i"
    SHARD_LOGS_PATH[$i]="$username@${WORKER_LIST_ARR[$i]}.stampede2.tacc.utexas.edu:/tmp/logs/mongo_log_shard_$i"
done
# define database that enable sharding
SHARD_DATABASE="usarraytest"
# define (collection:shard_key) pairs
SHARD_COLLECTIONS=(
    "arrival:_id"
)
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_SHARD_DATABASE=${SHARD_DATABASE} \
SINGULARITYENV_MSPASS_SHARD_COLLECTIONS=${SHARD_COLLECTIONS[@]} \
SINGULARITYENV_MSPASS_SHARD_LIST=${SHARD_LIST[@]} \
SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \
SINGULARITYENV_MSPASS_ROLE=dbmanager $SING_COM &

# ensure enough time for dbmanager to finish
sleep 30

# start a shard container in each worker node
# mipexec could be cleaner while ssh would induce more complexity
for i in ${!WORKER_LIST_ARR[@]}; do
    SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
    SINGULARITYENV_MSPASS_SHARD_ID=$i \
    SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
    SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \
    SINGULARITYENV_MSPASS_WORKER_ARG="--nprocs 48 --nthreads 1" \
    SINGULARITYENV_MSPASS_CONFIG_SERVER_ADDR="configserver/${NODE_HOSTNAME}.stampede2.tacc.utexas.edu:27018" \
    SINGULARITYENV_MSPASS_ROLE=shard \
    mpiexec.hydra -n 1 -ppn 1 -hosts ${WORKER_LIST_ARR[i]} $SING_COM &
done

# start a jupyter notebook frontend in the primary node
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \
SINGULARITYENV_MSPASS_SHARD_ADDRESS=${SHARD_ADDRESS[@]} \
SINGULARITYENV_MSPASS_SHARD_DB_PATH=${SHARD_DB_PATH[@]} \
SINGULARITYENV_MSPASS_SHARD_LOGS_PATH=${SHARD_LOGS_PATH[@]} \
SINGULARITYENV_MSPASS_DB_MODE="shard" \
SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM