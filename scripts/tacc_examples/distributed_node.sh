#!/bin/bash

#SBATCH -J mspass           # Job name
#SBATCH -o mspass.o%j       # Name of stdout output file
#SBATCH -p skx-dev          # Queue (partition) name
#SBATCH -N 3               # Total # of nodes (must be 1 for serial)
#SBATCH -n 3               # Total # of mpi tasks (should be 1 for serial)
#SBATCH -t 02:00:00        # Run time (hh:mm:ss)
#SBATCH -A MsPASS       # Allocation name (req'd if you have more than 1)

# working directory
WORK_DIR=$SCRATCH/mspass/workdir
# directory where contains docker image
MSPASS_CONTAINER=$WORK2/mspass/mspass_latest.sif

# command that start the container
SING_COM="singularity run --home $WORK_DIR $MSPASS_CONTAINER"

ml unload xalt
ml tacc-singularity

module list
pwd
date

mkdir -p $WORK_DIR

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

cd $WORK_DIR

# start a distributed scheduler container in the primary node
SINGULARITYENV_MSPASS_ROLE=scheduler $SING_COM &

# get the all the hostnames of worker nodes
WORKER_LIST=`scontrol show hostname ${SLURM_NODELIST} | \
             awk -vORS=, -v hostvar="$NODE_HOSTNAME" '{ if ($0!=hostvar) print $0 }' | \
             sed 's/,$/\n/'`
echo $WORKER_LIST

# start worker container in each worker node
SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_ROLE=worker \
mpiexec.hydra -n $((SLURM_NNODES-1)) -ppn 1 -hosts $WORKER_LIST $SING_COM &

# extract the hostname of each worker node
OLD_IFS=$IFS
IFS=","
WORKER_LIST_ARR=($WORKER_LIST)
IFS=$OLD_IFS

# specify the location where user wants to store the data
# should be in either tmp or scratch, default is scratch
SHARD_DB_PATH='scratch'

SLEEP_TIME=15

# start a shard container in each worker node
for i in ${!WORKER_LIST_ARR[@]}; do
    SINGULARITYENV_MSPASS_SHARD_ID=$i \
    SINGULARITYENV_SHARD_DB_PATH=$SHARD_DB_PATH \
    SINGULARITYENV_SLEEP_TIME=$SLEEP_TIME \
    SINGULARITYENV_MSPASS_ROLE=shard \
    mpiexec.hydra -n 1 -ppn 1 -hosts ${WORKER_LIST_ARR[i]} $SING_COM &
done

sleep 5
# start a dbmanager container in the primary node
for i in ${!WORKER_LIST_ARR[@]}; do
    SHARD_LIST[$i]="${WORKER_LIST_ARR[$i]}.stampede2.tacc.utexas.edu/${WORKER_LIST_ARR[$i]}.stampede2.tacc.utexas.edu:27017"
done
SINGULARITYENV_MSPASS_SHARD_LIST=$SHARD_LIST \
SINGULARITYENV_SLEEP_TIME=$SLEEP_TIME \
SINGULARITYENV_MSPASS_ROLE=dbmanager $SING_COM &

# start a jupyter notebook frontend in the primary node
SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM