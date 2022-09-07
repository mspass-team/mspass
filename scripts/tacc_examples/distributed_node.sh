#!/bin/bash

#SECTION 1:  slurm commands (see below for more details)
#SBATCH -J mspass           # Job name
#SBATCH -o mspass.o%j       # Name of stdout output file
#SBATCH -p skx-dev          # Queue (partition) name - system dependent
#SBATCH -N 3                # Total # of nodes 
#SBATCH -n 3                # Total # of mpi tasks (normally the same as -N)
#SBATCH -t 02:00:00         # Run time (hh:mm:ss)
#SBATCH -A MsPASS           # Allocation name (req'd if you have more than 1)


# SECTION 2:  Define the software environment
# Most HPC systems like stampede2 use a softwere module
# manager to allow each job to define any special packages it needs to
# run.  In our case that is only tacc-singularity.
ml unload xalt
ml tacc-singularity
module list
pwd
date


#SECTION 3:  Define some basic control variables for this shell
# this sets the working directory
# SCRATCH is an environment variable defined for all jobs on stempede2
WORK_DIR=$SCRATCH/mspass/workdir
# This defines the path to the docker container file.
# like SCRATCH WORK2 is an environment variable defining a file system
# on stampede2
MSPASS_CONTAINER=$WORK2/mspass/mspass_latest.sif
# specify the location where user wants to store the data
# should be in either tmp or scratch
DB_PATH='scratch'
# the base for all hostname addresses
HOSTNAME_BASE='stampede2.tacc.utexas.edu'
# Sets whether to use sharding or not (here sharding is turned on)
DB_SHARDING=true
# define database that enable sharding
SHARD_DATABASE="usarraytest"
# define (collection:shard_key) pairs
SHARD_COLLECTIONS=(
    "arrival:_id"
)
# This variable is used to simplify launching each container
# Arguments are added to this string to launch each instance of a
# container.  stampede2 uses a package called singularity to launch
# each container instances
SING_COM="singularity run $MSPASS_CONTAINER"


# Section 4:  Set up some necessary communication channels
# obtain the hostname of the node, and generate a random port number
NODE_HOSTNAME=`hostname -s`
echo "primary node $NODE_HOSTNAME"
LOGIN_PORT=`echo $NODE_HOSTNAME | perl -ne 'print (($2+1).$3.$1) if /c\d(\d\d)-(\d)(\d\d)/;'`
STATUS_PORT=`echo "$LOGIN_PORT + 1" | bc -l`
echo "got login node port $LOGIN_PORT"

# create reverse tunnel port to login nodes.  Make one tunnel for each login so the user can just
# connect to stampede2.tacc.utexas.edu
for i in `seq 4`; do
    ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 login$i
    ssh -q -f -g -N -R $STATUS_PORT:$NODE_HOSTNAME:8787 login$i
done
echo "Created reverse ports on Stampede2 logins"


# Section 5:  Launch all the containers
# In this job we create a working directory on stampede2's scratch area
# Most workflows may omit the mkdir and just use cd to a working
# directory created and populated earlier
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

if [ "$DB_SHARDING" = true ] ; then
    echo 'Using Sharding MongoDB'
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
        SHARD_LIST[$i]="rs$i/${WORKER_LIST_ARR[$i]}.${HOSTNAME_BASE}:27017"
        SHARD_ADDRESS[$i]="$username@${WORKER_LIST_ARR[$i]}.${HOSTNAME_BASE}"
        SHARD_DB_PATH[$i]="$username@${WORKER_LIST_ARR[$i]}.${HOSTNAME_BASE}:/tmp/db/data_shard_$i"
        SHARD_LOGS_PATH[$i]="$username@${WORKER_LIST_ARR[$i]}.${HOSTNAME_BASE}:/tmp/logs/mongo_log_shard_$i"
    done

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
        SINGULARITYENV_MSPASS_CONFIG_SERVER_ADDR="configserver/${NODE_HOSTNAME}.${HOSTNAME_BASE}:27018" \
        SINGULARITYENV_MSPASS_ROLE=shard \
        mpiexec.hydra -n 1 -ppn 1 -hosts ${WORKER_LIST_ARR[i]} $SING_COM &
    done

    # Launch the jupyter notebook frontend in the primary node.  
    # Run in batch mode if the script was
    # submitted with a "-b notebook.ipynb" 
    if [ $# -eq 0 ]; then
        SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
        SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
        SINGULARITYENV_MSPASS_SHARD_ADDRESS=${SHARD_ADDRESS[@]} \
        SINGULARITYENV_MSPASS_SHARD_DB_PATH=${SHARD_DB_PATH[@]} \
        SINGULARITYENV_MSPASS_SHARD_LOGS_PATH=${SHARD_LOGS_PATH[@]} \
        SINGULARITYENV_MSPASS_DB_MODE="shard" \
        SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM
    else
        while getopts "b:" flag
        do
            case "${flag}" in
                b) notebook_file=${OPTARG};
            esac
        done
        SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
        SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
        SINGULARITYENV_MSPASS_SHARD_ADDRESS=${SHARD_ADDRESS[@]} \
        SINGULARITYENV_MSPASS_SHARD_DB_PATH=${SHARD_DB_PATH[@]} \
        SINGULARITYENV_MSPASS_SHARD_LOGS_PATH=${SHARD_LOGS_PATH[@]} \
        SINGULARITYENV_MSPASS_DB_MODE="shard" \
        SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM --batch $notebook_file
else
    echo "Using Single node MongoDB"
    # start a db container in the primary node
    SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
    SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
    SINGULARITYENV_MSPASS_ROLE=db $SING_COM &
    # ensure enough time for db instance to finish
    sleep 10

    # Launch the jupyter notebook frontend in the primary node.  
    # Run in batch mode if the script was
    # submitted with a "-b notebook.ipynb" 
    if [ $# -eq 0 ]; then
        SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
        SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \
        SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM
    else
        while getopts "b:" flag
        do
            case "${flag}" in
                b) notebook_file=${OPTARG};
            esac
        done
        SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
        SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
        SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \
        SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM --batch $notebook_file
fi
