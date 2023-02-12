#!/bin/bash

if [ $# != 1 ]; then
  echo "Usage error:  run_mspass.sh notebook_file"
  echo "  This script is only running the notebook in batch mode"
  exit 1
else
  notebook_file=`pwd`/$1
fi


SING_COM="singularity run $MSPASS_CONTAINER"
# We always need the hostname of the node running this script
# it launches all containers other than the workers.  Workers 
# need to know this name only
NODE_HOSTNAME=`hostname -s`

#### Set up networking ####
# See User manual for guidance to adapt to other systems
# These are only needed if you are running interactively.
# They are commented out in this template to reduce conversion 
# efforts but you may need to enable this block for connections from 
# outside the cluster
# The approach used at TACC to get a unique port doesn't work on this cluster.
# Using fixed ports hoping this won't cause collisions
#LOGIN_PORT=8888
#STATUS_PORT=8787
#echo "using login node port $LOGIN_PORT"

#NUMBER_LOGIN_NODES=2
#LOGIN_NODE_BASENAME=login
#for i in `seq $NUMBER_LOGIN_NODES`; do
#    ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 $LOGIN_NODE_BASENAME$i
#    ssh -q -f -g -N -R $STATUS_PORT:$NODE_HOSTNAME:8787 $LOGIN_NODE_BASENAME$i
#done
#echo "Created reverse ports to carbonate head notes"
#### End networking section ####

mkdir -p $MSPASS_WORK_DIR
cd $MSPASS_WORK_DIR
pwd
env

# start a distributed scheduler container in the primary node
SINGULARITYENV_MSPASS_WORK_DIR=$MSPASS_WORK_DIR \
SINGULARITYENV_MSPASS_ROLE=scheduler $SING_COM &

# get the all the hostnames of worker nodes (those != node running script)
NODE_HOSTNAME=`hostname -s`
echo "primary node $NODE_HOSTNAME"
WORKER_LIST=`scontrol show hostname ${SLURM_NODELIST} | \
             awk -vORS=, -v hostvar="$NODE_HOSTNAME" '{ if ($0!=hostvar) print $0 }' | \
             sed 's/,$/\n/'`
echo $WORKER_LIST

# start worker container in each worker node
SINGULARITYENV_MSPASS_WORK_DIR=$MSPASS_WORK_DIR \
  SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
  SINGULARITYENV_MSPASS_ROLE=worker \
  mpiexec -n $((SLURM_NNODES-1)) -host $WORKER_LIST $SING_COM &
echo "mpiexec -n $((SLURM_NNODES-1)) -host $WORKER_LIST $SING_COM "


# start a db container in the primary node
SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
  SINGULARITYENV_MSPASS_WORK_DIR=$MSPASS_WORK_DIR \
  SINGULARITYENV_MSPASS_ROLE=db $SING_COM &
# ensure enough time for db instance to finish
sleep 10

# Launch the jupyter notebook frontend in the primary node.
# This example always runs the notebook on startup and 
# exits the job when the notebook code exits
# DO NOT add a & to the end of this line.  We need the script to block until
# the notebook exits

SINGULARITYENV_MSPASS_WORK_DIR=$MSPASS_WORK_DIR \
  SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
  SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
  SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \

# Note when the calling script exits the system will kill the containers 
# running in other nodes
SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM --batch $notebook_file
