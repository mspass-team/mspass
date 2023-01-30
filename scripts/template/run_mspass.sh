#!/bin/bash

if ($# != 1); then
  echo "Usage error:  run_mspass.sh notebook_file"
  echo "  This script is only running the notebook in batch mode"
  exit(-1)
else
  notebook_file=$1
fi

SING_COM="singularity run $MSPASS_CONTAINER"

#### Set up networking ####
# See User manual for guidance to adapt to other systems
NODE_HOSTNAME=`hostname -s`
LOGIN_PORT=`echo $NODE_HOSTNAME | perl -ne 'print (($2+1).$3.$1) if /c\d(\d\d)-(\d)(\d\d)/;'`
STATUS_PORT=`echo "$LOGIN_PORT + 1" | bc -l`
echo "got login node port $LOGIN_PORT"

NUMBER_LOGIN_NODES=4
LOGIN_NODE_BASENAME=login
for i in `seq $NUMBER_LOGIN_NODES`; do
    ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 $LOGIN_NODE_BASENAME$i
    ssh -q -f -g -N -R $STATUS_PORT:$NODE_HOSTNAME:8787 $LOGIN_NODE_BASENAME$i
done
echo "Created reverse ports on Stampede2 logins"
#### End networking section ####

mkdir -p $WORK_DIR
cd $WORK_DIR

# start a distributed scheduler container in the primary node
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_ROLE=scheduler $SING_COM &

# get the all the hostnames of worker nodes (those != node running script)
NODE_HOSTNAME=`hostname -s`
echo "primary node $NODE_HOSTNAME"
WORKER_LIST=`scontrol show hostname ${SLURM_NODELIST} | \
             awk -vORS=, -v hostvar="$NODE_HOSTNAME" '{ if ($0!=hostvar) print $0 }' | \
             sed 's/,$/\n/'`
echo $WORKER_LIST

# start worker container in each worker node
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
  SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
  SINGULARITYENV_MSPASS_ROLE=worker \
  mpiexec -n $((SLURM_NNODES-1)) -host $WORKER_LIST $SING_COM &
echo "mpiexec -n $((SLURM_NNODES-1)) -host $WORKER_LIST $SING_COM "


# start a db container in the primary node
SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
  SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
  SINGULARITYENV_MSPASS_ROLE=db $SING_COM &
# ensure enough time for db instance to finish
sleep 10

# Launch the jupyter notebook frontend in the primary node.
# This example always runs the notebook on startup and 
# exits the job when the notebook code exits
# DO NOT add a & to the end of this line.  We need the script to block until
# the notebook exits

SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
  SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
  SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
  SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \

# Note when the calling script exits the system will kill the containers 
# running in other nodes
  SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM --batch $notebook_file
