#!/bin/bash
#SBATCH -J mspass          # Job name
#SBATCH -o mspass.o%j      # Name of stdout output file
#SBATCH -p normal          # Queue (partition) name
#SBATCH -N 2               # Total # of nodes (must be 1 for serial)
#SBATCH -n 2               # Total # of mpi tasks (should be 1 for serial)
#SBATCH -t 02:00:00        # Run time (hh:mm:ss)
#SBATCH -A MsPASS          # Allocation name (req'd if you have more than 1)

# working directory
WORK_DIR=$SCRATCH/SAGE_2021
# directory where contains docker image
MSPASS_CONTAINER=$WORK/mspass/mspass_latest.sif

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

# control the interval between mongo instance and mongo shell execution
SLEEP_TIME=15
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
SINGULARITYENV_MSPASS_TMP_DATA_DIR='/tmp/data_files' \
SINGULARITYENV_MSPASS_SCRATCH_DATA_DIR=$WORK_DIR/data_files \
SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_WORKER_ARG="--nprocs 48 --nthreads 1" \
SINGULARITYENV_MSPASS_ROLE=worker \
mpiexec.hydra -n $((SLURM_NNODES-1)) -ppn 1 -hosts $WORKER_LIST $SING_COM &

# start a db container in the primary node
SINGULARITYENV_MSPASS_DB_PATH='tmp' \
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_TMP_DATA_DIR='/tmp/data_files' \
SINGULARITYENV_MSPASS_SCRATCH_DATA_DIR=$WORK_DIR/data_files \
SINGULARITYENV_MSPASS_ROLE=db $SING_COM &
# ensure enough time for db instance to finish
sleep 10

# start a jupyter notebook frontend in the primary node
SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_DB_ADDRESS=$NODE_HOSTNAME \
SINGULARITYENV_MSPASS_SLEEP_TIME=$SLEEP_TIME \
SINGULARITYENV_MSPASS_ROLE=frontend $SING_COM