#!/bin/bash

#SBATCH -J mspass           # Job name
#SBATCH -o mspass.o%j       # Name of stdout output file
#SBATCH -J mspass           # Job name
#SBATCH -N 1               # Total # of nodes (must be 1 for serial)
#SBATCH -t 02:00:00        # Run time (hh:mm:ss)

SCRATCH=/N/scratch/pavlis
WORK2=/N/slate/pavlis
# working directory
WORK_DIR=$WORK2/usarray
# directory where contains docker image
#MSPASS_CONTAINER=$WORK2/mspass/mspass_latest.sif
#MSPASS_CONTAINER=~/containers/mspass_latest.sif
MSPASS_CONTAINER=/N/slate/pavlis/containers/mspass_latest.sif
# specify the location where user wants to store the data
# should be in either tmp or scratch, default is scratch
DB_PATH=/N/slate/pavlis/usarray

ulimit -n 4096

# command that start the container
#SING_COM="apptainer  run --home /N/slate/pavlis -B /N/scratch/pavlis $MSPASS_CONTAINER"
#SING_COM="apptainer  run -B /N/slate/pavlis/usarray --home /N/slate/pavlis/usarray $MSPASS_CONTAINER"
#SING_COM="apptainer  run -B /N/slate/pavlis/usarray $MSPASS_CONTAINER"

echo $MSPASS_WORKER_ARG
echo $WORK_DIR
echo $MSPASS_CONTAiNER

module load apptainer/1.1.9

cd $WORK_DIR

export APPTAINERENV_MSPASS_WORKER_ARG="--nworkers=4 --nthreads=1 --memory-limit='8 GiB'"

apptainer  run \
  -B /N/slate/pavlis/usarray \
  --home $WORK_DIR \
	--env MSPASS_WORKER_ARG="--nworkers=8 --nthreads=1 --memory-limit=16G",MSPASS_DB_PATH=$DB_PATH,MSPASS_WORK_DIR=$WORK_DIR \
	$MSPASS_CONTAINER
