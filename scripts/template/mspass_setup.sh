#! /bin/bash

# See User's Manual for more guidance on setting these variables
export MSPASS_HOME=~/mspass
# full path to the singularity container
export MSPASS_CONTAINER=${MSPASS_HOME}/containers/mspass_latest.sif
# If needed list all file systems names that should be mounted when
# the container boots.  Usually an explicit path is best to avoid 
# errors from aliasing a directory name
export SINGULARITY_BIND=/N/slate/pavlis,/N/scratch/pavlis

export MSPASS_WORK_DIR=/N/slate/pavlis/test_scripts
export MSPASS_DB_DIR=/N/scratch/pavlis/usarray/db
export MSPASS_LOG_DIR=/N/scratch/pavlis/usarray/logs
export MSPASS_WORKER_DIR=/N/scratch/pavlis/usarray/work


export HOSTNAME_BASE="carbonate.uits.iu.edu"

if [ -z $MSPASS_RUNSCRIPT ] ; then
    export MSPASS_RUNSCRIPT=/N/slate/pavlis/test_scripts/run_mspass.sh
fi
