#! /bin/bash

# See User's Manual for guidance on setting these variables
export MSPASS_HOME=~/mspass
export MSPASS_CONTAINER=$MSPASS_HOME/containers/mspass_latest.sif

export WORK_DIR=/N/scratch/pavlis/mspass/workdir
export DB_PATH="/N/scratch/pavlis/work/db"

export HOSTNAME_BASE="uits.iu.edu"

if [ -z $MSPASS_RUNSCRIPT] ; then
    export MSPASS_RUNSCRIPT $MSPASS_HOME/cluster_definitions/run_mspass.sh
fi
