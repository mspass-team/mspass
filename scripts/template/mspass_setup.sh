#! /bin/bash

# See User's Manual for more guidance on setting these variables
export MSPASS_HOME=~/mspass
# full path to the singularity container
export MSPASS_CONTAINER=$WORK2/mspass/images/mspass_latest
# If needed list all file systems names that should be mounted when
# the container boots.  Usually an explicit path is best to avoid
# errors from aliasing a directory name
export SINGULARITY_BIND=~/others

# if not set this defaults to the directory where the run script is called
export MSPASS_WORK_DIR=$WORK/temp
# if not set this defaults to $MSPASS_WORK_DIR/db
#export MSPASS_DB_DIR=$WORK/temp/db
# if not set this defaults to $MSPASS_WORK_DIR/logs
#export MSPASS_LOG_DIR=$WORK/temp/logs
# if not set this defaults to $MSPASS_WORK_DIR/work
#export MSPASS_WORKER_DIR=$WORK/temp/work


export HOSTNAME_BASE="stampede2.tacc.utexas.edu"

if [ -z $MSPASS_RUNSCRIPT ] ; then
    export MSPASS_RUNSCRIPT=./run_mspass.sh
fi
