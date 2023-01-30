#!/bin/bash
# If your cluster uses slurm top needs to be some variant of this
# Check documentation for additional arguments that may be required or desirable
#SBATCH -J mspass           # Job name
#SBATCH -o mspass.o%j       # Name of stdout output file
#SBATCH -N 3                # Total # of nodes - 3 for this example
#SBATCH -n 3                # Total # of mpi tasks (normally the same as -N)
#SBATCH -t 02:00:00         # Run time (hh:mm:ss)

# This is a typical load line with module.   The equivalent functionality
# locally may be different.  Functionality is like shell "dot" initialization
# files
module load singularity
module load openmp
module list

# See User' Manual for guidance on building this file
# Actual file path is system/user dependent
source mspass_setup.sh
cd $WORK_DIR

if ($#==1); then
  notebook_file=$1
else
  while getopts "b:" flag
    do
        case "${flag}" in
            b) notebook_file=${OPTARG};
        esac
    done
fi
$MSPASS_RUNSCRIPT $notebook_file
