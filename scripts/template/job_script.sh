#!/bin/bash
# If your cluster uses slurm top needs to be some variant of this
# Check documentation for additional arguments that may be required or desirable
#SBATCH -J mspass           # Job name
#SBATCH -o pavlis.o%j       # Name of stdout output file
#SBATCH -N 2                # Total # of nodes - 3 for this example
#SBATCH -n 2                # Total # of mpi tasks (normally the same as -N)
#SBATCH -t 00:10:00         # Run time (hh:mm:ss)

# This is a typical load line with module.   The equivalent functionality
# locally may be different.  Functionality is like shell "dot" initialization
# files
module load singularity
module load openmpi
module list

# See User' Manual for guidance on building this file
# Actual file path is system/user dependent
source /N/slate/pavlis/test_scripts/mspass_setup.sh
# Note your jupyter notebook must be in this directory for this script
cd $MSPASS_WORK_DIR

if [ 1$#==1 ]; then
  notebook_file=$1
else
  while getopts "b:" flag
    do
        case "${flag}" in
            b) notebook_file=${OPTARG};
        esac
    done
fi
echo Running $notebook_file
$MSPASS_RUNSCRIPT $notebook_file
