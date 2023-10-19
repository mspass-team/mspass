#!/bin/bash
# If your cluster uses slurm top needs to be some variant of this
# Check documentation for additional arguments that may be required or desirable
#SBATCH -J mspass           # Job name
#SBATCH -o mspass.o%j       # Name of stdout output file
#SBATCH -p skx-normal       # Queue (partition) name - system dependent
#SBATCH -N 2                # Total # of nodes
#SBATCH -n 2                # Total # of mpi tasks (normally the same as -N)
#SBATCH -t 00:10:00         # Run time (hh:mm:ss)
#SBATCH -A MsPASS           # Allocation name (req'd if you have more than 1)

# This is a typical load line with module.   The equivalent functionality
# locally may be different.  Functionality is like shell "dot" initialization
# files
ml unload xalt
ml tacc-singularity
module list

# See User' Manual for guidance on building this file
# Actual file path is system/user dependent
if [[ -x "mspass_setup.sh"  ]]
then
    echo "the setup file is read "
else
    echo "the setup file is not executable"
    chmod +x mspass_setup.sh
fi

source mspass_setup.sh

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

if [[ -x "$MSPASS_RUNSCRIPT"  ]]
then
    echo "$MSPASS_RUNSCRIPT is ready"
else
    echo "$MSPASS_RUNSCRIPT is not executable"
    chmod +x $MSPASS_RUNSCRIPT
fi

$MSPASS_RUNSCRIPT $notebook_file
