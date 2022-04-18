.. _deploy_mspass_on_HPC:

Deploy MsPASS on HPC
====================

As described in :ref:`previous section <run_mspass_with_singularity>`,
We can run a MsPASS instance using the singularity command. However, lots of configurations are needed, especially when
user wants to run MsPASS on distributed nodes, where each node perform a different role. To simplify the configuration,
two example scripts are given: `single_node.sh <https://github.com/mspass-team/mspass/blob/master/scripts/tacc_examples/single_node.sh>`__
and `distributed_node.sh <https://github.com/mspass-team/mspass/blob/master/scripts/tacc_examples/distributed_node.sh>`__.

In this section, we will briefly describe how to use these scripts.

Please note that these scripts are written for the TACC environment, users might need to make some changes to the scripts to accustom them with their own HPC environment.

Quick Start
-----------

To run the MsPass on TACC Stampede machines, simply use: 

.. code-block::

    sbatch ./single_node.sh

or

.. code-block::

    sbatch ./distributed_node.sh
    
The sbatch commands will submit the batch file to the cluster. Then, the workload manager (Slurm) will allocate the nodes and run the batch script on the first node allocated.

After the script is commited and executed, a file will be created on the current directory (mspass.o{JOBID}), it stores all the log from MsPass:

.. code-block::

    Sun Mar 20 02:44:20 CDT 2022
    primary node c492-092
    got login node port 19292
    Created reverse ports on Stampede2 logins
    cleaning done
    [WARN  tini (144729)] [WARN  tini (144730)] Tini is not running as PID 1 and isn't registered as a child subreaper.
    Zombie processes will not be re-parented to Tini, so zombie reaping won't work.
    Zombie processes will not be re-parented to Tini, so zombie reaping won't work.

    [WARN  tini (206416)] Tini is not running as PID 1 and isn't registered as a child subreaper.
    Zombie processes will not be re-parented to Tini, so zombie reaping won't work.
    [WARN  tini (145112)] Tini is not running as PID 1 and isn't registered as a child subreaper.
    Zombie processes will not be re-parented to Tini, so zombie reaping won't work.
    To fix the problem, use the -s option or set the environment variable TINI_SUBREAPER to register Tini as a child subreaper, or run Tini as PID 1.
    [I 02:46:48.738 NotebookApp] Serving notebooks from local directory: /scratch/08431/ztyang/mspass/workdir
    [I 02:46:48.738 NotebookApp] Jupyter Notebook 6.2.0 is running at:
    [I 02:46:48.738 NotebookApp] http://c492-092.stampede2.tacc.utexas.edu:8888/?token=fee57b8bf77dc608fc32ecf6ad8d85a70467d84cf52b439d
    [I 02:46:48.738 NotebookApp]  or http://127.0.0.1:8888/?token=fee57b8bf77dc608fc32ecf6ad8d85a70467d84cf52b439d
    [I 02:46:48.738 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
    [C 02:46:48.749 NotebookApp]

        To access the notebook, open this file in a browser:
            file:///home1/08431/ztyang/.local/share/jupyter/runtime/nbserver-145143-open.html
        Or copy and paste one of these URLs:
            http://c492-092.stampede2.tacc.utexas.edu:8888/?token=fee57b8bf77dc608fc32ecf6ad8d85a70467d84cf52b439d
        or http://127.0.0.1:8888/?token=fee57b8bf77dc608fc32ecf6ad8d85a70467d84cf52b439d

As can be seen from above, you will get the token for jupyter notebook from the log. If the reverse tunnel port is enabled, you will get a login node port. Replace the 8888 with the new port number, you will get the external jupyter link. (In this case, ``http://stampede2.tacc.utexas.edu:19292/?token=fee57b8bf77dc608fc32ecf6ad8d85a70467d84cf52b439d``). In the meanwhile, another port will also be open for the status page, the port number is new port number + 1.

When MsPass is running, check the task status using:

.. code-block::

    showq | grep mspass
    
To stop the MsPass task, you can simply wait till the end of the run time, or explicitly cancel the task using:

.. code-block::

    scancel {JOBID}


Configuration for Single Node script
------------------------------------

single_node.sh is for setting up a standalone instance with the mongodb server, dask scheduler and worker, and the jupyter frontend all on a single node.

The script first setup some environment variables for HPCs:

.. code-block::

    #SBATCH -J mspass           # Job name
    #SBATCH -o mspass.o%j       # Name of stdout output file
    #SBATCH -p skx-dev          # Queue (partition) name
    #SBATCH -N 1                # Total # of nodes (must be 1 for serial)
    #SBATCH -n 1                # Total # of mpi tasks (should be 1 for serial)
    #SBATCH -t 02:00:00         # Run time (hh:mm:ss)
    #SBATCH -A MsPASS           # Allocation name (req'd if you have more than 1)

You might want to change some of these fields. Note: the queue name and allocation name here refer to 
`TACC Slurm Queues <https://portal.tacc.utexas.edu/user-guides/stampede2#running-queues>`__ and `TACC Allocation <https://portal.tacc.utexas.edu/allocations-overview>`__.

In HPC environment, users could only get access to the services on limited nodes (login1 - login4 in Stampede2). So we need to create reverse tunnel port to support external access to the jupyter notebook and the `status page <https://docs.dask.org/en/stable/diagnostics-distributed.html>`__.

.. code-block::

    for i in `seq 4`; do
        ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 login$i
        ssh -q -f -g -N -R $STATUS_PORT:$NODE_HOSTNAME:8787 login$i
    done

The code above creates one tunnel for each login node so that the users can just connect to stampede.tacc to visit the jupyter notebook and the status page. If you are not using a TACC machine, you might need to modify or delete this part to run on your own HPC environment.

Some other variables you might need to change:

- ``WORK_DIR``: This is a directory path to store all runtime dat.
- ``MSPASS_CONTAINER``: This is the path to the singualrity image to run.
- ``DB_PATH``: This variable indicates where the db data are stored, 
  there are two options: 'scratch' and 'tmp'. 'scratch' saves the DB data to a shared filesystem, 'tmp' saves the DB data to a local filesystem. 
  For more details on the difference between tmp and scratch, please refer to `TACC user guide <https://portal.tacc.utexas.edu/user-guides/stampede2#overview-filesystems>`__.


Configuration for Distributed Node Script
-----------------------------------------

To run MsPass on a distributed cluster, use the distributed_node script.

similar the single node script, the distribute node script also does setup at the beggining:

.. code-block::
        
    #SBATCH -J mspass           # Job name
    #SBATCH -o mspass.o%j       # Name of stdout output file
    #SBATCH -p skx-dev          # Queue (partition) name
    #SBATCH -N 3                # Total # of nodes (must be 1 for serial)
    #SBATCH -n 3                # Total # of mpi tasks (should be 1 for serial)
    #SBATCH -t 02:00:00         # Run time (hh:mm:ss)
    #SBATCH -A MsPASS           # Allocation name (req'd if you have more than 1)

The main difference here is the number of nodes and mpi tasks should be set to more than 1. One can set these two value to any number, but a larger number might result in longer waiting time in queue.

For the distribute node script, here are some variables:

- ``HOSTNAME_BASE``: This is the base address for each node, for example, on stampede environment, it should be 'stampede2.tacc.utexas.edu'
- ``DB_SHARDING``: This is a boolean value indicating whether the sharding is in use or not.
- ``SHARD_DATABASE``: This is the name of the database that users want to enable sharding.
- ``SHARD_COLLECTIONS``: This is the collection and index that users want to use for sharding, for example: ``("arrival:_id")``