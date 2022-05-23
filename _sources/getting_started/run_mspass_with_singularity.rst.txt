.. _run_mspass_with_singularity:

Run MsPASS with Singularity
===========================

Introduction to Singularity
---------------------------
Singularity is a container implementation that runs on HPC cluster environments. Singularity provides similar features as Docker, and can handle the “escalated privilege” issue of Docker. Singularity is compatible with Docker containers, which means that one can develop the container in Docker, and use Singularity as the runtime on HPC clusters. 


Create MsPASS Container
-----------------------
Singularity is compatible with docker container, which is a more widely-used implementation. So we would recommend building a singularity image from docker.

First of all, use the following command to load the singularity module on HPC system:

.. code-block::

    module load tacc-singularity

Then, execute:

.. code-block::

    singularity build mspass.simg docker://wangyinz/mspass

The command above will build a Singularity container corresponding to the latest released MsPass docker container, and save to mspass.simg in the current directory.


Running MsPass with Singularity in All-in-one Mode
--------------------------------------------------
MsPass consists of 3 components: Mongodb server, Workers and Scheduler. In this section, we will introduce how to run these applications (MongoDB, Dask/Spark) with Singularity on a single standalone machine.

To use the Singularity command to launch a MsPass container, one can use the entrypoint script. The entrypoint script contains all the commands for setting up and starting MongoDB and Dask/Spark. Users only need to configure some variables before running the script. 

To start a standalone instance, use:

.. code-block::

    SINGULARITYENV_MSPASS_ROLE=all;
    singularity run mspass.simg

You might want to collect the log output of dask-scheduler by adding > ./mspass_log 2>&1 & at the end of the command.
For more information on the usage of entrypoint script, please refer to the source code 
`here <https://github.com/mspass-team/mspass/blob/master/scripts/start-mspass.sh>`__. Some other important environment variables are listed below:

- ``SINGULARITYENV_MSPASS_WORK_DIR``: The directory that stores worker’s runtime data.
- ``SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS``: This is the IP address or hostname of the ``scheduler``. 
- ``SINGULARITYENV_MSPASS_DB_PATH``: This is the directory that stores mongodb data, should be either ``tmp`` or ``scratch`` on the TACC machine. 
- ``SINGULARITYENV_MSPASS_SCHEDULER``: User can use ``dask`` or ``spark`` to run parallel computations. 
  ``dask`` is the default.