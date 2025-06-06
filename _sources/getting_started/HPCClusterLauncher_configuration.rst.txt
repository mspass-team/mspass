.. _hpc_cluster_configuration:

================================================
HPCClusterLauncher Configuration
================================================
-------------------------
Overview
-------------------------
This document gives details of configurations parameters
needed to launch a valid instance of the
:code:`HPCClusterLauncher` object.  This document has
two section:

1.  General parameters that can be thought of as generic system
    configuration parameters. The keys for all these attributes are
    in the top level of the yaml file hierarchy.
2.  Parameters that define this particular instance of a virtual
    cluster.   All these parameters are at level 2 of the yaml file
    under the key :code:`HPC_Cluster`.

----------------------------
General attributes
----------------------------

File parameters
~~~~~~~~~~~~~~~~~

- *container*  path to the file created by running :code:`apptainer build`.
- *working_directory*  should be the path to the working directory for this
  project.  This is the directory a job script would usually run a
  cd command to make it the current directory for the job.  It is needed in this
  context to assure the containers all are rooted at a common point.
- *log_directory* will contain the log files created by the services run in
  the containers.
- *database_directory*  data directory for MongoDB.  MongoDB stores is information
  in data files that live in this directory.  If the system has file systems with
  different speeds use the fastest available as database performance can usually
  io bound.
- *worker_directory* worker containers require a scratch space to "spill" in some
  contexts when memory use is tight.  This directory should be unique to make it
  easy to clear this debris after a given run.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~
General system parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- *workers_per_node* is the number of worker processes that should be launched
  on each dedicated, worker node.   In most cases this should be the number of
  cores per node.   When working with large ensembles we have found it helpful at
  times to reduce this number to avoid memory faults.
- *primary_node_worker* if set nonzero a worker container with this many
  worker processes will be launched on the primary node.   This options is
  most useful on newer systems with enough cores that there are idle cpu cycles
  not consumed by the database or scheduler services that currently are always
  run on the primary node.
- *cluster_subnet_name* is what the name implies.   Cluster nodes are
  normally connected in a common subnet and this should provide the name to
  allow the job components to communicate correctly.
- *db_startup_delay* is the number of seconds to wait after launching the db
  container.   This is necessary because the MongoDB server startup is not trivial
  and many MsPASS jobs initiate a connection to MongoDB at the top of the
  run script.   The default is conservative, but for typical HPC jobs that can
  run for hours is relatively unimportant.

------------------------------
HPC_cluster parameters
------------------------------
This set of parameters must be found in the yaml file hierarchy under
the key *HPC_cluster* as can be seen in the template file.

- *job_scheduler* is the name of the job scheduler.  Currently the only
  job scheduler that has been tested with :code:`HPCClusterLauncher` is slurm.
  You may have to alter the python code of :code:`HPCClusterLauncher` if a
  different job scheduler is used on your cluster.
- *primary_host*, *database_host*, *scheduler_host*, and *worker_hosts* should
  normally be set to "auto".  When *primary_host* set to auto it is set to the
  first node returned by querying the job scheduler.
  If *database_host* or *scheduler_host* are set to "auto" that service will
  be run on the node set as primary.  When *worker_hosts* is set as "auto"
  worker containers with *workers_per_node* processes will be launched on
  all nodes not defined as the primary node.
- *container_run_command* is the command string used to launch a container.
- *container_run_args* is a string of secondary arguments required to launch
  a container.
- *container_env_flag* is passed as a flag when launching containers
  to control what is launched.  As described in the section on the shell
  script launch option the service a container runs is defined by
  environment variable values passed with the *contaienr_run_command*.
- *worker_run_command* is the command line program used to launch programs
  on other nodes.  Currently the only known mechanism is to use mpiexec.
  This attribute is an option only because some sites may use a special
  version of mpiexec that may have a different name.
- *setup_tunnel* and *tunnel_setup_command* are linked. *tunnel_setup_command*
  is only used if *setup_tunnel* is set True.  The purpose of *tunnel_setup_command*
  is to enable external communications to MsPASS from outside the cluster,
  It is necessary to set this up if you want to connect to dask diagnostics
  or run an interactive job with one or more compute nodes. For guidance on
  how to construct this shell script look at the shell-based launchers
  found `here on GitHub<https://github.com/mspass-team/mspass/tree/master/scripts>`__.

.. note::
   The variables *container_run_command*, *container_run_args*, and
   *container_env_flag* in the template file work only for
   apptainer.  If using the older singularity implementation the
   syntax is identical but "apptainer" should change to "singularity".
   The command line options for the two implementations are currently
   identical.   If a different container launcher is used at your site
   you may need to alter the python code of :code:`HPCClusterLauncher`.
