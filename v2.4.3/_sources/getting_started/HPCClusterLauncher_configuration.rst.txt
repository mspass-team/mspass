.. _hpc_cluster_configuration:

HPCClusterLauncher Configuration
================================

Overview
--------

``HPCClusterLauncher`` reads two groups of settings from YAML:

1.  General parameters that can be thought of as generic system
    configuration parameters.  The keys for all these attributes are
    at the top level of the YAML file hierarchy.
2.  Parameters that define this particular instance of a virtual
    cluster.  All these parameters are at level 2 of the YAML file
    under the case-sensitive key :code:`HPC_cluster`.

The launcher is distributed by the separate `mspass_launcher project
<https://github.com/mspass-team/mspass_launcher>`__.  Its release and
development schemas differ, so always start with the template shipped with
the launcher version you installed.  A missing or misspelled required key
normally raises ``KeyError``.

General attributes
------------------

File parameters
~~~~~~~~~~~~~~~~~

- *container* is the path to the file created by running
  :code:`apptainer build`.  Every allocated node must be able to read it.
- *working_directory* should be the path to the working directory for this
  project.  This is the directory a job script would usually run a
  ``cd`` command to make current.  It is needed in this context to assure the
  containers all are rooted at a common, shared point.
- *log_directory* is required by the configuration loader, but the HPC
  launcher does not pass it to the containers.  With the stock MsPASS image,
  logs therefore default to ``working_directory/logs``.
- *database_directory* is the data directory for MongoDB.  MongoDB stores its
  information in data files that live in this directory.  If the system has
  filesystems with different speeds, use the fastest suitable persistent
  filesystem because database performance can be I/O bound.
- *worker_directory* is also required by the configuration loader, but the
  launcher does not pass it to worker containers.  With the stock image, Dask
  spill files therefore default to ``working_directory/work``.

General system parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- *workers_per_node* is the number of worker processes that should be launched
  on each dedicated worker node.  In most cases this should be near the number
  of cores per node.  When working with large ensembles we have found it
  helpful at times to reduce this number to avoid memory faults.  The launcher
  configures one thread per worker.
- *primary_node_workers* (plural) controls how many worker processes are
  launched on the primary node.  Set it to ``0`` to reserve that node for the
  database, scheduler, and frontend.  It must be positive for a single-node
  allocation because there are no remote worker nodes.
- *cluster_subnet_name* is a required compatibility field, but release 0.1.1
  and the current development implementation only store it.  Slurm hostname
  discovery, not this value, determines service placement.
- *db_startup_delay* appears in the release 0.1.1 template but is not read by
  release 0.1.1 or current development code.  Do not rely on it to delay
  MongoDB startup.
- *worker_memory_limit* is optional on the current development branch and is
  not available in release 0.1.1.  When set, it is passed to remote Dask
  workers as ``MSPASS_DASK_WORKER_MEMORY_LIMIT`` (for example, ``2GB``).

HPC_cluster parameters
----------------------
This set of parameters must be found in the YAML file hierarchy under
the key *HPC_cluster* as can be seen in the template file.

- *job_scheduler* is the name of the job scheduler.  Currently
  :code:`HPCClusterLauncher` accepts only ``slurm``.  Supporting a different
  job scheduler requires launcher changes.
- *task_scheduler* is required and is passed to the container as
  ``MSPASS_SCHEDULER``.  ``dask`` is the template default and the documented
  HPC path.
- *primary_host*, *database_host*, *scheduler_host*, and *worker_hosts* should
  be set to ``auto``.  The launcher uses the first node returned by
  ``scontrol show hostname`` for the primary, database, and scheduler and uses
  the remaining nodes as remote workers.  Release 0.1.1 does not reliably
  support explicit worker hosts, and neither release 0.1.1 nor current
  development code initializes all required attributes when all service hosts
  are explicit.
- *container_run_command* is the command string used to launch a container,
  normally ``apptainer run``.
- *container_run_args* is a string of secondary arguments required to launch
  a container, commonly bind mounts and an Apptainer home directory.  The
  launcher tokenizes this string with ``split()``, so paths containing spaces
  are not supported reliably.
- *container_env_flag* is passed as the flag used to set environment values in
  service containers.  Apptainer uses ``--env``.  Some launcher paths still
  hard-code ``--env``, so this field does not make arbitrary container
  runtimes portable.
- *worker_run_command* is the MPI launcher command used by release 0.1.1,
  normally ``mpiexec``.  Development versions use a different set of MPI
  keys; copy them from the matching template rather than mixing schemas.
- *setup_tunnel* and *tunnel_setup_command* are intended to run a site-specific
  tunnel command for Dask diagnostics or an interactive session.  Do not
  enable them in release 0.1.1 or current development code: both construct the
  configured command but mistakenly rerun the preceding Slurm hostname
  command.  Use a site-approved procedure, such as the maintained
  `shell launcher examples
  <https://github.com/mspass-team/mspass/tree/master/scripts>`__, until that
  launcher bug is fixed.

Use the `release 0.1.1 template
<https://github.com/mspass-team/mspass_launcher/blob/v0.1.1/src/mspass_launcher/data/yaml/HPCClusterLauncher.yaml>`__
for the published package.  When intentionally testing development code, use
the `development template
<https://github.com/mspass-team/mspass_launcher/blob/main/src/mspass_launcher/data/yaml/HPCClusterLauncher.yaml>`__
from the same checkout.

.. note::
   Apptainer and older Singularity versions use compatible syntax for these
   settings.  For Singularity, change ``apptainer`` to ``singularity`` in
   *container_run_command*.  A different container runtime may require
   launcher code changes.
