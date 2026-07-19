.. _deploy_mspass_with_docker_compose:

Deploy MsPASS with Docker Compose
=================================

Docker Compose runs the MsPASS database, scheduler, worker, and JupyterLab
frontend in separate containers on one computer.  This is useful when you
want to inspect or restart each service independently.  For the simplest
desktop setup, use :ref:`Run MsPASS with Docker <run_mspass_with_docker>`
instead.

Prerequisites
-------------

Install Docker Desktop or Docker Engine with the Docker Compose plugin.  The
commands below use the current ``docker compose`` command (with a space).
Check the installation with:

.. code-block:: bash

   docker version
   docker compose version

Choose a writable project directory and run all commands from that directory.
The shipped configurations mount the current directory at ``/home`` in every
container, so notebooks, database files, logs, and results remain on the host.

How the containers work together
--------------------------------

An MsPASS deployment is made from containers with different roles.  Docker
Compose gives the containers a shared network and starts them with the
addresses and settings they need to communicate.  Understanding these roles
is helpful when you read a Compose file or diagnose a service that did not
start:

* ``frontend`` runs JupyterLab and connects the user's notebook to the
  database and the parallel scheduler.
* ``scheduler`` runs either a Dask scheduler or a Spark master.  It assigns
  parallel work to the workers.
* ``worker`` runs a Dask worker or Spark worker that performs the computation.
* ``db`` runs one standalone MongoDB server.  This is the database role used
  by the standard Dask and Spark examples on this page.
* ``dbmanager`` runs the MongoDB configuration and routing services for a
  sharded database.  It is used with one or more ``shard`` containers, not
  with the standalone ``db`` container.
* ``shard`` stores part of a sharded MongoDB database.  Multiple shards can
  distribute a large database across storage devices or hosts.
* ``all`` combines the frontend, scheduler, worker, and standalone database
  in one container.  It is the default role used by the simpler
  :ref:`single-container instructions <run_mspass_with_docker>`.

The image selects a role with the ``MSPASS_ROLE`` environment variable.  The
other important variables describe the scheduler and connect the services:

* ``MSPASS_SCHEDULER`` selects ``dask`` or ``spark``.  The default is
  ``dask``.
* ``MSPASS_SCHEDULER_ADDRESS`` gives workers and the frontend the hostname of
  the scheduler service.  In the supplied files that hostname is
  ``mspass-scheduler``.
* ``MSPASS_DB_ADDRESS`` gives the frontend the hostname of its database
  service.  It is ``mspass-db`` for a standalone database and
  ``mspass-dbmanager`` only for the sharded configuration.
* ``MSPASS_SHARD_LIST`` tells a database manager which shard services belong
  to the cluster.  Each entry has the form ``name/host:port``.
* ``MSPASS_SHARD_ID`` gives each shard a unique identity and keeps its data
  separate when shards share a mounted filesystem.
* ``MSPASS_JUPYTER_PWD`` optionally sets the Jupyter password.  If it is
  unset, Jupyter generates a login token and prints it in the frontend log.
  An empty value permits access without a password and should be used only in
  an appropriately protected environment.

Several port variables are also available: ``JUPYTER_PORT`` defaults to
``8888``, ``DASK_SCHEDULER_PORT`` to ``8786``, ``SPARK_MASTER_PORT`` to
``7077``, and ``MONGODB_PORT`` to ``27017``.  Most users should keep these
container-side defaults.  If one of those ports is already occupied on the
host, change the host side of its Compose ``ports`` mapping instead.  Service
addresses and health checks must agree with any container-side port changes.

Run the Dask configuration
--------------------------

The standard configuration is
:download:`compose.yaml <../../../data/yaml/compose.yaml>`:

.. literalinclude:: ../../../data/yaml/compose.yaml
   :language: yaml
   :linenos:
   :caption: Standard Docker Compose configuration using Dask

Save the file as ``compose.yaml`` in your project directory, then start it:

.. code-block:: bash

   docker compose up -d

Docker downloads the image automatically if it is not already installed.
The configuration starts four services:

* ``mspass-db`` runs a standalone MongoDB server.
* ``mspass-scheduler`` runs the Dask scheduler.
* ``mspass-worker`` starts four single-threaded Dask worker processes.
* ``mspass-frontend`` runs JupyterLab.

Check that the services are running:

.. code-block:: bash

   docker compose ps

Initial startup can take a minute.  If the frontend is not ready, view its
log with:

.. code-block:: bash

   docker compose logs mspass-frontend

Open ``http://127.0.0.1:8888/`` in a browser and enter the password
``mspass``.  The Dask dashboard is available at
``http://127.0.0.1:8787/status``.

When finished, stop and remove the containers with:

.. code-block:: bash

   docker compose down

Files in the project directory are not removed.  In particular, the startup
scripts create ``db/`` for MongoDB data, ``logs/`` for service logs, and
``work/`` for worker scratch files.

Common adjustments
------------------

The supplied file is intended for local use.  It publishes service ports on
all host interfaces, uses the known Jupyter password ``mspass``, and does not
enable MongoDB authentication.  On an untrusted network, choose a private
Jupyter password and bind published ports to loopback; for example, change
``8888:8888`` to ``127.0.0.1:8888:8888``.

Other common changes are:

* Change the host side of a port mapping if a port is already in use.  For
  example, ``9999:8888`` makes JupyterLab available on host port ``9999``.
* Adjust ``MSPASS_WORKER_ARG`` to change the number of Dask worker processes.
  Do not request more CPU or memory than Docker has available.
* Add the same bind mount to every service that needs access to waveform data
  stored outside the project directory.  Paths used by notebooks and workers
  must refer to the common path inside the containers.

After editing the file, check its resolved configuration before restarting:

.. code-block:: bash

   docker compose config
   docker compose up -d

Run the Spark configuration
---------------------------

MsPASS also provides
:download:`docker-compose_spark.yaml
<../../../data/yaml/docker-compose_spark.yaml>`:

.. literalinclude:: ../../../data/yaml/docker-compose_spark.yaml
   :language: yaml
   :linenos:
   :caption: Docker Compose configuration using Spark

Save the file in the project directory and run:

.. code-block:: bash

   docker compose -f docker-compose_spark.yaml up -d
   docker compose -f docker-compose_spark.yaml ps

This configuration replaces the Dask scheduler and worker with a Spark master
and worker.  It still uses the standalone ``mspass-db`` service, and the
frontend's ``MSPASS_DB_ADDRESS`` must therefore be ``mspass-db``.  The Spark
scheduler and database health checks delay dependent services until they are
ready.

Stop the Spark services with:

.. code-block:: bash

   docker compose -f docker-compose_spark.yaml down

Do not run the Dask and Spark configurations together in the same project;
they reuse service names and host ports.

Sharded MongoDB is a separate example
-------------------------------------

The ``mspass-dbmanager`` name is valid only in
``data/yaml/docker-compose_sharding.yaml``.  That file defines a
``mspass-dbmanager`` service with the ``dbmanager`` role and two MongoDB shard
services.  The standard Dask and Spark files use a standalone database named
``mspass-db`` and must not point their frontend to ``mspass-dbmanager``.

Troubleshooting
---------------

Use ``docker compose ps -a`` to find services that exited and ``docker
compose logs SERVICE`` to read a service's startup output.  The most common
causes are a port already in use, an unwritable bind-mounted directory, or
too little memory assigned to Docker.  If a configuration was edited, run
``docker compose config`` to catch YAML and variable-substitution errors.

For larger or multi-node deployments, continue with the
:ref:`virtual-cluster overview <getting_started_overview>` and
:ref:`HPC deployment guide <deploy_mspass_on_HPC>`.
