.. _deploy_mspass_with_docker_compose:

Deploy MsPASS with Docker Compose
=================================

Docker Compose is the advanced desktop path for running the main MsPASS
services in separate containers.  The configuration shipped with MsPASS runs
JupyterLab, MongoDB, a Dask scheduler, and Dask workers as four independently
managed services on one Docker host.  This separation makes the services
easier to inspect, restart, and scale than the all-in-one container described
in :ref:`Run MsPASS with Docker <run_mspass_with_docker>`.

The verified Dask configuration in ``data/yaml/compose.yaml`` is the primary
path on this page.  A Spark alternative from
``data/yaml/docker-compose_spark.yaml`` is covered after the Dask workflow.
Both configurations use one standalone MongoDB service; neither is a sharded
database deployment.  Docker Compose also does not turn multiple containers
into a multi-host cluster.  Read the :ref:`virtual-cluster overview
<getting_started_overview>` before moving a workload to multiple machines or
an HPC system.

Prerequisites
-------------

Complete the following checks before starting:

* Install Docker Desktop or Docker Engine and the Docker Compose v2 plugin.
  The commands on this page use ``docker compose`` (with a space), not the
  legacy ``docker-compose`` executable.
* Choose a writable host directory with enough space for notebooks, waveform
  data, the MongoDB database, logs, and Dask worker files.
* Make enough CPU and memory available to Docker for MongoDB, JupyterLab, the
  scheduler, and four Dask worker processes.  The worker count can be changed
  later.
* Make sure host ports ``27017``, ``8786``, ``8787``, and ``8888`` are
  available, or edit the host side of the port mappings before startup.

Confirm that the Docker client can reach the server and that Compose v2 is
available:

.. code-block:: bash

   docker version
   docker compose version

``docker version`` should show both ``Client`` and ``Server`` sections.

Understand the shipped stack
----------------------------

The Compose file defines these service names:

.. list-table::
   :header-rows: 1
   :widths: 24 48 28

   * - Service
     - Role
     - Published host ports
   * - ``mspass-db``
     - A standalone MongoDB server.  Its database files are written under
       ``db/`` in the host project directory.
     - ``27017``
   * - ``mspass-scheduler``
     - The Dask scheduler.  Port ``8786`` carries scheduler traffic and port
       ``8787`` serves the Dask dashboard.
     - ``8786``, ``8787``
   * - ``mspass-worker``
     - A Dask worker container configured to start four single-threaded worker
       processes.
     - None
   * - ``mspass-frontend``
     - JupyterLab configured to use ``mspass-db`` and
       ``mspass-scheduler`` through the Compose network.
     - ``8888``

All four services use the ``mspass/mspass`` image and bind the host directory
represented by ``${PWD}`` to ``/home`` in each container.  That shared bind
mount is the persistent project workspace.  The service names above also act
as hostnames inside the private Compose network.

The exact configuration documented here is available as
:download:`compose.yaml <../../../data/yaml/compose.yaml>`.

.. literalinclude:: ../../../data/yaml/compose.yaml
   :language: yaml
   :linenos:
   :caption: The shipped four-service Dask configuration

How the container roles are connected
-------------------------------------

Every service uses the same MsPASS image.  ``MSPASS_ROLE`` tells the image's
startup script which process to run:

``db``
   Run a standalone MongoDB server.

``scheduler``
   Run a Dask scheduler or Spark master, as selected by
   ``MSPASS_SCHEDULER``.

``worker``
   Run a worker for the selected scheduler.

``frontend``
   Run JupyterLab and connect it to the configured database and scheduler.

``all``
   Run the frontend, scheduler, worker, and standalone database together.
   This is the default single-container role described in
   :ref:`Run MsPASS with Docker <run_mspass_with_docker>`, not a service in
   the Compose files on this page.

The startup script also supports ``dbmanager`` and ``shard`` roles for a
sharded MongoDB deployment.  Those roles and their ``MSPASS_SHARD_LIST`` and
``MSPASS_SHARD_ID`` settings belong to the separate
``data/yaml/docker-compose_sharding.yaml`` example.  Do not use a
``dbmanager`` address in either standalone-database configuration documented
here.

The other connection settings must agree across services:

* ``MSPASS_SCHEDULER`` selects ``dask`` or ``spark``.
* ``MSPASS_SCHEDULER_ADDRESS`` is the scheduler's internal Compose hostname;
  both shipped files use ``mspass-scheduler``.
* ``MSPASS_DB_ADDRESS`` is the database's internal Compose hostname; both
  shipped files use ``mspass-db``.
* ``DASK_SCHEDULER_PORT`` or ``SPARK_MASTER_PORT``, ``MONGODB_PORT``, and
  ``JUPYTER_PORT`` set container listening ports.  If an internal port is
  changed, update every matching address, wait condition, and port mapping.
  To avoid a host-side conflict, change only the published side of a port
  mapping as described under troubleshooting.
* ``MSPASS_JUPYTER_PWD`` controls Jupyter authentication, and
  ``MSPASS_WORKER_ARG`` tunes the Dask worker processes.  Both are discussed
  below where they are used.

.. warning::

   The shipped file publishes its four ports on all host network interfaces,
   does not configure MongoDB authentication, and sets the Jupyter password
   to the well-known value ``mspass``.  Treat it as a local or trusted-network
   example.  Before using it on an untrusted network, change
   ``MSPASS_JUPYTER_PWD`` and prefix each published port with ``127.0.0.1:``,
   for example ``127.0.0.1:8888:8888``.  Do not use an empty Jupyter password.

Prepare a persistent project directory
--------------------------------------

Create a dedicated directory and enter it.  The following example uses a
directory under the current user's home directory:

.. code-block:: bash

   export MSPASS_PROJECT="$HOME/mspass-compose"
   mkdir -p "$MSPASS_PROJECT"
   cd "$MSPASS_PROJECT"

Save the downloadable configuration above as ``compose.yaml`` in this
directory.  If you are working from a source checkout, copy
``data/yaml/compose.yaml`` from that checkout instead.

The file deliberately uses ``${PWD}`` as its bind-mount source.  Always
``cd`` to the project directory before running the commands on this page,
including after opening a new terminal.  The explicit ``--project-directory``
option keeps the Compose project identity tied to the same directory; it does
not replace the need to set the shell's current directory for ``${PWD}``.

Validate the file and inspect the resolved bind-mount source before creating
containers:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml config

In the rendered output, every volume ``source`` should be the intended host
project directory and every volume ``target`` should be ``/home``.  Fix the
directory or YAML before continuing if either value is wrong.

Pull and start the services
---------------------------

Pull the image used by all four services, then create the Compose project in
detached mode:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml pull
   docker compose --project-directory "$PWD" --file compose.yaml up --detach

The shipped file does not specify an image tag, so Docker uses the default
``latest`` tag.  For a reproducible deployment, edit every ``image`` entry to
use the same published MsPASS release tag before running ``pull``.

Inspect all service containers, including any that exited during startup:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml ps --all

Initial startup can take a little time.  Follow the combined service output or
only the frontend output with:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml logs --follow --tail 100
   docker compose --project-directory "$PWD" --file compose.yaml logs --follow --tail 100 mspass-frontend

Pressing ``Ctrl-C`` while following logs stops only the ``logs`` command; the
detached services continue to run.

Connect to JupyterLab and Dask
------------------------------

When the frontend is ready, open ``http://127.0.0.1:8888/lab`` and enter the
password ``mspass``.  This behavior differs from the default single-container
workflow because the shipped Compose file explicitly sets
``MSPASS_JUPYTER_PWD``.  The startup script hashes the configured value before
passing it to Jupyter, but the source value remains visible in your local YAML
file.

For a private password, change ``MSPASS_JUPYTER_PWD`` under
``mspass-frontend`` and rerun the ``up --detach`` command from the previous
section.  Compose will recreate the frontend when its configuration changes.
To use Jupyter's generated-token behavior instead, remove that environment
entry, recreate the frontend, and read the token-bearing URL from
``mspass-frontend`` logs.

The Dask dashboard is available at ``http://127.0.0.1:8787/status``.  It shows
the four worker processes started by the default ``mspass-worker`` service.
Jupyter notebooks connect to the scheduler and database through their Compose
service names; do not replace those internal addresses with ``localhost``.

Persistent files
----------------

The container startup script treats the shared ``/home`` mount as the MsPASS
working directory and creates these directories in the host project:

``db/``
   Persistent MongoDB files, including the database under ``db/data/``.

``logs/``
   MongoDB, Dask scheduler, and Dask worker logs.

``work/``
   Dask worker scratch files.  This is operational storage, not the
   authoritative location for results.

Notebooks and input data can live elsewhere under the same project directory.
Removing Compose containers does not remove these bind-mounted files.  Stop
the stack before copying ``db/`` for backup; copying live MongoDB files can
produce an inconsistent backup.

Mount a second data filesystem
------------------------------

Large waveform archives often live outside the project directory.  Add a
second bind mount instead of copying that archive under ``${PWD}``, and use
the same container path in every service that needs to open those files.  For
example, export an absolute host path:

.. code-block:: bash

   export MSPASS_WAVEFORM_DIR="/absolute/path/to/waveforms"

Then add the second entry under ``volumes`` for ``mspass-frontend`` and
``mspass-worker`` and for any other service that consumes the files:

.. code-block:: yaml

   volumes:
     - "${PWD}/:/home"
     - "${MSPASS_WAVEFORM_DIR:?set MSPASS_WAVEFORM_DIR}:/waveforms:ro"

The ``:?`` check makes Compose stop with a useful error when the host variable
is unset.  ``:ro`` is appropriate for an input-only archive; omit it only
when container processes must modify that filesystem.  Paths stored in
MongoDB or passed to worker tasks must use the shared container path, such as
``/waveforms/event.mseed``, not the host-only path.  Apply the same mapping to
whichever Dask or Spark file you use, then rerun ``docker compose config`` and
confirm that every consuming service resolves the same source and target.

Routine operations
------------------

Run these commands from the project directory.  Show current state with:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml ps --all

Read recent logs for every service, or follow one service by name:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml logs --tail 200
   docker compose --project-directory "$PWD" --file compose.yaml logs --follow mspass-worker

To update the image and recreate services that use a newer image:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml pull
   docker compose --project-directory "$PWD" --file compose.yaml up --detach

When finished, stop and remove the service containers and their Compose
network:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml down

``down`` does not remove the ``mspass/mspass`` image or files in the host
project directory.  A later ``up --detach`` recreates the services with the
same persistent data.

Scale or tune Dask workers
--------------------------

The default worker service starts four worker processes with one thread each.
To run two worker-service replicas on the same Docker host, use:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml up --detach --scale mspass-worker=2

With the shipped ``MSPASS_WORKER_ARG``, two replicas create eight Dask worker
processes.  Do not scale beyond the CPU and memory assigned to Docker.  To use
fewer processes per replica, edit ``MSPASS_WORKER_ARG`` under
``mspass-worker``; for example, ``--nworkers=2 --nthreads=1`` requests two
single-threaded processes.  Rerun ``up --detach`` after changing the file.

Scaling here adds containers only on the current Docker host.  Use the
:ref:`HPC deployment guide <deploy_mspass_on_HPC>` for a batch-scheduled,
multi-node deployment.

Use Spark instead of Dask
-------------------------

MsPASS also ships a four-service Spark configuration.  It keeps the
standalone ``mspass-db`` and JupyterLab frontend, and replaces the Dask
scheduler and workers with a Spark master and worker:

:download:`Download docker-compose_spark.yaml
<../../../data/yaml/docker-compose_spark.yaml>`.

.. literalinclude:: ../../../data/yaml/docker-compose_spark.yaml
   :language: yaml
   :linenos:
   :caption: The shipped four-service Spark configuration

Save the downloaded file as ``docker-compose_spark.yaml`` in the persistent
project directory.  Validate it before startup, then use the same lifecycle
commands as the Dask stack with the Spark filename:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file docker-compose_spark.yaml config
   docker compose --project-directory "$PWD" --file docker-compose_spark.yaml pull
   docker compose --project-directory "$PWD" --file docker-compose_spark.yaml up --detach

Startup is health-gated.  The Spark scheduler healthcheck probes its internal
web interface on port ``8080``; ``mspass-worker`` starts only after that
service is healthy.  The frontend starts only after both ``mspass-db`` and
``mspass-scheduler`` are healthy, and its ``MSPASS_DB_ADDRESS`` names the same
``mspass-db`` service.  Keep the healthchecks, ``depends_on`` service names,
and connection settings aligned if services or internal ports are customized.

The Spark stack publishes MongoDB on ``27017``, the Spark master on ``7077``,
and JupyterLab on ``8888``.  Port ``8080`` is used only by the scheduler's
container-internal healthcheck and is not published to the host.  The stack
does not provide the Dask dashboard on ``8787``.  The persistence and security
guidance above applies to this file as well.

The Dask and Spark files reuse the same service names, project directory, and
default host ports.  Bring one stack down before starting the other in the
same project directory.  To stop the Spark stack, run:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file docker-compose_spark.yaml down

Troubleshooting
---------------

Docker or Compose is unavailable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If ``docker version`` cannot show a server section, start Docker Desktop or
the Docker Engine service and check the current account's Docker permission.
If ``docker compose version`` fails, install the Compose v2 plugin for the
Docker distribution in use.  The `official Compose installation guide
<https://docs.docker.com/compose/install/>`__ lists supported methods.

Data appears in the wrong host directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The bind source comes from ``${PWD}`` at Compose invocation time.  Stop and
return to the intended project directory, then rerun the ``config`` validation
command and inspect each volume ``source``.  Do not continue with a source
directory that contains unrelated or sensitive files because all services
receive read-write access to it.

A host port is already allocated
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default file publishes ``27017``, ``8786``, ``8787``, and ``8888``.  Stop
the conflicting program or edit only the host side of the relevant mapping.
For example, ``127.0.0.1:9999:8888`` makes Jupyter available on host port
``9999`` while preserving its container port.  Open
``http://127.0.0.1:9999/lab`` after that change.

A service exits or JupyterLab is not ready
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inspect state and the relevant startup output:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml ps --all
   docker compose --project-directory "$PWD" --file compose.yaml logs --tail 200 mspass-frontend mspass-db mspass-scheduler mspass-worker

Look for a port conflict, bind-mount permission failure, out-of-memory exit,
or a MongoDB or Dask startup error.  The Compose dependencies establish start
order, but they do not guarantee that an application is ready immediately.

Jupyter rejects the password or asks for a token
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the ``MSPASS_JUPYTER_PWD`` entry in the local Compose file.  After
changing or removing it, run ``up --detach`` again so the frontend is
recreated.  If the entry is absent, use the token-bearing URL printed by:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml logs --tail 100 mspass-frontend

The Dask dashboard shows no workers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Read both scheduler and worker logs and confirm that ``mspass-worker`` is
running:

.. code-block:: bash

   docker compose --project-directory "$PWD" --file compose.yaml ps --all
   docker compose --project-directory "$PWD" --file compose.yaml logs --tail 200 mspass-scheduler mspass-worker

Reduce the worker count if the host is short of memory.  If you changed
``MSPASS_SCHEDULER_ADDRESS``, restore the Compose service name
``mspass-scheduler`` so workers can resolve it on the private network.

Next steps
----------

* Read :ref:`Parallel Processing <parallel_processing>` before designing a
  larger Dask workflow.
* Work through the `MsPASS tutorial notebooks
  <https://github.com/mspass-team/mspass_tutorial>`__ from the persistent
  project directory.
* Return to the :ref:`single-container guide <run_mspass_with_docker>` if
  separate service lifecycle management is unnecessary.
* Read the :ref:`virtual-cluster overview <getting_started_overview>` and the
  :ref:`HPC deployment guide <deploy_mspass_on_HPC>` before moving beyond one
  Docker host.
