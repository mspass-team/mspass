.. _command_line_docker_desktop_operation:

Command Line Docker Desktop Operation
=====================================

This page compares the MsPASS desktop workflows and lists their most common
Docker commands.  For step-by-step setup, follow the linked guide for the
workflow you choose.

Choose a desktop workflow
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 35 45

   * - Workflow
     - Choose it when
     - Continue with
   * - Single container
     - You want the shortest command-line path for learning, notebooks, or
       interactive work on one computer.  MongoDB, JupyterLab, the Dask
       scheduler, and a Dask worker run in one container.
     - :ref:`Run MsPASS with Docker <run_mspass_with_docker>` is the canonical
       launch and troubleshooting guide and is the recommended CLI starting
       point for most desktop users.
   * - MsPASS Desktop
     - You prefer a graphical launcher and service-status display instead of
       managing containers in a terminal.
     - :ref:`Running MsPASS on a Desktop Computer <mspass_desktop>` covers the
       launcher installation and graphical workflow.
   * - Docker Compose
     - You need the database, scheduler, worker, and frontend in separately
       managed containers, or you want service-level logs and worker scaling
       on one Docker host.
     - :ref:`Deploy MsPASS with Docker Compose
       <deploy_mspass_with_docker_compose>` is the canonical multi-container
       guide and documents the shipped ``compose.yaml`` file.

Docker Compose separates services, but it does not by itself create a
multi-host cluster.  For work across compute nodes, start with the
:ref:`virtual-cluster concepts <getting_started_overview>` and then use the
:ref:`HPC deployment guide <deploy_mspass_on_HPC>`.

Shared prerequisites
--------------------

All three desktop workflows require a running Docker service.  Install Docker
Desktop or Docker Engine by following the `official Docker installation guide
<https://docs.docker.com/get-started/get-docker/>`__, then confirm that the
client can reach the server:

.. code-block:: bash

   docker version

The output should include both ``Client`` and ``Server`` sections.  Compose
users must also have the Compose v2 plugin:

.. code-block:: bash

   docker compose version

Before launching MsPASS, choose a dedicated writable project directory and
make enough disk space, memory, and CPU capacity available for the database,
JupyterLab, and Dask.  Docker Desktop users may need to adjust those resources
in Docker Desktop settings.

Docker Desktop provides the Docker service and a graphical interface on
Windows and macOS.  Docker Engine on Linux normally runs as a background
service and is managed from the command line.  Installing either product
usually requires administrator privileges.  Users who cannot or do not want
to use Docker can build the components from source, but that is a substantially
more involved procedure described in the `source-build wiki
<https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__.

Download the MsPASS image
-------------------------

The MsPASS image is hosted on `Docker Hub
<https://hub.docker.com/r/mspass/mspass>`__ and is also published in the
`GitHub Container Registry
<https://github.com/mspass-team/mspass/pkgs/container/mspass>`__.  Download the
standard image with:

.. code-block:: bash

   docker pull mspass/mspass

The first download may take several minutes.  The image is placed in Docker's
system-managed image store, not in the directory where the command was run,
so no new file will appear in that directory.  Docker Desktop's image view or
``docker image ls`` can be used to inspect downloaded images and their disk
usage.  The image occupies space on the system disk in addition to the space
needed for project data and the MongoDB database.

Container paths and host paths are different namespaces.  A path such as
``/home`` inside the container has no automatic connection to a project
directory on the host.  A bind mount makes that connection explicitly and is
what allows notebooks, database files, and results to survive after the
container exits.

Option 1: launch one container with ``docker run``
---------------------------------------------------

Change to a writable directory at the top of the project tree.  On a Unix-like
shell, start MsPASS with:

.. code-block:: bash

   docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

This short command has two arguments that are important to understand:

* ``-p 8888:8888`` publishes port ``8888`` from the container as port ``8888``
  on the host, allowing the host web browser to reach Jupyter.  If host port
  ``8888`` is already in use, ``-p 9999:8888`` publishes the same Jupyter
  service on host port ``9999``; use ``http://127.0.0.1:9999`` in that case.
* ``--mount src=`pwd`,target=/home,type=bind`` mounts the shell's current
  directory at ``/home`` inside the container.  You can replace ``pwd`` with
  an absolute host path instead of changing directories first.  The selected
  directory must be writable and must be shared with Docker Desktop where
  that product requires explicit file-sharing permission.

Without the bind mount, work written inside the container is not a reliable
way to preserve results: it becomes inaccessible after a temporary container
is removed and disappears when that container is deleted.  The mount is also
why files already in the host project directory appear in Jupyter's file
browser.

When the container starts, it prints messages from MongoDB, Dask, and Jupyter
to the terminal.  The final Jupyter messages contain a URL resembling:

.. code-block:: text

   Jupyter Server is running at:
   http://127.0.0.1:8888/lab?token=...

Copy the complete ``127.0.0.1`` URL, including its generated token, into a web
browser.  Startup can take a short time, so wait for the Jupyter URL rather
than assuming that the first messages mean the frontend is ready.  If a
different host port was selected, replace only the port in the URL.  New
Jupyter users can consult the `JupyterLab interface guide
<https://jupyterlab.readthedocs.io/en/stable/user/interface.html>`__.

On its first run in a project directory, the MsPASS startup creates ``db/``,
``logs/``, and ``work/`` under the mounted directory.  ``db/`` holds MongoDB
data, ``logs/`` holds service logs, and ``work/`` is scratch space used by
Dask or Spark.  Existing notebooks and waveform files in the project tree
also appear in Jupyter.  Open an existing notebook from the file browser or
use the launcher to create a Python notebook.

The Jupyter launcher can also open a terminal inside the container.  This is
useful for inspecting files or running monitoring commands such as ``top``.
The shell normally runs as ``root`` inside the container.  It is not host
root, but it can still modify anything in the read-write bind mount, so take
care with commands that alter files.

Dask worker processes
~~~~~~~~~~~~~~~~~~~~~

Some Python algorithms perform poorly when many Dask worker threads contend
for Python's Global Interpreter Lock.  For those workloads, multiple
single-threaded worker processes are often preferable.  The image accepts
Dask worker options through ``MSPASS_WORKER_ARG``.  For example:

.. code-block:: bash

   docker run -p 8888:8888 -e MSPASS_WORKER_ARG="--nworkers 4 --nthreads 1" --mount src=`pwd`,target=/home,type=bind mspass/mspass

Choose the worker count to fit the CPU and memory Docker has been assigned;
using every logical CPU is not always best if other applications must remain
responsive or each worker needs substantial memory.  Spark uses a different
worker model and does not use this Dask option.

To stop the foreground container, close or save active notebooks and press
Control-C in the launch terminal.  Jupyter may request a second Control-C to
confirm shutdown.  Do not interrupt MongoDB by forcibly killing Docker unless
a normal stop has failed.

Option 2: run separate services with Docker Compose
---------------------------------------------------

The single ``docker run`` command is convenient for a first session.  Docker
Compose is often easier for repeated work because the configuration is saved
in a YAML file and each MsPASS component has its own container and logs.  The
repository supplies this standard Dask configuration:

.. literalinclude:: ../../../data/yaml/compose.yaml
   :language: yaml
   :linenos:
   :caption: Standard compose.yaml file with database, Dask, and frontend services

Save it as ``compose.yaml`` in the project directory, change to that directory,
and start the services in the background:

.. code-block:: bash

   docker compose up -d

Compose reports that it created a project network and started the
``mspass-db``, ``mspass-scheduler``, ``mspass-worker``, and
``mspass-frontend`` services.  Container names are normally prefixed with the
project directory name.  Confirm that the services remain running with:

.. code-block:: bash

   docker compose ps

The database and scheduler may show a temporary starting state while their
health checks run.  If the frontend does not become ready, inspect its log:

.. code-block:: bash

   docker compose logs mspass-frontend

Open ``http://127.0.0.1:8888`` and enter the configured password ``mspass``.
The supplied Compose file sets a password, so its log may not display the
token-style URL produced by the single-container command.

A Python script stored as ``myjob.py`` in the project directory can be run in
the frontend service, which has both database and scheduler addresses:

.. code-block:: bash

   docker compose exec mspass-frontend python /home/myjob.py

When work is finished, save notebooks and stop the project cleanly:

.. code-block:: bash

   docker compose down

This removes the service containers and their private network but leaves the
bind-mounted project files, including ``db/``, on the host.

Understanding the Compose file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The top-level ``services`` mapping defines four instances of the same MsPASS
image, each with a different responsibility:

* ``mspass-db`` runs a standalone MongoDB server.
* ``mspass-scheduler`` runs the Dask scheduler.
* ``mspass-worker`` performs parallel tasks assigned by the scheduler.
* ``mspass-frontend`` runs JupyterLab and connects to both the scheduler and
  database.

This differs from the default single-container launch, where one container's
``all`` role starts all four components.  Separate services use somewhat more
memory but make individual components easier to configure, restart, scale,
and troubleshoot.

The ``volumes`` entries mount ``${PWD}`` at ``/home`` in every service.  All
services that read a waveform file must see it at the same container path.  If
waveforms live on another host filesystem, add the same mount to each service
that needs them.  For example:

.. code-block:: yaml

   volumes:
     - "${PWD}/:/home"
     - "/data:/mnt"

The ``image`` value selects the container image.  Leave it as
``mspass/mspass`` unless a particular released or development tag is required.
``MSPASS_WORKER_ARG`` controls the Dask worker layout; the supplied value runs
four single-threaded workers and can be reduced when CPU or memory is limited.

``MSPASS_ROLE`` selects what each container starts.  ``MSPASS_SCHEDULER``
selects Dask or Spark, ``MSPASS_SCHEDULER_ADDRESS`` tells workers and the
frontend how to reach the scheduler, and ``MSPASS_DB_ADDRESS`` tells the
frontend how to reach MongoDB.  In this standalone configuration the database
service is ``mspass-db``.  ``mspass-dbmanager`` is correct only in the separate
sharded MongoDB example.

Port mappings have the form ``HOST:CONTAINER``.  Normally, change only the
host side to resolve a collision.  Changing a container-side port also
requires matching changes to environment variables, service addresses, and
health checks.  The more detailed :ref:`Docker Compose deployment guide
<deploy_mspass_with_docker_compose>` documents Spark, database sharding,
configuration validation, and troubleshooting.

Common startup problems
~~~~~~~~~~~~~~~~~~~~~~~

If Docker reports that it cannot connect to the daemon, start Docker Desktop
or the Docker Engine service and repeat ``docker version``.  A permission
error on a mounted path usually means the host directory is not writable or
has not been shared with Docker Desktop.  An ``address already in use`` error
means that a published host port is occupied; choose another host port as
described above.

For a single container, keep the launch terminal open because it contains the
startup error and Jupyter URL.  In another terminal, ``docker ps -a`` shows
whether the container exited.  With Compose, use ``docker compose ps -a`` to
find an exited service and ``docker compose logs SERVICE`` to inspect it.
Run ``docker compose config`` after editing YAML; it catches indentation,
variable-substitution, and resolved-configuration problems before startup.

If workers repeatedly restart or Docker becomes unresponsive, reduce the
worker count and confirm that Docker has enough memory.  If Jupyter is running
but cannot open a notebook or waveform, confirm that the file is below the
mounted project directory and that the same host path is mounted into every
Compose service that needs it.

Persistence and access boundaries
----------------------------------

The documented single-container and Compose workflows bind-mount the host
project directory at ``/home`` in the container.  The current MsPASS startup
script uses that location as its Docker working directory and creates these
service directories as needed:

``db/``
   MongoDB database files.

``logs/``
   MongoDB, scheduler, and worker logs.

``work/``
   Worker scratch files, not the authoritative copy of project results.

Files in the bind-mounted directory remain on the host when a container is
stopped or removed.  Files written elsewhere in a container are not a
persistence strategy and may disappear when that container is recreated.
Stop MsPASS before copying ``db/`` for backup so that MongoDB is not being
modified during the copy.

A bind mount is also a read-write access grant: processes and interactive
shells in the container can change or delete host files under the project
directory.  Mount only the project data that MsPASS needs, keep unrelated or
sensitive files outside that directory, and use normal host backups.

Network and credential safety
-----------------------------

The simple commands on this page and the shipped ``data/yaml/compose.yaml``
publish their ports on the host interfaces.  The Compose example also uses
the known Jupyter password ``mspass`` and does not configure MongoDB
authentication.  Treat these as local or trusted-network examples.  On an
untrusted network, bind published ports to loopback (for example,
``127.0.0.1:8888:8888``) and choose a private Jupyter password.  The
:ref:`Compose deployment guide <deploy_mspass_with_docker_compose>` explains
those changes.

Treat a Jupyter token or password as a credential.  Do not post a token-bearing
URL in a shared log or expose the service ports through a firewall without an
appropriate authentication and network-security plan.

Where to go next
----------------

* Use the :ref:`single-container Docker guide <run_mspass_with_docker>` for
  image selection, cross-platform bind-mount syntax, Dask worker tuning, and
  detailed troubleshooting.
* Use the :ref:`Docker Compose deployment guide
  <deploy_mspass_with_docker_compose>` for configuration validation, service
  logs, secure port changes, scaling, and lifecycle management.
* Read :ref:`Advanced Setup Considerations <advanced_setup_considerations>`
  when you need to manage source or Python-environment installations, or the
  :ref:`Conda installation guide <deploy_mspass_with_conda>` when containers
  are not the right fit.
* Move to the :ref:`virtual-cluster concepts <getting_started_overview>` and
  :ref:`HPC deployment guide <deploy_mspass_on_HPC>` before adapting a
  desktop workflow to a batch-scheduled or multi-node system.
