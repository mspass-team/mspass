.. _run_mspass_with_docker:

Run MsPASS with Docker
======================

This page expands the :ref:`desktop quick start <quick_start>` into a
repeatable, single-container workflow.  The default MsPASS image starts
JupyterLab, MongoDB, a Dask scheduler, and a Dask worker together.  This
all-in-one mode is well suited to learning MsPASS and to interactive work on
one computer.

The examples use the Docker command-line interface so that the same workflow
works with Docker Desktop and Docker Engine.  They keep the container attached
to the terminal, which makes startup messages and a clean shutdown easy to
observe.

Prerequisites
-------------

Before starting, you need:

* Docker Desktop on Windows, macOS, or Linux, or Docker Engine on Linux.  Use
  the `official Docker installation guide
  <https://docs.docker.com/get-started/get-docker/>`__ for your platform.
* Permission to run Docker commands.  On Windows and macOS, start Docker
  Desktop before opening a terminal.  On Linux, make sure the Docker service
  is running and that your account has the required access.
* An existing, writable project directory with enough free space for your
  notebooks, waveform files, MongoDB database, logs, and temporary worker
  files.
* Enough memory and CPU capacity for JupyterLab, MongoDB, and Dask to run at
  the same time.  Docker Desktop users can review the resource allocation in
  Docker Desktop settings.

Confirm that both the Docker client and its server are available:

.. code-block:: bash

   docker version

The command should display both ``Client`` and ``Server`` sections.  A
server-connection error usually means that Docker Desktop or the Docker Engine
service is not running, or that the current account lacks permission to use
it.

Pull or update the MsPASS image
-------------------------------

The primary image is published on `Docker Hub
<https://hub.docker.com/r/mspass/mspass>`__.  Pull the current image before the
first run and whenever you want to update MsPASS:

.. code-block:: bash

   docker pull mspass/mspass:latest

The same image is also published through the `GitHub Container Registry
<https://github.com/mspass-team/mspass/pkgs/container/mspass>`__.  If you use
that registry, substitute ``ghcr.io/mspass-team/mspass:latest`` in both the
``docker pull`` and ``docker run`` commands on this page.

``latest`` follows the most recently published default image.  For a
reproducible project, select an available release tag in the registry and use
that tag consistently instead.  Pulling a newer image does not modify an
already-created container; recreate the container as described in
:ref:`docker_image_update` to use the new image.

Docker stores images in its own data area, not in the directory where the pull
command is run.  ``docker system df`` reports the disk space used by local
images, containers, and volumes.

Prepare a project directory and bind mount
------------------------------------------

Choose one host directory as the root of the project.  It can contain
notebooks and input data before MsPASS starts.  The directory must already
exist because Docker's ``--mount`` form reports an error for a missing bind
source.

The launch command maps that host directory to ``/home`` inside the container:

.. code-block:: text

   --mount "type=bind,source=/absolute/path/to/project,target=/home"

Replace ``/absolute/path/to/project`` with the absolute path on the host.  Keep
the full mount specification quoted, especially when a path contains spaces.
For example, a Windows PowerShell path can be written as
``source=C:\Users\you\mspass-project``.

If the terminal is already in the project directory, these shell-specific
forms avoid typing the full path:

* Linux or macOS shell: ``source=${PWD}``
* Windows PowerShell: ``source=$($PWD.Path)``

For example, the complete Linux or macOS mount argument is
``--mount "type=bind,source=${PWD},target=/home"``.

The bind mount is read-write.  Processes in the container can therefore
create, change, and remove files in the host project directory.  This access
is required for normal MsPASS operation; keep unrelated or sensitive files
outside that directory.

Launch MsPASS
-------------

Run the following command after replacing the source path with your project
directory:

.. code-block:: bash

   docker run --name mspass-local --publish 127.0.0.1:8888:8888 --mount "type=bind,source=/absolute/path/to/project,target=/home" mspass/mspass:latest

This is the canonical quick-start launch with a few operational safeguards:

* ``--name mspass-local`` gives the container a stable name for later
  ``logs``, ``stop``, and ``start`` commands.
* ``--publish 127.0.0.1:8888:8888`` maps JupyterLab to port ``8888`` only on
  the local computer.  It does not intentionally expose JupyterLab to other
  computers on the network.
* ``--mount ...`` makes the project directory persistent at ``/home``.
* Omitting ``--rm`` keeps the stopped container available for a later
  ``docker start``.

The command runs in the foreground and prints the service logs.  Initial
startup can take a little time while MongoDB and Dask become ready.  Keep this
terminal open while using MsPASS.

If host port ``8888`` is already in use, select another host port while
leaving the container port unchanged.  This example uses host port ``9999``:

.. code-block:: text

   --publish 127.0.0.1:9999:8888

Connect to JupyterLab
---------------------

Near the end of the startup output, JupyterLab prints one or more URLs that
contain a temporary access token.  Open the URL beginning with
``http://127.0.0.1:8888/`` in a browser on the same computer.  Treat the token
as a credential and do not post or share the full URL.

If the launch terminal is no longer visible, display the output from another
terminal:

.. code-block:: bash

   docker logs --tail 100 mspass-local

When a different host port is used, preserve the path and token from the
printed URL but replace its port with the host port.  For the earlier example,
open ``http://127.0.0.1:9999/lab?token=...``.

JupyterLab's file browser starts at ``/home``, so it shows the contents of the
host project directory.  Open an existing notebook or use the launcher to
create a notebook, console, or terminal.  The `JupyterLab interface guide
<https://jupyterlab.readthedocs.io/en/stable/user/interface.html>`__ explains
the workspace if it is unfamiliar.

A JupyterLab terminal opens a Bash shell *inside the container*, not on the
host.  In the standard image that shell runs as ``root`` and therefore has
administrative access inside the container as well as read-write access to the
bind-mounted project directory.  It is useful for inspecting logs or running
monitoring commands such as ``top``, but use system-administration commands
carefully: changes under ``/home`` also change the host files.

Persistent data layout
----------------------

The startup script uses the mounted ``/home`` directory as the MsPASS working
directory.  On the first run it creates service directories alongside the
project files:

``db/``
   Persistent MongoDB files.  The database storage itself is under
   ``db/data/``.

``logs/``
   MongoDB, Dask scheduler, and Dask worker logs.  These files are useful when
   a service does not start or exits unexpectedly.

``work/``
   Dask or Spark worker scratch space.  Its contents are operational temporary
   data, not the authoritative copy of project results.

Notebooks, waveform files, and other project content remain elsewhere under
the same host directory.  Because all of these paths are bind-mounted,
stopping or removing the container does not delete them.  By contrast, a run
without the bind mount stores its state only in the container and is not a
safe persistent workflow.

Stop MsPASS before copying ``db/`` for backup or moving the project directory;
copying live MongoDB files can produce an inconsistent backup.  Do not
manually delete files under ``db/data/`` to solve a startup problem without
first preserving a copy and understanding the database consequences.

Routine operations: inspect, stop, and restart
----------------------------------------------

Show the current and stopped container records:

.. code-block:: bash

   docker ps --all --filter "name=^/mspass-local$"

Follow the live logs from another terminal with:

.. code-block:: bash

   docker logs --follow mspass-local

``Ctrl-C`` stops only the log-following command in that terminal.  It does not
stop a container that was started elsewhere.

To stop the foreground launch, press ``Ctrl-C`` once in its terminal and allow
the cleanup messages to finish.  The MsPASS entrypoint shuts down MongoDB and
the worker processes before exiting.  From another terminal, request the same
graceful stop with a longer timeout than Docker's usual default:

.. code-block:: bash

   docker stop --timeout 60 mspass-local

Avoid killing the container or immediately pressing ``Ctrl-C`` again during
normal shutdown, because doing so can interrupt database cleanup.

Restart the same container with its original port, mount, image, and
environment settings, and attach it to the terminal:

.. code-block:: bash

   docker start --attach mspass-local

Close active notebooks before stopping the container.  Files saved in the
mounted project directory remain available after a restart.

.. _docker_image_update:

Use a newly pulled image
~~~~~~~~~~~~~~~~~~~~~~~~

An existing container remains tied to the image from which it was created.
After saving work, stop and remove that container, pull the desired image, and
run the launch command again:

.. code-block:: bash

   docker stop --timeout 60 mspass-local
   docker rm mspass-local
   docker pull mspass/mspass:latest

Removing ``mspass-local`` does not remove the bind-mounted project directory.
Recreate it with the same ``docker run`` command under :ref:`Launch MsPASS
<run_mspass_with_docker>`, changing the image tag or options if needed.

Optional Dask worker tuning
---------------------------

Python-heavy functions can run more effectively as multiple single-threaded
Dask worker processes than as one multithreaded worker because of Python's
Global Interpreter Lock.  To request four worker processes with one thread
each, add this option before the image name in the ``docker run`` command:

.. code-block:: text

   --env "MSPASS_WORKER_ARG=--nworkers 4 --nthreads 1"

Choose a worker count that fits the CPUs and memory assigned to Docker, and
leave capacity for MongoDB, JupyterLab, the operating system, and other
applications.  More workers are not always faster; benchmark a representative
workflow and reduce the count if the system starts swapping or becomes
unresponsive.

The entrypoint also accepts ``MSPASS_DASK_WORKER_MEMORY_LIMIT``.  Its default
value of ``0`` disables Dask's per-worker memory limit.  Advanced users can set
an explicit value such as ``2GB`` with another ``--env`` option, but the limit
must be practical for every worker process:

.. code-block:: text

   --env "MSPASS_DASK_WORKER_MEMORY_LIMIT=2GB"

Environment options are fixed when a container is created.  Remove and
recreate ``mspass-local`` to change them; ``docker start`` reuses the original
settings.

Troubleshooting
---------------

Docker cannot connect to the server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start Docker Desktop, or start the Docker Engine service on Linux, and rerun
``docker version``.  On Linux, a permission error for the Docker socket is an
account or installation configuration issue; follow the post-install guidance
for the installed Docker distribution rather than running the MsPASS
container with unrelated workarounds.

The container name is already in use
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``docker run`` cannot create a second container named ``mspass-local``.  Use
``docker ps --all`` to inspect the existing one.  Restart it with ``docker
start --attach mspass-local``, or stop and remove it before creating a
replacement.

Port 8888 is unavailable
~~~~~~~~~~~~~~~~~~~~~~~~

Another application or container is using the host port.  Change only the
host side of the publication, for example to
``--publish 127.0.0.1:9999:8888``, and use port ``9999`` in the browser URL.

The project is missing or the mount is denied
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Confirm that the source is an existing absolute path and that the full
``--mount`` value is quoted.  Docker Desktop may ask permission to share a
host directory or drive.  Also confirm that the host account can read and
write the project directory.  On Linux, files created by the container can
have container-user ownership; inspect ownership before changing permissions
recursively.

JupyterLab does not appear
~~~~~~~~~~~~~~~~~~~~~~~~~~

Run ``docker ps --all`` to see whether the container is still running, then
read ``docker logs --tail 100 mspass-local``.  Look first for a bind-mount
error, a port conflict, an out-of-memory termination, or a MongoDB error in
``logs/mongo_log``.  If startup is simply still in progress, wait for the
token-bearing JupyterLab URL rather than opening an unauthenticated URL.

The machine becomes slow or the worker exits
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check Docker's CPU and memory allocation and monitor the container with
``docker stats mspass-local``.  Reduce ``--nworkers``, stop other
memory-intensive applications, or increase Docker Desktop's resources when
the host has capacity.  Review the Dask worker log in ``logs/`` for the
specific failure.

Next steps
----------

* Work through the `MsPASS tutorial notebooks
  <https://github.com/mspass-team/mspass_tutorial>`__ from a project directory
  mounted at ``/home``.
* Use the :ref:`MsPASS Desktop guide <mspass_desktop>` if you prefer a
  graphical launcher.
* Use the :ref:`Docker Compose deployment guide
  <deploy_mspass_with_docker_compose>` when you want independently managed
  database, scheduler, worker, and frontend containers.
* Read the :ref:`virtual-cluster overview <getting_started_overview>` before
  moving beyond a single computer, and follow the :ref:`HPC deployment guide
  <deploy_mspass_on_HPC>` for a batch-scheduled cluster.
* Contributors who need a source build can follow the `MsPASS source-build
  instructions
  <https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__.
