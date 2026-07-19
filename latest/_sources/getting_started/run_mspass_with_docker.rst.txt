.. _run_mspass_with_docker:

Run MsPASS with Docker
======================

This page expands the :ref:`desktop quick start <quick_start>` for users who
want to run MsPASS from a terminal.  The standard image starts JupyterLab,
MongoDB, a Dask scheduler, and a Dask worker together in one container.

Prerequisites
-------------

Install Docker Desktop on Windows or macOS, or Docker Desktop or Docker Engine
on Linux.  The `Docker installation guide
<https://docs.docker.com/get-started/get-docker/>`__ has instructions for each
platform.  Start Docker before running the commands below.

Download the MsPASS image
-------------------------

The MsPASS image is hosted on `Docker Hub
<https://hub.docker.com/r/mspass/mspass>`__.  Download it with:

.. code-block:: bash

   docker pull mspass/mspass

The first download can take a few minutes.  Docker stores the image in its own
data area, so no files appear in the directory where you run this command.
The download therefore consumes space in Docker's image store rather than in
your project directory.  Use Docker Desktop's image view or ``docker image
ls`` to inspect downloaded images and their sizes.
The image is also available from the `GitHub Container Registry
<https://github.com/mspass-team/mspass/pkgs/container/mspass>`__.

Run MsPASS
----------

Create a project directory, change to that directory in a Linux or macOS
terminal, and run:

.. code-block:: bash

   docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

The host computer and the container have separate filesystem namespaces.  A
path on the host does not automatically exist at the same path inside the
container.  The mount option explicitly connects the host project directory
to the container's ``/home`` directory.

The two important options are:

``-p 8888:8888``
   Makes the JupyterLab server available on port ``8888`` of your computer.

``--mount src=`pwd`,target=/home,type=bind``
   Maps the current project directory to ``/home`` in the container.  Files
   written under ``/home`` remain in the project directory after the container
   stops.

Keep the terminal open while using MsPASS.  Startup messages appear there as
MongoDB, Dask, and JupyterLab become ready.

Connect to JupyterLab
---------------------

Near the end of the startup output, JupyterLab prints a URL containing an
access token.  Copy the URL beginning with ``http://127.0.0.1:8888/`` into a
browser on the same computer.  Do not share the token-bearing URL.

The exact messages and token change from run to run, but the useful portion
looks similar to this:

.. code-block:: text

   Jupyter Server is running at:
   http://127.0.0.1:8888/lab?token=ced2d40475df...
   Use Control-C to stop this server and shut down all kernels.

Wait for this message before trying to connect.  The earlier MongoDB and Dask
messages show that startup is progressing, but do not mean the web interface
is ready.

JupyterLab's file browser shows the contents of ``/home``, which is the project
directory you mounted.  You can open an existing notebook or create a new
notebook, console, or terminal.  See the `JupyterLab interface guide
<https://jupyterlab.readthedocs.io/en/stable/user/interface.html>`__ if the
interface is unfamiliar.

The first run creates three service directories in the project directory:

``db/``
   MongoDB database files.

``logs/``
   Logs from MongoDB, the scheduler, and the worker.

``work/``
   Temporary space used by Dask or Spark workers.

Do not omit the mount option for normal work.  Without it, notebooks, database
files, and other results remain only inside the container and can be lost when
the container is removed.

Use a terminal inside JupyterLab
--------------------------------

The JupyterLab launcher includes a ``Terminal`` option as well as notebook and
console options.  It opens a shell inside the MsPASS container with ``/home``
as the project area.  This can be useful for running a Python script, examining
files, or monitoring CPU and memory with tools such as ``top``.  Type ``bash``
if the initial shell lacks familiar line editing or other interactive features.

The shell normally runs as ``root`` *inside the container*.  That is not the
same as being root on the host, but the shell can still modify or delete host
files through the read-write ``/home`` bind mount.  Use administrative and
file-removal commands carefully.  Software installed or files created outside
the mounted directory are container-local and may be lost when the container
is removed.

Stop MsPASS
-----------

Save and close active notebooks, then press ``Ctrl-C`` in the terminal that is
running the container.  Allow the shutdown messages to finish so MongoDB and
the worker processes can stop cleanly.  Files in the mounted project directory
remain available for the next run.  Jupyter may ask you to press ``Ctrl-C`` a
second time to confirm shutdown.

Other host shells
-----------------

The ``pwd`` form above is for Linux and macOS shells.  In Windows PowerShell,
run the following command from the project directory:

.. code-block:: powershell

   docker run -p 8888:8888 --mount "src=$($PWD.Path),target=/home,type=bind" mspass/mspass

For any shell, you can replace the current-directory expression with the
absolute path to an existing project directory.  Quote the full mount value
when the path contains spaces.  For example:

.. code-block:: text

   --mount "src=/absolute/path/to/project,target=/home,type=bind"

Optional settings
-----------------

Use another host port
~~~~~~~~~~~~~~~~~~~~~

If port ``8888`` is already in use, change the first port number.  For example:

.. code-block:: bash

   docker run -p 9999:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

In this case, replace port ``8888`` with ``9999`` in the JupyterLab URL.

Tune Dask workers
~~~~~~~~~~~~~~~~~

Python-heavy processing can benefit from multiple single-threaded Dask worker
processes because Python threads are limited by the Global Interpreter Lock.
To request four workers with one thread each, use:

.. code-block:: bash

   docker run -p 8888:8888 -e MSPASS_WORKER_ARG="--nworkers 4 --nthreads 1" --mount src=`pwd`,target=/home,type=bind mspass/mspass

Choose a worker count that fits the CPUs and memory available to Docker, while
leaving resources for MongoDB, JupyterLab, and the operating system.  More
workers are not always faster.

Troubleshooting
---------------

Docker cannot connect
~~~~~~~~~~~~~~~~~~~~~

Start Docker Desktop, or start the Docker Engine service on Linux.  Running
``docker version`` should display both ``Client`` and ``Server`` sections.  A
permission error on Linux usually means the account has not been configured to
use the Docker service.

The project directory is missing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Make sure the mount source is an existing directory and that your account can
read and write it.  Docker Desktop may also ask for permission to share a host
directory or drive.

JupyterLab does not appear
~~~~~~~~~~~~~~~~~~~~~~~~~~

Read the startup messages in the terminal first.  Common causes are a port
conflict, a missing mount directory, insufficient memory, or Docker not
running.  Wait for the token-bearing JupyterLab URL before opening the browser.

The machine becomes slow
~~~~~~~~~~~~~~~~~~~~~~~~

Reduce the Dask worker count, stop other memory-intensive applications, or
increase the CPU and memory assigned to Docker Desktop when the computer has
capacity.  Worker and scheduler details are recorded under ``logs/``.

Next steps
----------

* Work through the `MsPASS tutorial notebooks
  <https://github.com/mspass-team/mspass_tutorial>`__.
* Use the :ref:`MsPASS Desktop guide <mspass_desktop>` if you prefer a
  graphical launcher.
* Use the :ref:`Docker Compose deployment guide
  <deploy_mspass_with_docker_compose>` when you want the database, scheduler,
  workers, and JupyterLab in separate containers.
* Read the :ref:`HPC deployment guide <deploy_mspass_on_HPC>` before moving to
  a batch-scheduled cluster.
