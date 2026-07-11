.. _quick_start:

MsPASS Desktop Quick Start
==========================

This page gets MsPASS running on one desktop or laptop with Docker and
JupyterLab.  It is intended for a first local run, not for a multi-node
cluster deployment.

Prerequisites
-------------

You need:

* Docker Desktop on macOS or Windows, or Docker Engine on Linux.  Install it
  with the `official Docker instructions
  <https://docs.docker.com/get-started/get-docker/>`__.
* A terminal and a writable project directory.
* Port ``8888`` available on the host.

Start Docker Desktop on macOS or Windows before continuing.  On Linux, make
sure the Docker daemon is running and that your account can run ``docker``.
Confirm the installation with:

.. code-block:: console

   docker version

Create a project directory
--------------------------

MsPASS writes notebooks, data, database files, logs, and worker scratch files
under its ``/home`` directory.  Bind-mount a host project directory there so
those files remain after the container stops.

If you already have a project directory, such as the top level of a cloned
MsPASS tutorial repository, use that directory and skip the creation commands.

On Linux or macOS, use a POSIX shell:

.. code-block:: bash

   mkdir -p "$HOME/mspass-project"
   cd "$HOME/mspass-project"

On Windows, use PowerShell:

.. code-block:: powershell

   New-Item -ItemType Directory -Force "$HOME\mspass-project" | Out-Null
   Set-Location "$HOME\mspass-project"

The run commands below use the shell's absolute current-directory value and
quote the complete mount specification.  This safely handles paths containing
spaces and avoids legacy backtick command substitution around ``pwd``.
Docker Desktop may ask you to approve access to the selected host directory.

Pull the MsPASS image
---------------------

Download the current image from Docker Hub.  Running this command again later
refreshes the local image when a newer one is available.

.. code-block:: console

   docker pull mspass/mspass:latest

Run MsPASS
----------

From the project directory, run the command for your terminal.

Linux or macOS:

.. code-block:: bash

   docker run --name mspass-quickstart --rm --publish 127.0.0.1:8888:8888 --mount "type=bind,source=${PWD},target=/home" mspass/mspass:latest

Windows PowerShell:

.. code-block:: powershell

   docker run --name mspass-quickstart --rm --publish 127.0.0.1:8888:8888 --mount "type=bind,source=$($PWD.Path),target=/home" mspass/mspass:latest

Leave this terminal open.  The container starts MongoDB, a local Dask
scheduler and worker, and JupyterLab.  First startup can take a little while.

``--publish 127.0.0.1:8888:8888`` binds JupyterLab only to the host's
loopback interface, keeping it local to this computer.  If you intentionally
need a different host port or network binding, consult :ref:`Run MsPASS with
Docker <run_mspass_with_docker>` before changing this option.

Open JupyterLab
---------------

Watch the terminal output for the Jupyter URL containing a generated token.
It will resemble:

.. code-block:: text

   http://127.0.0.1:8888/lab?token=<generated-token>

Copy the complete ``127.0.0.1:8888`` URL, including the ``token`` query, into
a browser on the same computer.  The actual token is unique to this run; do
not copy the placeholder above.  If Jupyter also prints a URL containing a
container hostname, use the ``127.0.0.1`` URL for this local setup.

Keep your work
--------------

In Jupyter, ``/home`` is the project directory you created on the host.
Notebooks and results saved there are immediately visible in
``mspass-project`` outside the container.  MsPASS also creates ``db``,
``logs``, and ``work`` subdirectories there.  Do not delete or move those
directories while MsPASS is running.

The ``--rm`` option removes the stopped container, but it does not remove
files in the bind-mounted project directory.  Reusing the same directory on
the next run restores access to your notebooks and local database files.

Stop MsPASS
-----------

Save your notebooks, return to the terminal running Docker, and press
``Ctrl-C``.  Allow the shutdown messages to finish before closing the
terminal.  If that terminal is no longer available, stop the named container
from another terminal and allow time for service cleanup:

.. code-block:: console

   docker stop --timeout 60 mspass-quickstart

Where to go next
----------------

* Continue with :ref:`Run MsPASS with Docker <run_mspass_with_docker>` for a
  detailed explanation of the single-container workflow, port and network
  binding choices, worker configuration, and troubleshooting.
* See :ref:`Command Line Docker Desktop Operation
  <command_line_docker_desktop_operation>` for the broader command-line and
  Docker Compose overview, or :ref:`Deploy MsPASS with Docker Compose
  <deploy_mspass_with_docker_compose>` for the multi-container configuration.
* Work through the `MsPASS tutorial notebooks
  <https://github.com/mspass-team/mspass_tutorial>`__.
* Use the :ref:`MsPASS User Manual <user_manual_introduction>` for the data
  model, database, processing, and error-handling concepts.
* For multi-node work, start with :ref:`MsPASS Virtual Cluster Concepts
  <getting_started_overview>` and the :ref:`HPC deployment guide
  <deploy_mspass_on_HPC>`.
