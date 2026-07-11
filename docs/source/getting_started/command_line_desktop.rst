.. _command_line_docker_desktop_operation:

Docker CLI and Compose Overview
===============================

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

Prefer loopback-only port publications such as ``127.0.0.1:8888:8888`` for a
desktop workflow.  The single-container guide uses that form.  The shipped
``data/yaml/compose.yaml`` instead publishes JupyterLab, MongoDB, and Dask
ports on all host interfaces, uses the known Jupyter password ``mspass``, and
does not configure MongoDB authentication.  Treat that file as a local or
trusted-network example.  The :ref:`Compose deployment guide
<deploy_mspass_with_docker_compose>` explains the loopback bindings and
password changes to make before use on an untrusted network.

Treat a Jupyter token or password as a credential.  Do not post a token-bearing
URL in a shared log or expose the service ports through a firewall without an
appropriate authentication and network-security plan.

Common command translation
--------------------------

The commands below assume that the single-container guide created a running
container named ``mspass-local`` or that the current directory contains the
``compose.yaml`` used to start the Compose project.  The detailed guides own
the launch, restart, image-update, and configuration procedures.

.. list-table::
   :header-rows: 1
   :widths: 16 42 42

   * - Operation
     - Single container
     - Docker Compose
   * - Show status, including stopped containers
     - ``docker ps --all --filter "name=^/mspass-local$"``
     - ``docker compose ps --all``
   * - Read recent frontend logs
     - ``docker logs --tail 100 mspass-local``
     - ``docker compose logs --tail 100 mspass-frontend``
   * - Open a diagnostic shell
     - ``docker exec --interactive --tty mspass-local bash``
     - ``docker compose exec mspass-frontend bash``
   * - Run ``/home/myjob.py`` with the service connection settings
     - In a JupyterLab terminal, run ``python /home/myjob.py``.
     - ``docker compose exec mspass-frontend python /home/myjob.py``
   * - Request a graceful stop
     - ``docker stop --timeout 60 mspass-local``
     - ``docker compose stop --timeout 60``

The Compose script command intentionally uses ``mspass-frontend``.  In the
shipped configuration, that service receives both ``MSPASS_DB_ADDRESS`` and
``MSPASS_SCHEDULER_ADDRESS``; ``mspass-scheduler`` does not receive the
database address.

``exec`` requires a running container.  The current standard image starts
``exec`` commands as ``root`` inside the container.  That is not the same as
host ``root``, but the process can still change or delete host files through
the read-write project bind mount.  Use an interactive shell carefully.
Changes made outside ``/home`` from such a shell are container-local and may
be lost on recreation.  In Compose, ``stop`` preserves the service containers
so that ``docker compose start`` can restart them; ``docker compose down``
removes the service containers and their network but leaves the bind-mounted
project files on the host.

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
