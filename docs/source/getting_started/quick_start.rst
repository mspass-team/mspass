.. _quick_start:

Getting Started in a Nutshell
=============================

This page is the shortest path to running MsPASS on a desktop or laptop with
Docker.  For a multi-node system, start with :ref:`getting_started_overview`
instead.

1. Install Docker
-----------------

Install Docker using the `official instructions
<https://docs.docker.com/get-started/get-docker/>`__.  On macOS and Windows,
start Docker Desktop before continuing.

2. Choose a working directory
-----------------------------

Open a terminal and change to the directory where you want to keep your
notebooks and data.  If you are using an MsPASS tutorial, choose the top-level
directory of the tutorial repository.

3. Get or refresh the container
-------------------------------

This step is required the first time and can be repeated to get updates:

.. code-block:: console

   docker pull mspass/mspass

4. Launch MsPASS
----------------

On Linux or macOS, run:

.. code-block:: console

   docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

Leave the terminal open while MsPASS is running.  The command mounts your
current working directory as ``/home`` inside the container, so notebooks and
data saved there remain on your computer after the container stops.

.. note::

   If the path to your working directory contains spaces, replace ``src=`pwd```
   with ``src="$PWD"``.  Windows PowerShell users should instead use
   ``src="$($PWD.Path)"``.

5. Open JupyterLab
------------------

The terminal output will include one or more web addresses containing a
generated token.  Copy the complete ``http://127.0.0.1:8888`` address,
including its token, into a browser on the same computer.

When you finish, save your work, return to the terminal, and press ``Ctrl-C``
to stop MsPASS.

Next steps
----------

* :ref:`Run MsPASS with Docker <run_mspass_with_docker>` explains the desktop
  workflow and troubleshooting in more detail.
* The `MsPASS tutorial notebooks
  <https://github.com/mspass-team/mspass_tutorial>`__ provide worked examples.
* :ref:`MsPASS Virtual Cluster Concepts <getting_started_overview>` introduces
  cluster deployment.
