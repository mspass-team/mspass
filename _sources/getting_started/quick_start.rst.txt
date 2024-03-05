.. _quick_start:

Getting Started in a Nutshell
===============================

Overview
-------------
This section is a concise summary of steps required to run a MsPASS.
It has limited explanations for what is done at each step.   The main use
is as a quick reminder for experienced users and as the starting point
for the impatient.  This summary assumes you are running on a desktop
using docker.   It is not appropriate if you are running on a cluster
with multiple nodes.  If that is your situation, you should refer to
the appropriate, longer sections of the `Getting Started` section.

1. Install docker
---------------------
Fetch and install docker following instructions on the
`docker web site <https://docs.docker.com/get-docker/>`__.

2. Launch docker desktop (MacoOS and Windows)
---------------------------------------------
MacOS and Windows have a GUI application (Icon tagged "Docker") that you will
need to launch unless it is already running.

3. Get or Refresh the container (optional)
-------------------------------------------
If you have not run MsPASS before you will need to get the docker container
from the standard repository.  Alternatively if you want to get the most
recent updates you may also need to do this step.

.. code-block::

    docker pull mspass/mspass

4. Launch the container
-------------------------
Launch whatever version of a "terminal" is used on your platform.
Decide what directory (folder) you want to use as a working directory.
If you are running one of our tutorials use the top-level of the tutorials
repository. Otherwise, use whatever is appropriate for your setup.
Make whatever you choose your current directory and run this incantation:

.. code-block::

    docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

This should generate a stream of output ending in something like the following:

.. code-block::

    [I 11:02:38.655 NotebookApp] Serving notebooks from local directory: /home
    [I 11:02:38.655 NotebookApp] Jupyter Notebook 6.2.0 is running at:
    [I 11:02:38.655 NotebookApp] http://7b408535513f:8888/?token=ced2d40475df024c3544e7bd4aa0ea4676e0c88ae85be7db
    [I 11:02:38.656 NotebookApp]  or http://127.0.0.1:8888/?token=ced2d40475df024c3544e7bd4aa0ea4676e0c88ae85be7db
    [I 11:02:38.656 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
    [C 11:02:38.673 NotebookApp]

        To access the notebook, open this file in a browser:
            file:///root/.local/share/jupyter/runtime/nbserver-57-open.html
        Or copy and paste one of these URLs:
            http://7b408535513f:8888/?token=ced2d40475df024c3544e7bd4aa0ea4676e0c88ae85be7db
         or http://127.0.0.1:8888/?token=ced2d40475df024c3544e7bd4aa0ea4676e0c88ae85be7db

.. warning::
  The start line using the unix shell trick with `pwd` can fail in some situations if
  the path to which it resolves has a space bar character (" ") such as "My Documents".
  If that happens choose a different work directory or replace `pwd` in the
  incantation above with the space escaped (e.g. "My\ Documents").

5. Connect to the container with a web browser
--------------------------------------------------
If you don't have one running already, launch your favorite web browser.  Use
cut-and-paste or type one of the http addresses in the output into the url
box of the browser. That should launch the jupyter lab control panel.  If you
are not familiar with jupyter lab or jupyter notebook you will need to refer
to tutorials for that application.  The jupyter developer's introduction
can be found
`here.<http://justinbois.github.io/bootcamp/2020_fsri/lessons/l01_welcome.html>`__
There are many other tutorials easily found with a web search.
