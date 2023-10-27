.. _run_mspass_with_docker:

Run MsPASS with Docker
======================

Prerequisites
-------------

Docker is required in normal use to run MsPASS on desktop systems.
The alternative is a more complicated installation of the components
built from source as described on
`this wiki page<https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__.
Docker is the piece of software you will use to run and manage
any containers on your desktop system.

Docker is well-supported on all current desktop operating systems and
has simple install procedures described in detail in the
product's documentation found `here <https://docs.docker.com/get-docker/>`__
The software can currently be downloaded at no cost, but you must have
administrative privileges to install the software.
The remainder of this page assumes you have successfully installed
docker.  For Windows or Apple user's it may be convenient to launch the
"docker desktop" as an alternative to command line tools.

Download MsPASS Container
-------------------------

The MsPASS container image is built and hosted on `Docker Hub <https://hub.docker.com/r/mspass/mspass>`__.
It is also available in the `GitHub Container Registry <https://github.com/mspass-team/mspass/pkgs/container/mspass>`__.
Once you have docker setup properly, use the following command in a terminal
to download the MsPASS image from Docker Hub to your local machine:

.. code-block::

    docker pull mspass/mspass

Be patient the first time you issue this command for your systems
as this can take a few minutes depending on your internet speed.
Note you can run this command from anywhere and the files are stored in
a system directory (folder) whose location depends upon the host
operating system.   Be aware that the MsPASS container will consume of the order of
500 Mb of disk space on your system disk so you should be sure you are not
pushing the limits of your system disk.
When you pull the container docker loads data only in a
system dependent data space so you will not see anything happen
in the directory where you run this command.  The recommended way to
manage disk usage is through docker desktop or docker command line
tools.   See docker's documentation for information now how to do that.

It can be confusing to understand where data is stored in a containerized environment
because file paths are always mapped from local file path names to
container file names.  They are usually different.
In the discussion below files names we reference that reside inside a container will be set in italics.
File names on the physical system will be referred to with a normal font text.


Run MsPASS Container in All-in-one Mode
---------------------------------------

Most MsPASS processing on a desktop begins by running a variant of the
following on the command line:

.. code-block::

    docker run -p 8888:8888 --mount src=/Users/myusername/myproject,target=/home,type=bind mspass/mspass

The ``-p 8888:8888`` argument maps port ``8888`` on your system to the container's ``8888`` port.
That pair of arguments are needed to allow your local web browser to
connect to the Juypter notebook server running in the container.
``8888`` is the default port for the Jupyter Notebook frontend.
If there are collisions with ``8888`` port on your system (uncommon),
change the first number
to "map" the local system port number to ``8888`` in the container.
For example,  if you use ``-p 9999:8888`` the URL you use to connect to the
Jupyter notebook would need to be altered to use ``9999`` as the port number

The lengthy incantation in the argument following the  ``--mount``
argument is used to "map" a local file system path to a
defined mount point in the container.
In this example the local system directory, "/Users/myusername/myproject",
will be mapped to the directry called */home* in the container.
*/home* is a standard mount point
directory on the unix system the container runs.
An standard alternative is */mnt*, but most people prefer
*/home* as the name makes more sense.
That mapping is necessary
to save your results to your local system.   Without the
``--mount`` incantation any results
you produce in a run will disappear when the container exits.

A useful, alternative way to launch docker on a linux or MacOS system
is to use the shell ``cd`` command in the terminal you are using to make
your project directory the "current directory".   Then you can
cut-and-paste the following variation of the above into that terminal
window and */home* in the container will be mapped to your
"current directory":

.. code-block::

    docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

When the container boots it splashes a bunch of text to the terminal from
which it was launched announcing successful lauching of
required MsPAS components.
The last part of the output will look something
like this

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

Use the standard cut-and-paste operation to paste the URL beginning with ``http://127.0.0.1:8888``
to your favorite web browser (Note if you need to use port mapping, which is
not common, you would need to change the 8888 to the mapped value - 9999 in the
example above.).   That URL should resolve and a Jupyter notebook home page
should come up in the browser.
This page assumes you know where to go from here.
If you are not familiar with Jupyter Notebook, refer to the
`documentation found here <https://jupyter-notebook.readthedocs.io/en/stable/ui_components.html>`__ .

The root directory of the notebook contains three different directories, *db*, *logs*, and *work*,
that will have been created in your working directory the first time you launch
the mspass container in that directory.
*db* contains MongoDB's database files.
*logs* contains the logs generated by the database, the scheduler, and the worker.
*work* is a local scratch space used by dask/spark.
Other files in your project data should also show up in the file browser.
(Note if you do not use the ``--mount`` option everything shown on the home
page will disappear when the contaienr is exited.  The default is what it
is because the majority of "dockerized" applications are run as background
processes and that approach makes cleanup automatic. That mode is
rarely useful on a desktop use with MsPASS.)
Normal use at this point is to open an existing notebook to be run
(double-click the notebook's file name) or create one with the `New` button
on the notebook home page.

A final point worth noting is that it is often useful when working
interactively with mspass on a desktop to open a "Terminal" in the
container.  The `New` button has a `Terminal` item in addition to the
`Python 3` button that is used to create a new notebook.  If you select
`Terminal` you will get a black web browser window (usually a tab on any
newer browser) with the cryptic ``#`` prompt of the default Bourne shell.
Most users will want to immediately launch a ``bash`` (Note we do not currently
have any other advanced shell commands in the mspass container.) shell
instead of the more primitive sh. i.e. we recommend you type ``bash`` in the
new terminal window as it gives you things like line editing not available with
the old-school Bourne shell.   Be warned that with docker you are running as
root in the container.   You can thus run sysadmin commands.  That can be
useful, but it is a sharp knife that can cut you.   Be sure you know what
you are doing before you alter any files with bash commands in this
terminal.   A more standard use is to run common monitoring commands like
``top`` to monitor memory and cpu usage by the container.

If you are using dask on a desktop, we have found many algorithms perform
badly because of a subtle issue with python and threads.   That is, by
default dask uses a "thread pool" for workers with the number of threads
equal to the number of cores defined for the docker container.
Threading with python is subject to poor performance because of
something called the Global Interpreter Lock (GIL) that causes multithread
python functions to not run in parallel at all with dask.  The solution
is to tell dask to run each worker task as a "process" not a thread.
(Note pyspark does this by default.)  A way to do that with dask is to
launch docker with the following variant of above:

.. code-block::

    docker run -p 8888:8888 -e MSPASS_WORKER_ARG="--nworkers 4 --nthreads 1" --mount src=`pwd`,target=/home,type=bind mspass/mspass

where the value after `--nworkers` should be the number of worker tasks
you want to have the container run.   Normally that would be the number of
cores defined for the container which be default is less than the number of
cores for the machine running docker.

Finally, to exit close any notebook windows and the Jupyter notebook
home page.   You will usually need to type a `ctrl-C` in the terminal
window you used to launch mpass via docker.
