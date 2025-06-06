.. _command_line_docker_desktop_operation:

Command Line Docker Desktop Operation
=========================================
Overview
------------
Most desktop users will likely want to use the
:code:`mspass_desktop` application described
in :ref:`this section of the MsPASS documentation<mspass_desktop>`.
If you have problems with installing or operating that graphical
interface application or if you just prefer to do things
from the command line you need to reference this document.

MsPASS can be run on Desktop computers via the
`docker desktop application <https://docs.docker.com/get-docker/>`__
using a unix shell.   There are two different ways to
do that described in separate subsections below:
(1) with a single, albeit complex command line incantation, or
(2) a procedure using a the "compose" option of the docker
application.

Installing and Launching Docker Desktop
-----------------------------------------

Docker is required in normal use to run MsPASS on desktop systems.
The alternative is a more complicated installation of the components
built from source as described on
`this wiki page <https://github.com/mspass-team/mspass/wiki/Compiling-MsPASS-from-source-code>`__.
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
On those platforms the docker daemon is launched by the usual method
and runs under a graphical interface displayed on your desktop.
For linux, once installed docker runs as a daemon process automatically launched
when the system boots.   On linux systems only the command line interface
is available to manage containers.

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
When you pull the container, docker loads data only in a
system dependent data space so you will not see anything happen
in the directory where you run this command.  The recommended way to
manage disk usage is through docker desktop or docker command line
tools.   See docker's documentation for information now how to do that.

It can be confusing to understand where data is stored in a containerized environment
because file paths are always mapped from local file path names to
container file names.  They are usually different.
In the discussion below files names we reference that reside inside a container will be set in italics.
File names on the physical system will be referred to with a normal font text.

Option 1:  launching with docker run command
-----------------------------------------------

To run MsPASS from the command line cd to a writable directory at the
top of a directory tree where you the data for your project are located.
Most MsPASS processing is then initiated with a variant of the
following on the command line:

.. code-block::

    docker run -p 8888:8888 --mount src=`pwd`,target=/home,type=bind mspass/mspass

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
In this example the "current directory" for the launching shell
is defined with the shell incantation `pwd`.  The command
will be map the current directory to */home* in the container.
*/home* is a standard mount point
directory on the unix system the container runs.
That mapping is necessary
to save your results to your local system.   Without the
``--mount`` incantation any results
you produce in a run will disappear when the container exits.

You can also substitute the full path to a local directory
in place of `pwd`.   In that case, you would not need to cd to
the desired directory first.  You must, however, have write protection
for that directory.

When the container boots it splashes a bunch of text to the terminal from
which it was launched announcing successful launching of
required MsPASS components.
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

The root directory of the notebook contains three different directories,
*db*, *logs*, and *work*,
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
home page.   You will usually need to type a `ctrl-C` twice in the terminal
window you used to launch mpass via docker to force the container to exit.

Option 2:  The docker compose command
---------------------------------------
Using the standard configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :code:`docker run` command is good for a quick start or for testing, but
the complex incantation makes it less desirable for repeated use.
For those who want to run MsPASS repeatedly and not use the :code:`mspass-desktop`
GUI, the :code:`docker compose` command described in this section may
be of interest.

To use :code:`docker compose` you will need a configuration file
in the "yaml" (Yet Another Markup Language) format.   The following
example should work for most desktop/laptop systems.

.. literalinclude:: ../../../data/yaml/compose.yaml
   :language: yaml
   :linenos:
   :caption: Standard compose.yaml file to run dask as four services on a destkop.

To get started with :code:`docker compose` the easiest solution is to use the
cut-and-paste in the box above and save the content to your project
directory to a file you could save as "compose.yaml".   In a terminal
cd to the project directory and issue this command:

.. code-block::

    docker compose up -d

You can expect an output similar to the following:

.. code-block::

  [+] Running 5/5
  ✔ Network downloads_default               Created                         0.0s
  ✔ Container downloads-mspass-scheduler-1  Started                         0.3s
  ✔ Container downloads-mspass-db-1         Started                         0.3s
  ✔ Container downloads-mspass-frontend-1   Started                         0.4s
  ✔ Container downloads-mspass-worker-1     Started                         0.4s

where the "downloads" prefix on the tags above will be the name of your run directory.
The example above was run from the "Downloads"directory.

It is usually advisable to verify all the containers are still running with
this command:

.. code-block::

     docker compose ps

which should yield an output similar to the following:

.. code-block::

  NAME                           IMAGE           COMMAND                  SERVICE            CREATED              STATUS                    PORTS
  downloads-mspass-db-1          mspass/mspass   "/usr/sbin/tini -s -…"   mspass-db          About a minute ago   Up 38 seconds (healthy)   0.0.0.0:27017->27017/tcp
  downloads-mspass-frontend-1    mspass/mspass   "/usr/sbin/tini -s -…"   mspass-frontend    About a minute ago   Up 38 seconds             0.0.0.0:8888->8888/tcp, 27017/tcp
  downloads-mspass-scheduler-1   mspass/mspass   "/usr/sbin/tini -s -…"   mspass-scheduler   About a minute ago   Up 38 seconds (healthy)   0.0.0.0:8786-8787->8786-8787/tcp, 27017/tcp
  downloads-mspass-worker-1      mspass/mspass   "/usr/sbin/tini -s -…"   mspass-worker      About a minute ago   Up 38 seconds             27017/tcp

To get the url information needed to connect to the jupyter server
issue the following command:

.. code-block::

    docker compose logs mspass-frontend | grep http

The output should contain one of the url's you can use to connect to jupyter.

If you want to just run a python script, you can submit it to be run
from the command line.  e.g. assuming the containers are all running
and the python script to be run is "myjob.py", that script can be
run as follows:

.. code-block::

    docker compose exec mspass-scheduler python myjob.py

When finished, you should shut the system down cleanly with the following
command issued from the same project directory:

.. code-block::

    docker compose down

Configuration File Content
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An important benefit of :code:`docker compose` is that it emphasizes
that MsPASS can be abstracted as four distince "services".
The yaml file explicitly defines that with the *services* key at the top
of the file.   The individual service name tags are defined by
the indentation structure of the yaml.  They are the four names shown from
output examples above:  *mspass-db, mspass-scheduler, mspass-worker,* and
*mspass-frontend*.   You should realize that if running with
MsPASS with :code:`docker compose` you are running four different instance
of the MsPASS container with each instance running only one service.
That is in contrast to the :code:`docker run` approach where only
a single instance of the container is running and that instance is
running all four of these "services".  In a desktop environment the two
approaches are functionally similar.   The :code:`docker compose` approach
has a small memory penalty but has the main advantage of being
more configurable via the yaml configuration file.

Configuration parameters you may need to change follow.  If a parameter
is not listed the best advice is to not change it unless you have
a good reason to do so.

- *volumes* appears in all four services.  It defines what directories
  are to be mounted where on each container.   Functionally the line
  "${PWD}/:/home" in the default yaml file serves the same function as the
  following in the :code:`docker run` incantation above:
  "--mount src=`pwd`,target=/home,type=bind".   i.e. both tell docker to
  mount the current directory on "/home" in the container.   The most
  likely situation where this parameter would need to change is if you
  have waveform data on a second file system distinct from where you
  want to store the database.   That would be common, for example, on a
  desktop with a SSD disk used to store system files and the MongoDB
  database, but with a secondary, larger, slower magnetic disk used to
  store the more voluminous waveform data.   The current MsPASS container
  has only one alternative mount point to "/home"; the other stock ubuntu
  generic mount point "/mnt".   The syntax for that is to add a yaml
  list following the *volumes* key.  For example, if we wanted to add
  a mount for a local file system called /data to the container /mnt
  directory that section of the yaml file would look like this:

.. code-block::

    volumes:
      - ${PWD}/:/home
      - /data:/mnt

- *image* is the name tag for the docker container being run.   There are
  multiple tags for different versions and a "dev" version found
  `here <https://github.com/mspass-team/mspass/pkgs/container/mspass>`__ .
  If you need to run a different instance of the container you will
  need to change the value associated with *image* in the file.
- The *mspass-worker* service has an attibute with the key
  *MSPASS_WORKER_ARG*.  For most uses we would advise setting the
  valued of "--nworkers" to the number of cores on the desktop system.
  Reduce this value if you need to do other work on the system you
  are using while mspass is running.
- Each service except *mspass-worker* have a *port* parameter.   You
  should only change one of those attribute if you encounter a communication
  problem created by a firewall that requires port mapping.  That is unlikely
  to ever be an issue with the *mspass-scheduler* or *mspass-db* as in
  this mode communications are always between processes running on the
  same system.  The most likely, but still rare, issue could be the
  ports for *mspass-frontend*.  The jupyter server in the container
  runs on port 8888.  If you are connecting to a machine remotely to run
  mspass with :code:`docker compose` you may need to do port mapping
  if 8888 is blocked by a firewall.

  Advanced Options
  ~~~~~~~~~~~~~~~~~~~

  The :code:`docker compose` approach provides a rich collection of options
  beyond those illustrated in our default configuration file shown above.
  More examples can be found in the mspass github repository in the
  *data/yaml* directory.  The most important example there is
  *docker-compose_spark.yml* which shows how to lauch a pyspark cluster.
  The default shown above will run the dask scheduler. 
