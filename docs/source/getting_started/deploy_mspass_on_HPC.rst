.. _deploy_mspass_on_HPC:

Deploying MsPASS on an HPC cluster
=====================================

Overview
-----------------
First, by HPC (High-Performance Computing) we mean a cluster of multiple node
linked by high speed interconnections designed for large-scale, parallel
processing.  If you are not familiar with modern concepts of this type of
hardware and how they interact in HPC systems you should first do
some background reading started with the section in our Getting Started pages
found at `ref::_getting_started_overview`.

An axiom for working with MsPASS is that any workflow you need to
develop should first be prototyped on a desktop system.
HPC systems are by definition designed to run large jobs that run
on multiple nodes and use many cores.   It is always best to test run
any workflow on a subset of your data.   For most people that is
easiest on their office desktop machine.  With that model you first construct the
python code defining your workflow within a jupyter notebook
on your desktop machine, transfer that notebook
to the HPC system you want to use for the (presumably) much larger data
set, and then face the new idioms of the HPC system you will be using.

Like your desktop system every HPC cluster has a set of local idioms.
Examples, are file system directory names and variations in the
software used to run jobs on the cluster.   If there are other people
in your institute who use MsPASS on the same cluster, your job will be much
easier.   If that is your situation then the section below titled
"Running MsPASS with Existing Configuration Scripts" should get you started.
If you are the first person
in your institute to use MsPASS with the cluster you are using, you will
need to do some nontrivial work to configure the cluster setup.  A sketch
of that process is below in the section titled "Configuring MsPASS on an HPC cluster" .
The background for what is needed to do a Configuration
can be found in `ref::_getting_started_overview`.

Running MsPASS with Existing Configuration Scripts
------------------------------------------------------

Get a Copy of Configuration Scripts
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
You may first want to search the suite of configuration scripts found
on github `here<https://github.com/mspass-team/mspass/tree/master/scripts>`__
If the system you are using has a folder there you should download the
scripts from the appropriate folder and you should be able to proceed.
We assume here the file name convention is the same as that for the
set in the folder `template`.   If the file names for your institution
are different you will have to do some additional work to puzzle out
what was done.   If there are deviations we recommend the author
supply a README file.

If the files you need are not on github and you are aware of colleagues
using mspass you may need to contact them and ask for their working
startup scripts.   If you are a trailblazer, then you will need to jump
to the section below titled "Configureing MsPASS on an HPC cluster".
You can then use the next section for reference when you are actively
workig with MsPASS on that system.

Build MsPASS Container with Singularity
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Singularity is a container implementation that is used on all HPC
clusters we know of.   HPC systems do not use docker for security
reasons because docker defaults to allowing processes in the container
to run as root.  Singularity, however, is compatible with docker
in the sense that it can pull containers constructed with docker and
build a file to run on the HPC cluster.   That is the process we illustrate
in this section.

Because HPC clusters commonly support a wide range of applications all
HPC clusters we
know of have a software management system that controls what software
is loaded in your environment.   At TACC, where MsPASS was initially
developed, they use a command line tool called `module`.  On TACC systems
the proper incantation is the following:

.. code-block::

    module load tacc-singularity

A more stock version might omit the "tacc-" part of that name.  Once your
shell knows about singularity you can create an instance of the MsPASS
container.  Unless you have a quota problem we recommend you
the container in the directory `~/mspass/containers`.   Assuming that
directory exists the following commands can be used to generate your
working copy of the MsPASS container:

.. code-block::

    cd ~/mspass/containers
    singularity build mspass_latest.sif docker://mspass/mspass

When the command exists you should now see the file "mspass_latest.sif"
in the current directory.

Edit template scripts
^^^^^^^^^^^^^^^^^^^^^^^^^^
Overview
""""""""""""""
HPC clusters are designed to run large jobs and are not well-suited to
interactive work.  We reiterate that you are advised to develop
your notebook on a desktop system using docker and a small subset of
the data you need to process.  This section assumes you have such a
workflow debugged and ready to release on the full data set.
It also assumes you aren't a trailblazer and you have a
template "job script" you or someone else at your institute
has created that you only need to modify.

The standard way to run a mspass "job" on HPC systems is through a
set of unix shell scripts.   We use that model because all HPC
centers use the unix shell as the "job control" language.   Old-timers
from the days of mainframe computers from IBM and CDC may remember
older, more primitive job control languages like the long dead
IBM JCL (job control language).  The concept is the same but
today the language is a version of the unix shell.  At present all our
examples use the shell dialect called `bash`.

Our standard template uses three shell scripts that work together to
run a mspass workflow.   The section heading titles below
use the names of the template files.  You can, of course
change any of the file names provided you know how they are used.

mspass_setup.sh
"""""""""""""""""""""""
Before you run your first job you will almost certainly need to
create a private copy of the template file `mspass_setup.sh`.
This script does little more than define a set of shell environment
variables to define where on the cluster file system the job
can find your data and the mspass container.  The file puts in one place
all the parameters that most users will need to customize.
The idea is each user-dataset combination will normally
require edits to this file.  Guidance on each follow.

`MSPASS_HOME` is used like many software packages to define the home
base for the software.  In the MsPASS case it is used to define the
location of the singularity container needed to run MsPASS.  If you
created a private copy of the container in the section above you will
not need to alter this parameter at all.   If mulitple people at your
institute run MsPASS, there may be a master copy of the MsPASS container
you can use in this definition.

`WORK_DIR` and `DB_PATH` define data locations.   `WORK_DIR` is the run
directory.   In shell lingo it defines what "." is for the notebook.
`DB_PATH` is assumed to define a (user writable) directory where the
database is to be found or created.   Be aware at this point that there
are large database performance tradeoffs with different types of file
systems that are commonly available in HPC systems.   See the configuration
section below for details.   How much to worry about this issue is
dependent on the size of the dataset.  Most can just set it to some working
directory and ignore this issue until performance becomes an issue.

`HOSTNAME_BASE` should be set to the network subnet name the cluster runs in.
That is usually necessary because all clusters we know of use a shortened
name convention for individual nodes (i.e. the hostname has no "." that
is used for subnet naming.)  If you are using an existing configuration
file you almost certainly can use the value you inherited.

`MSPASS_RUNSCRIPT` defines what in section :ref:`getting_started_overview`
is called a "virtual cluster".   The file is normally static for a
particular cluster, although there may be mulitple options.   e.g. the
standard template file has versions with or without MongoDB "sharding".
For most applications this line will be changed until performance
becomes and issue and you find it necessary to do some advanced tuning.
This file standardizes one or more local configurations.  The last section
of this document describes how that file may need to be modified if
you are the first to use mspass on a cluster.

job_script.sh
"""""""""""""""""""
`job_script.sh` is the shell script you submit that runs your "job" on
the cluster.   Standard usage with slurm as the workload manager to run
the workflow in the jupyter notebook file `myworkflow.ipynd` is;

.. code-bloc

    sbatch job_script.sh myworkflow.ipynb

The template file assumes the file `mspass_setup.sh` defined above
and the notebook file, `myworkflow.ipynb`, are present in the
directory defined by `WORK_DIR`.

The only thing you would normally need to change in `job_script.sh` are
the run parameters passed to slumm with the `#SBATCH` lines at the top
of the file.  There are always cluster-dependent options you will need to
understand before running a large job.    Consult local documentation
before setting these directives and submiting your first job.
You also need to read the

Running a notebook interactively
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
In some cases is may be necessary or helpful to develop your
workflow, which in the MsPASS case means the code blocks in a
jupyter notebook, on the cluster.  Even if you developed the notebook
on a desktop it is often necessary to run the same test you prototyped on
the HPC cluster before running a very large job.   The simplest way to
do that is to just run the notebook as above and verify you got the same
answer you got on the version you debugged on your desktop.
You may need to follow the procedure here if you need to do some additional
interactive debugging or your desktop has limitations (e.g. memory size)
that you cannot simulate on your desktop.  This section describes
the basic concepts required to do that.   Details will differ with
how you communicate with the HPC cluster you are using include the
system you are using to run a web browser for interaction with the
notebook server.  i.e. some extrapolation may be needed to make this work.
Furthermore, the complexity of ths section should be a warning that this
entire process is not a good idea, at least for getting strarted,
unless you have no other option.

The procedure for running MsPASS interactively is similar to that
for running docker on a desktop system found in :ref:`run_mspass_with_docker`.
There are two key differences:  (1) you launch MsPASS with singularity
(or something else) instead of docker and (2) there are a lot of
potential network issues this manual cannot fully cover.  This subsection
is mainly aimed to address the first.  We provide only some initial suggestions
below for potential networking issues.

We assume that the interactive job you need to run is
suitable for the all-in-one configuration
we use in docker.   In that configuration all the individual MsPaSS components
are run as different processes in one container on one node.   Our template
script for setup is called `single_node.sh`.  A method to launch MsPASS in that
mode with slurm would be to enter the following command:

.. code-block::

    sbatch single_node.sh

You should then use the `squeue` slurm command to monitor when your job
starts or watch for the appearance of the output file defined by slurm
commands in `single_node.sh`.  Typically use the unix `cat` command to print the
output file.   The output is similar to what one sees with docker run.
The following is an example output generated this way
on the Indiana University cluster called "carbonate"::

  singularity version 3.6.4 loaded.
  Currently Loaded Modulefiles:
  1) quota/1.8                      8) boost/gnu/1.72.0
  2) git/2.13.0                     9) gcc/9.1.0
  3) xalt/2.10.30                  10) openblas/0.3.3
  4) core                          11) intel/19.0.5
  5) hpss/8.3_u4                   12) totalview/2020.0.25
  6) gsl/gnu/2.6                   13) singularity/3.6.4
  7) cmake/gnu/3.18.4              14) openmpi/intel/4.0.1(default)
  /N/slate/pavlis/usarray
  Thu Jan 26 10:43:56 EST 2023
  {"t":{"$date":"2023-01-26T15:44:10.476Z"},"s":"I",  "c":"CONTROL",  "id":20697,   "ctx":"main","msg":"Renamed existing log file","attr":{"oldLogPath":"/N/slate/pavlis/usarray/logs/mongo_log","newLogPath":"/N/slate/pavlis/usarray/logs/mongo_log.2023-01-26T15-44-10"}}
  [I 10:44:16.973 NotebookApp] Serving notebooks from local directory: /N/slate/pavlis/usarray
  [I 10:44:16.974 NotebookApp] Jupyter Notebook 6.2.0 is running at:
  [I 10:44:16.974 NotebookApp] http://c4:8888/?token=e7464f3b156b27efcaf2c9e52197b40068c5eefd8231a955
  [I 10:44:16.974 NotebookApp]  or http://127.0.0.1:8888/?token=e7464f3b156b27efcaf2c9e52197b40068c5eefd8231a955
  [I 10:44:16.974 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
  [C 10:44:17.036 NotebookApp]

  To access the notebook, open this file in a browser:
      file:///N/slate/pavlis/usarray/.local/share/jupyter/runtime/nbserver-11604-open.html
  Or copy and paste one of these URLs:
      http://c4:8888/?token=e7464f3b156b27efcaf2c9e52197b40068c5eefd8231a955
   or http://127.0.0.1:8888/?token=e7464f3b156b27efcaf2c9e52197b40068c5eefd8231a955


Like the docker case the information to connect to Jupyter is found in the
last few lines.  For the above example the key line is::

  [I 10:44:16.974 NotebookApp] http://c4:8888/?token=e7464f3b156b27efcaf2c9e52197b40068c5eefd8231a955

In this case `c4` is the hostname that for this cluster was shortened for
simplicity of communication within the cluster.  If connecting from outside
the cluster, which would be the norm, for this example we would need to
modify that url.   Your use will vary, but in this case the connection
would use the following url::

  http://c4.uits.iu.edu:8888/?token=e7464f3b156b27efcaf2c9e52197b40068c5eefd8231a955

you would paste into a browser.   We emphasize the detailed URL you would used
is heavily site dependent.  There can be a great deal more complexity than this
simplified example where all we change is the hostname.   You can universally
expect to need a more complex mapping to get the remote connection from
your browswer through the cluster firewall.   The mechanism may be defined in
the script defined by `MSPASS_RUNSCRIPT`, but it might not be either.
Some guidance can be found in the networking configuration subsection below and
by looking at other implementation found on in the scripts directory
of the mspass github site.

There is a final, very important warning when running a "job" interactively
started with slurm.  When you finish the interactive work you
should kill your running "job" immediately.   If you don't the node will sit around
doing nothing until the time limit you specified expires.   If you ignore
this warning you can quickly burn your entire allocation with no results.
With slurm the way to terminate an interactive job is:

.. code-block::

    squeue -u myusername
    scancel jobid

Where you would run that pair of commands sequentially.  For the first Substitute
your user name.  The output will show an "id" with a format something like this::

  JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
  3298684   general   mspass   pavlis  R       4:33      1 c4

For this example `jobid` is 3298684.  That job is "killed" by the command
`scancel 3298684`.

Finally, some clusters have a simplified procedure to run interactive jobs
through some form of "gateway".   For example, Indiana University has
a "Research Desktop" (RED) system that provides a way to run a window on your
local system that makes appear like a linux desktop.   In that case,
running an interactive job is exactly like running with docker except
you use singularity and can run jobs on many nodes.
In addition, the batch submission is not necessary and you can run
the configuration shell script interactively.  For the RED example you
can explicitly launch and "ineractive job" that creates a terminal
window.  Inside that terminal you can then run:

.. code-block::

     source single_node.sh

which should generate an output similar to that above for the sbatch example.
Connection to the jupyter notebook server is then simple via a web browser
running on top of the gateway.

Setting Up Configuration Files on a new Cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Overview
"""""""""""
If you are a trailblazer at your institution and need to configure MsPASS for
your local cluster, you may want to first review the material in this
User's Manual fourd in the section :ref:`getting_started_overview`.
That provides some fundamental concepts on HPC systems and how those concepts
are abstracted in MsPASS to produce a virtual cluster.  This section
focuses on the nuts and bolts of what you might have to change in your
local configuration.  The descriptions here are limited to the simpler
situation with a single instance of the database server (not "sharded").
Sharding is an advanced topic and we assume if you are needing that feature
you are hardy enough you solve the problem yourself.  This section assumes
you have a copy of the file in the mspass scripts/template directory
called "run_mspass.sh".  You may also find it useful to compare that file
to the examples for specific sites.

The "Role" Concept
"""""""""""""""""""""
In the section titled :ref:`getting_started_overview` we discuss in
detail the abstraction we used in MsPASS to define what we call
a "virtual cluster".   A key idea in that abstraction is a set of
functional boxes illustrated in Figure :numref:`_HPC_config_figure1`.
The function each box illustrated there is defined by what we call
its "role".   The keywords defining "role", with one line descriptions of what functionality
they enable are the followings:

- *db* creates and manages the MongoDB server
- *scheduler* is the dask or spark manager that controls data flow to and from
- *worker* task that do all the computational task.
- *frontend* is the jupyter notebook server,
  which means it also is the home of the master python script that drives your workflow.

Note the configuration illustrated in Figure :numref:`_HPC_config_figure1`
is a graphical illustration of that created with the template `run_mspass.sh`
script.

.. _HPC_config_figure1:

.. figure:: ../_static/figures/FiveNodesNoSharding.jpg
     :width: 600px
     :align: center

     Block diagram of virtual cluster that is defined by the `run_mspass.sh`
     template file.   This illustrates the geometry for five nodes, but
     the configuration is open-ended.   If more than 5 nodes are used any
     additional nodes will be set with role == "worker".   Notice with this
     configuration all roles other than worker are run on the same node
     as the job script is executed illustrated here as "node 1"

How Different Roles are Run
"""""""""""""""""""""""""""""""""""
Notice from Figure :numref:`_HPC_config_figure1` that all 4 roles are
launched as separate instances of the singularity container.   In the script
they are all launched with variations of this following:

.. code-block::

  SING_COM="singularity run $MSPASS_CONTAINER"
  SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
       SINGULARITYENV_MSPASS_ROLE=scheduler $SING_COM &

where we illustrate the definition of the symbol `SING_COM` for
clarity only.  In the actual script that line appears earlier.
The above is the actual launch line for the scheduler.  Note the following
that follow for all other roles:

-  The run command is preceded by a set of shell variable definitions
   that all begin with the keyword `SINGULARITYENV`.   An odd feature of
   singularity is any shell symbol it detects that begin with
   `SINGULARITYENV` have that keyword stripped and the result posted to
   a shell environment variable that is available to the container boot script,
   which in mspass is called `start-mspass-sh`,
   (That shell script is not something you as user would ever change but
   it may be instructive to look at that file to understand this setup.
   That file can be found in the mspass github site at the top of the directory
   chain.)  For example, when the above line is executed the variable
   `MSPASS_ROLE` is set to "scheduler".
-  Notice the container is launched as a background process using the
   standard unix shell "&" idiom.   Notice that
   all lines that execute $SING_COM contain the "&" symbol
   EXCEPT the jupyter notebook server that is the last line in the
   script.   That syntax is important.  It cause the shell running the
   script to block until the notebook exits.   When the master job
   script exits singularity does the housecleaning to kill all the running
   containers on multiple nodes running in the background.
-  The instances of the containe for the `db` and `frontend` role launch
   are similar to the scheduler example above but with different
   SINGULARITYENV inputs.   The `worker` launching is different, however,
   and is the topic of the next section.

Launching Workers
""""""""""""""""""
Launching workers is linked to a fundamental problem you will face
in adapting the template script to a different cluster:   node-to-node
communications.   There are three low-level issues you will need to
understand before proceeding:
1.   How are nodes addressed?  i.e. what symbolic name does node A need to
     know to talk to node B?
2.   What communication channel should be used between nodes?

For the first, all the examples we know use a short form of hostname
addressing that strips a subnet description.   You are probably familiar with
this idea working on any local network.  e.g. the machine in my office has
the long name "quakes.geology.indiana.edu".   That name resolves as a valid
hostname on the internet because it is advertised by campus name servers.
Within my department's subnet, however, I can reference to the same machine with
the simpler name "quakes".   The same shorthand is standard on any clusters
we know of so short hostnames are the norm.

That background is necessary to explain this incantation you will find
in run_mspass.sh:

.. code-block::

  NODE_HOSTNAME=`hostname -s`
  WORKER_LIST=`scontrol show hostname ${SLURM_NODELIST} | \
             awk -vORS=, -v hostvar="$NODE_HOSTNAME" '{ if ($0!=hostvar) print $0 }' | \
             sed 's/,$/\n/'`

The first line returns the human readable name of the node on which the
script is being executed.  The -s, which is mnemonic for short, strips
the subnet name from the fully qualified hostname. As noted above
it may not be required on your site as it is common to use only the base
name to reference nodes.

The second line, which truly deserved the incantation title,
sets the shell variable `WORKER_LIST` of all nodes allocated to this job
excluding the node running the script (result of the hostname command).
To help clarify here is the section of output produced by this
script run with four nodes on an Indiana University cluster::

    Lauching scheduler on primary node
    c23,c31,c41

where c23, c31, and c41 are the hostnames of the three compute nodes
slurm assigned to this job.
That list is used to launch each worker in another shell incantation that
follows immediately after the above:

.. code-block::

  SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR \
    SINGULARITYENV_MSPASS_SCHEDULER_ADDRESS=$NODE_HOSTNAME \
    SINGULARITYENV_MSPASS_ROLE=worker \
    mpiexec -n $((SLURM_NNODES-1)) -host $WORKER_LIST $SING_COM &

This uses the openmp command line tool `mpiexec` to launch the
container on all the nodes except the first one in the list.
This is only using mpi as a convenient way to launch background
processes on nodes slurm assigns to the job.  An alternative
that might be preferable at other sites is do the same thing with a
shell loop and calls to ssh.   The mpi implementation shown here, however,
is known to work and one or more versions of mpi are universally available
at HPC centers at the time this manual was written.

Communications
"""""""""""""""""""
Last, but far from least you may need to sort out some fundamental
issues about how networking is implemented on your cluster.  There are
two different issues you may need to consider:

1.  Are there any network communication restrictions between compute nodes?
    `Dask<https://dask-chtc.readthedocs.io/en/latest/networking.html>`__
    and `spark<https://www.ibm.com/docs/en/zpfas/1.1.0?topic=spark-configuring-networking-apache>`__
    have different communication setups described in the links in this
    sentence.  The general pattern seems to be that clusters are normally
    configured to have completely open communication between nodes
    within the cluster but are appropriately paranoid about connections
    with the outside world.  You probably won't need to worry about
    connectivity of the compute nodes, but problems are not inconceivable.
2.  A problem you are guaranteed to face is how to connect to a job running
    on the cluster.   The simplest example is needing to connect to the
    jupyter notebook server for an interactive run.  We reiterate that isn't
    a great idea, but you will likely eventually need to use that feature
    to solve some problem that you can't solve easily with batch submissions.
    A more universal need is to run real-time
    `dask diagnostics<https://docs.dask.org/en/stable/diagnostics-distributed.html>`__.
    These are an important tool to understand bottlenecks in a parallel workflow that
    are limiting performance.  For dask diagnostics to work you will need to
    connect on some port (default is 8787) to the node running the scheduler.
    The fundamental problem both connection face is that cluster are
    normally accessible from outside
    only through "login nodes" (also sometimes called head nodes).
    The login nodes are sometimes called a network "gateway" to the cluster.

  Our template script addresses item 2 by a variant of that
  describe in
  `this dask package extension documentation<https://dask-chtc.readthedocs.io/en/latest/networking.html>`__.
  That source has some useful background to explain the following
  approach we use in our template run_mspass.sh script:

  ..code-block::


    NODE_HOSTNAME=`hostname -s`
    LOGIN_PORT=`echo $NODE_HOSTNAME | perl -ne 'print (($2+1).$3.$1) if /c\d(\d\d)-(\d)(\d\d)/;'`
    STATUS_PORT=`echo "$LOGIN_PORT + 1" | bc -l`
    echo "got login node port $LOGIN_PORT"

    NUMBER_LOGIN_NODES=4
    LOGIN_NODE_BASENAME=login
    for i in `seq $NUMBER_LOGIN_NODES`; do
      ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 $LOGIN_NODE_BASENAME$i
      ssh -q -f -g -N -R $STATUS_PORT:$NODE_HOSTNAME:8787 $LOGIN_NODE_BASENAME$i
    done

The complexity of the first section using perl solves a potential problem
automatically.   Because login nodes are nearly always shared by multiple
users a fixed port for a connection to the login can easily cause a
mysterious collision if two people attempt to use the same port to
access the login node.   The approach used here is that used at TACC.
The perl command converts the compute node's hostname to a port number.
Since while you run your job you are the only one who can access that node
that will guarantee a unique connection.   That approach may not work
at other sites.  A simpler solution that might be suitable for many
sites is to just set LOGIN_PORT and STATUS_PORT to some fixed numbers
known to not collide with any services on the login node.

The second second solves a second potential problem.   A large cluster
will have multiple login nodes.  The example in the template file is
set for TACC where there are four login nodes with names
login1, login2, login3, and login4.  Thus, above we set
`NUMBER_LOGIN_NODES` to 4 and the shell variable `LOGIN_NODE_BASENAME` to
"login".   You will need to change those two variables to the
appropriate number and name for your cluster.   The ssh lines in the
for loop set up what is called an ssh tunnel from the compute node
to all the login nodes.   It is necessary to create that in the job script
as the job scheduler normally assigns the you to the least busy login
node when you try to connect to one of them.   The mechanism above allows
you to access the jupyter notebook listening on port 8888 to the port
number created in the earlier incantation from the compute node name.
Similarly the dask status port 8787 is mapped to the value of $STATUS_PORT.

We emphasize that none of the network complexity is required in two situations
we know of:

1.  If you only intend to run batch jobs connections to the outside will not
    be needed and you can delete all the network stuff from the template.
    In fact, we recommend you prepare a separate run script, which you
    migh call `run_mspass_batch.sh` that doesn't simply deletes all the
    network stuff above.
2.  Some sites may have a science gateway setup to provide a mechanism to
    run jobs interactively on the cluster.  The example noted earlier used
    at Indiana called "RED" is an example.   With RED you launch a window on
    your desktop that behaves as if you were at the system console for the
    login node.   In that situation the ssh tunnel stuff is not necessary.
    A web browser running in the RED window can connect directly with
    port 8888 and port 8787 on the compute node once the job starts.
