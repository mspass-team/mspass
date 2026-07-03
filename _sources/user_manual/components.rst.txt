.. _mspass_components:

MsPASS Components
========================
MsPASS Services
----------------------
Much of the modern IT environment centers around the abstract concept of
a "service".  That concept has evolved for decades and is, in fact, a
central focus of cloud systems.   You can read more about that in
`an introduction to service-oriented architecture <https://aws.amazon.com/what-is/service-oriented-architecture/>`__.
For the purpose of this manual, you should understand that a service
is a version of a black box.   If you put in something it understands it
will spit out an answer you can use.   If you give it something it doesn't
understand it will give you some kind of error.

With that brief overview, MsPASS always expects to
have access to three primary services:

1.  A *database service* that uses the MongoDB API
    (Application Programming Interface).
2.  A *scheduler service* to manage tasks to be executed on the generic
    concept of a "cluster" of multiple processors.   The default MsPASS
    scheduler is `dask <https://www.dask.org/>`__
    but the framework also supports
    `pyspark <https://spark.apache.org/docs/latest/api/python/index.html>`_.
3.  A *frontend* service that runs a compatible version of python.  Normally
    that is an instance of a `jupyter server <https://jupyter.org/>`_ that creates
    a user interface you can work with interactively.   Note that large
    jobs on HPC systems are normally run in a mode where there technically is
    not a "frontend service" but the batch job just runs a simple python
    interpretter to drive the workflow.  You should think of jupyter as
    more like a gateway that allows you to run MsPASS interactively.

Launching MsPASS services
--------------------------
A "service" requires some piece of software to be running somewhere that
your workflow can access.   That software can be remote, such as in a web
application, or on the same system where your job is running.  In all
cases that application is normally called a "server".   For example,
most database services are implemented with a "database server" including
MongoDB used in MsPASS.

How the services/servers are launched is system dependent.  You may be
able to connect to a running instance of an appropriate server, but in
most cases you need to launch them outside the python script or
jupyter notebook that defines your workflow.  For that reason,
MsPASS has a set of "launchers":

1.  When you are running on a desktop/laptop system we recommend you use the
    `mspass-desktop` command line tool described in :ref:`mspass_desktop`.
2.  When running on an HPC system the recommended way to handle launching
    is with the python `HPCClusterLauncher` class.  Setting up MsPASS to run
    on an HPC platform is described in :ref:`deploy_mspass_on_HPC`.
3.  Running MsPASS on a cloud system can be simpler or harder depending on
    how much work someone else has done for you.   Earthscope
    `GeoLab <https://www.earthscope.org/data/geolab/>`_
    automates launching the "frontend" service by operating as a JupyterHub
    gateway.   At the time this page was written,
    they had a running instance of dask as the "scheduler service".
    You then need only connect to it with the right incantation.
    The "database service" there needs to be launched manually.
    To see how to launch the database server on GeoLab,
    see the jupyter notebooks for the
    course we taught in 2025 found in
    `this section <https://github.com/mspass-team/mspass_tutorial/tree/master/Earthscope2025>`_
    of the MsPASS tutorials, GitHub repository.
    If you are launching a custom cloud setup, all three services need to be
    launched manually through a system dependent mechanism.   Good luck with
    that unless you have a lot of help from knowledgeable people.

Client concepts
------------------
Most modern "services", including all those in MsPASS, require a *client*
to interact with any service.  A *client* is a piece of software that runs
under the control of your user credentials that rigidly enforces the rules
the service enforces to interact with it.   A client is always essential in
a multiuser system to keep user A from colliding with user B.

In a python application, which is what a "MsPASS workflow" is, a particular
type of client is generally defined by a python class.  An instance of
that client is created by invoking the class constructor to create the
object that is the client.  For example, most user's of MsPASS are familiar
with the obspy FDSN web services client.   The stock incantation to create
one is

.. code-block:: python

   from obspy.clients.fdsn import Client

   webclient = Client("EARTHSCOPE")

Note it is critical to recognize that clients all assume there is a
server/service it will be asked to access.   A particular instance
may need data to tell it where that service is running.  For the
obspy example above that "data" is the magic string "EARTHSCOPE".
If you dig into their code, you will find "EARTHSCOPE" is just an alias that
triggers a connection setup to a service managed by Earthscope.
The point is instantiating a client of any kind
normally requires an exchange of computer bodily fluids between
your code (the client) and the service.  With python much of that
data is often defaulted but you may need a complicated incantation.
For instance, at the time of this writing this is the incantation
required to interact with Amazon's S3 store system where Earthscope
is migrating all the waveform data they manage:

.. code-block:: python

   from earthscope_sdk import EarthScopeClient

   client = EarthScopeClient()
   creds = client.user.get_aws_credentials()

   S3_ACCESS_POINT = "earthscope-mseed-res-na3mtd4fq5kz7pntcyr1uh46use2a--ol-s3"
   BUCKET = S3_ACCESS_POINT

   session = boto3.Session(
     aws_access_key_id=creds.aws_access_key_id,
     aws_secret_access_key=creds.aws_secret_access_key,
     aws_session_token=creds.aws_session_token,
   )
   s3_client = session.client(
                    "s3",
                    config=Config(
                        request_checksum_calculation="when_required",
                        response_checksum_validation="when_required",
                    ),
                )

That code creates an instance of a "client" for interacting with S3
data.  The object created is referenced above with the symbol `s3_client`.

The MsPASS Client
-------------------

Because the MsPASS framework depends upon three "services" a starting point for
all MsPASS workflows is to set up the clients that the workflow will need to run.
We simplify that process by defining a MsPASS client that
is best thought of as a container holding clients needed to operate MsPASS.
As a result almost all MsPASS workflow examples you will will see in this
manual and in the tutorials begin with some variation of this incantation:

.. code-block:: python

   from mspasspy.client import Client
   mspass_client = Client()

That example uses the defaults for the constructor, but
as you can see from the
`docstring <https://www.mspass.org/python_api/mspasspy.client.html>`_
there are optional arguments
to define connection data for "database" and "scheduler" services.

There are two "getter" methods that are used in most MsPASS workflows.
The first is the one to fetch the database client:

.. code-block:: python

   dbclient = mspass_client.get_database_client()

which does what the method name implies - set the symbol `dbclient` as
a reference to a running database client.   The database client itself is
not always needed so many workflows call the following that uses the
client under the hood:

.. code-block:: python

   db = mspass_client.get_database("mydb")

which instantiates a handle to particular set of data defined in
this example with the string "mydb".

Finally, some workflows need to interact with the "scheduler" service.
The most common example is the dask schedule has a facility for
monitoring a job in real time called
`dask diagnostics <https://docs.dask.org/en/stable/dashboard.html>`_.
For that reason, you will see many example MsPASS jupyter notebooks that
contain this construct in a code box:

.. code-block:: python

   dask_client = mspass_client.get_scheduler()
   dask_client

When used in a jupyter notebook the last line generates a block of
hypertext that contains connection data to link to dask diagnostics.
The object defined above by the `dask_client` symbol also contains
a long list of methods described in the dask documenation.

Notice the mspass client does not have a method to fetch the "frontend".
The reason is that in MsPASS the "frontend" is, by definition, the
thing used to run your workflow.   In python class lingo the "frontend" is "self".
Note also that the API for the mspass client emphasizes an important
assumption to always keep in mind.   That is, the mspass framework
assumes the database and scheduler services are available to the workflow.
If that assumption is invalid, your job will fail.


Clients in a Parallel Setting
-------------------------------
The "P" in MsPASS stands for "parallel".   Currently MsPASS is the
only general-purpose package for handling seismology data that
provides mechanisms for generic parallel processing.   The
key component to do that is the "scheduler" service.
That service abstracts the idea of parallel processing.
For details the reader can consult numerous online sources for
general concepts and the
ref:`parallel_processing` section of this user manual.
This section of this manual is a classic example of the reason why
hypertext is better than a linear manual for a system like MsPASS.
If you are not familiar with parallel processing concepts you may
need to do some auxiliary study to comprehend the rest of this
page.

A parallel environment presents special problems for a system like
MsPASS that depends on a service like a MonogDB database server.
The reasons is that the abstraction of parallel "workers" used by
scheduler services (aka dask or pyspark) treat the workers as
independent entities that are stupid and know only information you feed them.
As a result, if a worker needs to access a MongoDB server it needs to be
told how to do that.  The simplest, stateless way to do that would be to
instantiate an instance of a database client on each call to a processing
function needing database access.   You might guess and would be right
that that is a really really bad idea if your workflow is processing a large
number of data that would require that to be done for every datum.
In fact, timing data show instantiating a database client takes a significant
fraction of 1 s on most systems.   Even a fraction of a second times a million
is a very long time.

To solve this issue, earlier versions of MsPASS simply serialized the
database client.  We found we could get by with that for a while until
we scaled up processing to the order of millions of data objects.
That turned out to eventually crash the database server from a
"too many open files" error caused by the server not releasing
connections each serialized client initiated.  There is currently
no solution to this problem with pyspark.   With dask we found a solution
in what they call a
`worker plugin <https://distributed.dask.org/en/stable/plugins.html>`_.
Later versions of MsPASS define a python class called
`MongoDBWorker` that is a child of dask's `WorkerPlugin`.   To use
this feature all parallel workflows should contain a variant of the
following before doing any parallel database operations:

.. code-block:: python

   from mspasspy.util.db_utils import MongoDBWorker
   from mspasspy.client import Client

   mspass_client = Client()
   dask_client = mspass_client.get_scheduler()
   parallel_dbworker = MongoDBWorker(mspass_client)
   dask_client.register_worker_plugin(parallel_dbworker)

What that does is instantiate an instance of a MongoDB client
on each worker that is kept memory resident in the worker.
Further, if the worker dies the client is recreated when the
scheduler relaunches the worker.   We have found this approach is
more stable and has improved the performance of parallel workflows
because each worker client doesn't normally have to create any
new connections to the server.

All mspass functions that use database services
(notably waveform data readers and writers) handle interaction
with the parallel plugin automatically.  If you are developing a
custom processing function that has a database handle as an
argument you need to deal with this issue.  The following
skeleton example illustrates how to handle that situation:

.. code-block:: python

   from mspasspy.util.db_utils import fetch_dbhandle

   def myfunction(query,dbname):
     """
     Example function illustrating use of fetch_dbhandle.

     This example would return a TimeSeriesEnsemble of data
     yielded by a query defined by arg0.  It is a simple replacement
     for read_distibuted_data.

     Note dbname is a string defining a particular database the server
     will need to access.
     """
     db = fetch_dbhandle(dbname)
     cursor = db.wf_TimeSeries.find(query)
     # proper form would trap a null return here - omitted for simplicity
     ens = db.read_data(cursor,collection="wf_TimeSeries")
     return ens

The key point here is that with that construct `myfunction` can
be run in a parallel constuct using map-reduce or the Futures interface
(see :ref:`parallel_processing`).   The essential thing to do is to send the
function only the database name and then use the `fetch_dbhandle` inside
the function to create the database handle to the database the name references.

.. note::

   Another example of a client that needs to be handled carefully
   in parallel is the an s3 client like the example earlier in this page.
   A prototype for a worker plugin similar to the database client described
   above has been developed.   You may find a final implementation in
   the latest version of MsPASS.   As is normal this page may be
   behind that development.
