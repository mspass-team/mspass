.. _parallel_processing:

Parallel Processing
===========================
Introduction
~~~~~~~~~~~~~~~~~
One of the primary goals of MsPASS was a framework to make
parallel processing of seismic data no more difficult than running
a typical python script on a desktop machine.   In modern IT lingo
our goals was a "scalable" framework.  The form of parallelism we
exploit is a one of a large class of problems that can reduced to
what is called a directed acyclic graph (DAG) in computer science.
Any book on "big data" will discuss this concept.
Chapter 1 of Daniel (2019) has a particularly useful description using
a cooking analogy.  Our framework supports two well developed systems
for handling such problems called Spark and Dask.
Dask is the default.

An important perspective on this issue is that classical seismic processing systems
concepts are well matched to the DAG Dask and Spark use for scheduling.  Classic
seismic reflection processing systems runs a data set through a series of
processing algorithms.  Each processor acts on either a single signal at
at time (e.g. time-domain filtering) or a group of signals (what we call
a TimeSeriesEnsmble in MsPASS) to produce an output of one or more modified
signals.   e.g. stacking takes an ensemble and produces a single output
while migration takes an input ensemble and produces a (modified) ensemble.
All these examples can be implemented in MsPASS framework to take advantage
of the repetitious nature of the processors.  That is, all can be the thought of
as black boxes that take one or more seismic data objects as input and emit
more or modified data objects (not necessarily of the same type or number as
the inputs).  A full processing workflow is assembled by chaining a series of
processing steps with data moving between the processing steps by a system
dependent process.   Dask and Spark generalize this by fragmenting the processing
into multiple processors and/or nodes moving the data between steps by a
process largely opaque to you as the user.  An added benefit that is less
obvious is that lower-level parallel granularity is possible for a
python function designed to exploit the capability.  This user manual,
however, focuses exclusively on algorithm level granularity.   We refer
the reader to extensive printed and online documentation for DASK and Spark
for functionality needed to add parallelism inside an individual python
function.

Data Model
~~~~~~~~~~~~

A key concept to understand about both Spark and Dask is the data model.
A simple way to view the concept is that we aim to break "our data"
into a finite set of *N* things (objects) that are each to be handled
identically.   The raw input to any complete seismic data set fits this
idea;  the "data set" is a set of *N* raw signals.  A serial version of
processing a typical data set could be reduced to a loop over each of the *N*
objects applying one or more algorithms to each datum one at a time.   In Spark
and Dask the data are treated collectively as a single data set.
The "data set" is abstracted by a single container that is used
to symbolically represents the
entire data set even though it (normally) is not expected to fit in the
computer's memory.   Spark refers to this abstraction as a
Resilient Distributed Dataset (RDD) while Dask calls the same thing a "bag".

In MsPASS the normal content of a bag/RDD is a dataset made up of *N*
MsPASS data objects:  TimeSeries, Seismogram, or one of the ensemble of
either of the atomic types.  An implicit assumption in the current
implementation is that any processing
was proceeded by a data assembly and validation phase.
Procedures for data import and data validation
are described in this section :ref:`importing_data`.
A key concept here is that "importing" a data set to MsPASS means the
entire data set has been assembled and loaded either into the gridfs
file system internal to MongoDB or an index has been built for all data
stored in local or cloud-based files.   The entire data set can, if necessary,
be a mix of the two
because the MsPASS readers abstract the entire process of loading data
for processing.  In addition, any required normalization data (i.e.
data in the *site*, *channel*, and/or *source* collections) must be
loaded and cross-referencing validated as described in :ref:`normalization`.
Once the data set is fully assembled the bag/RDD defining the abstraction of the
data is easily defined by a variant of the following code section:

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.io.distributed import read_distributed_data

  mspass_client = Client(database_name="exampledb", scheduler="dask")
  dbhandle = mspass_client.get_database()
  dask_client = mspass_client.get_scheduler()
  query = {}            # An empty query selects the full collection
  # Example uses no normalization; most reads use the normalize argument.
  mybag = read_distributed_data(
      dbhandle,
      query=query,
      collection="wf_TimeSeries",
      scheduler="dask",
  )

The final result of this example script is a Dask bag, which here is
assigned to the python symbol *mybag*.  The bag is created by
the MsPASS *read_distributed_data* function from the database, query, and
collection arguments.  Internally, the reader partitions the matching
waveform documents without loading the complete data set into the calling
process.
The *read_distributed_data* function is more or less a conversion between
external storage and the processing framework (Dask or Spark) that use
a common concept of a buffered list.

Functional Programming Concepts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Overview
-----------
A second key concept we utilize for processing algorithms in MsPASS is the
abstraction of
`functional programming <https://en.wikipedia.org/wiki/Functional_programming>`__,
which is a branch of programming founded on
`lambda calculus <https://en.wikipedia.org/wiki/Lambda_calculus>`__.
For most seismologists that abstraction is likely best treated only as
a foundational concept that may or may not be helpful depending on your
background. It is important, however,
because all the parallel processing algorithms utilize a functional
programming construct to run an algorithm on a dataset abstracted with
a bag or RDD.  There are two forms we use in MsPASS with these general forms:

.. code-block:: python

  x=y.map(functional)

and

.. code-block:: python

  x=y.fold(functional)

Spark calls the latter operation by the more common name *reduce*.

These two constructs can be thought of as black boxes that handle inputs
as illustrated below:

  - simple figure here showing map and reduce in a graphical form -

We expand on each of these constructs below.

The map operator
--------------------

A *map* operator takes one input and emits a modified version of
the input as output.  The inputs and outputs of a map are often the same type (e.g. a time-invariant filter),
but not always (e.g the *bundle* algorithm takes a TimeSeriesEnsemble as
and input and emits a SeismogramEnsemble).   A concrete example for
the application of a simple filter in dask is:

.. code-block:: python

  # Assume dbhandle is set as a Database class as above.
  d_in=read_distributed_data(
      dbhandle, query={}, collection="wf_TimeSeries", scheduler="dask"
  )
  d_out=d_in.map(signals.filter, "bandpass", freqmin=1, freqmax=5, object_history=True, alg_id='0')
  d_compute=d_out.compute()

This example applies the obpsy default bandpass filter to all data
stored in the wf_TimeSeries collection for the database to which dbhandle
points.  The *read_distributed_data* line loads that data as a Dask bag
we here call *d_in*.  The map operator applies the algorithm defined by
the symbol *signals.filter* to each object in *d_in* and stores the
output in the created (new) bag *d_out*.    The last line is way you tell dask to
"go" (i.e. proceed with the calculations) and store the computed result in the *d_compute*.
The idea and reasons for the concept of "lazy" or "delayed"
operation is discussed at length in various sources on dask (and Spark).
See the `Dask Bag documentation <https://docs.dask.org/en/stable/bag.html>`__
for more on lazy or delayed operation.  The final output, which we chose above
to give a new symbol name of :code:`d_compute`, is a Python list held in the
calling process.  For a large data set that may exhaust driver memory, so a
production workflow normally terminates with a distributed database save.

The same construct in Spark, unfortunately, requires a different set of
constructs for two reasons:  (1) pyspark demands a functional
programming construct called a lambda function, and (2) spark uses a
different construct for handling delayed computations.  The following
example is the translation of the above to Spark:

.. code-block:: python

  # Assume dbhandle is set as a Database class as above and context is
  # Spark context object also created earlier
  d_in=read_distributed_data(
      dbhandle,
      query={},
      collection="wf_TimeSeries",
      scheduler="spark",
      spark_context=context,
  )
  d_out=d_in.map(lambda d: signals.filter(d,"bandpass", freqmin=1, freqmax=5, object_history=True, alg_id='0'))
  d_compute=d_out.collect()

Notice the call to map in spark needs to be preceded by a call to the *parallelize*
method of the SparkContext object, which is called inside *read_distributed_data*.
That operator is more or less a constructor for the container that Spark
calls an RDD that is assigned the symbol d_out in the example above.
The following line, which from a programming perspective is a call to the map method of the RDD we call
d_out, uses the functional programming construct of a lambda function.
This tutorial in `realpython.com <https://realpython.com/python-lambda/>`_
and `this one <https://www.w3schools.com/python/python_lambda.asp>`_ by w3schools.com
are good starting points.

Both scripts create a final processed data set Python associates with the
symbol :code:`d_compute`.  Dask ``compute`` and Spark ``collect`` materialize
that result in the calling process.  Since a bag or RDD is designed for a data
set that may not fit in memory, most MsPASS workflows instead terminate with
a distributed database save operation.

Reduce/fold operators
-------------------------
A second parallel construct we use is the `Reduce` clause of the `MapReduce`
paradigm that was a core idea in Hadoop (see the `Apache Hadoop MapReduce
tutorial
<https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html>`__).
Hadoop's MapReduce model was an ancestor of both Spark and Dask.

The generic problem of stacking (averaging) a set of signals
is an example familiar to all seismologists that can be used to illustrate
what a `Reduce` operator is.
The following is a crude MsPASS serial implementation of
stacking all the members of an ensemble:

.. code-block:: python

  ensemble=db.read_ensemble_data(cursor)
  stack=TimeSeries(d.member[0])
  for i in range(len(d.member)-1):
    stack += ensemble.member[i+1]

That code is pretty simple because the += operator is defined for the TimeSeries
class and handles time mismatches.  It is not robust for several reasons and
could be done other ways, but that is not the key point.  The point is
that the operation is summing a set of TimeSeries objects to produce the
single result stored with the symbol :code:`stack`.

We will get to the rules that constrain :code:`Reduce` operators in a moment, but
it might be more helpful to you as a user to see how that algorithm
translates into dask/spark.  MsPASS has a parallel stack algorithm found
`here <https://github.com/mspass-team/mspass/blob/master/python/mspasspy/reduce.py>`__
It is used in a parallel context as follows for dask:

.. code-block:: python

  res = ddb.fold(lambda a, b: stack(a, b))

For spark the syntax is identical but the name of the method changes to reduce:

.. code-block:: python

  res = rdd.reduce(lambda a, b: stack(a, b))

The :code:`stack` symbol refers to a python function that is actually quite simple. You can view
the source code `here <https://github.com/mspass-team/mspass/blob/master/python/mspasspy/reduce.py>`__.
It is simple because most of the complexity is hidden behind the +=
symbol that invokes that operation in C++ (`TimeSeries::operator+=` for anyone
familiar with C++) to add the right hand side to the left hand side of
the operator.  The python function is also simplified significantly by
the use of python decorator defined by this line in the stack source code:

.. code-block:: python

  @mspass_reduce_func_wrapper

which is a generic wrapper to adapt any suitable reduce function to MsPASS.

The final issue we need to cover in this section is what exactly is meant
by the phrase "any suitable reduce function" at the end of the previous paragraph?
To mesh with the reduce framework used by spark and dask a function has
to satisfy `the following rules <https://en.wikipedia.org/wiki/Reduction_operator>`__ :

1. The first two arguments (a and b symbols in the example above)
   must define two instances of the same type
   that are to be combined in some way.
2. The function must return an object of the same type as the inputs.
3. The combination algorithm must be commutative and associative.

The commutative and associative restriction arises because in a parallel setting a type
reduce operation like a summation is done on multiple processors and
eventually summed to a single output.  Which processor does what part of the
sum is completely determined by the scheduler so an order cannot be
assumed.

A simple summary of the role of reduce operators in algorithms is this:
any operator that can be expressed mathematically as a summation operator
is a candidate for a reduce.   The stack example above involves summing
a set of TimeSeries objects, but the approach can be used at lower levels.
In particular, reduce is a commonly used tool to implement threading in
pure python code that implements some summation operation.  Turning the
summation loop into a reduce operator can parallelize the loop.  Users
should consider that approach in writing pure python algorithms.

Futures
-------------
Fundamentals
#############
Versions of python newer than 3.2 have a built in
`concurrent.futures <https://docs.python.org/3/library/concurrent.futures.html>`_
module.   That module provides threading support for any python application.
Since the release of dask distributed they have supported a distributed
memory variant of the futures api.   That is, dask Futures are used much
like stock python Futures but can run on a cluster with many nodes.
The stock python module, in contrast, only works on a single machine
(shared memory system).  MsPASS uses dask Futures for the `sliding_window_pipeline`
feature described above.  Although it may be possible to create a workflow
mixing dask and python Futures, note that is not recommended as it is
intrisically dangerous and prone to mysterious failures.   The examples
in this section use dask Futures interface
but you should be aware the standard python API is very similar.

The `dask documentation <https://docs.dask.org/en/stable/futures.html>`_
on this topic is excellent.   If you are reading this, you should
use that to understand the general concepts.  For MsPASS jobs there
are a few things to highlight:

-  The Futures interface is ideal for a pipeline processing workflow.
   That means, any workflow that could be expressed as a string of
   map operators without a reduce operation is easily converted to
   one you could implement with Futures.
-  To run a pipeline workflow with Futures the sequence of map
   operators need to be bundled into a single function.
   For actual examples, see
   `the 2026 Earthscope tutorial course material <https://github.com/mspass-team/mspass_tutorial/tree/master/Earthscope2026>`__.
   For a generic example in pseudocode suppose I wanted to run
   algorithm A, B, and C on a dask bag of data.   The map version of that
   sequence might be:

.. code-block:: python

   import dask.bag as dbg
   mydata = dbg.from_sequence(input_list_defining_data)
   mydata = dbg.map(reader_function)   #  something to read data from sequence
   mydata = dbg.map(A)
   mydata = dbg.map(B)
   mydata = dbg.map(C)
   result = mydata.compute()

   To run that with the Futures interface, you would need to create a
   python function that merges the pipeline steps into function call.
   Here is a sketch of what how that would be laid out without the
   real life required complexity of all the arguments to the processing
   functions:

.. code-block:: python

   def my_process_function(x):
     """
     Example function to run process read, A, B, and C with Futures.
     Arg0 (x) is a value of one of the contents of "input_list_defining_data"
     in version using map operations above.
     """
     d = read_function(x)
     d = A(d)
     d = B(d)
     d = C(d)
     return d

A naive use of that function on a complete data set is the following:

.. code-block:: python

   from dask.distributed import as_completed
   from mspasspy.client import Client
   mspass_client = Client()
   dask_client = mspass_client.get_scheduler()
   f_list = list()
   for x in input_list_defining_data:
       f = dask_client.submit(my_process_function,x)
       f_list.append(f)
   for f in as_completed(f_list):
       y = f.result()
       function_to_handle_result(y)

This example uses the `as_completed` dask function.   The
"function_to_handle_result" is a stub for something like a writer
to save the results something that combines the result (a variant of Reduce).
See the dask distributed documentation for details on `as_completed`.

-  The toy example above did not include any arguments to the
   hypothetical functions used inside "my_process_function".   Arguments are nearly
   always required that are not simple constants like `collection="wf_TimeSeries"`.
   It is important to realize that any data passed as arguments to a
   function used in any parallel construct (map or submit) needs to be
   serialized, transmitted to the worker, and reconstructed by the worker
   before the function start work on a particular piece of data.
   Large, auxiliary data objects that are required arguments can thus
   create a serious performance issue.   Two common examples in waveform
   processing are "Matchers" used by :py:func:`mspasspy.db.normalize.normalize`
   and obspy's
   `Tau-P travel time calculator <https://docs.obspy.org/packages/autogen/obspy.taup.tau.TauPyModel.html>`_.
   We have found signicant improvement in performance by using dask's
   `scatter <https://distributed.dask.org/en/latest/locality.html>`_ to preload large data objects.
   See the source in that link for details on how to use that function and
   more about why it is often useful.

MsPASS Sliding Window Pipeline
#################################
In version 2.4.0 of MsPASS we added a special function for handling
large data sets that uses the Futures API called
:py:func:`mspasspy.workflow.sliding_window_pipeline`.
The basic structure of the alorithm is similar to the tutorial
example with `submit` above but it adds an important variation
for handling large data.  That is, if one applied that algorithm to
a list of more than a few hundred things we found the scheduler
would be overwhelmed.   At best performance suffers.  If the list is
too long you can crash the scheduler.  To keep that from happening
instead of submitting everyting at once submits are pushed into
a queue of a finite size (the sliding window).   The scheduler then
only needs to send a single task (run the function sent with submit
on the data passed to it) at a time to each worker with a simple
queue algorithm.   That approach has several advantages
for typical seismic workflows:

1. It provides a stable way to handle very large lists like all the
   wf_miniseed documents that define the raw inputs to a waveform processing
   workflow.  Prior to our development of the `sliding_window_pipeline`
   function we had been unable to process data sets with atomic data
   (TimeSeries or Seismogram objects) driven by a list of documents
   returned by a MongoDB query.  That approach with a bag created using
   `dask.bag.from_sequence` would not work when the list was larger than
   around 10,000 documents.   `sliding_window_pipeline` does no suffer
   from that problem if the slliding window size is set properly.
2. Memory use is more predictable than one using either bag.map or the
   map method of the dask distibuted client.  The reason, in fact,
   item 1 is a factor is because of memory management issues with
   stock use of dask's map operators.  Most MsPASS workflows can be
   reduced to three generic steps:  (a) create a waveform object from
   some input desciption, (b) modify that datum, and (c) save the
   modified data.   The input to (a) for atomic data in MsPASS is
   normally a MongoDB document.  In nearly all cases when that document(s)
   is used to create a waveform object, the size of what is passed to
   step (b) bloats by several orders of magnitude.   That seems to fool
   both the dask and spark schedulers causing them to submit too many
   tasks to each worker causing memory faults when multiple instances
   try to allocate additional memory.  With the `sliding_window_pipeline`
   memory use is always exactly predictable as the sliding window size
   times the nominal input data size times an algorithm dependent
   factor (e.g., 2 if the algorithm makes a temporary copy of the input.)
   See :ref:`memory_management`
   for more on this topic.

.. note::

   The sliding window pipeline is an interesting example of the utility
   of modern AI applications in scientific computing.  There seems no
   definitive pubished source on the idea as it is an application of
   some common computer science concepts.   The generic algorithm is considered
   "best practice" in other fields according to Gemini, although I'm
   unable to validate that claim.  The point is that the idea for this
   algorithm came from asking Gemini the right question, not by digging
   through the published literature.

Schedulers
---------------
As noted previously MsPASS currently supports two different schedulers:
Dask (the default) and Spark.   Both do very similar things but are known
to perform differently in different cluster environments.  Users needing to
push the system to the limits may need to evaluate which perform better in
their environment.

.. note::

   Although MsPASS still support both pyspark and dask, most of our
   real data experience has used daks.  Furthermore, the Futures
   feature of dask, which is the basis for the `sliding_window_pipeline` ,
   does not seem to have an equivalent in pyspark.   Support for
   spark should be considered marginal and may, in fact, be dropped in
   future releases.

In MsPASS we use Spark and Dask to implement the "master-worker"
model of parallel computing.   The "master" is the scheduler that hands off
task to be completed by the workers.  A critical issue this raises is how
the data is handled that the workers are told to process?  Both Spark
and Dask do that through "serialization".  The schedulers move atomic
data between processes by serializing the data and then having the other
end deserialize it.   How and when that happens is a decision made by
the scheduler.  That process is one of the primary limits on scalability of
this framework.   e.g. it is normal for a single worker calculation to be
much slower than a simple loop implementation because of the serialization
overhead.  The default serialization for both PySpark (The native tongue of
Spark is Scalar.  PySpark is the python api.)
and Dask (Python is the native tongue of Dask.) is pickle.   It is important
to recognize that if you write your own application in this framework the
data object you pass to map and reduce operators must have a pickle operator
defined.  That function needs to be as fast as possible as it will be
called a lot in a parallel environment.

Another limit on scalability of this framework is that before the computations,
Dask and Spark need to create a task graph for task scheduling.
Task scheduling breaks your program
into many medium-sized tasks or units of computation.
These tasks are typically a function call which in MsPASS
usually involves passing a non-trivial amount of data to the task
(one or more seismic data objects).
The schedulers represent these tasks as nodes in a graph
with links between nodes defining how data moves between tasks.
The task scheduler uses
this graph in a way that respects these data dependencies and leverages parallelism where
possible.  Multiple independent tasks can be run simultaneously that are
are data driven. Usually this scheduling
overhead is relatively small unless the execution time for
processing is trivial.

For more information, the dask documentation found
`here <https://docs.dask.org/en/latest/scheduling.html>`__ is a good
starting point.

Examples:
~~~~~~~~~~~~~
Atomic Data Example
-------------------------------
The simplest workflow is one that works only with atomic
data (i.e. TimeSeries or Seismogram objects).  The example
example in the Data Model section above is of this type.
The following fragment is similar with a few additional processing steps.
It reads all data indexed in the data base as Seismogram objects,
runs a demean operator,
runs a simple bandpass filter, windows the data to a smaller range
defined by the window_seis function defined at he top, it
using the data start time, and then saves the results.

.. code-block:: python

  cursor=db.wf_Seismogram.find({})
  # read -> detrend -> filter -> window
  # example uses dask scheduler
  data = read_distributed_data(db, cursor)
  data = data.map(signals.detrend,type='demean')
  data = data.map(signals.filter,"bandpass",freqmin=0.01,freqmax=2.0)
  # windowing is relative to start time.  300 s window starting at d.t0+200
  data = data.map(lambda d : WindowData(d,200.0,500.0,t0shift=d.t0))
  data_out = data.compute()

That same workflow using `sliding_window_pipeline` is this:

.. code-block:: python

   def atomic_example(doc,dbname):
       """
       Example function merging atomic processing example functions
       into a single function for dask client submit in
       sliding_window_pipeline.
       """
       db = fetch_dbhandle(dbname)   # parallel method to fetch from worker memory
       d = db.read_data(doc)
       d = signals.detrend(d,type="demean")
       d = signals.filter(d,"bandpass",freqmin=0.01,freqmax=2.0)
       d = WindowData(d,200.0,500.0,t0shift=d.t0)
       return d

   import mspasspy.client as msc
   mspass_client = msc.Client()
   dask_client = mspass_client.get_scheduler()
   db = mspass_client.get_database("mydatabase")
   cursor = db.wf_Seismogram.find({})
   doclist = list(cursor)
   data_out = sliding_window_pipeline(doclist,
                       atomic_example,
                       dask_client,
                       db.name)

where in for this case I added the code to instantiate a MsPASS Client
omitted in the map example.   It illustrates that `sliding_window_pipeline`
requires an instance of the dask client.  With the map version the
dask client is hidden inside the bag map method.

Ensemble Example
----------------------
Handling ensembles is similar to atomic data, but the starting point
to create one is different.   In particular, this example uses
`read_distributed_data` with a list of dictionaries that define
queries as inputs.  The ensemble groups are the results of each
query.  In this case, that is common source gathers defined by
source_id values found in input.

.. code:: python

  srcidlist = db.wf_Seismogram.distinct("source_id")
  querylist=list()
  for srcid in srcidlist:
      query = {"source_id" : srcid}
      querylist.append(query)

  data = read_distributed_data(querylist,db,collection="wf_Seismogram")
  data = data.map(signals.detrend,type='demean')
  data = data.map(signals.filter,"bandpass",freqmin=0.01,freqmax=2.0)
  # windowing is relative to start time.  300 s window starting at d.t0+200
  data = data.map(lambda d : WindowData(d,200.0,500.0,t0shift=d.t0))
  data_out = data.compute()

The comparable code using `sliding_window_pipeline` is:

.. code:: python

  def process_ensemble(query,db.name):
    """
    Processing function for submit with ensemble example.
    """
    db = fetch_dbhandle(db.name)
    cursor = db.wf_Seismogram.find(query)
    # more robust code would handle a null cursor here - omitted for simplicity
    ens = db.read_data(cursor,collection="wf_Seismogram")
    ens = signals.detrend(ens,type="demean")
    ens = signals.filter(ens,"bandpass",freqmin=0.01,freqmax=2.0)
    # ator and rtoa do not work on ensembles
    # the concept applies only to atomic data
    for i in range(len(ens.member)):
        d = ens.member[0]  # use d as a shorthand for convenience
        d.ator(d.t0)
        d = WindowData(d,200.0,500.0)
        d.rota()
        ens.member[0] = d
    return ens

    import mspasspy.client as msc
    mspass_client = msc.Client()
    dask_client = mspass_client.get_scheduler()
    db = mspass_client.get_database("mydatabase")
    srcidlist = db.wf_Seismogram.distinct("source_id")
    querylist=list()
    for srcid in srcidlist:
        query = {"source_id" : srcid}
        querylist.append(query)
    data_out = sliding_window_pipeline(querylist,
                            process_ensemble,
                            dask_client,
                            db.name)

HPC deployment
--------------
The processing APIs above are independent of a particular batch scheduler,
container runtime, filesystem, or center.  Deployment details are maintained
separately because fixed queue names, hostnames, tunnel commands, and container
modules become obsolete quickly:

* Start with the :ref:`cluster concepts <getting_started_overview>` for the
  distinction between an HPC batch scheduler and the Dask or Spark task
  scheduler.
* Follow the :ref:`HPC deployment guide <deploy_mspass_on_HPC>` for current
  Slurm, Apptainer, service-role, interactive, and shell-launch guidance.
* Use :ref:`HPCClusterLauncher configuration <hpc_cluster_configuration>` for
  the launcher-version-specific YAML keys and known limitations.

The checked-in ``scripts/tacc_examples`` and older Singularity/Stampede2
examples are historical references, not portable job scripts.  Current HPC
deployments normally use Apptainer, but every resource directive, process
launcher, filesystem path, network route, and interactive-access method must
follow the current documentation for the target center.  In particular, do
not copy obsolete reverse-tunnel commands: use the center's supported access
method described by the current deployment guide.
