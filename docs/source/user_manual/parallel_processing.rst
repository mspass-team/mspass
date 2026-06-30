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
what is called a directed cyclic graph (DAG) in computer science.
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
Resiliant Distributed Dataset (RDD) while Dask calls the same thing a "bag".

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

  from mspasspy.db.client import DBClient
  from mspasspy.db.database import (Database,
                       read_distributed_data)
  dbclient=DBClient()
  dbname='exampledb'   # This sets the database name - this is an example
  dbhandle=Database(dbclient,dbname)
  query={}
  # code here can optionally produce a valid string defining query
  # as a valid MongoDB argument of for find method.  Default is no query
  cursor=dbhandle.wf_TimeSeries.find(query)
  # Example uses no normalization - most read use normalize (list) argument
  mybag=read_distributed_data(dbhandle,cursor)
  # For spark this modification is needed - context needs to be defined earlier
  #myrdd=read_distributed_data(dbhandle,cursor,format='spark',spark_context=context)

The final result of this example script is a dask bag (RDD for the commented
out variant), which here is
assigned to the python symbol *mybag*.  The bag is created by
the MsPASS *read_distributed_data* function from the "cursor" object returned by
the MongoDB find method.  A "cursor" is an iterable form of a list that
utilizes the same concept as Spark and Dask - the full thing does not
always exist in memory but is a moving window into the data set.
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

  x=y.accumulate(functional)

Noting that Spark calls the later operation the (more common) name *reduce*.

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

  # Assume dbhandle is set as a Database class as above
  cursor=dbhandle.wf_TimeSeries.find({})
  d_in=read_distributed_data(dbhandle,cursor)
  d_out=d_in.map(signals.filter, "bandpass", freqmin=1, freqmax=5, object_history=True, alg_id='0')
  d_compute=d_out.compute()

This example applies the obpsy default bandpass filter to all data
stored in the wf_TimeSeries collection for the database to which dbhandle
points.  The *read_distributed_data* line loads that data as a Dask bag
we here call *d_in*.  The map operator applies the algorithm defined by
the symbol *signals_filter* to each object in *d_in* and stores the
output in the created (new) bag *d_out*.    The last line is way you tell dask to
"go" (i.e. proceed with the calculations) and store the computed result in the *d_compute*.
The idea and reasons for the concept of of "lazy" or "delayed"
operation is discussed at length in various sources on dask (and Spark).
We refer the reader to (LIST OF A FEW KEY URLS) for more on this general topic.
The final output, which we chose above to give a new symbol name
of :code:`d_compute`, is bag containing the processed data.

The same construct in Spark, unfortunately, requires a different set of
constructs for two reasons:  (1) pyspark demands a functional
programming construct called a lambda function, and (2) spark uses a
different construct for handling delayed computations.  The following
example is the translation of the above to Spark:

.. code-block:: python

  # Assume dbhandle is set as a Database class as above and context is
  # Spark context object also created earlier
  cursor=dbhandle.wf_TimeSeries.find({})
  d_in=read_distributed_data(dbhandle,cursor,format='spark',spark_context=context)
  d_out=d_in.map(lamda d : signals.filter(d,"bandpass", freqmin=1, freqmax=5, object_history=True, alg_id='0'))
  d_compute=d_out.collect()

Notice the call to map in spark needs to be preceded by a call to the *parallelize*
method of the SparkContext object, which is called inside *read_distributed_data*.
That operator is more or less a constructor for the container that Spark
calls an RDD that is assigned the symbol d_out in the example above.
The following line, which from a programming perspective is a call to the map method of the RDD we call
d_out, uses the functional programming construct of a lambda function.
This tutorial in `realpython.com  <https://realpython.com/python-lambda/>`_
and `this one <https://www.w3schools.com/python/python_lambda.asp>`_ by w3schools.com
are good starting points.

Both scripts create a final processed data set python associates
with the symbol :code:`d_compute`.   A potentially confusing issue for
beginners is that the content of :code:`d_compute` are largely opaque.
The reason is that both a bag and RDD are designed to handle a data set
that will not fit in memory.  Dask and Spark have different methods
for disaggregating the container, but most MsPASS workflows would normally
terminate with a database save operation.

Reduce/fold operators
-------------------------
A second parallel construct we use is the the `Reduce` clause of the `MapReduce`
paradigm that was a core idea in Hadoop
(see for example the document in `this link <https://www.talend.com/resources/what-is-mapreduce/>`_ )
that was the ancestor of both Spark and Dask.

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
`cocurrent.futures<https://docs.python.org/3/library/concurrent.futures.html>`_
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

The `dask documentation<https://docs.dask.org/en/stable/futures.html>`_
on this topic is excellent.   If you are reading this, you should
use that to understand the general concepts.  For MsPASS jobs there
are a few things to highlight:

-  The Futures interface is ideal for a pipeline processing workflow.
   That means, any workflow that could be expressed as a string of
   map operators without a reduce operation is easily converted to
   one you could implement with Futures.
-  To run a pipeline workflow with Futures the sequence of map
   operators need to be bundled into a single function.
   For actual examples see our tutorials for the 2026 Earthscope
   short course found
   `here<https://github.com/mspass-team/mspass_tutorial/tree/master/Earthscope2026>`_.
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
   `Tau-P travel time calculator<https://docs.obspy.org/packages/autogen/obspy.taup.tau.TauPyModel.html>`_.
   We have found signicant improvement in performance by using dask's
   `scatter<https://distributed.dask.org/en/latest/locality.html>`_ to preload large data objects.
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


New Organization for discussion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cluster fundamentals
~~~~~~~~~~~~~~~~~~~~~~~
Overview of what one has to deal with to configure a parallel system
in a distributed cluster versus a multicore workstation.   Here are things
I can think of we need to discuss:

- batch Schedulers
- node-to-node communications
- containers in a distributed environment
- to shard or not to shard, that is the question
- io performance issues and choices (relates to file system related configuration)

Configuration
~~~~~~~~~~~~~~~
subsections for each of the above topics centered on example.

I think we should reorganize the script to have related
setups grouped by the categories we choose for this
user manual section (as much as possible - there may
be some order dependence)

Start of old section
~~~~~~~~~~~~~~~~~~~~~~
Configuration
~~~~~~~~~~~~~~~~~~
Overview
------------
Some configuration will be needed to run MsPASS in a HPC system or
a departmental cluster.   The reason is that the
environment of an HPC cluster has numerous complications not found on a
desktop system.  The example we give
here is what we use for testing the system on Stampede2 at TACC.
This section can be thought of as a lengthy explanation centered on
the example in our github page for configuring MsPASS in a
large, distributed memory system like TACC's Stampede2.
To read this page we recommend you open a second winodw or tab on
your web browser to the current file in the mspass source code
directory called :code:`scripts/tacc_examples/distributed_node.sh`.
The link to the that file you can view on your web browser is
`here <https://github.com/mspass-team/mspass/blob/master/scripts/tacc_examples/distributed_node.sh>`__.
We note there is an additional example there for running MsPASS
on a single node at TACC called :code:`scripts/tacc_examples/single_node.sh`
you can access directly
`here <https://github.com/mspass-team/mspass/blob/master/scripts/tacc_examples/single_node.sh>`__,
The single node setup is useful for testing and may help your understanding
of what is needed by being much simpler.  We do not discuss that
example further here, however, because a primary purpose for using
MsPASS is processing data in a large HPC cluster like TACC.

nxt para needs to say tis is a shelll script and the section below
are grouped by functional issues then list them (singularity, mongodb, and ?)


Workload Manager Setup
-------------------------
It uses a workload manager software installed there called :code:`Slurm`
and the associated command keyword :code:`SBATCH`.   If your
system does not have Slurm there will be something similar
(notably Moab or Torque) that
you will need to substitute.   Perhaps obvious but things like
file system configuration will need changes to match your local environment.

:code:`Slurm` is used as a batch control system to schedule a "batch" job on
a large cluster like Stampede2.  Batch jobs are submitted to be run on
compute notes by submitting a file the command line tool called :code:`sbatch`.
The submitted file is a expected to be a unix shell script that runs
your "batch job".   To be run under :code:`Slurm` the
shell script normally defines a set of run configuration parameters
defined in the first few lines of the script.  Here is a typical examples:

.. code-block:: bash

  #!/bin/bash
  #SBATCH -J mspass          # Job name - change as approrpiate
  #SBATCH -o mspass.o%j      # Name of stdout output file redirection
  #SBATCH -p normal          # Queue (partition) name
  #SBATCH -N 2               # Total # of nodes requested (2 for this example)
  #SBATCH -n 2               # Total # of mpi tasks
  #SBATCH -t 02:00:00        # Run time (hh:mm:ss)

This example requests 2 nodes (-N 2) for a run time of 2 hours (-t line) submitted
to TACC's "normal" queue (-p normal).   Note the :code:`Slurm` configuration parameters
are preceded by the keyword :code:`#SBATCH`.   The lines begin with the "#"
symbol which the unix shell will treat as a comment.   That is done for a
variety of reasons but one important practical one is to test the syntax of a
script on a head node without having to submit the full job.

MsPASS was designed to be run in a container.   For a workstation environment
we assume the container system being used is docker.   Running
MsPASS with docker is described on
`this wiki page <https://github.com/mspass-team/mspass/wiki/Using-MsPASS-with-Docker>`__.
All HPC systems we know have a docker compatible system called
:code:`singularity`.   Singularity can be thought of as docker for a large
HPC cluster.   The most important feature of singularity for you as a user
is that it uses exactly the same container file as docker.  i.e. you "pull" the
docker container and that is used by singularity in a very similar fashion to
the way it used by docker as follows:

.. code-block:: bash

  singularity build mspass.simg docker://wangyinz/mspass

For more about running MsPASS with singularity consult our
wiki page found
`here <https://github.com/mspass-team/mspass/wiki/Using-MsPASS-with-Singularity-(on-HPC)>`__.
Since our examples here were constructed on TACC' Stampede2 you may also
find it useful to read their page on using singularity found
`here <https://containers-at-tacc.readthedocs.io/en/latest/singularity/01.singularity_basics.html>`__

There is a single node mode you may want to run for testing.
You can find an example of how to configure Stampede2 to run on a single
node in the MsPASS scripts/tacc_examples found on github
`here <https://github.com/mspass-team/mspass/tree/master/scripts/tacc_examples>`__.
We focus is manual on configuration for a production run using multiple
nodes, that is a primary purpose of using MsPASS for data processing.
The example we give here is the

There are two ways we could deploy our system on stampede2, which are single node mode and distributed mode.
You could refer those two job script in our /scripts/tacc_examples folder in our source code. Here we would
introduce the common parts and elements in both scripts.

In both modes, we would specify the working directory and the place we store our docker image. That's why
these two lines are in the job scripts:

.. code-block:: bash

  # working directory
  WORK_DIR=$SCRATCH/mspass/single_workdir
  # directory where contains docker image
  MSPASS_CONTAINER=$WORK2/mspass/mspass_latest.sif

The paths for these two variables can be changed according to your case and where you want to store the image.
And it doesn't matter if the directory doesn't exist, the job script would create one if needed.

Then we define the SING_COM variable to simplify the workflow in our job script. On Stampede2 and most of HPC
systems, we use Singularity to manage and run the docker images. There are many options to start a container
using singularity, which you could refer to their documentation. And for those who are not familiar with Singularity,
here is a good `source <https://containers-at-tacc.readthedocs.io/en/latest/singularity/01.singularity_basics.html>`__
to get start with.

.. code-block:: bash

  # command that start the container
  module load tacc-singularity
  SING_COM="singularity run $MSPASS_CONTAINER"

Then we create a login port based on the hostname of our primary compute node we have requested. The
port number is created in a way that guaranteed to be unique and availale on your own machine. After the
execution of your job script, you would get the ouput file, and you could get the url for accessing the
notebook running on your compute node. However, from you own computer, you should use this login port to
access it instead of 8888 which typically is the port we will be using in jupyter notebook because
we reserve the port for transmitting all the data and bits through the reverse tunnel.

.. code-block:: bash

  NODE_HOSTNAME=`hostname -s`
  LOGIN_PORT=`echo $NODE_HOSTNAME | perl -ne 'print (($2+1).$3.$1) if /c\d(\d\d)-(\d)(\d\d)/;'`

Next, we create reverse tunnel port to login nodes and make one tunnel for each login so the user can just
connect to stampede.tacc. The reverse ssh tunnel is a tech trick that could make your own machine connect to
the machines in the private TACC network.

.. code-block:: bash

  for i in `seq 4`; do
    ssh -q -f -g -N -R $LOGIN_PORT:$NODE_HOSTNAME:8888 login$i
  done

For single node mode, the last thing we need to do is to start a container using the command we defined
before:

.. code-block:: bash

  DB_PATH='scratch'
  SINGULARITYENV_MSPASS_DB_PATH=$DB_PATH \
  SINGULARITYENV_MSPASS_WORK_DIR=$WORK_DIR $SING_COM

Here we set the environment variables inside the container using this syntactic sugar SINGULARITYENV_XXX.
For more information, you could view the usage `here <https://sylabs.io/guides/3.0/user-guide/appendix.html>`_.
We define and set different variables in different containers we start because in our start-mspass.sh, we
define different bahavior under different *MSPASS_ROLE* so that for each role, it will execute the bash
script we define in the start-mspass.sh. Though it looks complicated and hard to extend, this is prabably
the best way we could do under stampede2 environment. In above code snippet, we basically start the container
in all-in-one way.

There are more other ways we start a container and it depends on what we need for the deployment. You could
find more in the distributed_node.sh job script. For example, we start a scheduler, a dbmanager and a front-end
jupyter notebook in our primary compute node and start a spark/dask worker and a MongoDB shard replica on each
of our worker nodes. Also you could find that the environment variables needed are different and you could find
the corresponding usage in the start-mspass.sh script in our source code. We hide the implementation detail and
encapsulate it inside the Dockerfile. One more thing here is we specify number of nodes in our sbatch options,
for example 4, stampede2 would reserve 4 compute nodes for us, and we would use 1 node as our primary
compute nodes and 3 nodes as our worker nodes. Therefore, if you need 4 worker nodes, you should sepcify 5
as your sbatch option for nodes.
