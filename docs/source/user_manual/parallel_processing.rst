.. _parallel_processing:

Parallel Processing
===================

Introduction
------------
One of the primary goals of MsPASS is to make seismic-data processing scale
from a desktop to a cluster without requiring a different scientific
algorithm.  MsPASS expresses a workflow as a directed **acyclic** graph (DAG):
each node is a task and each edge represents a dependency or data movement.
The framework supports both Dask and Spark for scheduling these graphs.  Dask
is the default and is also required for the Futures-based workflow described
below.

This model is a natural match for classical seismic processing.  A workflow
passes atomic signals or ensembles through a sequence of processors.  Some
processors preserve the input type, such as a filter; others change it, such
as stacking an ensemble into one signal.  Dask and Spark partition the dataset,
schedule independent tasks concurrently, and move data between those tasks.
This chapter focuses on that algorithm-level parallelism.  Parallelism inside
one processing function is an advanced topic covered by the Dask, Spark,
NumPy, and compiled-library documentation.

Data Model
----------
Both schedulers represent a dataset as one logical container whose full
contents need not fit in one process's memory.  Spark calls its container a
Resilient Distributed Dataset (RDD); Dask calls the comparable collection a
Bag.  A serial loop applies the same function to *N* objects one at a time,
whereas a Bag or RDD lets the scheduler apply that function across partitions
of those *N* objects.

The normal elements of an MsPASS Bag or RDD are
:py:class:`TimeSeries <mspasspy.ccore.seismic.TimeSeries>`,
:py:class:`Seismogram <mspasspy.ccore.seismic.Seismogram>`, or ensembles of
either atomic type.  Before processing, waveform documents and sample storage
should be indexed and validated as described in :ref:`importing_data`.  Any
required ``site``, ``channel``, or ``source`` normalization data should also
be loaded and cross references checked as described in :ref:`normalization`.
The indexed dataset may mix sample data held in GridFS, conventional files,
and supported object-storage layouts because the MsPASS readers use each
waveform document's storage metadata to select the appropriate reader.

:py:func:`read_distributed_data
<mspasspy.io.distributed.read_distributed_data>` currently accepts three
families of input:

1. A :py:class:`Database <mspasspy.db.database.Database>` plus an optional
   ``query`` creates a Bag or RDD of atomic data.
2. A pandas, Dask, or Spark DataFrame creates atomic data and requires a
   separate ``Database`` handle.
3. A list of query dictionaries plus a ``Database`` creates a Bag or RDD of
   ensembles, one ensemble per query.

A raw PyMongo cursor is **not** a supported first argument.  The following is
the normal atomic Dask setup.  :py:class:`Client <mspasspy.client.Client>`
manages the database and scheduler connections, while ``collection`` selects
the waveform collection explicitly.

.. code-block:: python

   from mspasspy.client import Client
   from mspasspy.io.distributed import read_distributed_data

   mspass_client = Client(database_name="exampledb", scheduler="dask")
   db = mspass_client.get_database()
   dask_client = mspass_client.get_scheduler()

   query = {}
   mybag = read_distributed_data(
       db,
       query=query,
       collection="wf_TimeSeries",
       scheduler="dask",
   )

For Spark, construct the client with ``scheduler="spark"`` and pass its
``SparkContext`` to the reader:

.. code-block:: python

   spark_client = Client(database_name="exampledb", scheduler="spark")
   db = spark_client.get_database()
   spark_context = spark_client.get_scheduler()
   myrdd = read_distributed_data(
       db,
       query={},
       collection="wf_TimeSeries",
       scheduler="spark",
       spark_context=spark_context,
   )

Creating either container is lazy: it defines data and tasks but does not by
itself materialize every waveform in the driver process.

Functional Programming Concepts
-------------------------------

Overview
~~~~~~~~
MsPASS uses the functional-programming operations provided by Dask and Spark.
The two central forms are ``map`` and reduction:

.. code-block:: python

   mapped = dataset.map(process_one)

   # Dask Bag: fold is lazy and compute triggers execution.
   dask_result = mapped.fold(combine_two).compute()

   # Spark RDD: reduce is an action and returns the combined value.
   spark_result = mapped.reduce(combine_two)

``map`` transforms each element independently.  Dask ``fold`` and Spark
``reduce`` repeatedly combine pairs until one value remains.  Dask also
provides ``foldby`` for grouped reductions.  See the
:ref:`map-only DAG figure <DAG_figure>` and
:ref:`grouped-reduce figure <Reduce_figure>` in the memory-management chapter
for diagrams of the corresponding data flow.  The
`Dask Bag documentation <https://docs.dask.org/en/stable/bag.html>`__ and
`Spark RDD programming guide
<https://spark.apache.org/docs/latest/rdd-programming-guide.html>`__ describe
the complete operation sets and their lazy-execution models.

The map operator
~~~~~~~~~~~~~~~~
A map operation consumes one dataset element and emits one result.  Input and
output types are often identical, but that is not required.  This Dask example
applies the MsPASS wrapper around ObsPy's band-pass filter:

.. code-block:: python

   from mspasspy.algorithms import signals
   from mspasspy.io.distributed import read_distributed_data

   data_in = read_distributed_data(
       db,
       query={},
       collection="wf_TimeSeries",
       scheduler="dask",
   )
   data_out = data_in.map(
       signals.filter,
       "bandpass",
       freqmin=1.0,
       freqmax=5.0,
       object_history=True,
       alg_id="0",
   )
   computed = data_out.compute()

``computed`` is a Python list held by the calling process, not another Bag.
Collecting an entire large dataset this way can exhaust driver memory.  A
production workflow normally ends with a distributed save rather than
materializing every waveform in the driver.

Spark RDD ``map`` accepts only the mapping callable, so bind the filter
arguments with a lambda or a small named function:

.. code-block:: python

   data_in = read_distributed_data(
       db,
       query={},
       collection="wf_TimeSeries",
       scheduler="spark",
       spark_context=spark_context,
   )
   data_out = data_in.map(
       lambda d: signals.filter(
           d,
           "bandpass",
           freqmin=1.0,
           freqmax=5.0,
           object_history=True,
           alg_id="0",
       )
   )
   computed = data_out.collect()

``collect`` is also a driver-memory operation.  Dask ``compute`` and Spark
``collect`` are actions: they trigger the lazy graph and wait for its result.

Reduce and fold operators
~~~~~~~~~~~~~~~~~~~~~~~~~
Stacking illustrates reduction.  The serial core of an unnormalized sum is:

.. code-block:: python

   from mspasspy.ccore.seismic import TimeSeries

   # ensemble is an existing TimeSeriesEnsemble with at least one member.
   stacked = TimeSeries(ensemble.member[0])
   for member in ensemble.member[1:]:
       stacked += member

MsPASS provides the pairwise
:py:func:`stack <mspasspy.reduce.stack>` function, adapted to the framework by
``mspass_reduce_func_wrapper`` in :mod:`mspasspy.util.decorators`.  Applied to a Dask Bag
or Spark RDD of compatible data, the corresponding reductions are:

.. code-block:: python

   from mspasspy.reduce import stack

   dask_stack = bag.fold(stack).compute()
   spark_stack = rdd.reduce(stack)

A valid reduction function must obey these constraints:

1. Its first two arguments have the same type and represent values to combine.
2. It returns that same type so partial results can be combined again.
3. Its combination is associative and, for scheduler-independent ordering,
   commutative.

The scheduler is free to build and combine partial results in different
orders.  Algorithms that depend on input order are therefore not valid general
reductions.  Grouped operations should use Dask ``foldby`` or an equivalent
keyed Spark operation.  A reduce can consume inputs without returning each
one, so preserve error logs for dead data before reduction as described in
:ref:`handling_errors`.

Futures
-------

Fundamentals
~~~~~~~~~~~~
Python's `concurrent.futures module
<https://docs.python.org/3/library/concurrent.futures.html>`__ provides thread and process
executors on one machine.  Dask Distributed exposes a similar Futures API
whose tasks can run across a cluster.  MsPASS uses Dask Futures for
:py:func:`sliding_window_pipeline <mspasspy.workflow.sliding_window_pipeline>`.
Mixing independent Python and Dask executors is possible, but doing so without
an explicit resource plan can oversubscribe CPUs and memory.

A map-only Bag pipeline might be written as:

.. code-block:: python

   import dask.bag as dbag

   data = dbag.from_sequence(input_list_defining_data)
   data = data.map(reader_function)
   data = data.map(algorithm_a)
   data = data.map(algorithm_b)
   data = data.map(algorithm_c)
   results = data.compute()

The comparable unit submitted as one Future bundles those steps into one
callable:

.. code-block:: python

   def process_one(input_description):
       datum = reader_function(input_description)
       datum = algorithm_a(datum)
       datum = algorithm_b(datum)
       datum = algorithm_c(datum)
       return datum

A direct submission loop looks like this:

.. code-block:: python

   from dask.distributed import as_completed
   from mspasspy.client import Client

   mspass_client = Client(database_name="exampledb", scheduler="dask")
   dask_client = mspass_client.get_scheduler()

   futures = [
       dask_client.submit(process_one, item)
       for item in input_list_defining_data
   ]
   for future in as_completed(futures):
       result = future.result()
       function_to_handle_result(result)

Submitting a very large input list all at once can overload the scheduler and
retain too many results.  The sliding-window helper below bounds the number of
in-flight Futures.

Arguments to ``map`` or ``submit`` are serialized and transferred to workers.
Repeatedly sending a large read-only object, such as a travel-time model or a
normalization matcher, can dominate execution time.  Dask's ``scatter`` method
can publish such an object once and pass Futures to the workers instead.  See
the `Dask data-locality documentation
<https://distributed.dask.org/en/stable/locality.html>`__ before choosing
between normal arguments, ``scatter``, and worker initialization.

MsPASS sliding-window pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :py:func:`sliding_window_pipeline
<mspasspy.workflow.sliding_window_pipeline>` helper keeps only a bounded set of
Dask Futures active.  With ``sliding_window_size="auto"``, the window is
``task_per_worker`` (default 2) times the number of workers reported by the
Dask client.  An explicit integer overrides that calculation.  The bound makes
memory use and scheduler load more predictable when a small document or query
expands into a much larger waveform.

The first argument is described as an iterable, but the current implementation
also calls ``len`` and indexes later elements.  Use a sized, indexable sequence
such as a list or NumPy array rather than a one-pass generator.  Extra
processing arguments belong in ``pfunc_args`` and ``pfunc_kwargs``; the fourth
positional argument is ``sliding_window_size``, not an argument to the
processing function.

The function's docstring says that processing results are returned directly
when ``completion_function`` is omitted.  The current implementation instead
appends outputs only when a completion function is supplied, so omitting one
runs the tasks but normally returns an empty list.  Until that implementation
defect is fixed, supply a completion function when the caller needs results,
and keep those results small for a large run.  This executable example shows
the argument and result contract:

.. code-block:: python

   from mspasspy.workflow import sliding_window_pipeline

   def add_offset(value, offset):
       return value + offset

   def keep_result(value):
       return value

   values = [1, 2, 3]
   shifted = sliding_window_pipeline(
       values,
       add_offset,
       dask_client,
       sliding_window_size=2,
       pfunc_args=[10],
       completion_function=keep_result,
   )

For production waveform processing, the processing or completion function
normally saves each result and returns only a small identifier or status
dictionary.  An optional ``accumulator`` can combine completion results instead
of returning a list.  See :ref:`memory_management` for choosing worker counts,
partition sizes, and a safe number of concurrent tasks.

Schedulers
----------
MsPASS supports Dask, the default, and Spark.  Both implement a scheduler-worker
model, but only Dask provides the Futures interface used by
``sliding_window_pipeline``.  Most current MsPASS examples and production
experience use Dask; Spark remains available for RDD map/reduce workflows.

Schedulers move task arguments and results between processes through
serialization.  Python objects normally use pickle, while MsPASS data objects
provide bindings that make their C++ state serializable.  Custom objects passed
to workers must also be serializable.  Serialization can make one worker task
slower than the equivalent local function call, so tasks should do enough work
to amortize that overhead.

The scheduler must also construct and track the task graph.  Too many tiny
partitions create excessive scheduling overhead; too few large partitions can
exhaust worker memory and reduce parallelism.  The
:ref:`memory-management chapter <memory_management>` develops those tradeoffs.
For scheduler details, start with the
`Dask scheduling documentation <https://docs.dask.org/en/stable/scheduling.html>`__.

Examples
--------

Atomic Dask workflow
~~~~~~~~~~~~~~~~~~~~
The following workflow reads atomic Seismograms, removes their mean, applies a
band-pass filter, windows 300 seconds relative to each datum's start time, and
saves the result.  ``WindowData`` accepts ``t0shift`` directly, so an
``ator``/``rtoa`` round trip is unnecessary.

.. code-block:: python

   from mspasspy.algorithms import signals
   from mspasspy.algorithms.window import WindowData
   from mspasspy.client import Client
   from mspasspy.io.distributed import (
       read_distributed_data,
       write_distributed_data,
   )

   mspass_client = Client(database_name="mydatabase", scheduler="dask")
   db = mspass_client.get_database()
   dask_client = mspass_client.get_scheduler()

   data = read_distributed_data(
       db,
       query={},
       collection="wf_Seismogram",
       scheduler="dask",
   )
   data = data.map(signals.detrend, type="demean")
   data = data.map(
       signals.filter,
       "bandpass",
       freqmin=0.01,
       freqmax=2.0,
   )
   data = data.map(lambda d: WindowData(d, 200.0, 500.0, t0shift=d.t0))

   saved_ids = write_distributed_data(
       data,
       db,
       data_are_atomic=True,
       collection="wf_Seismogram",
       data_tag="atomic_example",
       scheduler="dask",
       dask_client=dask_client,
   )

``write_distributed_data`` triggers the lazy computation and returns identifiers
for live saved data, avoiding a driver-sized list of waveform objects.

Atomic sliding-window workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For a very large list of waveform documents, bundle read, processing, and save
into the submitted function.  ``Client`` registers the database worker plugin
used by ``fetch_dbhandle`` when the function receives only a database name.

.. code-block:: python

   from mspasspy.algorithms import signals
   from mspasspy.algorithms.window import WindowData
   from mspasspy.client import Client
   from mspasspy.util.db_utils import fetch_dbhandle
   from mspasspy.workflow import sliding_window_pipeline

   def process_atomic(document, database_name):
       worker_db = fetch_dbhandle(database_name)
       datum = worker_db.read_data(
           document,
           collection="wf_Seismogram",
       )
       datum = signals.detrend(datum, type="demean")
       datum = signals.filter(
           datum,
           "bandpass",
           freqmin=0.01,
           freqmax=2.0,
       )
       datum = WindowData(datum, 200.0, 500.0, t0shift=datum.t0)
       return worker_db.save_data(
           datum,
           collection="wf_Seismogram",
           data_tag="atomic_sliding_window",
           return_data=False,
       )

   def keep_status(status):
       return status

   mspass_client = Client(database_name="mydatabase", scheduler="dask")
   db = mspass_client.get_database()
   dask_client = mspass_client.get_scheduler()
   documents = list(db.wf_Seismogram.find({}))

   save_status = sliding_window_pipeline(
       documents,
       process_atomic,
       dask_client,
       pfunc_args=[db.name],
       completion_function=keep_status,
   )

The output list contains small dictionaries from ``save_data`` rather than
waveforms.  For an input table too large to hold as a list, construct bounded
batches outside this helper or use ``read_distributed_data`` with its
``scratchfile`` option.

Ensemble Dask workflow
~~~~~~~~~~~~~~~~~~~~~~
For ensembles, pass a list of query dictionaries to
``read_distributed_data``.  This example creates one common-source ensemble
per ``source_id``.  The helper windows every live member relative to that
member's own start time.

.. code-block:: python

   from mspasspy.algorithms import signals
   from mspasspy.algorithms.window import WindowData
   from mspasspy.io.distributed import read_distributed_data

   source_ids = db.wf_Seismogram.distinct("source_id")
   queries = [{"source_id": source_id} for source_id in source_ids]

   def window_members(ensemble):
       for index, datum in enumerate(ensemble.member):
           if datum.live:
               ensemble.member[index] = WindowData(
                   datum,
                   200.0,
                   500.0,
                   t0shift=datum.t0,
               )
       return ensemble

   data = read_distributed_data(
       queries,
       db,
       collection="wf_Seismogram",
       scheduler="dask",
   )
   data = data.map(signals.detrend, type="demean")
   data = data.map(
       signals.filter,
       "bandpass",
       freqmin=0.01,
       freqmax=2.0,
   )
   data = data.map(window_members)
   processed_ensembles = data.compute()

Collecting is shown for clarity.  Save or reduce the Bag instead when the
ensemble list is too large for driver memory.

Ensemble sliding-window workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The same group definition can drive one ensemble per Future.  This version
saves on the worker and returns only save-status dictionaries.

.. code-block:: python

   from mspasspy.algorithms import signals
   from mspasspy.algorithms.window import WindowData
   from mspasspy.client import Client
   from mspasspy.util.db_utils import fetch_dbhandle
   from mspasspy.workflow import sliding_window_pipeline

   def process_ensemble(query, database_name):
       worker_db = fetch_dbhandle(database_name)
       cursor = worker_db.wf_Seismogram.find(query)
       ensemble = worker_db.read_data(
           cursor,
           collection="wf_Seismogram",
       )
       ensemble = signals.detrend(ensemble, type="demean")
       ensemble = signals.filter(
           ensemble,
           "bandpass",
           freqmin=0.01,
           freqmax=2.0,
       )
       for index, datum in enumerate(ensemble.member):
           if datum.live:
               ensemble.member[index] = WindowData(
                   datum,
                   200.0,
                   500.0,
                   t0shift=datum.t0,
               )
       return worker_db.save_data(
           ensemble,
           collection="wf_Seismogram",
           data_tag="ensemble_sliding_window",
           return_data=False,
       )

   def keep_status(status):
       return status

   mspass_client = Client(database_name="mydatabase", scheduler="dask")
   db = mspass_client.get_database()
   dask_client = mspass_client.get_scheduler()
   source_ids = db.wf_Seismogram.distinct("source_id")
   queries = [{"source_id": source_id} for source_id in source_ids]

   save_status = sliding_window_pipeline(
       queries,
       process_ensemble,
       dask_client,
       pfunc_args=[db.name],
       completion_function=keep_status,
   )

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
