.. _parallel_io:

Parallel I/O in MsPASS
======================

*Gary L. Pavlis*

Why parallel I/O matters
------------------------

A familiar high-performance-computing observation is that adding processors
eventually turns a CPU-bound workflow into an I/O-bound workflow.  Every I/O
path has finite throughput: a disk or parallel filesystem, a network link, or
the MongoDB server can become the shared bottleneck.  MsPASS currently deals
with three broad forms of I/O:

* **File I/O.**  Reads and writes are buffered, but the calling task blocks
  until each operation completes.  Parallel workers can overlap independent
  operations, although enough concurrent access will saturate the filesystem.
* **Network and object-storage I/O.**  Throughput and latency depend on both
  endpoints and every network segment between them.  Remote FDSN service calls
  are normally much slower and less predictable than access to data staged near
  the compute workers.
* **MongoDB transactions.**  Every request crosses a client/server boundary.
  Bulk operations and cursor-backed sequential scans amortize that latency far
  better than one-document-at-a-time transactions.

Parallel I/O cannot remove those limits.  It uses multiple workers and bulk
operations to approach the available throughput while the middle of a typical
read-process-write workflow consumes CPU.  More workers do not necessarily
make a job faster; tune worker count, partition size, and storage layout as a
single resource problem.  See :ref:`memory_management` and :ref:`io` for the
related memory and storage tradeoffs.

The two main interfaces are
:py:func:`read_distributed_data
<mspasspy.io.distributed.read_distributed_data>` and
:py:func:`write_distributed_data
<mspasspy.io.distributed.write_distributed_data>`.  Import both from
``mspasspy.io.distributed``.  The reader creates a lazy Dask Bag or Spark RDD;
the writer terminates that graph, saves its contents, and returns small save
identifiers rather than waveform objects.

Reading distributed data
------------------------

Input forms
~~~~~~~~~~~

``read_distributed_data`` accepts three input families:

1. A :py:class:`Database <mspasspy.db.database.Database>` plus an optional
   ``query`` creates atomic data from a waveform collection.
2. A pandas, Dask, or Spark DataFrame plus a separate ``Database`` handle
   creates atomic data.  This supports metadata originating in relational or
   tabular systems, provided its columns satisfy the MsPASS schema.
3. A list of MongoDB query dictionaries plus a ``Database`` handle creates
   ensembles, one ensemble per query.

A raw PyMongo cursor is not a supported first argument.  For Database input,
the function internally scans the selected documents with a cursor and builds
the scheduler container.  By default that intermediate document table is held
in driver memory.  The ``scratchfile`` option can reduce the driver-memory
cost for very large atomic collections; the current Spark scratch-file branch
is incomplete, so use that option with Dask.

Atomic Dask input is normally initialized as follows:

.. code-block:: python

   from mspasspy.client import Client
   from mspasspy.io.distributed import read_distributed_data

   mspass_client = Client(database_name="exampledb", scheduler="dask")
   db = mspass_client.get_database()
   dask_client = mspass_client.get_scheduler()

   data = read_distributed_data(
       db,
       query={"data_tag": "raw"},
       collection="wf_TimeSeries",
       scheduler="dask",
   )

For Spark, supply the context explicitly:

.. code-block:: python

   spark_client = Client(database_name="exampledb", scheduler="spark")
   db = spark_client.get_database()
   spark_context = spark_client.get_scheduler()
   data = read_distributed_data(
       db,
       query={"data_tag": "raw"},
       collection="wf_TimeSeries",
       scheduler="spark",
       spark_context=spark_context,
   )

Creating ``data`` is lazy.  Dask ``compute``, Spark ``collect``, or the
distributed writer triggers waveform construction.  Reader failures produce
dead atomic objects marked with ``is_abortion=True`` so that the workflow can
handle them through the standard :ref:`error model <handling_errors>`.

Ensemble input
~~~~~~~~~~~~~~

Building ensembles with a reduce operation can create large intermediate
groups.  The reader instead accepts a list of queries that directly defines
the groups.  This example creates one TimeSeriesEnsemble per ``source_id``:

.. code-block:: python

   source_ids = db.wf_TimeSeries.distinct("source_id")
   queries = [{"source_id": source_id} for source_id in source_ids]
   ensembles = read_distributed_data(
       queries,
       db,
       collection="wf_TimeSeries",
       scheduler="dask",
   )

Each query is executed by a scheduled task.  The matching documents are read
through :meth:`Database.read_data <mspasspy.db.database.Database.read_data>`
to construct one ensemble.  Choose groups small enough to fit comfortably in
one worker's memory.

When the members of an ensemble use raw binary file storage and share a file,
the Database reader can sort them by ``foff`` and use the compiled sequential
reader.  That preserves buffering and avoids one open/close pair per member.
This optimization depends on a consistent ``dir``/``dfile`` grouping and valid
``foff`` and ``npts`` metadata; it is not available for arbitrary formats or
members scattered among files.

Normalization
~~~~~~~~~~~~~

``normalize`` applies matchers to atomic data or ensemble members, whereas
``normalize_ensemble`` applies matchers to ensemble Metadata.  Matchers are
described in :ref:`normalization`.  A
:py:class:`MiniseedMatcher <mspasspy.db.normalize.MiniseedMatcher>` matches
``net``, ``sta``, ``chan``, ``loc``, and time to cached channel metadata.  It
does not directly process an ensemble object, but the distributed reader
applies entries in ``normalize`` to the members.

For an ensemble, ``container_to_merge`` may provide one Metadata-like object
per query before ensemble normalization.  Its Bag/RDD must align in length and
partitioning with the reader output.  This is powerful but fragile; whenever a
query itself can carry the needed group key, prefer an explicit small mapping
function after reading.

Writing distributed data
------------------------

Example 1: default distributed read and write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``write_distributed_data`` requires both the parallel container and the target
``Database``.  It is a terminal operation: internally it calls Dask ``compute``
or Spark ``collect`` and returns a Python list.  Do not call ``compute`` or
``collect`` after it.

.. code-block:: python

   from mspasspy.algorithms.signals import detrend
   from mspasspy.io.distributed import write_distributed_data

   data = read_distributed_data(
       db,
       collection="wf_TimeSeries",
       scheduler="dask",
   )
   data = data.map(detrend, type="demean")
   saved_ids = write_distributed_data(
       data,
       db,
       data_are_atomic=True,
       collection="wf_TimeSeries",
       data_tag="processed",
       scheduler="dask",
       dask_client=dask_client,
   )

For atomic input, the result contains an ObjectId for each live saved datum and
``None`` for dead entries.  For ensemble input, each element is the list of
member ObjectIds returned for one ensemble.  Consequently ``len(saved_ids)``
is not by itself a count of live waveform documents for ensemble data.

For atomic Dask data, the writer saves sample arrays in map tasks, translates
Metadata to documents, and uses ``map_partitions`` plus MongoDB
``insert_many`` to amortize document transactions.  Spark uses the analogous
``mapPartitions`` path.  Ensemble writes naturally bulk-insert the live member
documents of each ensemble.  Dead data are buried in ``cemetery`` by default;
``cremate=True`` discards them.  Large numbers of bodies can still create
significant database traffic.

The default ``storage_mode="gridfs"`` sends sample arrays through MongoDB.
``storage_mode="file"`` writes sample arrays to conventional storage.  Ensure
that both the MongoDB volume and any sample-data filesystem have enough space
before a large run.  ``overwrite=True`` is supported only with GridFS.

File-storage layout
~~~~~~~~~~~~~~~~~~~

The waveform document records these attributes for file-backed data:

* ``dir`` and ``dfile`` identify the path.  Define them before writing when a
  deliberate layout is required.
* ``foff`` is the byte offset of an atomic datum within a shared file.
* ``npts`` determines raw binary sample length.
* ``nbytes`` records byte length for formatted data where sample count alone
  is insufficient.

With the default double-precision raw format, a TimeSeries occupies
``8 * npts`` sample bytes and a three-component Seismogram occupies
``3 * 8 * npts`` sample bytes, excluding metadata and container overhead.

The writer computes ``foff`` and related size metadata.  If ``dir`` or
``dfile`` is absent it uses safe defaults, including a UUID-based filename,
but this can accidentally create one file per datum.  That layout performs
poorly at scale.  A single file shared by all workers is also usually a
bottleneck.  Prefer project-specific groups—commonly one file per event,
station/time interval, or ensemble—large enough to amortize open/close costs
without concentrating all I/O on one file.

For raw binary ensemble output, defining ``dir`` and ``dfile`` on the ensemble
lets the compiled writer append all live members in contiguous blocks with one
open/close cycle.  The member documents retain the offsets required for later
atomic or optimized ensemble reads.

Set output names with a map function.  Do not reuse input file names for
processed output unless overwriting is genuinely intended and supported.

.. code-block:: python

   from mspasspy.ccore.utility import ErrorSeverity

   def set_output_name(datum):
       datum["dir"] = "wf/example_project"
       if datum.is_defined("source_id"):
           datum["dfile"] = f"source_{datum['source_id']}.dat"
       else:
           datum["dfile"] = "undefined_source.dat"
           datum.elog.log_error(
               "set_output_name",
               "set_output_name: source_id is undefined",
               ErrorSeverity.Complaint,
           )
       return datum

   data = data.map(set_output_name)
   saved_ids = write_distributed_data(
       data,
       db,
       collection="wf_TimeSeries",
       storage_mode="file",
       scheduler="dask",
       dask_client=dask_client,
   )

Example 2: Spark atomic writes to file storage
----------------------------------------------

Spark RDD ``map`` accepts one callable, so bind processing arguments in a
lambda or named function.  The writer does not accept ``spark_context``; only
the reader needs it.

.. code-block:: python

   from obspy import UTCDateTime

   def station_year_path(datum):
       if datum.dead():
           return datum
       codes = [
           datum[key] if datum.is_defined(key) else ""
           for key in ("net", "sta", "chan", "loc")
       ]
       datum["dir"] = "_".join(codes)
       datum["dfile"] = f"{UTCDateTime(datum.t0).year}.dat"
       return datum

   data = read_distributed_data(
       db,
       collection="wf_miniseed",
       scheduler="spark",
       spark_context=spark_context,
   )
   data = data.map(station_year_path)
   saved_ids = write_distributed_data(
       data,
       db,
       data_are_atomic=True,
       collection="wf_TimeSeries",
       storage_mode="file",
       scheduler="spark",
       data_tag="spark_file_example",
   )

Example 3: ensemble normalization and output
--------------------------------------------

This example preserves the common-source workflow while using the current
reader contract.  The queries are arg0 and the Database is arg1.  Source
metadata is applied to each ensemble; MiniSEED channel metadata is applied to
its members.  Output naming is set on ensemble Metadata so all members share
one raw binary file.

.. code-block:: python

   import dask.bag as dbag

   from mspasspy.algorithms.bundle import bundle_seed_data
   from mspasspy.db.normalize import MiniseedMatcher, ObjectIdMatcher

   source_matcher = ObjectIdMatcher(
       db,
       "source",
       attributes_to_load=["lat", "lon", "depth", "time", "_id"],
   )
   channel_matcher = MiniseedMatcher(db)

   source_ids = db.wf_miniseed.distinct("source_id")
   queries = [{"source_id": source_id} for source_id in source_ids]
   number_partitions = 8
   query_metadata = dbag.from_sequence(
       queries,
       npartitions=number_partitions,
   )
   data = read_distributed_data(
       queries,
       db,
       collection="wf_miniseed",
       normalize=[channel_matcher],
       normalize_ensemble=[source_matcher],
       container_to_merge=query_metadata,
       scheduler="dask",
       npartitions=number_partitions,
   )

   def set_ensemble_output(ensemble):
       ensemble["dir"] = "wf/common_source"
       ensemble["dfile"] = f"source_{ensemble['source_id']}.dat"
       return ensemble

   data = data.map(bundle_seed_data)
   data = data.map(set_ensemble_output)
   saved_member_ids = write_distributed_data(
       data,
       db,
       data_are_atomic=False,
       collection="wf_Seismogram",
       storage_mode="file",
       scheduler="dask",
       dask_client=dask_client,
       data_tag="ensemble_example",
   )

``bundle_seed_data`` requires channel orientation metadata.  In a real workflow, inspect
dead members and error logs before relying on this conversion, and verify that
``source_id`` was posted to ensemble Metadata by the selected matcher.

Example 4: intermediate saves
-----------------------------

The distributed writer ends a graph.  For a checkpoint that should remain in
the same lazy graph, map :meth:`Database.save_data
<mspasspy.db.database.Database.save_data>` with ``return_data=True`` so the
saved waveform continues downstream:

.. code-block:: python

   data = read_distributed_data(db, collection="wf_TimeSeries")
   data = data.map(
       db.save_data,
       collection="wf_TimeSeries",
       data_tag="checkpoint",
       return_data=True,
   )
   # Additional map/reduce operations can follow.
   final_ids = write_distributed_data(
       data,
       db,
       collection="wf_TimeSeries",
       data_tag="final",
       dask_client=dask_client,
   )

This serializes a Database handle into worker tasks and performs per-datum
database transactions, so it can be slower and less robust than a clean
checkpoint boundary.  For expensive stages, a safer pattern is to terminate
with ``write_distributed_data`` and start a new graph from a query on the
checkpoint ``data_tag``:

.. code-block:: python

   checkpoint_ids = write_distributed_data(
       data,
       db,
       collection="wf_TimeSeries",
       data_tag="checkpoint",
       dask_client=dask_client,
   )
   data = read_distributed_data(
       db,
       query={"data_tag": "checkpoint"},
       collection="wf_TimeSeries",
   )

Validate a large run by querying its ``data_tag`` and by examining cemetery and
abortion records, not merely by comparing the length of a returned list.  Use a
run-specific tag or query when counting failures so earlier cemetery entries
are not mistaken for failures from the current workflow.
