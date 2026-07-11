.. _io:

I/O in MsPASS
========================================

Overview
-----------------------
Input/output (I/O) performance can easily be the main throttle
on the speed of a workflow in MsPASS.   The reason is a corollary of an
old saying about the definition of a "supercomputer".  The old saying is
that a supercomputer is a computer that turns all compute-bound jobs into
I/O-bound jobs.   The scalability of MsPASS, which means it runs the
same with one or a thousand workers (just faster), means the more processors
you throw at a job the more likely it will be that it becomes I/O-bound.
For that reason the focus of this section is the parallel readers.
However, most of the key concepts are equally applicable to serial
jobs.

I/O in a modern computer system is highly abstracted.  It is one of
the earliest examples of how computer languages abstract a complex
process into a simple command.   The earliest version of FORTRAN had
the READ and WRITE commands that are a type example.
IO has become increasingly complex as computers have evolved.
We thus first give a brief overview of key concepts that impose
limits of IO performance on different kinds of hardware.   That
discussion could extend to book length, but we review only the basics
at a high level that we hope can help users understand what factors
might limit the performance of their processing job.  We then review the
API for parallel processing and discuss a few practical guidelines for
avoiding IO bottlenecks.

.. note::

  Storage and data-service architectures evolve quickly.  EarthScope is
  actively documenting its `cloud migration <https://docs.earthscope.org/>`__,
  so verify service-specific performance guidance before designing a new
  large workflow.

IO Concepts
---------------
*Concept 1:  IO is transfer of data to and from main memory*.  The
most basic thing to recall and understand is that any time a computer
algorithm does IO it means data has to be transferred from something to
the computer's main memory (RAM).  That process can take from a few clock
cycles to millions (or many orders of magnitude more) of clock cycles depending on
what the "something" is.

*Concept 2:  Buffering*.  For more than 50 years all but a tiny fraction
of computer processing jobs used buffered IO.
In buffered IO a program does not read directly from any device but always
reads from a memory buffer.  Other elements of the system manage the flow of
data in and out of the buffer.  All MsPASS IO uses stock, buffered IO
functions.

*Concept 3:  Blocking*.   When an I/O statement is executed in a programming language,
including Python, the normal behavior is for the program to be put in a wait state
until the code that does the actual operation completes.   That waiting is
what is meant by *blocking IO*.   With normal blocking IO your program will
always wait until the I/O operation returns before continuing execution.
Individual MsPASS reads and writes are blocking.  A Dask or Spark workflow can
still overlap many calls by running tasks concurrently.

*Concept 4:  communication channels*.  Today most I/O is abstracted as
taking place via a communication channel that can be manipulated by
an appropriate API.   That can be explicit like the FILE* handle in C
or more abstract like the generic read handle in ObsPy's
`read <https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html>`__ function.
In all cases the idea is the handle provides a mechanism to
move data to/from some external location from/to the memory space of your program.

*Concept 5:   open/close*.  No IO is possible unless the interface that
defines a communication channel is initialized and made valid.   That
is traditionally done through some form of "open".  Whatever the syntax,
an "open" means creation of a handle that defines a particular instance of
a communication channel.   A "close" destroys the connection and releases
the resources allocated to a create a communication channel.

With the above it may be helpful to summarize the generic sequence of
steps that happen when reading the data for a seismic data object,
which is really just a generic read.

#.  Some version of "open" is used to create the handle to fetch the data.
#.  One or more distinct "read" operations are requested through the
    handle.   Each distinct read will block until the operation is completed.
    How long of a delay that represents in your program is dependent on
    a long list of variables.   With files reads may or may not also involve calls to
    "seek" to tell the handle to locate a specific piece of a file.
    That operation can be very fast if nothing is required (For example, rewind a file
    that is already positioned to the beginning.), but also may be slow
    as a new buffer has to be loaded with the section of the file
    containing the section requested.
#.  The handle is closed and destroyed when no longer needed by the algorithm.

Writes are similar, except data is pushed to a buffer from your program
instead of loaded into program memory space as directed by your program.
(Note some programs read and write to the same communication channel
but there is currently no such example in MsPASS except communications
with the MongoDB server, which is the next item)
For both reads and writes delays are inevitable in all three of the
above generic steps.

*Concept 6:  database server communications*.   Interaction with MongoDB
is a special case of IO in MsPASS.   Database communication is
notorious for creating performance problems if handled
carelessly for two reasons:  (1)  Each interaction (also sometimes
called a transaction) with the MongoDB server creates an action/reaction delay.
For reads that means the caller sends a request to the MongoDB server
(itself an IO operation), the server has to process the request, and then
return the result to the caller (a second IO operation).  In a write the
data to be saved is packaged and sent to the server, handled by the server,
and a response acknowledging success (or failure) is returned.  All
pymongo read/write calls block until the circuit above completes creating
built in delays.   (2) the communication channel for MongoDB is
always a TCP/IP network connection.   The speed of that communication
relative to disk transfer speeds
can be an issue on some systems.  A larger issue, however, is that when multiple
processes are pushing large amounts of data the pipe can fill and limit
throughput.  A type example of this is gridfs read/write discussed below.

The above are all generic ideas, and are only a part of the complexity
of modern IO systems.   They are sufficient, however, to summarize the
factors that cause IO bottlenecks on most seismic processing jobs, in general,
and MsPASS in particular.  In the next section we describe how the
current versions of the parallel readers and writers of MsPASS are
designed to improve IO performance on clusters.  We also appeal to the
concepts above in the final section that discusses known bottlenecks in
IO performance to avoid.


Parallel IO in MsPASS
-------------------------
We use parallel containers in MsPASS to provide a mechanism to handle
data sets that cannot fit in cluster memory.  As stated numerous times
in this User's Manual we use what is called a "bag" in Dask and
"RDD" (Resilient Distributed Dataset) in PySpark for this purpose.
To be truly parallel creation of a bag/RDD must be parallelized.
Similarly, saving the result of a computation defined as one of them
should also be parallelized.  If they were not parallelized the
IO stage(s) would nearly always be the throttle controlling the
run time.  In this section we discuss our current implementation
to help the user understand current limitations of that processing.

Read
~~~~~~~
For both Dask and PySpark the parallel read function is
:py:func:`read_distributed_data <mspasspy.io.distributed.read_distributed_data>`.
The docstring and :ref:`CRUD operations <CRUD_operations>`
section of this manual have more details on
the nuts-and-bolts of running this function, but from the perspective of
IO performance it may be helpful to discuss how that function
is parallelized for different options of arg0, which defines the primary
input method to the function.

#.  *DataFrame input*.   When the input is a DataFrame each worker
    is assigned a partition of the received DataFrame to read.
    Tasks read DataFrame partitions on the workers assigned by the scheduler.
    Each task works through the rows that
    define the partition to output a bag/RDD of data objects with the same partitioning
    as the DataFrame.   The scheduler assigns additional partitions to
    workers as they complete each one as usual.
    Note normally the output bag/RDD never exists
    as a complete entity but defines only an intermediary stage of
    the data passed down a pipeline to other processing algorithms.
    Note that the database is accessed in this mode only if
    normalization during reading is requested
    (see :ref:`Normalization <normalization>`) or if any data
    are stored with GridFS.
#.  *Database input*.  Database input is, in many respects, a variant of
    DataFrame input with an added serial processing delay.  That is,
    when the input is a Database handle it applies an optional
    query (default is the entire wf collection specified)
    and then attempts to load all wf documents.   To allow for very large
    data set where the documents alone would overflow the available
    memory space the function has a :code:`scratchfile` option
    that stages the results to a scratch file before creating the
    bag/RDD.   Users should recognize that a large delay is built into this
    process from the time required to construct the intermediate DataFrame
    of documents, particularly if a scratch file is used.  Once the
    bag/RDD of documents is created the reader parallelization works
    the same as for a DataFrame with minor differences that have little
    to no impact on performance.
#.  *List of queries*.  Parallel ensemble reads are driven by a list of
    MongoDB queries.  Since PyMongo uses a Python dict as the query
    format, that means the function is driven by a list of Python dict
    containers.  The read operation in that situation is more elaborate,
    although not necessarily more time consuming, than the atomic,
    parallel, read algorithms described above.   A bag/RDD is created
    from the list of dict
    containers and partitioned as usual.   The scheduler normally assigns
    partitions sequentially to workers as with atomic data.
    The parallelism is based on one query per list element.  The scheduler
    assigns those elements to workers by partition.  For each bag component
    the algorithm first issues
    a query to MongoDB using that dict content.
    The query is then used to drive the
    :py:meth:`mspasspy.db.database.Database.read_data` method with
    a cursor input.   That creates a new ensemble object that is passed
    into the processing pipeline.  This algorithm has no serial
    steps, but can be bottlenecked if a large number of workers are
    defined that often run queries simultaneously.  Otherwise performance
    is normally limited by the speed of the IO operations embedded in
    the document definitions for the ensemble members.  As with atomic
    operations, when GridFS storage is used throughput can be limited by the
    MongoDB service and its network path.

Write
~~~~~~~~~~
As with reading, parallel writes in MsPASS all should go through
a common function called
:py:func:`write_distributed_data <mspasspy.io.distributed.write_distributed_data>`.
The function creates write tasks from the bag/RDD partitions and the
scheduler assigns those tasks to available workers.  The way data are handled by writers,
however, is more complex.

The first detail to understand about the parallel writer is it splits
up the task into two distinctly different operations:  (1)  saving
the sample data and (2) saving the Metadata as set of MongoDB documents.
Each instance of the writer handles these two operations in that order.
The first calls the private ``Database._save_sample_data`` method.
The speed of that operation is completely controlled by the "storage_mode"
chosen and the communication path the choice implies.

Saving the Metadata for each seismic object is different as the
performance is mainly limited by MongoDB and the way the writers
create the wf collection documents.   Atomic level insertions
(i.e. one wf document at a time) is
the absolute slowest way to handle this operation as each insert
(a "collection" method of MongoDB)
blocks until the server responded that it has completed the
operation.  To reduce such delays
:py:func:`write_distributed_data <mspasspy.io.distributed.write_distributed_data>`
always tries to insert multiple documents in each interaction with
the MongoDB server.   The method is different for atomic data and
ensembles:

#.  For atomic data the algorithm uses Dask ``map_partitions`` or Spark
    ``mapPartitions``.  A partition normally contains multiple data objects.
    The writer then uses MongoDB ``insert_many`` for the waveform documents
    extracted from that partition.  This amortizes database round trips over
    the partition instead of issuing one ``insert_one`` call per object.
#.  For ensembles the ensemble itself provides a logical grouping.
    That is, instead of using :code:`insert_many` by partition we
    use the set of documents extracted from each ensemble member.
    This similarly amortizes document insertion over the ensemble members.

A final detail about writing is the handling of "dead" data.
The overall behavior is controlled by a boolean argument to
:py:func:`write_distributed_data <mspasspy.io.distributed.write_distributed_data>`.
called ``cremate``.  When true, dead data are cremated using the
:py:meth:`cremate<mspasspy.util.Undertaker.Undertaker.cremate>` method of
the :py:class:`Undertaker<mspasspy.util.Undertaker.Undertaker>` class.
As the imagery of the name implies little to nothing is left of dead
data that are cremated.  The default for ``cremate`` is False.
In that situation dead data are "buried", which means a record of
why they were killed is saved in a special collection called
:code:`cemetery`.
See the section
:ref:`CRUD operations <CRUD_operations>` for more about these
concepts.  For this discussion a key point is that lots of dead data
with the default setting of :code:`cremate=False` can impose a
bottleneck because a call to :code:`insert_one` will happen with
each dead datum and each writer instance will block until that
transaction is completed.

Known IO Bottlenecks to avoid
---------------------------------
Excessive file open/close
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
At this time most workflows in MsPASS are driven by data stored on
a sequence of files.   e.g. a common starting point is a collection of
miniseed files downloaded from one or more FDSN data centers.
A key point to recognize is that a file open or close is not a zero
cost operation.   On modern systems the time is generally of the order of
milliseconds, but keep in mind a millisecond today is typically a
million cpu clock cycles.  The time spent doing an open/close is
always wasted wall time when your job is likely not using all available CPU cycles.
A common legacy layout in seismology uses one SAC file per datum.  The time
to open and read a single SAC
file is irrelevant when you are working interactively because a millisecond
is not a perceptible delay to a human, but for the cpu of your computer
it is substantial.  For large data sets, avoid one-file-per-datum layouts
unless measurement on the target storage system shows they are acceptable.

The easiest way to avoid excessive open/close in processing is to
structure your algorithm into ensembles (gathers in reflection-processing jargon)
whenever possible.  Both the miniseed and native binary readers for
ensembles use an internal algorithm to minimize the number of file
open and close calls.   They do that by basically loading all data
from common files, as defined by the `dir` and `dfile` attributes
stored in the database, as a group when assembling an ensemble.
For workflows driven by atomic data there is currently no way to avoid
an open/close for each read.

Lustre file-systems and many small files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The SAC model of one file per datum will really get you in trouble if
you try to process a large dataset with many workers on a large HPC cluster.
The fundamental reason is that HPC clusters are (currently at least)
tuned to handling large simulation jobs.  Most simulation programs
read small values of initial input data and then run a huge calculation
for long periods of time.   That means the systems are not tuned to
handle large numbers of file open/close transactions.

At the time of this writing the type example of this issue is when
a job has to interact with a `Lustre file system <https://www.lustre.org/>`__.
Lustre is a heavily
used package today that HPC clusters use to set up huge virtual
file systems.  Lustre is a parallel file system that allows
many processes to be doing reads or writes simultaneously.
A problem arises because Lustre handles two concepts we noted above
fundamentally differently: (1)  open/close functions are implemented
via a database server while (2) reads and writes are implemented through
a "mover" that handles moving data to and from memory to the
designated storage location on a large disk array.  What that means is
that open/close tends to be relatively slow while reads and writes to
an open file are about as fast as technologically possible today.
There is also a state dependency in file opens.   Lustre keeps a cache
of files it knows about so it is faster to reopen the same file shortly
after closing it than to open a completely different file.

The SAC model is particularly problematic for large data set if they
are staged to a Lustre file system.  Consider, for example, a relatively
modest data set today of around a million waveforms.  Suppose we staged
that data to a Lustre file system.  First, you will find just copying it
there and handling it will be a problem because those commands also have
to do lots of file open/close operations.  If you then tried to run a
workflow based on atomic data each element of the bag/RDD that would
be needed to initiate a parallel workflow will require an open, read,
and close sequence.   If every file has a unique file name that means
a database transaction with the Lustre "Metadata server" is required for
each datum.  There are known examples (not by us, but similar legacy codes)
where workloads with very high metadata-operation rates have overloaded a
Lustre metadata server.  Measure and limit that load on shared systems.

We know of three solutions to address this issue at present with
different tradeoffs.

#.  Use the ensemble strategy noted above to structure data into
    logical groupings to reduce the number of open/close operations.
    If there is no scientific grouping, consider using ensembles
    defined by distinct files.
#.  If your workflow absolutely has to be done with atomic data
    you should still aim to assemble data into larger files that
    are logical for your data.  As a minimum that will reduce
    the load on the Lustre metadata server.  Although untested, we
    suspect a parallel job using atomic data performance can be improved
    by sorting the list of documents retrieved from MongoDB by the
    combined key of dir::dfile.   Lustre will cache files it knows about
    after an initial open so secondary opens of the same file
    may be faster.
#.  Use GridFS when its MongoDB and network tradeoffs are preferable to
    metadata pressure on the shared file system.

.. note::
  Developments are in progress to improve IO performance on HPC
  and cloud systems using newer software systems that address some of these
  issues.  The above list of workarounds may change if those developments
  are successful.

MongoDB data path (GridFS)
~~~~~~~~~~~~~~~~~~~~~~~~~~
One MsPASS storage option is MongoDB's GridFS system.
GridFS separates file metadata from file chunks.
That is, the equivalent of a file name is handled differently
than the container that holds the actual data.  The first is held in a
(normally smaller) collection called "fs.files" and the second in a
collection called "fs.chunks".

With GridFS, waveform samples pass through the MongoDB deployment and its
network path.  PyMongo uses a connection pool rather than literally one
connection for an entire job, but server capacity or aggregate network
bandwidth can still become the bottleneck as worker concurrency grows.

MongoDB `sharding <https://www.mongodb.com/docs/manual/sharding/>`__ can
distribute data and request load across multiple shard servers.  It also adds
substantial operational and shard-key complexity.  Benchmark GridFS on the
target deployment rather than assuming that sharding or file storage will
always be faster.

Web-service URL reads
~~~~~~~~~~~~~~~~~~~~~~~~
Waveforms can be downloaded from FDSN data centers through standardized web
services such as EarthScope's
`FDSN dataselect service <https://service.earthscope.org/fdsnws/dataselect/>`__.
MsPASS has a capability of defining a waveform segment with URL,
which makes it possible to define an entire data set
as residing remotely and accessible through the web service interface.
A very large workflow should not issue one remote request per datum without
careful rate control and retry handling:

#.  Remote service latency and provider rate limits can throttle every worker.

#.  Individual requests can fail transiently, so a workflow must retry and
    verify completeness.

For conventional clusters, stage large acquisitions to suitable local or
shared storage, validate completeness, and perform QC before large-scale
analysis.  On cloud systems, use the access method recommended by the data
provider and benchmark it for the intended concurrency.

.. note::

  EarthScope's cloud services are evolving.  Consult the current
  `EarthScope cloud documentation <https://docs.earthscope.org/>`__ rather
  than assuming that an FDSN URL and a cloud-native object have the same
  performance characteristics.

Using a slow disk for your database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
MongoDB workloads commonly include small, random I/O operations, so the
database's storage latency can strongly affect a workflow.  SSD or NVMe
storage often helps that access pattern even when bulk waveform files must
reside on a larger shared array.  Storage tiers, caches, and network paths
vary widely; benchmark the database and waveform locations on the target
system instead of assuming capacity or media type alone predicts throughput.
