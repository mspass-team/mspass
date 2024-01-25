.. _parallel_io:

Parallel IO in MsPASS
============================================
*Gary L. Pavlis*
--------------------

What is parallel IO and why is it needed?
--------------------------------------------

There is an old adage in higher performance computing that a supercomputer
is a machine that turns CPU-bound jobs into IO-bound jobs.   Any user of
MsPASS who runs the framework on a sufficiently large number of nodes will
learn the truth of that adage if they throw enough cores at a data
processing problem they need to solve.  Few seismologists have the
background in computing to understand the fundamental reasons why that claim is
true, so I will first briefly review the forms of IO in MsPASS and how they
can limit performance.

Axiom one to understand about IO is that ALL IO operations have a potential
bottleneck defined by the size of the pipe (IO channel) the data are
being pushed/pulled through.   If you are moving the data through a
network channel the highest throughput possible is the number of bytes/s
that channel can handle.   If you are reading/writing data to a local disk
the most data you can push through is speed the disk can sustain for reads/writes.
I next discuss factors in MsPASS that define how MsPASS may or may not saturate
different IO channels.

Like everything in computing IO systems have evolved significantly with time.
At present there are three fundamentally different IO systems MsPASS
handles.
1.  *Regular file IO*.   For decades normal reads and writes to a file system
    are defined by a couple of key abstractions.   The first is *buffering*.
    When you "open" a file in any computing language the language creates a data
    object we can generically call a "file handle" (a "FILE" in C and often
    called a "file object" in python documentation).  Under the hood that file
    handle always includes a "buffer" that is a block of contiguous memory
    used by any reads or writes with that file handle.  A write happens very fast if
    the buffer is not full because the pipe speed is defined by memory bandwidth.
    Similarly, reads are very fast if the data are already loaded into memory.
    That process has been tuned for decades in operating system code to
    make such operations as fast as possible.   Problems happen when you try to
    push more data than the buffer size or try to read a block of data larger
    than the buffer.  Then the speed depends upon how fast the disk controller
    (network file servers for distributed files)
    can move the data from a disk to the buffer.  Similar constraints occur when
    reading more data that can fit in the buffer.  The second concept
    that is important to undertand for
    regular file IO is *blocking*.   All current file IO operations in MsPASS use
    blocking IO, which is the default for standard IO operations in C++ and python.
    There are many online articles on this concept, but the simple idea is that
    when you issue a read or write function call in C++ or python
    (the two primary languages used directly in MsPASS) the program stops
    until the operating system defines the operation as completed.  That delay
    will be small if the data written fit in the buffer or being read are already
    loaded in the buffer.  It not, the program sits in a wait state until
    the operating system says it is ready.
2.  *Network connections*.  Internet IO is a much more complex operation than
    file-based IO.  Complexity also means the variance of performance is wildly
    variable.  There are a least three fundamental reasons for that in the current
    environment.  First, the two ends of a connection are usually completely
    independent.   For example, MsPASS has support for web-service queries
    from FDSN data centers.  Those sources have wildly different performance, but
    they all have one thing in common:  they have glacial speeds relative to
    any HPC cluster IO channel.   Second, long-haul connections are always
    slower than a local connection.   Packets from remote sources often have
    to move through dozens of routers while local internet connections in
    a cluster/cloud system can by many orders of magnitude faster. Finally,
    in every example I know of internet data movement involves far more
    computer software lines to handle the complexity of the operations.
    As a result internet IO operations always consume more CPU cycles than
    simple file-based IO operations.  In MsPASS internet operation are
    currently limited to web-service interaction with FDSN data centers, but
    it is expected that cloud computing support that we are planning to
    develop will involve some form of internet-style IO within the cloud
    service.
3.  *MongoDB transactions*.  In MsPASS we use MongoDB as a database to
    manage data.   MongoDB, like all modern dbms system, uses a
    client-server model for all transactions with the database.
    Without "getting into the weeds", to use a cliche, the key idea
    is that interactions with MongoDB are all done through a manager
    (the MongoDB server).   That is, an application can't get any
    data stored in the database without politely asking the manager (server).
    (Experienced readers will recognize that almost all internet services
    also use some form of this abstraction.)  The manager model
    will always introduce a delay because any operation requires
    multiple IO interactions:  request operation, server acknowledge,
    ready, receive, acknowledge success.   Some of those are not required in some
    operations but the point is a conversation is required between the
    application and the server that always introduces some delay. That
    delay is nearly always a lifetime in CPU cycles.  In our experience a
    typical single, simple MongoDB transaction like `insert_one` takes at
    least of the order of a millisecond.  That may seem fast to you as a human,
    but keep in mind that is about a million clock cycles on a modern CPU.
    On the other hand, "bulk" operations (e.g. `insert_many`) with this model
    often take about the same length of time as a single document
    transaction like `insert_one`.   In addition, MongoDB "cursor"
    objects help the server anticipate the next request and also dramatically
    improves database throughput.  The point is, that appropriate use
    of bulk operators can significantly enhance database throughput.  We
    discuss implementation details of how bulk read/writes
    have been exploited in the parallel readers and
    writers of MsPASS.

.. note::

  At the time this document was written IRIS/Earthscope was in the process of
  developing a fundamental revision of their system to run in a cloud system.
  IO in cloud systems has it's own complexity that we defer for now.
  When that system mature and MsPASS evolves to use it look for updates
  to this section.  Parallel IO and cloud systems have an intimate
  relation, and we anticipate future use of parallel file systems in the
  cloud environment could dramatically improve the performance of some workflows.

With that introduction what then is parallel IO?   Like most modern
computing ideas that one can mean different things in different contexts.
In the context of MsPASS at present it means exploiting the parallel
framework to *reduce* IO bottlenecks.  I emphasize *reduce* because
almost any workflow can become IO bound if you throw enough processors at
it.  Tuning a workflow often means finding the right balance of memory and
number of CPUs needed to get the most throughput possible.  In any case,
the approach used at present is to utilize multiple workers operating
independently.  Since almost all seismic processing jobs boil down to
read, process, and write the results, the issue is how to balance the read
and writes at the start and end of the chain with CPU tasks in the middle.

In the current implementation of MsPASS parallel IO is centered on
two functions defined in the module `mspasspy.io.distributed` called
`read_distributed_data` and `write_distributed_data`.   At present the
best strategy in MsPASS to reduce IO bottlenecks is to use these
two functions as the first and last steps of a parallel workflow.
I now describe details of how these two functions operate and limitations of what
they can do.  I finish this section with examples of how the parallel
readers and writes can be used in a parallel workflow.  Note that to
simplify the API we designed this interface to handle both atomic
and ensemble objects.  The way the two are handled in reads and writes,
however, are drastically different.  Hence, there is a subsection for
the reader and writer descriptions below for atomic (TimeSeries
and Seismogram data) and ensemble (TimeSeriesEnsemble and SeismogramEnsemble data).

read_distributed_data - MsPASS parallel reader
------------------------------------------------
Overview
~~~~~~~~~~~
The `docstring <https://www.mspass.org/python_api/mspasspy.io.html#mspasspy.io.distributed.read_distributed_data>`__
for this function is an important sidebar to this section.   As usual it gives
details about usage, but also discusses a bit of an oddity of this function
I reiterate here.   The design goal to use a common function name to create a
parallel container for all seismic data objects and at the same time
provide the mechanism to parallelize the reader created some anomalies in
the argument structure.  In particular, three arguments to this function
interact to define the input needed to generate a parallel container
when the lazy(spark)/delayed(dask) operations are initiated
(the operation usually initiated by a dask `compute` or pyspark `collect`).
The docstring for the `read_distibuted_data` function gives more details,
but a key point is that
arg0 (`data` in the function definition) can be one of three things:
(1) a :class:`mspasspy.db.database.Database` object, (2) an implementation of
a `Dataframe` (dask, pyspark, or pandas), or (3) a list of python
dictionaries that define valid MongoDB queries.  The first two options
can be used to create a parallel dataset of atomic seismic objects.
Item 3 is the only direct mechanism in MsPASS to create a parallel dataset of
ensembles.  I describe how that works in the two sections below on atomic and
ensemble data.

Atomic data
~~~~~~~~~~~~~
A bag/RDD of atomic data can be constructed by `read_distributed_data`
from one of two possible inputs:  (1) MongoDB documents retrieved through
a :class:`mspasspy.db.database.Database` handle driven by an optional
query, or (2) an implementation of a Dataframe.   It is
important to realize that both cases set the initial
content the bag/RDD as the same thing:  a sequence of python dictionaries
that are assumed to be MongoDB documents with sufficient content
to allow construction of one atomic seismic object from each document.

Forming the initial bag/RDD of documents has different input delay issues for
Dataframe versus Database input. It important to recognize the strengths and
weaknesses of the alternative inputs.
1. *Dataframe*.  Both dask and pyspark have parallel
   implementations for Dataframe.   For either scheduler creating the initial bag/RDD
   amounts to converter methods defined for that scheduler.  Specifically,
   for dask we use the `to_bag` method of their Dataframe and for pyspark
   we run the `to_dict` method to convert the Dataframe to a pyspark RDD.
   Whether or not this input type is overall faster than reading form
   a Database depends upon how you create the Dataframe.  We implemented
   Dataframe as an input option mainly to support import of data indexed
   via a relational database system. In particular, dask and spark both have
   well-polished interfaces for interaction with any SQL server.   In addition,
   although not fully tested at this writing,
   an Antelope "wfdisc" table can be imported into a dask or spark Dataframe
   through standard text file readers. I would warn any reader that the
   the devil is in the details in actually using a
   relational database via this mechanism, but
   prototypes demonstrate that approach is feasible for the framework.
   You should just realize there is not yet any standard solution.

2. *Database*. Creating a bag/RDD of atomic objects
   from a Database is done with a completely different algorithm but
   the algorithm uses a similar intermediate container to build the bag/RDD.
   An important detail we implemented for
   performance is that the process uses a MongoDB cursor to create an
   intermediate (potentially large) list of python dictionaries.   With
   dask that list is converted to a bag with the `from_sequence` method.
   With spark the RDD is created directly from the standard
   `parallelize` method of `SparkContext`.  A key point is that a using a
   cursor to sequentially load the entire data set has a huge impact on
   speed. The same list of data loaded using a MongoDB cursor versus the same
   documents loaded randomly by single document queries differ by many orders of
   magnitude.   The reason is that MongoDB stages (buffers) documents that
   define the cursor sequence.   A sequential read with a cursor is largely
   limited by the throughput of the network connection between a worker
   and the MongoDB server.  On the other hand, that approach is memory
   intensive as the `read_distributed_data` by default will attempt to
   read the entire set of documents into the memory of the scheduler node.
   Most wf documents when loaded are of the order of 100's of bytes.  Hence,
   a million wf document list will require of the order of 0.1 Gbytes, which
   on modern computers is relatively small.   Anticipating the possibility of
   even larger data sets in the future, however, `read_distributed_data` has a
   `scratchfile` option that will first load
   the documents into a scratch file and use an appropriate dask or
   spark file-reader to reduce the memory footprint of creating the
   bag/RDD.

Both input modes create an intermediate bag/RDD equivalent to a large
list of documents.  The function internally contains a map operator that
calls the constructor for either `TimeSeries` or `Seismogram` objects
from the attributes stored in each document.   The output of the
function with atomic data is then an bag/RDD of the atomic data
defined by the specified collection argument.  Note that any
constructor failures in the reader with have the boolean
attribute with the key `is_abortion` set True.  The name is
appropriate since objects that fail on constructor a "unborn".

Ensemble data
~~~~~~~~~~~~~~~
Building a bag/RDD of ensembles is a very different problem than building
a bag/RDD of atomic objects.   The reason is that ensembles are more-or-less
grouped bundles of atomic data.   In earlier versions of MsPASS we
experimented with assembling ensembles with a reduce operator.
That can be done and it works, BUT is subject to a very serious memory
hogging problem as described in the section on
:ref:`memory management<_memory_management>`.  For that reason, we
implemented some complexity in `read_distributed_data` to reduce
the memory footprint of a parallel job using ensembles.

We accomplished that in `read_distributed_data` by using a completely
different model to tell the function that the input is expected to
define an ensemble.  Specifically, the third option for the type of
arg0 (`data` symbol in the function signature) is a list of python
dictionaries.  Each dictionary is ASSUMED to be a valid MongoDB query
that defines the collection of documents that can be used to
construct the group defining a particular ensemble.   Between the oddity of
MongoDB's query language and the abstraction of what an ensemble means
it is probably best to provide an example to clarify what I mean.
The following can be used to create a bag of `TimeSeriesEnsemble`
objects that are a MsPASS version of a "common source/shot gather":

.. code-block:: python

  srcid_list = db.wf_TimeSeries.distinct('source_id')
  querylist=[]
  for srcid in srcid_list:
    querylist.append({'source_id' : srcid})
  data = read_distributed_data(querylist,collection='wf_TimeSeries')
  ... processing code goes here ...

Notice that the for loop creates a list of python dictionaries
that when used with the MongoDB collection find method will
yield a cursor.  Internally the function iterates over that cursor
to load the atomic data to create ensemble container holding
that collection of data.  A weird property of that concept
in this context, however, is that when and where that happens
is controlled by the scheduler.   That is, I reiterate that
`read_enemble_data` only creates the template defining the task the
workflow has to complete to "read" the data and emit a container of,
in this case, `TimeSeriesEnsemble` objects.   That is why this
is a parallel reader because for this workflow the scheduler would
assign each worker a read operation for one ensemble as a task.
Hence, constructing the ensembles, like the atomic case above, is
always done with one each worker initiating a processing chain by
constructing, in the case above, a `TimeSeriesEnsemble` that is passed
down the processing chain.

There is one other important bit of magic in the `read_distributed_data`
that is important to recognize if you need to maximize input speed.
`read_distributed_data` can exploit a feature of
:class:`mspasspy.db.database.Database` that can dramatically reduce
reading time for large ensembles.   When reading ensembles if the
`storage_mode` argument is set to "file", the data were originally
written with the (default) format of "binary", and the file grouping
matches the ensemble (e.g. for the source grouping example above
the sample data are stored in files grouped by source_id.)
there is an optimized algorithm to load the data.   In detail, the
algorithm sorts the inputs by the "foff" attribute and reads the sample
data sequentially with C++ function using the low-level binary
C function `fread`.   That algorithm can be very fast as buffering
creates minimal delays in successive reads and, more importantly,
reduces the number of file open/close pairs compared to
a simpler iterative loop with atomic readers.
See the docstring of :class:`mspasspy.db.database.Database` for details.

write_distributed_data - MsPASS parallel writer
---------------------------------------------------
Overview
~~~~~~~~~~
Parallel writes present a different problem from reading.
The simplest, but not fastest, approach to writing data is to use the
`save_data` method of :class:`mspasspy.db.database.Database` in a loop.
Here is a (not recommended) way to terminate a workflow in that way
for a container of `TimeSeries` objects:

.. code-block:: python

  ...  Processing workflow above with map/reduce operations ...
  data = data.map(db.save_data,storage_mode='file')

Although the save operation will operate in parallel it is has two
hidden inefficiencies that can increase run time.
1.  Every single datum will invoke at least one transaction with the MongoDB
    server to save the wf document created from the Metadata of each
    `TimeSeries` in the container. Worse, if we had used the default
    storage_mode of "gridfs" the sample data for each datum would have to
    be pushed through the same MongoDB server used for storing the wf documents.
2.  Each save is this algorithm requires an open, seek, write, close operation on a particular
    file defined for that datum.

The `write_distributed_data` function was designed to reduce these known
inefficiencies.  For the first, the approach used for both atomic and
enemble data is to do bulk database insertions.   At present the only
mechanism for reducing the impact of item 2 is to utilize ensembles
and store the waveform data in naturally grouped files.  (see examples below)

.. note::

  A possible strategy to improve IO performance with atomic operations
  is to use on of several implementation of parallel files.
  With that model the atomic-level open/close inefficiency could
  potentially be removed.  The MsPASS team has experimented with this
  approach but because there is currently no standardized support
  for that feature. Future releases may add that capability.

An additional issue with saving data stored in a bag/RDD is a memory
issue.   That is, most online examples of using dask or spark terminate
a workflow with a call to the scheduler's method used to convert a
lazy/delayed/futures entity (bag or RDD) into a result.
(The dask function is `compute` and the comparable pyspark function is `collect`.).
Prior to V2 of MsPASS the `save_data` function, if used as above, would return
a copy of the datum is saved.  In working with large data sets we learned
that following such a save with `compute` or `collect` could easily abort
the workflow with a memory fault.   The reason is that the return of `compute`
and `collect` when called on a bag/RDD is an (in memory) python list of
the data in the container.  To reduce the incidence of this problem
beginning in V2 `save_data` was changed to return only the ObjectId of the
saved waveform by default.  For the same reason `write_distributed_data`
does something similar;  it returns a list of the ObjectIds of saved
wf documents.  In fact, that is the only thing it ever returns.
That has a very important corollary that all users must realize;
`write_distributed_data` can ONLY be used as the termination of a
distinct processing sequence.   It cannot appear as the function to be applied in
a map operator.   In fact, user's must recognize that unless it
aborts with a usage exception, `write_distributed_data`
always calls dask bag's `compute` method or pyspark's rdd `collect` method
immediately before returning.   That means that `write_distributed_data`
always initiates any pending delayed/lazy computations defined
earlier in the script for the container.
Here is a typical fragment for atomic data:

.. code-block:: python

  data = read_distributed_data(db,collection='wf_TimeSeries')
  data = data.map(detrend)
  # other processing functions in map operators would typically go here
  wfidslist = write_distributed_data(data,collection='wf_TimeSeries')
  # wfidslist will be a python list of ObjectIds

Saving an intermediate copy of a dataset within a workflow is
currently considered a different problem than that solved by
`write_distributed_data`.  Example 4 in the "Examples" section below
illustrates how to do an intermediate save.

In addition to efficiency concerns, users should also always keep in mind
that before starting a large processing task they should be sure the
target of the save has sufficient storage to hold the processed data.
The target of all saves is controlled at the top level by the
`storage_mode` argument.   There are currently two options.
When using the default of "gridfs" keep in mind the data sample will be stored
in the same file system as the database.   When `storage_mode="file"` is
used the storage target depends upon how the user chooses to set the two
attributes `dir` and `dfile`.  They control the file names where the
sample data will be written.   Below I describe how to set these two
attributes in each datum of a parallel dataset.

Atomic data
~~~~~~~~~~~~~~
Atomic data are handled in three stages by `write_distributed_data`.
These three stages are a pipeline with a bag/RDD entering the top of the
pipeline and a list of ObjectIds flowing out the bottom.

1. The sample data of all live data (The sample data for any datum marked dead
   are always dropped.) are saved.  That operation occurs in a map operator
   so each worker performs this operation independently.   Note the limitation
   that with gridfs storage all that data has to be pushed through the
   MongoDB server.  For file storage an open, seek, write, close operation is
   required for each datum.  If multiple workers attempt to write to the same
   file, file locking can impact throughput.   Note that is not, however,
   at all a recommendation to create one file per datum.  As discussed
   elsewhere that is a very bad idea with large data sets.
2. Documents to be saved are created from the Metadata of each live datum.
   The resulting documents are returned in a map operation to overwrite the
   data in the bag/RDD.
   At the same stage dead data are either "buried" or "cremated".   The
   former can be a bottleneck with large numbers of data marked dead as it
   initiates a transaction with MongoDB to save a skeleton of each dead datum in
   the "cemetery" collection.  If the data are "cremated" no record of them
   will appear in the Database.
3. The documents that now make up the bag/RDD are saved to the Database.
   The speed of that operation is enhanced by using a bulk insert by
   "partition" (bag and RDD both define the idea of a partition.  See the
   appropriate documentation for details.)  That reduces the number of
   transactions with the MongoDB to the order of :math:`N/N_p` where :math:`N` is the
   number of atomic data and :math:`N_p` is the number of partitions defined for
   the bag/RDD.  Said another way, that algorithm reduces the time to save
   the wf documents by approximately a factor of :math:`1/N_p`.

What anyone should conclude from the above is that there are a lot of complexities
in the above that can produce large variances in the performance of a write operation
with `write_distributed_data`.

Ensembles
~~~~~~~~~~~~
Ensembles, in many respects, are simpler to deal with than atomic data.
The grouping that is implicit in the concept of what defines
an ensemble may, if properly exploited, add a level of
homogeneity that can significantly improve write performance relative to the
same quantity of data stored as a bag/RDD of atomic objects.  With
ensembles the bag/RDD is assumed a container full of a common type of
ensemble.   Like the atomic case the algorithm is a pipeline
(set of map operators) with ensembles entering the top and ObjectIds
exiting the pipeline that are returned by the function.   An anomaly
is that with ensembles the return is actually a list of lists of
ids, with one list per ensemble and each list containing
the list of ids saved from that ensemble.  In addition, the pipeline is
streamlined to two stages (task) run through map operators:
1.  Like the atomic case the first thing done is to save the sample data.
    A special feature of the ensemble writer is that if the
    storage_mode argument is set to 'file' and the format is not
    changed from the default ('binary'), the sample data will be written
    in contiguous blocks provided 'dir' and 'dfile' are set in the
    ensemble Metadata container.   In that situation the operation is done with
    a C++ function using fwrite in a mode limited only by the speed of
    the target file system.   As with the comparable feature noted
    above for the reader a further huge advantage this gives is
    reducing the number of file open/close pairs to the number of
    ensembles in the data set.
2.  A second function applied through a map operator does two
    different tasks that are done separately in the atomic algorithm:
    (a) translation of each member's Metadata container to a
    python dictionary that is suitable as a MongoDB document, and
    (b) actually posting the documents to the
    defined collection with the MongoDB interface.
    There are two reasons these are merged in this algorithm.
    The first is that grouping for a bulk insert is natural
    with an ensemble.   The function calls insert_many on the collection
    of documents constructed from live members of the ensemble.
    The second is an efficiency in handling dead data.
    A problem arises because of the fact that
    ensemble members can be killed one of two ways:  (a) they arrive
    at the writer dead, or (b) the conversion from Metadata to
    a document has a flaw that the converter flags as invalid.
    The first is normal.  The second can happen if required Metadata
    attributes are invalid or, more commonly, if the `mode` argument
    is set as "cautious" or "pedantic".   In both cases the contents of
    dead data are, by default, "buried".   Like the atomic case large
    numbers of dead data can create a performance hit as each dead datum
    has a separate funeral (inserting a document in the "cemetery"
    collection).  In contrast, the documents for live data are saved
    with bulk write using the MongoDB `insert_many` collection method
    as noted above.
    With the same reasoning as above this algorithm reduces database transaction
    time for this writer by a factor of approximately :math:`1/N_m`
    here :math:`N_m` is the average number of live ensemble members
    in the data set.



Handling storage_mode=="file"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are major complication in handling any data where the output is
to be stored as a set of files. There are two technical issues
users may need to consider if tuning performance is essential:

1.  Read/write speed of a target file system can vary by orders of
    magnitude.   In most cases speed comes at a cost and the storage space
    available normally varies inversely with IO speed.  Our experience
    is that if multiple storage media are available the fastest
    file system should be used as the target used by MongoDB for
    data storage.   The target of sample data defined by the schema
    you use for `dir` and `dfile` may need to be different to assure
    sufficient free space.
2.  Many seismologists favor the model required by SAC for data storage.
    SAC requires a unique file name for each individual datum.
    (SAC only supports what we call a `TimeSeries`.)
    *For a large data set that is always a horrible model for defining
    the data storage layout.*  There are multiple reasons that
    make that statement a universal truth today, but the reasons are
    beside the point for this manual.   Do a web search if you want to
    know more.  The point is your file definition model should
    never use the one file per atomic datum unless your data set is small.
    On the other hand, the opposite end member of one file for the
    entire data set is an equally bad idea for the present implementation
    of MsPASS.   If all workers are reading or writing to a single
    file you are nearly guaranteed to throttle the workflow from
    contention for a single resource (file locking).  Note we have
    experimented with parallel file containers that may make the
    one file for the dataset model a good one, but that capability is
    not ready for prime time.  For now the general solution is to
    define the granularity in whatever structure makes sense for your
    data.  e.g. if you are working with event data, it usually makes sense
    to organize the files with one file per event.

With those caveats, I now turn to the problem of how you actually
define the file layout of files saved when you set `storage_mode='file'`
when running `write_distributed_data`.

The problem faced in producing a storage layout is that different
research projects typically need a different layout to define some
rational organization.   MsPASS needs to support the range  of options
from a single unique file name for each atomic datum saved to
all the data stored in one and only one file.  As noted above, for
most projects the layout requires some series of logically defined
directories with files at the leaves of the directory tree.
The approach we used utilizes Metadata (MongoDB document) attributes
with key names
borrowed from CSS3.0.  They are:

- `dir` directory name.  This string can define a full or relative path.
- `dfile` file name at the leaf node of a file system path.
- `foff` is the file offset in bytes to the first byte of data for a given datum.  It is essential when multiple data objects are saved in a the same file.  Readers use a "seek" method to initiate read at that position.
- `npts` the number of samples that define the signal for an atomic datum.
  Note that for `TimeSeries` data with default raw output that translates to
  :math:`8 \times npts` bytes and for `Seismogram` objects the
  size is :math:`3\times 8 \times npts`.
- When using a format other than the default of "binary", we use the
  `nbytes` argument to define the total length of binary data to be loaded.
  That is necessary with formatted data because every format has a different
  formula to compute the size.

In writing data to files, the first two attributes (`dir` and `dfile`)
have to be defined for the writer as input.
The others are computed and stored on writing in the document associated
with that datum when the save is successful.  Rarely, if ever, do
you want to read from files and have the writer use the same file to write
the processed result.   That is, in fact, what will happen if you
read from files and then run `write_distributed_data` with
`storage_mode='file'`.  Instead, you need a way to set/reset the values of
`dir` and `dfile` for each datum.
Note that "datum" in this context can
be either each atomic datum or ensemble objects.  The default behavior
for ensembles is to have all ensemble members written to a common file
name defined by the `dir` and `dfile` string defined in the ensemble's
Metadata container.   In either case, the recommended way to set the
`dir` and `dfile` arguments is with a custom function passed through a
map operator.  Perhaps the easiest way to see this is to give an
example that is a variant of that above:

.. code-block:: python

  def set_names(d):
    """
    Examples setting dir and dfile from Metadata attributes assumed
    to have been set earlier.  Example sets a constant dir value
    with file names set by the string representation of source_id.
    """
    dir = 'wf/example_project'   # sets dir the same for all data
    # this makes setting dfile always resolve and not throw an exception
    # elog entry is demontrates good practice in handling such errors.
    if 'source_id' in d:
      dfile = "source_{}.dat".format(str(source_id))
    else:
      dfile="UNDEFINED_source_id_data"
      message = "set_names (WARNING):  source_id value is undefined for this datum\n"
      message += "Data written to default dfile name={}".format(dfile)
      d.elog.log_error(message,ErrorSeverity.Complaint)
    d['dir'] = dir
    d['dfile'] = dfile
    return d
  data = read_distributed_data(db,collection='wf_TimeSeries')
  data = data.map(detrend)
  # other processing functions in map operators would typically go here
  data = data.map(set_names)
  wfidslist = write_distributed_data(data,collection='wf_TimeSeries')
  # wfidslist will be a python list of ObjectIds

A final point for this section is that to make the writer as robust as
possible there is a default behavior to handle the case where
`dir` and/or `dfile` are not defined.  The default for `dir` is
the current (run) directory.  The handling of `dfile` is more elaborate.
We use a "uuid generator" to create a unique string to define dfile.
Although that makes the save robust, be aware this creates the very
case we stated above should never ever be used:  the SAC model with
one file name per datum.

Examples
----------
Example 1:  Default read/write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This example illustrates the simplest example for initiating a workflow
with `read_distributed_data` and terminating it with `write_distributed_data`.
It also illustrates a couple of useful generic tests to verify
things went as expected:

.. code-block:: python

  # Assumes imports and db defined above
  data = read_distributed_data(db,collection='wf_TimeSeries')
  # processing functions in map operators would go here
  #
  # note we don't call computer/collect after write_distributed_data
  # it initates the lazy computations
  wfidslist = write_distributed_data(data,
              collection='wf_TimeSeries',
              data_tag='example1',
              )
  # this will give the maximum number of data possible to compare to nwf
  print("Size of list returned by write_distributed_data=",len(wfidslist))
  # This is the number actually saved
  nwf = db.wf_TimeSeries.count_documents({'data_tag' : 'example1'})
  print("Number of saved wf documents=",nwf)
  # this works only if cemetery was empty at the start of processing
  ndead = db.cemetery.count_documents({})
  print("Number of data killed in this run=",ndead)

Note the reader always reads the data as directed by attributes of
the documents in the `wf_TimeSeries` collection.
The writer defaults to writing data to `gridfs` to the same collection,
but with a `data_tag` used to separate data being written from the
input indexed in the same collection.

Example 2:  atomic writes to file storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This example is a minor variant of the example in the section discussing
how `dir` and `dfile` are used with file IO above.  There are three differences:

1.  It organizes output into directories defined by SEED station code and
    writes file all the files from a given year in different files.
    (e.g. path = "II_AAK_BHZ_00/1998").
2.  The reader access the wf_miniseed collection.   That assures the seed
    station codes should be defined for each datum.
3.  I use the pyspark variant which requires the SparkContext constructs
    see in this example.

.. code-block:: python

  def set_dir_dfile(d):
    """
    Function used to set dir and dfile for example 2.

    dir is set to a net_sta_chan_loc name (e.g. II_AAK_BHZ_00) and
    dfile is set to the year of the start time of each datum.
    Used for make so input d is assumed to be a TimeSeries.
    Important:  assumes the seed codes are set with the
    fixed keys 'net','sta','chan', and 'loc'.   That works in this
    example because example uses miniseed data as an origin.
    Edited copy is returned.  Dead data are returned immediately with no change.
    """
    if d.dead():
      return d
    if d.is_defined('net'):
      net=d['net']
    else:
      net=''
    if d.is_defined('sta'):
      sta=d['sta']
    else:
      sta=''
    if d.is_defined('chan'):
      chan=d['chan']
    else:
      chan=''
    if d.is_defined('loc'):
      loc=d['loc']
    else:
      loc=''
    # notice that if none of the seed codes are defined the directory
    # name is three "_" characters
    dir = net+'_'+sta+'_'+chan+'_'+loc
    d['dir'] = dir
    t0 = d.t0
    year = UTCDateTime(t0).year
    d['dffile'] = str(year)
    return d

  # these are needed to enable spark instead of dask defaults
  import pyspark
  sc = pyspark.SparkContext('local','example2')
  # Assume other imports and definition of db is above
  data = read_distributed_data(db,
            collection='wf_miniseed',
            scheduler='spark',
            spark_context=sc,
            )
  data = data.map(lambda d : set_dir_dfile(d))  # spark syntax
  # processing functions in map operators would go here
  #
  # note we don't call computer/collect after write_distributed_data
  # it initates the lazy computations
  wfidslist = write_distributed_data(data,
              collection='wf_TimeSeries',
              storage_mode='file',
              scheduler='spark',
              spark_context=sc,
              data_tag='example2',
              )
  # These are identical to example 1
  # this will give the maximum number of data possible to compare to nwf
  print("Size of list returned by write_distributed_data=",len(wfidslist))
  # This is the number actually saved
  nwf = db.wf_TimeSeries.count_documents({'data_tag' : 'example2'})
  print("Number of saved wf documents=",nwf)
  # this works only if cemetery was empty at the start of processing
  ndead = db.cemetery.count_documents({})
  print("Number of data killed in this run=",ndead)

Example 3:   Parallel read/write of ensembles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This example illustrates some special considerations needed to handle
ensembles.  Features this illustrate are:

#.  The example reads and forms `TimeSeriesEnsemble` objects grouped by
    the `source_id` attribute.   The algorithm shown will only work if
    a previous workflow has set the `source_id` value in each datum.
    Any datum without `source_id` defined would be dropped from this
    dataset.
#.  We show the full set of options for normalization with ensembles.
    Ensemble normalization is complicated by the fact that there are two
    completely different targets for normalization:  (a) ensemble Metadata, and
    (b) each (atomic) ensemble member.   The reader in this example
    does that at load time driven by the two arguments:
    `normalize_ensemble` and `normalize`.  As the names imply
    `normalize_ensemble` is applied to the ensemble's Metadata
    container while the operators defined in `normalize` are applied in
    a loop over members. This example loads source data in the
    ensemble and channel data into ensemble members.
#.  This example uses an approach that is a complexity
    required as an implementation detail for the parallel reader
    to support normalization by ensemble by the reader.  It uses
    the `container_to_merge` option that provides a generic way
    to merge a consistent bag/RDD of other data into the container
    constructed by `read_distributed_data`.  By "consistent" I
    mean the size and number of partitions in the bag/RDD passed
    with that argument must match that of the container being constucted
    by `read_distributed_data`.  In this case, what using that argument
    does is load a `source_id` value in the ensemble Metadata of each
    component of the `data` container constucted by `read_distibuted_data`.
    The reader has a structure that the algorithm to merge the two
    containers is run before attempting to do any normalization.
    (i.e. any normalization defined by either `normalize` or `normalize_ensemble`.)
#.  The writer uses `storage_mode='files'` and the default "format".   As
    noted above when undefined the format defaults to a raw binary
    write of the sample data to files with the C fwrite function.
    We set `dir` and `dfile` in the ensemble's Metadata container
    that the writer takes as a signal to write all ensemble data in
    the same file defined by the ensemble `dir` and `dfile`.

.. code-block:: python

  def set_dir_dfile(d):
    """
    Function used to set dir and dfile for example 3.

    This example sets dir as a constant and sets the file
    name, which is used by ensemble, with the source_id string
    and a constant suffix of .dat
    """
    if d.dead():
      return d
    dir='wf_example3'
    suffix='.dat'
    # this example can assume source_id is set. Could not get
    # here otherwise
    srcid = d['source_id']
    dfile = str(srcid)
    dfile += suffix
    d['dir']=dir
    d['dfile']=dfile
    return d

  # Assume other imports and definition of db is above
  # This loads a source collection normalizer using a cache method
  # for efficiency.  Note it is used in read_distibuted_data below
  source_matcher = ObjectIdMatcher(db,
                        "source",
                        attributes_to_load=['lat','lon','depth','time','_id'])

  srcid_list = db.wf_miniseed.distinct('source_id')
  querylist=[]
  for srcid in srcid_list:
    querylist.append({'source_id' : srcid})
  source_bag = bag.from_sequence(querylist)
  # This is used to normalize each member datum using miniseed
  # station codes and time
  mseed_matcher = MiniseedMatcher(db)
  data = read_distributed_data(db,
            collection='wf_miniseed',
            normalize=[mseed_matcher],
            normalize_ensemble=[source_matcher],
            container_to_merge=source_bag,
            )
  # algorithms more appropriate for TimeSeries data would be run
  # here with one or more map operators

  # normalization with channel by mseed_matcher allows this
  # fundamenal algorithm to be run.  Converts TimeSeriesEnsemble
  # objects to SeismogramEnsemble objects
  data = data.map(bundle)

  # other processing functions for Seismogram in map operators would go here

  # finally set the dir and dfile fields
  data = data.map(set_dir_dfile)
  # note we don't call computer/collect after write_distributed_data
  # it initates the lazy computations
  wfidslist = write_distributed_data(data,
              collection='wf_Seismogram',
              storage_mode='file',
              scheduler='spark',
              data_tag='example3',
              )
  # These are identical to example 1
  # this will give the maximum number of data possible to compare to nwf
  print("Size of list returned by write_distributed_data=",len(wfidslist))
  # This is the number actually saved
  nwf = db.wf_Seismogram.count_documents({'data_tag' : 'example3'})
  print("Number of saved wf documents=",nwf)
  # this works only if cemetery was empty at the start of processing
  ndead = db.cemetery.count_documents({})
  print("Number of data killed in this run=",ndead)

Example 4: Intermediate processing result save
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
It is sometimes necessary, particularly in a research context,
to have a workflow save an intermediate result.   In the context of
a parallel workflow, that means one needs to do a save
within a sequence of calls to map/reduce operators.
As noted above `write_distributed_data` always is a terminator
for a chain of lazy/delayed calculations.  It always returns
some version of a list of ObjectIds of the saved wf documents.

One approach for an intermediate save is to immediately
follow a call to `write_distributed_data` with a call to
`read_ensemble_data`.   In general that approach, in fact,
is what is most useful in the context.  Often the reason for
an intermediate save is to verify things are working as you
expected.   In that case, you likely will want to
explore the data a bit before moving on anyway.
e.g. I usually structure work with MsPASS into a set of
notebooks were each one ends up with the data set saved in a particular,
often partially processed, state.

An alternative that can be useful for intermediate saves
is illustrated in the following example:

.. code-block:: python

   data = read_distributed_data(db,collection='wf_TimeSeries')
   # set of map/reduce operators would go here
   data = data.map(lambda d : db.save_data(
                                     d,
                                     collection='wf_TimeSeries',
                                     data_tag='save1',
                                     return_data=True,
                                     )
   # more map/reduce operators
   wfids = write_distributed_data(data,
              collection='wf_TimeSeries',
              data_tag='finalsave',
              )

where we used mostly defaults on all the function calls to keep the
example simple.  Rarely would that be the right usage.
A critical feature is the `return_data=True` option send to the
`save_data` method of `Database`.  With that option the method
returns a copy of the atomic datum it received with additions/changes
created by the saving operation.

The approach above is most useful for production workflows where
the only purpose of the intermediate save is as a checkpoint in the
event something fails later in the workflow and you need to the
intermediate case because it was expensive to compute.  As noted
above, it may actually be faster to do the following instead:

.. code-block:: python

   data = read_distributed_data(db,collection='wf_TimeSeries')
   # set of map/reduce operators would go here
   write_distributed_data(data,
              collection='wf_TimeSeries',
              data_tag='save1',
              )
   data = read_distributed_data(db,
              query={'data_tag' : 'save1'},
              collection='wf_TimeSeries')

   # more map/reduce operators
   wfids = write_distributed_data(data,
              collection='wf_TimeSeries',
              data_tag='finalsave',
              )
