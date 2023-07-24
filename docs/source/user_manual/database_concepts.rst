.. _database_concepts:

Database Concepts
========================

NEEDS NEW SECTION ON VERIFY AND CLEAN

NonSQL Database
------------------------

| MsPASS uses a NonSQL database called MongoDB.   NonSQL is a generic
  name today for a database system that does not utilize the Structured
  Query Language (SQL).  SQL is the standard language for interacting
  with relational database systems like Oracle, MySQL, PostGRES, etc.
  One type of NonSQL database is a "document database".  MongoDB is
  classified as a document database.   Readers unfamiliar with the
  concept of a document database are referred to online sources which
  are far better than anything we could produce.   A good starting point
  is the `MongoDB tutorial
  introduction <https://docs.mongodb.com/manual/introduction/>`__.
  Another good source is `this
  one <https://www.tutorialspoint.com/mongodb/index.htm>`__ on
  tutorialspoint.

Schema
------

Overview
~~~~~~~~

| Wikepedia defines a database schema as follow:

  | The term "`schema <https://en.wiktionary.org/wiki/schema>`__"
    refers to the organization of data as a blueprint of how the database
    is constructed (divided into database tables in the case of `relational
    databases <https://en.wikipedia.org/wiki/Relational_databases>`__)
    the schema defines a set of attributes, tables (relations), and how
    they can be linked (joined).
| As this definition states, in a relational database like CSS3.0 the
  schema defines a set of attributes, tables (relations), and how they are
  linked (joined).   MsPASS uses a "nonSQL database", which means the interaction
  is not with Structured Query Language (SQL).   We use a particular
  form of nonSQL database called a "document database" as implemented in
  the open source package `MongoDB <https://www.mongodb.com/>`__.
  The top-level concept for understanding MongoDB is name-value pairs.
  One way of thinking of MongoDB is that it only implements each attribute
  as a name-value pair:  the name is the key that defines the concept and
  the attribute is the thing that defines a particular instance of that
  concept.  The contents can
  be something as simple as an integer number or as elaborate as any python
  object.  Tables (relations) are synonymous with what is called a *collection*
  in MongoDB.

| Although a *collection* is conceptually similar to a table
  it is operationally very different.  In MongoDB a *collection* contains
  one or more *documents*, which play the same role as a single tuple in
  a relational database.  In a relational database an attribute has one
  keyword used to define the content that is visualized as a table with
  a header line defining the attribute name.  A MongoDB document, in contrast,
  effectively has the name tag with each entry.  That is, a MongoDB document is made
  up of a set of name-value pairs.  For readers already familiar with python
  the name-value pairs map exactly into the concept of a python dict.  The
  python API (pymongo), in fact, retrieves documents into
  a python dict.  One critical point about that
  distinction is that a relational database has to define a mechanism to
  flag a particular cell in a table as null.   In a MongoDB document a null
  is defined a true null;   a key-value pair not defined in the document.

| We close this section by noting that a schema is not required by
  MongoDB. As we discussed in detail in :ref:`data_object_design_concepts`
  MsPASS data objects are implemented in C++.   Strong typing in C++
  makes a schema a necessary evil to make the system more robust.
  A schema also provides a necessary central location to define the
  namespace of what kind of content is expected for a particular key.

Design Concepts
~~~~~~~~~~~~~~~~~

A properly designed database schema needs to prioritize the problem it
aims to solve.   The schema for MsPASS was aimed to address the
following design goals:

#. *Efficient flow through Spark and DASK.* A key reason MongoDB was chosen as
   the database engine for MsPASS was that it is cleanly integrated with
   Spark and DASK.   Nonetheless, the design needs to minimize database
   transaction within a workflow.   Our aim was to try to limit database
   transaction to reading input data, saving intermediate results, and
   saving a final result.
#. *KISS (Keep It Simple Stupid).* Experience has shown clearly that
   complex relational schemas like CSS3.0 have many, sometimes subtle,
   issues that confound beginners.  A case in point is that large
   organizations commonly have a team of database managers to maintain
   the integrity of their database and optimize performance.   An
   important objective of our design is to keep it simple so scientists
   do not have to become database managers to work with the system.
#. *Efficient and robust handling of three-component seismograms.*
   Although MsPASS supports :ref:`scalar seismic
   data, <data_object_design_concepts>` our view is that the
   greater need in the community is an efficient system for handling 3C
   data.   In reality, our schema design ended up completely neutral on
   this point; scalar and 3C data are handled identically.  The only
   differences is what attributes (Metadata) are required for each data type.
#. *Provide a clean mechanism to manage static metadata.* MsPASS is a
   system designed to process a "data set", which means the data are
   preassembled, validated, and then passed into a processing chain.
   The first two steps (assembly and validation) are standalone tasks
   that require assembly of waveform data and a heterogenous collection
   of metadata from a range of sources.   Much of that problem has been
   the focus of extensive development work by IRIS and the FDSN.
   Furthermore, obspy already had a well-developed, solid system
   for interaction with FDSN web services.  We saw no reason to
   "reinvent the wheel" and lean heavily on obspy's web service tools
   for assembling data from FDSN sources.  The MsPASS schema for
   receiver metadata can, in fact, be thought of a little more than a
   dialect of StationXML.   Similarly, the MsPASS schema for source
   metadata can be thought of as a dialect of QuakeML.
   Furthermore, because we utilized obspy's web service tools the
   python objects obspy defines for storing source and receiver metadata
   are mostly echoed in the schema.
#. *Extensible.* We know from previous experience that a relational DBMS
   tends to be too rigid in a research environnment,
   and creates barriers to progress.  A primary goal of MsPASS was to
   produce a framework flexible enough to support the rich variety of
   processing needs in seismolog.
   Seismic reflection processing and seismic network catalog
   preparation are two examples of repetitive processing in
   seismology.  In both areas traditional relational database management
   systems have proven merit. A research system needs greater flexility to
   handle unanticipated new ideas and approaches without starting from
   scratch.  A goals was to provide a mechanism for users to extend
   the database with little to no impact on the core system.

| On the other hand, we have explicitly avoided worrying about problems
  we concluded were already solved.  These are:

#. *Catalog preparation.*   At this time a primary job of most
   operational seismic networks of all scales is preparation of a
   catalog of seismic events and linking that information to data used
   to generate the event location and source parameters.  There are
   multiple commercial and government supported systems for solving
   this problem.   We thus treat catalog data as an import problem.
#. *Real time processing*.   Although there are elements of MsPASS that
   are amenable to near real time processing of streaming data, we view
   real time processing as another solved problem outside the scope of
   this system.

Schema in MsPASS
----------------
Overview
~~~~~~~~~
| We reiterate the important concept that in
  MongoDB a *collection* is roughly equivalent to a table (relation)
  in a relational database.  Each collection holds one or more *documents*.
  A single document is roughly equivalent to a tuple in a relational database.
  In this section we describe how we group documents into collections defined
  in MsPASS.   These collections and the attributes they contain are the
  *schema* for MsPASS.  In this section we describe how the schema of MsPASS is
  defined and used to maintain the integrity of a database.
  A useful feature of MsPASS is that the schema is readily
  adaptable.  We defer custom schema definitions to a section in "Advanced
  Topics".

Some Key Concepts
~~~~~~~~~~~~~~~~~~~
ObjectId
:::::::::
MongoDB collections always utilize a unique identifier they call an
:code:`ObjectId` to provide a bombproof, unique identifier for a single document
in a collection.  MongoDB automatically generates one id with the special
name :code:`_id` whenever a new document is added to a collection.   An important
thing to realize is two absolutely identical documents, which can readily
be saved from a python dict or our Metadata container, can be saved to
a collection and they will be treated as different because they will each
get a different :code:`_id` assigned.   That is good or bad depending on the
perspective.  It can be bad in an application where duplicates
create a problem, but we assert that for most data processing it is
a good thing.  We contrast this with the experience we have had with relational
databases where a job can abort on a write because of a duplicate
database key problem.  That never happens with MongoDB, but the flip side
of the coin is it is very easy to unintentionally save pure duplicates.

Because ObjectIds are guaranteed to be unique we use them extensively inside
MsPASS to provide indices and especially as a tool to create cross-references
to common data like station and source Metadata.

ObjectIds are stored in MongoDB as a binary object we normally store in
its raw form using pymongo.  Users should be aware that a human readable
form can be obtain in python by using the str attribute of ObjectId class.  (i.e. if
:code:`myid` is an ObjectId loaded from MongoDB, the readable form is :code:`myid.str`)
For more on ObjectIds the following site is a good introduction_.

.. _introduction: https://www.tutorialspoint.com/mongodb/mongodb_objectid.htm

Normalized Data
::::::::::::::::::

When we started this development we planned to create a purely flat
Metadata space through what MongoDB calls an *embedded data model*.
As we gained experience on the system, however, we realized all seismology
Metadata was better suited to make more use of what MongoDB documentation
calls a *normalized data model*.  The generic concepts these terms
describe can be found here_.

.. _here: https://www.tutorialspoint.com/mongodb/mongodb_data_modeling.htm

At this time there are two sets of Metadata we handle by normalization.
They are familiar concepts to anyone familiar with the relational database
schema CSS3.0 used, for example, in Antelope.  The concepts involved are:

*   *Station (instrument) related Metadata.*   These are described below and actually
    define two collections with the names :code:`site` and :code:`channel`.  The
    distinctions are a bit subtle and better left to the more detailed
    discussion below.
*   *Source related Metadata.*   Any event driven processing needs information
    about seismic sources that are associated with the signals to be
    analyzed.  That data is stored in this collection.

A common feature of all "normalized" collection data is that they define a
subset of data that is are shared by many waveforms.  In that situation it
is more efficient in both storage and database maintenance to keep the
related data together.  Readers familiar with relational systems
understand this same concept as our site, channel, and source collections
are similar to the CSS3.0 site, sitechan, and origin tables respectively.

A key feature of normalized data is that we need a fast index to link the
normalized data to our waveform data.  In all cases we use the ObjectId of
the normalized collection as the standard index.   As noted above all documents in
MongoDB automatically are assigned an ObjectId accessible with key
:code:`_id`.  For all normalized Metadata we use a convention wherein we
store the ObjectId of a related document in another collection using
a composite key name constructed as :code:`collection_id`, where :code:`collection`
is the name of the collection and :code:`_id` is a literal meant to imply
an ObjectId normally accessible through the "_id" key.   For example,
we use :code:`site_id` to refer to documents in the :code:`site` collection.
That means that when :code:`site_id` appears in another collection it is a
reference to the ObjectId (referenced directly with alternate key :code:`_id`
in the site collection) of the related document in the :code:`site` collection.

The major motivation for using the normalized data model for handling
source and receiver metadata is the data involved have two important
properties.   First, since MsPASS was designed as a system for efficiently
handling an assembled data set, the data these collections hold can be treated
as static (immutable) within a workflow.   Waveform data readers must thus do
what is MongoDB's version of a database join between the waveform collection
and one or more of the normalizing collections.   Second, in every case
we know the source and receiver metadata are small compared to any
data set for which one would need to use the parallel processing machinery
of MsPASS.  That means the time to query the normalizing collections is
always expected to be much smaller than the time to query a waveform collection that often
has millions of documents. Although experience showed that expectation was
true, we also found there are situations where embedded database operations
can be a bottleneck in a workflow.   For that reason we developed a set of
normalization classes in python that cache tables of attributes needed for
normalization.
That idea is described in detail in a related section of
this User's manual called  :ref:`Normalization<normalization>`.

Waveform Processing
~~~~~~~~~~~~~~~~~~~~~~~
Concepts
::::::::::

A first-order concept in our database design is that a processing workflows
should driven by one primary collection.  We emphasize that idea by
stating this rule:

  Rule 1:
    Before running any workflow the input waveform collection
    must be populated to define all Metadata required to run the workflow.

That means there is normally a significant *preprocessing* effort
required to prep the dataset.  Existing tools to aid this process are
currently available in the modules found under `mspasspy.preprocessing`.
We stress, however, that preparing data for processing gets increasingly
complicated as the size of a dataset grows as the probability of an
unanticipated data problem increase with the size of a dataset.  Never underestimate the
universal concept of `Murphy's Law <https://www.dictionary.com/browse/murphy-s-law>`__.
Although at this writing the functionality is only planned, an
essential tool is to run a verification tool to validate data before running
a large job.  For the time being user's are encouraged to implement a
validation tool customized to known data issues for their data set.
In particular, users should make use of the `verify` and `clean` methods
that can solve most common metadata problems.

With that background, there are two core collections used to manage waveform data.
They are called :code:`wf_TimeSeries` and :code:`wf_Seismogram`.
These two collection are the primary work areas to assemble a working data set.
In addition, because of SEED data is now a universal standard in seismology
we define the :code:`wf_miniseed` collection.   :code:`wf_miniseed`
documents are similar to :code:`wf_TimeSeries` documents but have
deeply embedded miniseed specific content.  The most notable are the
dogmatic use of station naming codes defined by four standard keys:
"net", "sta", "chan", and "loc".   In contrast, the related
:code:`wf_TimeSeries` collection actively discourages use of station
code keys treating them as normalization attributes.   A simple way
to distinguish the use of :code:`wf_miniseed` versus :code:`wf_TimeSeries`
is that if your workflow is to be initiated from raw, miniseed data
use the :code:`wf_miniseed` collection.  If you save intermediate results
that are :code:`TimeSeries` objects they should be saved in :code:`wf_TimeSeries`.
We would emphasize, however, that saving data to :code:`wf_TimeSeries`
currently requires more storage than comparable miniseed data.   Most
miniseed data is compressed and storage is reduced to approximately one byte
per sample.  :code:`wf_TimeSeries` data are normally stored in the raw
binary form (Done, in fact with the low-level binary fwrite in C.), which
expands the data to 8 bytes per sample.  There is a tradeoff in IO performance
with format.   Miniseed data is slightly slower to read or write because of the
overhead in cracking the complex format.  Raw fread/fwrite, in the other hand,
can be very fast even if the volume is 8 times larger.   As usual with
such issues of extreme performance is needed in your application, produce a
benchmark to evaluate performance on the actual hardware involved.

We elected to keep data describing each of the two atomic data types in MsPASS,
:code:`TimeSeries` and :code:`Seismogram`, in two different collections.  The
main reason we made the decision to create two collections instead of one
is that there are some minor differences in the Metadata that would
create inefficiencies if we mixed the two data types in one place.
If an algorithm needs to have inputs of both TimeSeries and Seismogram
objects (e.g. array deconvolution where a TimeSeries defines the source
wavelet and the data to be deconvolved are Seismogram object) it can still
be handled, but the queries can actually happen faster because they
can be issue against two smaller sets.

The key point about the use of the wf collections is that all serial processing
can be reduced to this pseudocode logic::

  1) Create database handle
  2) Point the handle at wf_Seismogram, wf_TimeSeries, or wf_miniseed as appropriate
  3) Create a MongoDB cursor (find all or issue a query)
  4) foreach x in cursor:
      1i)  Run a sequnce of functions on x
      2i)  Save the result


Parallel jobs are very similar but require creation of an RDD or Dask bag
to drive the processing.  Our parallel api, described
in the section :ref:`Parallel Processing<parallel_processing>`,
simplifies the conversion from a serial to parallel job.  In any case,
the equivalent parallel pseudocode logic is this::

  1) Create database handle
  2) Point the handle at wf_Seismogram, wf_TimeSeries, or wf_miniseed as appropriate
  3) Run the read_distributed_data function to parallelize the input operation
  4) Run parallel version of each processing function
  5) Run write_distributed_data function to save the result with parallel IO

A simple perspective on the difference is that the loop for the serial
job becomes is implied in the parallel job.  Spark or dask schedules which
datum is run through which of a set of parallel jobs.
(see :ref:`Parallel Processing<parallel_processing>` section of ths manual)

Waveform Data Storage
~~~~~~~~~~~~~~~~~~~~~~

Overview
:::::::::::::

All seismogram read operations access one of the wf Collections.
The default behavior is to read all key-value pairs in a single document
and insert most of the attributes into the Metadata for one
TimeSeries or Seismogram objects.  Normalized data can be
loaded automatically if requested and the wf collection has the proper
cross-referencing ids defined.   For more about how to handle
normalization during read see the section titled :ref:`Normalization<normalization>`.

Writers are more complicated because they may have to deal with any
newly generated attributes and potentially fundamental changes in the
nature of the waveform we want to index.  *e.g.*, a stack can become
completely inconsistent with the concept of a station name and may
require creation of a different set of attributes like a point
in space to define what it is.  If the concept matches an existing
schema attribute that existing key should be used.  If not, the user
can and should define their own attribute that will automatically be saved
and defined by the schema.
Note by default save methods are not dogmatic about enforcing
a schema definition.   The main advantage of defining an attribute
in the schema definition is that automatic type enforcement is then
automatic.
If the key is not defined in the wf schema
the automatic type conversions will not be feasible.  Similarly, NEVER EVER
write a new attribute to an datum's Metadata if the key is already defined
in the schema.  Doing so will guarantee downstream problems.  For more
on schema enforcement see the section titled
:ref:`CRUD Operations in MsPASS<_CRUD_operations>`.

Users must also realize that the sample data in Seismogram or TimeSeries objects
can be constructed from :code:`wf` documents in multiple ways.
  #. The sample data
     can be stored in the more conventional method of CSS3.0 based systems
     as external files.   In this case, we use the same construct as CSS3.0 where
     the correct information is defined by three attribures:  :code:`dir`, :code:`dfile`, and
     :code:`foff`.   The default behavior is to save data as
     as native 64 bit floating point numbers.   As noted earlier
     that is the most efficient way
     write the sample data as the :code:`Seismogram.data` array and the :code:`TimeSeries.data`
     vector can then be read and written with the C functions fread and fwrite respectively from
     the raw buffers.  The readers also support a `format` option.
     We use obspy's readers and writers when a format is defined.   All
     formats supported by obspy are supported seamlessly.  Performance depends
     completely on the performance of the obspy reader or writer.
  #. A special case for ensembles is to write all the data for the ensembles
     into a single file.   Similarly, the ensemble reader scans the
     values of `dir` and `dfile` and reads the data in file order to
     reduce the number of file open/close delays.   Using that approach for
     ensembles is known to signficantly improve I/O performance.
  #. The sample data for MsPASS data objects can also be saved
     through a mechanism called :code:`gridfs` in MongoDB.  When this
     method is used the waveform sample data are managed
     by file system like handles inside MongoDB.  This method is the
     default for all writers.  Readers determine how they should
     get the data from the wf collection document used to drive
     the readers.  We discuss strengths and weaknesses of this approach
     relative to file I/O below.
  #. A limitation of gridfs is that the sample data are stored in the same
     disk area where MongoDB stores it's other data.  This can be a
     limitation for system configurations that do not contain a modern
     large virtual file system or any system without a single disk
     file system able to store the entire data set and any completed results.
  #. MsPASS has limited support for reading from a network port via
     a url defined in the wf document.   Most of that code is currently
     incomplete and is expected to be fleshed out when Earthscope
     finalizes plans for their upcoming cloud-based data management.
  #. MsPASS has a prototype reader for cloud storage in AWS based on
     SCEC's implementation.   That system is expected to also evolve as
     Earthscope moves to cloud storage.

gridfs storage
:::::::::::::::
When data are saved to gridfs, MongoDB will automatically create two
collections it uses to maintain the integrity of the data stored there.
They are called :code:`fs.files` and :code:`fs.chunks`.   Any book on MongoDB and
any complete online source will discuss details of gridfs and these
two collections.  A useful example is this tutorial_.

   .. _tutorial: https://www.tutorialspoint.com/mongodb/mongodb_gridfs.htm

You as a user do will not normally need to interact with these collections
directly.   The database readers and writers handle the bookkeeping
for you by maintaining an index in either of the wf collections to
link to the gridfs collections.   Cross-referencing ids and special
attributes are defined in the schema documentation.

The biggest strength of using `gridfs` is simplicity.   That is the main
reason it is currently the default.  A gridfs based write does not
require any definitions for file management.   It does, however, have
some serious drawbacks:

 #. We have found that I/O performance of gridfs is slower than
    most file-based reads and writes.  The reason is the data all flow
    through a common, network channel through the (single instance by default)
    MongoDB server
 #. Large data sets can present problems as the storage is aggregated into
    the same file system as database storage.
 #. Deleting intermediate results at the end of a workflow can be
    awkward.   Data edits are possible only through MongoDB, which can
    be both slow and awkward.   In contrast, it is relatively easy to
    write all intermediate saved data into files in a designated directory.
    The large waveform data files are then easily, and quickly removed
    with standard unix shell tools at the end of the job.  Cleaning out
    wf collection documents for intermediate saves are also easy provided
    you use appropriate data tags.

File storage
:::::::::::::

The main alternative storage model is external files.  We use the same
concepts to manage data in external files as CSS3.0.  Data in file
storage is managed by five attributes:

   #. :code:`dir` a directory path identifier in a file system.  We assume all
      users are familiar with this concept.
   #. :code:`dfile` the "file name" that defines the leaf of the directory (path)
      tree structure.
   #. :code:`foff` is a byte offset to the start of the data of interest.
      Classic earthquake data formats like SAC do not use this concept and
      put only one seismogram in each file.  Multiple objects can be stored
      in a single file using common dir and dfile fields but different foff
      values.
   #. :code:`nbytes` or :code:`npts` are attributes closely related to :code:`foff`.   They
      define the size of the block of data that needs to be read from the
      position of :code:`foff`.  :code:`nbytes` is used by the things like
      the miniseed reader, while raw binary reads always utilize
      :code:`npts` to drive calls to the C function fread.
   #. :code:`format`, when set, defines a format different from native
      binary floating point samples.   As noted earlier any format
      attribute string matching a format supported by obspy should work.
      (Caution:  the authors have only tested the miniseed reader.)
      When the format attribute is not defined the data are assumed to
      be native floating point samples.

Both TimeSeries and Seismograms internally use a data array that is a contiguous
memory block.  As stated above, the default storage mode for external files is a raw
binary memory image saved by writing the memory buffer to the external
file (defined by :code:`dir` and :code:`dfile`) using the low level C fwrite function
that is wrapped in the python standard by the :code:`write` method of
standard file handles described in many tutorials like
`this one<https://docs.python.org/3/tutorial/inputoutput.html>`__.

TimeSeries objects store data as vector of binary "double" values, which for
decades now has implied an 8 byte floating point number stored in the IEEE
format.  (Note historically that was not true.   In the early days of
computers there were major differences in binary representations of
real numbers.   We make an assumption in MsPASS that the machines in the
cluster used for processing have the same architecture and a double is
identical on all machines.)  Similarly, a Seismogram stores data in a
contiguous buffer of memory but the memory block is 3 x :code:`npts` doubles.
The buffer is ordered in what numpy calls FORTRAN order meaning the matrix is
stored with the row index fastest (also called column order).  In any case,
key point is that for efficiency the data for a Seismogram is also normally
read and written using low level binary :code:`read` and :code:`write` methods of the
python file handle class.  Only if a format is defined is a more complex
reader involked.   Be warned that all formats come at a cost and are always
slower than raw fread/fwrite calls for the same array of data.

Finally, we reiterate the point above about how we handle ensemble data.
In general, I/O for ensembles can be significantly faster with file-based
I/O than random readers at the atomic data object level.  The reason
is that when fread and fwrite are used for the time to open and close
the file is comparable if not larger than the read or write time.

Summary
:::::::::

The main idea you as a user will need to understand is that a single
document in one of the wf collections contains all the information
needed to reconstruct the object (the read operation) that is the
same as that saved there previously (the save operation).  The
name-value pairs of each document stored in a wf collection are either
loaded directly as Metadata or used internally to load other Metadata
attributes or to guide readers for the sample data.   Readers
handle which storage model to use automatically.

Writers create documents in a wf collection that allow you to recreate the
saved data with a reader.  The write process has some complexities
a reader does not have to deal with.   That is, writers have more options
to deal with (notably the storage mode) that control their behavior and
have to handle potential inconsistencies created by a processing
workflow.  The :code:`Schema` class (described in more detail below) manages
automatically mapping Metadata to database attributes where possible.
To avoid fatal write errors we emphasize the following as a rule:

   Rule 2:
     Make sure any custom Metadata keys do not match existing schema keys.
     If you change the meaning or data  type stored with that key,
     you can create any range of downstream problems and could abort the
     final save of your results.

The final point about the way MsPASS handles readers and writers is that
we worked hard to abstract the process as much as possible.   A reader
should just work without the user having to be concerned about what is
going on under the hood.  Writing is not always automatic.  To extend the
"under the hood" analogy, you can't drive a car without first starting
the engine and you can't go until you put the car in gear.  In the same
way writers usually need more information to get them moving.

elog
~~~~~~

The error log information is stored in a MongoDB collection called :code:`elog`.
The elog collection holds log messages that should
automatically be posted and saved in a MsPASS workflow.  The elog
collection saves any entries in ErrorLogger objects that are
contain in both Seismogram and TimeSeries objects.   The
main idea of an ErrorLogger is a mechanism to post errors of any level
of severity to the data with which the error is associated, preserve a
record that can be used by the user to debug the problem, and allow
the entire job to run to completion even if the error made the data
invalid.  More details about this idea can be found in the :ref:`Data
Objects <data_object_design_concepts>` section.

A special case is data killed during processing.  Any datum
marked dead will have have no entry in any wf collection.
Instead data killed will, by default, generate single document in
one of two collections with the colorful names `cemetery`
and `abortions`.   We use two different collections to distinguish
to fundamentally different ways data can be killed:
a datum killed during normalprocessing will generate a document in
the `cemetery` collection, while objects that a
never born are stored in the `abortions` collections.  That is, a dead
datum is an abortion only if it was killed before it was fully
constructed by a reader.

Documents found in the `cemetery` collection store the minimal amount
of information needed to identify the body.   They contain
three standard attributes:

#. The `error_log` attribute contains a list of one or more error
   messages that were posted to the killed datum before it was killed.
   Retrieving those messages should define why the datum was killed.
#. The `tombstone` attribute is a subdocument
   (i.e. the type of x=datum["tombstone"] is a python dict.)
   containing the Metdata contents of the killed datum.
#. There is alway an ObjectId that links the docoment
   to the original waveform read by the workflow.  The name will
   be of the form `wf_collection` where "collection" is the
   waveform collection name.  Currently that means one of the
   following:  wf_TimeSeries, wf_Seismogram, or wf_miniseed.

Entries in the `abortions` collection
should always be viewed as a serious error that needs to be
corrected.   Most abortions can be avoided by proper use of the
`verify` method followed by an appropriate `clean` operation as
discussed below.  At present the only way an abortion can be defined
is during a read operation when the document being used to construct
the datum has a serious problem.  The most common "serious problem"
is attributes stored with the wrong type or missing attributes
defined as "required".  The other potential issue is failure of
the section of the reader loading the waveform sample data.
That can be caused by a range of possible read errors with files.
In the future URL-based cloud readers are likely to have their
own set of issues that could generate "abortions".  The contents of
an `abortions` document is the same as the `cemetery` collections
described above for consistency.   They can be distinguished only
by the collection name.

history
~~~~~~~

An important requirement to create a reproducible result from
data is a mechanism to create a full history that can be used to recreate
a workflow.  The same mechanism provides a way for you to know the sequence
of processing algorithms that have been applied with what tunable parameters
to produce results stored in the database.  The history collection stores this
information.   Most users should never need to interact directly with this
collection so we omit any details of the history collection contents from
this manual.  Users should, however, understand the concepts described
in :ref:`Data Object Design Concepts<data_object_design_concepts>`.
A simple description of the content of this collection is that the
history collection contains a dump of the :code:`multimap` container
used in the C++ code base to define the processing history G-tree.


Normalizing collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~

site and channel
::::::::::::::::::

The :code:`site` collection is intended as a largely static table
that can be used to
`normalize <https://docs.mongodb.com/manual/core/data-model-design/>`__
a wf collection.   The name is (intentionally) identical to the CSS3.0
site table.   It's role is similar, but not identical to the CSS3.0
table.  Similarly, :code:`channel` plays the same role as the :code:`sitechan`
table in CSS3.0.  They are similar in the sense that :code:`site` is
used to find the spatial location of a recording instrument.
In the same way :code:`channel` acts like :code:`sitechan` in that it is used
to define the orientation of a particular single channel of seismic
data.   Both collections, however, mix in concepts CSS3.0 stores
in a collection of static tables used for maintaining station metadata.
Antelope users will know these as the collection of tables generated
when `sd2db <https://brtt.com>`__ is run on a SEED file from an FDSN
data center.  We expand on this below, but the following are useful
summaries for Antelope and obspy users:

* Antelope user's should think of the channel collection as nearly identical
  to a join of the  CSS3.0 site and sitechan tables with response data handled
  completely differently through obspy.

* Obspy users can think of both :code:`site` and :code:`sitechan` as a way to
  manage the same information obspy handles with their
  `Inventory <https://docs.obspy.org/packages/autogen/obspy.core.inventory.inventory.Inventory.html>`__
  object.  In fact, channel documents produced from
  `StationXML <https://www.fdsn.org/xml/station/>`__
  files contain an image of an obspy
  `Channel <https://docs.obspy.org/packages/autogen/obspy.core.inventory.channel.Channel.htmlobject>`__
  object saved with pickle.

We emphasize that :code:`site` and :code:`channel` support SEED indexed metadata, but
they do not demand it.  We use the :code:`ObjectId` of documents in both
collections as the primary cross-referencing key.  The :code:`ObjectId` keys are
referenced in collections outside of :code:`site` and :code:`channel`
(i.e. wf_TimeSeries and wf_Seismogram) with the keys :code:`site_id` and :code:`chan_id`
respectively.  :code:`wf_miiseed`, on the other hand, is dogmatic about
requiring SEED station code attributes.  The reason is that miniseed data has
the concept of station codes hard wired into the format even though they
are excess baggage in processing.

Although the :code:`ObjectId` can be thought of as primary keys, we provide
some support for two alternative indexing methods.

 * *SEED net, sta, chan, loc keys*.  Any data obtained from FDSN
   data centers like IRIS-DMC distribute data in the SEED
   (Standard for the Exchange of Earthquake Data) or miniSEED
   format.  MiniSEED data is SEED data with minimal metadata.
   The primary keys SEED uses to define a specfic channel of data are
   three string attributes: (1) a network code referred to as :code:`net` in
   MsPASS, (2) a station code (:code:`sta`), (3) a channel (:code:`chan`), and
   a "location" code (:code:`loc`).   :code:`site` documents extracted from StationXML
   files will always contain :code:`net`, :code:`sta`, and :code:`loc` names while
   :code:`channel` documents add the :code:`chan` attibute.  For documents generated
   from StationXML keys (3 keys for :code:`site` and 4 for :code:`channel`) can
   be properly viewed as alternate keys to locate documents related to a
   particular station (:code:`site`) or channel (:code:`channel`).  With SEED data it
   is important to realize that those keys are frequently not sufficient
   to locate a single document.  All SEED-based data (StationXML) also
   use a pair of time range attributes that we call :code:`starttime` and
   :code:`endtime`.   Both are unix epoch times that define a time span for which
   the associated document's data are valid.   These are used for a whole
   range of practical issues in recording of continuous data, but the
   key point is any query for a unique document in both the :code:`site` and
   :code:`channel` collection require a time stamp that needs to be tested
   against a time range defined by :code:`starttime` and :code:`endtime`.

 *  We also provide some limited support for a form of spatial query.
    The use of a spatial query was a design decision based
    on the author's experiences using CSS3.0's site table as implemented
    in Antelope.   Antelope uses the station name and a time period as a
    key to find location information for a waveform.   That model works
    well for bulletin preparation but creates a dilemma for processed
    waveforms;  the concept of a "station name" is meaningless for many
    types of processed waveform.  Two type examples, are a phased array
    beam and Common Conversion Point (CCP) stacks of receiver functions.
    On the other hand, many such processed waveforms have a space concept
    that needs to be preserved.  Hence, the location information in the
    collection may relate to some more abstract point like  piercing point
    for a CCP stack.   In this mode the :code:`Object_Id` stored as :code:`site_id`
    or :code:`chan_id` is the only index. The difference is geospatial queries
    in MongoDB can be used as an alternate index.  We note that
    geospatial queries can also be used on :code:`site` and :code:`channel` collections
    created with StationXML files too provided the user constructs the
    index with one of MongoDB's Geospatial query constructs.
    There are numerous tutorials today on this topic.  The
    MongoDB documentation can be found
    `here<https://www.mongodb.com/docs/manual/geospatial-queries/>`__.

A spatial query to link anything to a point in the :code:`site` or :code:`channel` collection has
two complexities:  (1) all spatial queries require a uncertainty
specification that are data and implementation dependent, and (2)
sometimes, but not always, a vertical position (site_elev) needs to be
defined.  The first is readily solved with the geospatial indexing
capabilities of MongoDB.   Geospatial queries can define a radius of
uncertainty to efficiently find one or more documents linked to a
circle defined relative to a query point.  The size of such a circle
is always a data dependent choice;  a scientist working with free
oscillations of the earth require station coordinates with minimal
precision, while an active source experiment often requires submeter
location precision.   We treat vertical positions differently.  The
common key to define vertical position is :code:`site_elev` or :code:`chan_elev`.
How to handle
vertical position is application dependent.  *e.g.* to look up the
location of an operational GSN station, it may be necessary to
distinguish borehole and vault instruments that are deployed at many
stations.   In contrast, a point defined by piercing points for a CCP
stack would normally be assumed referenced to a common, fixed depth so
site_elev may not even be needed.  We deal with this complexity by a
defining another rule that user's need to recognize and abide by:

  Rule 3:
    The site and channel collections should only contain metadata relevant to
    the data set.   Used documents are not a problem but waste space.
    Missing metadata is a problem as it will always lead to dropped data.
    Assembly of a working data set usually requires linking documents in :code:`site`
    and/or :code:`channel` to wf_Seismogram documents and channel to wf_TimeSeries
    using keys :code:`site_id` and :code:`chan_id` respectively.

MsPASS has some supported functions to add in building the site and channel
collections and building links to wf collections.   The details are best
obtained from the docstrings for functions in :code:`mspasspy.db.database` and
:code:`mspass.preprocessing.seed` and tutorials on raw data handling.

As noted earlier :code:`site` is a near match in concept to the css3.0 table
with the same name, but :code:`channel` is is more than its closest analog in
css3.0 called sitechan.   The key difference between :code:`channel` and sitechan
is that :code:`channel` contains not just orientation information, but **may**
contain all the metadata needed to define the response characteristics of the
channel to which it is linked.  We stress **may** because for a generic
processing system response information must be optional.   Traditional reflection
processing has, at best, only limited response information (e.g. the
sensor corner frequency is an optional parameter in SEGY) and a large fraction of
processing functions have no need for detailed response data.  In contrast,
some common applications like moment tensor inversions and surface wave dispersion
measurements demand detailed response metadata.   We address this problem
by leaning heavily on the existing infrastructure for handling response data
in obspy.   That is, obspy defines a python class they call :code:`Inventory`.
The :code:`Inventory` class is a complicated data structure that is best thought of,
in fact, as a image of the data structure defined by an FDSN StationXML file.
Embedded in that mess is the response data, but obspy has build a clean
API to obtain the response information from the :code:`Inventory`.   In MsPASS
we handle this problem by storing a pickled image of the :code:`Inventory` object
related to that channel accessible via the key :code:`serialized_inventory`.

Finally, we emphasize that if your final processing workflow requires
metadata in :code:`site` and/or :code:`channel` you should verify
methods for matching in wf_Seismogram and/or wf_TimeSeries resolve.
That means, you must either set the attributes `site_id` or
`channel_id` in each document.
If you are working with raw miniseed data indexed with documents in
wf_miniseed, you must either set the values of :code:`channel_id`
with the function :code:`normalize_mseed` or plan to use the inline
normalization function called :code:`normalize` in combination with
a preconstructed instance of :code:`MiniseedMatcher`.
(see section titled :ref:`Normalization<normalization>`)
Any incomplete
entries will be dropped in final processing.  Conversely, if your workflow
does not require any receiver related Metadata (rare), these collections
do not need to be dealt with at all.

source
::::::::

The source collection has much in common with site, but
has two fundamental differences:  (1) the origin time of each source
needs to be specified, and (2) multiple estimates are frequently
available for the same source.

The origin time issue is a more multifaceted problem than it might at
first appear.  The first is that MongoDB, like ArcGIS, is map-centric
and stock geospatial queries lack a depth attribute, let alone a time
variable.   Hence, associating a waveform to a source position defined
in terms of hypocenter coordinates (:code:`latitude`, :code:`longitude`,
:code:`depth`, and :code:`time` attributes in :code:`source`) requires a multistage query that can
potentially be very slow for a large data set.

The other issue that distinguishes origin time is that it's accuracy
is data dependent.   With earthquakes it is always estimated by an
earthquake location algorithm, while with active source it normally
measured directly.  The complexity with active source data is a
classic case distinguishing "precision" from "accuracy".   Active
source times relative to the start time of a seismogram may be very
precise but not accurate.  A type example is multichannel data where
time 0 of each seismogram is defined by the shot time, but the
absolute time linked to that shot may be poorly constrained.   We
address this problem in MsPASS through the concept of UTC versus
"Relative" time definined in all MsPASS data objects.  See the :ref:`Data
Object section <data_object_design_concepts>` on BasicTimeSeries
for more on this topic.

A final point about the source collection is the issue of multiple
estimates of the same event.   The CSS3.0 schema has an elaborate mechanism
for dealing with this issue involving three closely related tables
(relations):  event, origin, assoc, and arrival.   The approach we
take in MsPASS is to treat that issue as somebody else's problem.
Thus, for the same reason as above we state rule 4 which is very
similar to rule 3:

  Rule 4:
    The source collection should contain any useful source
    positions that define locations in space and time (attributes
    :code:`source_lat`, :code:`source_lon`, :code:`source_depth`, and :code:`source_time`).  Linking
    each document in a wf collection to the desired point in the source
    collection is a preprocessing step to define a valid dataset
    when required.
    The link should normally be done by inserting the :code:`ObjectId` of
    the appropriate document in :code:`source` as in wf_Seismogram or
    wf_TimeSeries with the key :code:`source_id`.

A first-order limitation this imposes on MsPASS is that it means that
normal behavior should be that there is a one-to-one mapping of a single
:code:`source` document to a given wf document as defined by the :code:`source_id` key.
Note MongoDB is flexible enough that it would be possible to support
multiple event location estimates for each wf document but that is not
a feature we have elected to support.  As noted elsewhere, we consider the
catalog preparation problem a solved problem with multiple solutions best
done by those systems.

A final point about :code:`source` is that we emphasize normalizing :code:`source`
by defining :code:`source_id` values in wf collections should always be thought of
as an (optional) preprocessing step.   On the other hand,
if your workflow requires source
information, you must complete the association of records in source to
wf_Seismogram and/or wf_TimeSeries documents before your primary processing.
Any entries not associated will be dropped if required.

Database command line tools
-----------------------------

dbverify
~~~~~~~~~
This command line tool should be run on any database before starting
a long-running workflow.  Usage line is the following:

.. code-block:: bash

    dbverify dbname [-t testname] [-c col ... ]
          [-n n1 ...] [-r k1 ...][-e n] [-v] [-h | --help]

`dbverify` runs one of set of possible tests (defined by the -t parameter)
on one or more collections defined via the -c parameter.  The -n and
-r options are required for some tests as described below.


At present the "testname" associated with the -t flag must be one of
the following:  **normalization**, **required**, or **schema_check**.
These tests are (see also the docstring for the program):

#. **normalization**.   This test can be used to validate cross-references
   to standard normalizing collection done via ObjectIds.  When running
   this test the -n argument is required.  The arguments following -n
   should be normalizing collection names to be checked.
   The standard name supported in MsPASS are `channel`, `site`, and `source`
   but the test does not distinguish these special names.  That is, for
   the collection defined by an argument like "-n site" each wf collection document is
   tested for a key with the name `site_id`.   If it is missing it prints
   an error message saying so.  If found it then checks that id associated
   with that key is found in the normalizing collection (in this case  `site`).
   If the id is not found it prints a different message.
#. **required**.  Certain attributes are often required for a workflow to
   execute successfully.   Use this to validate that all documents in a wf
   collection have those keys.  This requires the -r option with a list of
   keys to be checked for existence.  Any documents lacking an keys defined
   will generate a print message.
#. **schema_check**.  This test is designed mainly to check for any attributes
   that are not consistent with the schema defined for the database.
   That mainly useful to identify any type mismatches in values associated
   with one or more keys and missing attributes the schema defines as required.

Running all the tests described above on a database is strongly
recommended before running any large job.   Few things are more annoying
that waiting a week to run your job and finding it aborted immediately
because of a database problem that would have been anticipated by
running this command line tool.  Note the -e option has a default that
limits the number of messages logged because often if one document has a
problem all of them do.  If -e didn't limit output running `dbverify` on
a collection with a few million documents could generate a very large log file.

dbclean
~~~~~~~~
The purpose of the `dbclean` tool is to fix most if not all problems detected
by `dbverify`.  It's usage line is:

.. code-block:: bash

    dbclean dbname collection [-ft] [-d k1 ...] [-r kold:knew ... ] [-v] [-h]

where "dbname" is the name of the database and "collection" is the
collection within that database that the fixes are to be applied.
The problems that `dbclean` currently can fix are:

#.  The `-ft` flag enables type fixing.  That is, it checks the type for
    each attribute defined in the schema and attempts to correct where
    possible (e.g. converting integers to an attribute expected to be a float.).
    The program while print an error if it encounters an attribute that
    cannot be converted (e.g. some strings cannot be converted to numbers).
#.  The `-d` flag tells the program to delete all attributes from the
    list of keys that follow the -d argument.
#.  The `-r` flag is mnemonic for "rename".  It is expected to be followed by
    pairs of string key names separated by a ":".  The left string is
    expected to be the old value and the right the key that is to be used
    as a replacement.


normalize_mseed
~~~~~~~~~~~~~~~~~~
Because miniseed format data is currently the standard way to distribute
earthquake data we supply this special command-line tool to
efficiently create the cross-referencing id's between `wf_miniseed`
and the `channel` collection.   An important feature of this tool
that it can do this operation much faster than a naive use of
record-by-record matching in a python loop driven by a cursor.
It uses what MongoDB calls a "buld update" to stage multiple
updates submitted in blocks to the server.  The record of our design
work with this tool is on github and shows the approach used here
can be orders of magnitude faster than the naive algorithm.

The usage line is:

.. code-block:: bash

  python normalize_mseed.py dbname [--normalize_site --blocksize n -wfquery querystring]

where dbname is the MongoDB name of the database to be normalized.
Default normalizes only "channel" writing a `channel_id` entry for each
`wf_miniseed` record for which a match was found  Use the
`-normalize_site` to also create `site_id` entries at the same time.
If the workflow will ultimately use `Seismogram` objects that option
is strongly recommended.

`-blocksize` is used to change the default number of updates pushed to
the MongoDB server per call.  The default should normally be fine.

Finally, `-wfquery` can be used to only run the tool on a subset of the
documents in `wf_miniseed`.


Summary
-------

The details above may obscure a few critical points about what the
database in MsPASS does for you and what you must keep in mind to use
is correctly.

*  All parallel workflows should normally be driven by data assembled into
   the wf_miniseed, wf_TimeSeries, and/or wf_Seismogram collections.  Subsets (or all) of
   one of these collections define a parallel dataset that is the
   required input for any parallel job.
*  The Database API simplifies reading and writing.
   We abstract the always complex process of reading and writing
   the :code:`save_data` and
   :code:`read_data` methods of the python class Database.  See the reference manual
   for details.
*  Assembling one or more of the wf collections should
   always be viewed as a preprocessing step to build a clean dataset.  That
   model is essential for efficiency because all the complexity of real
   data problems cannot be anticipated and are best treated as a special
   problem you as a user are responsible for solving.
*  Assembling the metadata stored in :code:`site`, :code:`channel`, and/or :code:`source`
   is also always treated as a preprocessing problem.   Linking of these
   normalized collections to wf_Seismogram and/or wf_TimeSeries is
   required if the associated metadata is needed in your workflow.
   Linking has many complications and is discussed further in the section
   titled :ref:`Normalization<normalization>`.
*  Use the command tools described above to aid in identifying potential
   database problems and fixing them.   
