.. _database_concepts:

Database Concepts
========================

NonSQL Database
------------------------

| MsPASS uses a NonSQL database called MongoDB.   NonSQL is a generic
  name today for a database system that does not utilize the structure
  query language (SQL).  SQL is the standard language for interacting
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

Wikepedia defines a database schema as follow:

| The term "`schema <https://en.wiktionary.org/wiki/schema>`__"
  refers to the organization of data as a blueprint of how the database
  is constructed (divided into database tables in the case of `relational
  databases <https://en.wikipedia.org/wiki/Relational_databases>`__)
  the schema defines a set of attributes, tables (relations), and how
  they can be linked (joined).

As this definition states, in a relational database like CSS3.0 the
schema defines a set of attributes, tables (relations), and a how they are
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
Although a *collection* is conceptually similar to a table
it is operationally very different.  In MongoDB a *collection* contains
one or more *documents*, which play the same role as a single tuple in
a relational database.  In a relational database an attribute has one
keyword used to define the content that is visualized as a table with
a header line defining the attribute name.  A MongoDB document, in contrast,
effectively has the name tag with each entry as a MongoDB document is made
up of a set of name-value pairs.  For readers already familiar with python
the name-value pairs map exactly into the concept of a python dict.  The
python API (pymongo), in fact, retrieves documents into a data structure
that behaves exactly like a python dict.  One critical point about that
distinction is that a relational database has to define a mechanism to
flag a particular cell in a table as null.   In a MongoDB document a null
is defined a true null;   a key-value pair not defined in the document.

We close this section by noting that a schema is not required by
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
#. *Extensible.* A DBMS cannot be too rigid in a research environnment,
   or it will create barriers to progress.  This is especially important to MsPASS as our
   objective is to produce a system for seismic research, not a
   production system for repetitive processing of the similar data.
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
for can be obtain in python by using the str attribute of ObjectId class.  (i.e. if
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

At this time there are three sets of Metadata we handle by normalization.
They are familiar concepts to anyone familiar with the relational database
schema CSS3.0 used, for example, in Antelope.  The concepts involved are:

*   *Station (instrument) related Metadata.*   These are described below and actually
    define two collections with the names :code:`site` and :code:`channel`.  The
    distinctions are a bit subtle and better left to the more detailed
    discussion below.
*   *Source related Metadata.*   Any event driven processing needs information
    about seismic sources that are aassociated with the signals to be
    analyzed.  That data is stored in this collection.

A common feature of all "normalized" collection data is that they define a
subset of data that is are shared by many waveforms.  In that situation it
is more efficient in both storage and database maintenance to keep the
related data together.  Readers familiar with relational systems
understand this same concept as our site, channel, and source collections
are similar to the CSS3.0 site, sitechan, and origin tables respectively.

A key feature of normalized data is we need a fast index to link the
normalized data to our waveform data.  In all cases we use the ObjectId of
the normalized collection as the index.   As noted above all documents in
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
handling an assembled data set, the data these collections can be treated
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
normalization.   That idea is described below in the section :ref:`NEW SUBSECTION TO BE ADDE DTO THIS FILE`

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
a large job.

With that background, there are two collections used to manage waveform data.
They are called :code:`wf_TimeSeries` and :code:`wf_Seismogram`.
These two collection are the primary work areas to assemble a working data set.
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
  2) Point the handle at wf_Seismogram or wf_TimeSeries as appropriate
  3) Create a MongoDB cursor (find all or issue a query)
  4) foreach x in cursor:
      1i)  Run a sequnce of functions on x
      2i)  Save the result


Parallel jobs are very similar but require creation of an RDD or Dask bag
to drive the processing.  Our parallel api, described elsewhere (LINK)
simplifies the conversion from a serial to parallel job.  In any case,
the equivalent parallel pseudocode logic is this::

  1) Create database handle
  2) Point the handle at wf_Seismogram or wf_TimeSeries as appropriate
  3) Run the Database.read_distributed_data method to build parallel dataset
  4) Run parallel version of each processing function
  5) Run Database.save_distributed_data method

A simple perspective on the difference is that the loop for the serial
job becomes is implied in the parallel job.  Spark schedules which
datum is run through which of a set of parallel jobs.

Waveform Data Storage
~~~~~~~~~~~~~~~~~~~~~~

Overview
:::::::::::::

All seismogram read operations access one of the wf Collections.
The default behavior is to read all key-value pairs in a single document
and insert most of the attributes into the Metadata for one
TimeSeries or Seismogram objects.  Normalized data (see above) are
loaded automatically by default.

Writers are more complicated because they may have to deal with any
newly generated attributes and potentially fundamental changes in the
nature of the waveform we want to index.  *e.g.*, a stack can become
completely inconsistent with the concept of a station name and may
require creation of a different set of attributes like a point
in space to define what it is.  If the concept matches an existing
schema attribute that existing key should be used.  If not, the user
can and should define their own attribute that will automatically be saved.
The only limitation is that if the key is not defined in the wf schema
the automatic type conversions will not be feasible.  Similarly, NEVER EVER
write a new attribute to an datum's Metadata if the key is already defined
in the schema.  Doing so will guarantee downstream problems.

Users must also realize that the sample data in Seismogram or TimeSeries objects
can be constructed from :code:`wf` documents in one of two ways.  First, the sample data
can be stored in the more conventional method of CSS3.0 based systems
as external files.   In this case, we use the same construct as CSS3.0 where
the correct information is defined by three attribures:  :code:`dir`, :code:`dfile`, and
:code:`foff`.   Unlike CSS3.0 MsPASS currently requires external file data to be
stored as native 64 bit floating point numbers.   We force that restriction
for efficiency as the :code:`Seismogram.data` array and the :code:`TimeSeries.data`
vector can then be read and written with fread and fwrite respectively from
the raw buffers.  The alternative (second) method for storing sample data
in MsPASS is through a mechanism called :code:`gridfs` in MongoDB.  When this
method is used the waveform sample data are managed
by file system like handles inside MongoDB.  That process is largely hidden
from the user, but there are two important things to recognize about
these two models for data storage:

  #.  The :code:`gridfs` method is expected to be superior to file storage for
      large clusters because it facilitates parallel io operations.  With
      files two processes can collide trying access a common file, especially
      with a writer.
  #.  A limitation of gridfs is that the sample data are stored in the same
      disk area where MongoDB stores it's other data.  This can be a
      limitation for system configurations that do not contain a modern
      large virtual file system or any system without a single disk
      file system able to store the entire data set and any completed results.

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

File storage
:::::::::::::

The alternative storage model is external files.  We use the same
concepts to manage data in external files as CSS3.0.  Data in file
storage is managed by four attributes:

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
      position of :code:`foff`.

Both TimeSeries and Seismograms use a data array that is a contiguous
memory block.  The default storage mode for external files is a raw
binary memory image saved by writing the memory buffer to the external
file (defined by :code:`dir` and :code:`dfile`) using the low level C fwrite function
that is wrapped in the python standard by the :code:`write` method of
standard file handles described in many tutorials like this one_.

   .. _one: https://docs.python.org/3/tutorial/inputoutput.html).

TimeSeries stores data as vector of binary "double" values, which for
decades now has implied an 8 byte floating point number stored in the IEEE
format.  (Note historically that was not true.   In the early days of
computers there were major differences in binary representations of
real numbers.   We make an assumption in MsPASS that the machines in the
cluster used for processing have the same architecture and a doubles are
idenitical on all machines.)  Similarly, a Seismogram stores data in a
contiguous buffer of memory but the memory block is 3 x :code:`npts` doubles.
The buffer is order in what numpy calls FORTRAN order meaning the matrix is
stored with the row index fastest (also called column order).  In any case,
key point is that for efficiency the data for a Seismogram is also
read and written using low level binary :code:`read` and :code:`write` methods of the
python file handle class.

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
     If change the meaning or data  type stored with that key,
     you can create any range of downstream problems and could abort the
     final save of your results.

elog
~~~~~~

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

A special case is data killed during processing.   Any datum from a MsPASS
processing module that was killed should contain an elog entry that the
level :code:`Invalid`.   The sample data in killed Seismogram or TimeSeries data
is not guaranteed to be valid, and may, in fact, be empty.   Hence, killed
data have to be handled specially.   All elog entries from such data will
be preserved in this collection.   In addition, the document for killed
data will contain a dict container with the key "metadata".   That dict is
an recasting of the Metadata of the datum that was killed.  It is neeed,
in general, to sort out what specific datum to which the error was attached.
The documents in elog for live data contain an :code:`ObjectId` that is a link back
to the wf collection where that waveform was saved.

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
in - link to new document in this manual on ProcessingHistory concepts --

TODO:  Leaving this section for now.  Needs to use the figure used in
our AGU poster.  Main reason to punt for now is to needs to include a
clear description of how the global and object level history interact.
Global is under development at this writing.


Normalized collections
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
  to the CSS3.0 sitechan table with response data handled through obspy.

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
respectively.

Although those :code:`ObjectId` can be thought of as primary keys, we provide
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
    index with (NEEDS A REFERERENCE HERE - We need a database method for this)

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
    Assembly of a working data set requires linking documents in :code:`site`
    and/or :code:`channel` to wf_Seismogram documents and channel to wf_TimeSeries
    using keys :code:`site_id` and :code:`chan_id` respectively.

MsPASS has some supported functions to add in building the site and channel
collections and building links to wf collections.   The details are best
obtained from the docstrings for functions in :code:`mspasspy.db.database` and
:code:`mspass.preprocessing.seed` and tutorials on raw data handling.

As noted earlier :code:`site` is a near match in concept to the css3.0 table
with the same name, but :code:`channel` is is more than its closes analog in
css3.0 called sitechan.   The key difference between :code:`channel` and sitechan
is that :code:`channel` contains not just orientation information, but **may**
contain all the metadata needed to define the response characteristics of the
channel to which it is linked.  We stress **may** because for a generic
processing system response information must be optional.   Traditional reflection
processing has, at best, only limited response information (e.g. the
sensor corner frequency is an optional parameter in SEGY) and a large fraction of
processing functions have not need for detailed response data.  In contrast,
some common applications like moment tensor inversions and surface wave dispersion
measurements demand detailed response metadata.   We address this problem
by leaning heavily on the existing infrastructure for handling response data
in obspy.   That is, obspy defines a python class they call :code:`Inventory`.
The :code:`Inventory` class is a complicated data structure that is best thought of,
in fact, as a image of the data structure defined by an FDSN StationXML file.
Embedded in that mess is the response data, but obspy has build a clean
api to obtain the response information from the :code:`Inventory`.   In MsPASS
we handle this problem by storing a pickle image of the :code:`Inventory` object
related to that channel.   (TO DO:   our current implementation may not
be correct on this point.  see discussion)

Finally, we emphasize that if your final processing workflow requires
metadata in :code:`site` and/or :code:`channel` you must complete preprocessing to
define linking ids in wf_Seismogram and/or wf_TimeSeries.  Any incomplete
entries will be dropped in final processing.  Conversely, if your workflow
does not require any receiver related Metadata (rare), these collections
do not need to be dealt with at all.

source
::::::::

The source collection has much in common with site, but
has two fundamental differences:  (1) the origin time of each source
needs to be specified, and (2) multiple estimates are frequently
available for the same source.

The origin time issue is a more multifaceted problem that it might at
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

A final point about the source table is the issue of multiple
estimates of the same event.   The CSS3.0 has an elaborate mechanism
for dealing with this issue involving three closely related tables
(relations):  event, origin, assoc, and arrival.   The approach we
take in MsPASS is to treat that issue as somebody else's problem.
Thus, for the same reason as above we state rule 3 which is very
similar to rule 2:

  Rule 4:
    The source collection should contain any useful source
    positions that define locations in space and time (attributes
    :code:`source_lat`, :code:`source_lon`, :code:`source_depth`, and :code:`source_time`).  Linking
    each document in a wf collection to the desired point in the source
    collection is a preprocessing step to define a valid dataset.
    The link should always be done with by inserting the :code:`ObjectId` of
    the appropriate document in :code:`source` as in wf_Seismogram or
    wf_TimeSeries with the key :code:`source_id`.

A first-order limitation this imposes on MsPASS is that it means that
normal behavior should be that there is a one-to-one mapping of a single
:code:`source` document to a given wf document as defined by the :code:`source_id` key.
Note MongoDB is flexible enough that it would be possible to support
multiple event location estimates for each wf document but that is not
a feature we have elected to support.  As noted other places we consider the
catalog preparation problem a solved problem with multiple solutions.

A final point about :code:`source` is that we emphasize normalizing :code:`source`
by defining :code:`source_id` values in wf collections should always be thought of
as an (optional) preprocessing step.   If your workflow requires source
information, you must complete the association of records in source to
wf_Seismogram and/or wf_TimeSeries documents before your primary processing.
Any entries not associated will be dropped.


history_object and history_global
:::::::::::::::::::::::::::::::::::::::::
An important requirement to create a reproducible result from
data is a mechanism to create a full history that can be used to recreate
a workflow.  The same mechanism provides a way for you to know the sequence
of processing algorithms that have been applied with what tunable parameters
to produce results stored in the database.  The history collections stores this
information.   Most users should never need to interact directly with this
collection so we defer any details of how these are stored and managed to
the reference manual.   The assumption you as a reader need to understand is
that the default behavior of all MsPASS modules is to not preserve history.
The idea is that when you need to retain that information you would rerun
the workflow with history saving enabled for each processing step.
Examples where this might be needed are preparing a final dataset to link to
a publication or as an archive you expect to need to work with at a later date.

Summary
-------

The details above may obscure a few critical points about what the
database in MsPASS does for you and what you must keep in mind to use
is correctly.

*  All parallel workflows should normally be driven by data assembled into
   the wf_TimeSeries and/or wf_Seismogram collections.  Subsets (or all) of
   one of these collections define a parallel dataset that is the
   required input for any parallel job.
*  The Database api simplifies the processing of reading and writing.
   We abstract the always complex process of reading and writing to :code:`save` and
   :code:`read` methods of the python class Database.  See the reference manual
   for details.
*  Assembling the wf_Seismogram and/or wf_TimeSeries collection should
   always be viewed as a preprocessing step to build a clean dataset.  That
   model is essential for efficiency because all the complexity of real
   data problems cannot be anticipated and are best treated as a special
   problem you as a user are responsible for solving.
*  Assembling the metadata stored in :code:`site`, :code:`channel`, and/or :code:`source`
   is also always treated as a preprocessing problem.   Linking of these
   normalized collections to wf_Seismogram and/or wf_TimeSeries is
   required if the associated metadata is needed in your workflow.

Preprocessing/Import collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Overview
:::::::::

We use the MongoDB database to manage waveform data import.  Waveform data
import should always be understood as another component of preprocessing
needed to assemble a working data set.   The reason we are dogmatic on that
principle is that our TimeSeries and Seismogram containers were designed to
be totally generic, while every single data format we know of has
implicit assumptions about the nature of the data.   For example,
has intrinsic assumptions the data are multichannel, seismic-reflection data and
SEED was designed for archive of permanent observatory data.
We discuss import methods currently available in MsPASS in separate
sections below.

SEED and MiniSEED
:::::::::::::::::::

The Standard for the Exchange of Earthquake Data (SEED) format is the primary
format used by global data centers in seismology.   It has also become
a common format for raw data handling from portable earthquake recording
instruments supplied by the IRIS-PASSCAL program.   The most complete
support for data import in MsPASS is based on SEED and/or so called
miniSEED (miniseed) data.  For those unfamiliar with these terms miniseed
is a subset of SEED data that contains only the minimal metadata required
to define a set of data contained in package of data.  (We say "package"
instead of "file" because miniseed can and has been used as a network
transfer format because the data bundled into a serial string of packets.
For more details about SEED and miniseed can be found
`here <https://ds.iris.edu/ds/nodes/dmc/data/formats/seed/>`__ ).

Python modules to handle the import of SEED data are packages found
under :code:`mspasspy.preprocessing.seed`.   Our current implementation depends
upon obspy's miniseed reader that imposes some restrictions.
A fundamental scalability problem in the current version of obspy's reader
is it makes what we might call the SAC model of data management.  That is,
SAC and obspy both work best if the data are fragmented loaded with one
file per Trace object (equivalent in concept to mspasspy.ccore.TimeSeries).
That model produces a serious scalability problem on large data sets, especially if
they are stored on large virtual disk arrays common today in HPC centers.
The authors have seen example where simply deleting a data set with the
order of a million files can take days to complete on such a system.
Thus that model is completely at odd with the goal of building a high performance
parallel system.

To address this problem our current implementation to import miniseed data
uses a compromise solution where we concatenate logically related miniseed
files into larger files of data.  Type examples are: (1) "event gathers", which
means a file of all data related to particular earthquake (event) and (2)
"receiver gathers" where data are grouped by station.   As a somewhat extreme
example, a year of USArray data for teleseismic earthquakes is known to
define of the order of 10^7 files per year if stored using the obspy/sac model.
(The entire teleseismic data set approaches 10^9 waveform segments.)
When merged into event files the number reduces to the order of 1000 per year.
That is known to eliminate the file name management problem at the cost of
needing to work with potentially very large files that can create memory problems.
That problem is particularly acute at the present because of a fundamental
problem with obspy's reader miniseed reader; when given a large file their
reader will try to eat the whole file and convert the data to a potentially
large list of Trace objects bundled into a Stream container.  We plan to
eventually implement a foff index as used in CSS3.0's wfdisc table, but
that idea is not currently supported.  (For those familiar with raw data
handling :code:`foff` in css3.0 implementation is used as a argument to the low
level, C function fseek to position the read pointer to a particular
position in a file containing multiple waveform segments.  A more efficent
reader would also need to store the number of bytes to load to know the
range of data defining data to be uncoded to produce a single Trace/TimeSeries
object.)

Our current code in the module :code:`mspasspy.preprocessing.seed.ensembles`
imports data through a two step procedure:

1.  Run the following function on each seed file that is a bundle of
    multiple channels of data:

    .. code-block:: python

       def dbsave_seed_ensemble_file(db,file,gather_type="event",keys=None):

    where :code:`file` is assumed to be a miniseed file and :code:`db` is a :code:`Database`
    object, which is our database handle class.  The :code:`dbsave_seed_ensemble_file`
    function builds only an index of the given file and writes the index to
    a special collection called :code:`wf_miniseed`.

2.  The same data can be loaded into memory as a MsPASS :code:`TimeSeriesEnsemble`
    object using the related function with this signature:

    .. code-block:: python

       def load_one_ensemble(doc,
                  create_history=False,
                  jobname='Default job',
                  jobid='99999',
                  algid='99999',
                  ensemble_mdkeys=[],
		              apply_calib=False,
                  verbose=False):

    where :code:`doc` is a document retrieved from the wf_miniseed collection.
    For example, the following shows how an entire dataset of miniseed files indexed
    previously with dbsave_seed_ensemble_file can be read sequentially:

    .. code-block:: python

       from mspasspy.db.client import Client
       from mspasspy.db.database import Database
       from mspasspy.preprocessing.seed.ensembles import load_one_ensemble


       dbname="mydatabase"   # set to the name of your MongoDB database
       client=Client()
       db=Database(client,dbname)
       dbwf=db.wf_miniseed
       curs=dbwf.find()   # insert a query dict in the find function to limit number
       for doc in curs:
         ensemble=load_one_ensemble(doc)
         # Save these as TimeSeries objects
         for d in ensemble.member:
           db.save_data(d)

The above would produce a bare bones set of documents in the wf_TimeSeries
collection.   For some processing like noise correlation studies that may
be enough.   For any event-based processing the data will need to be
linked to the :code:`channel` and :code:`source` collections.   Current capability is
limited to ensemble processing and is best understood by examining the
sphynx generated documentation for the following functions:  *link_source_collection,
load_hypocenter_data_by_id, load_hypoceter_data_by_time, load_site_data*, and
:code:`load_channel_data`.   In addition, see our tutorial section for a detailed
example of how to use these functions.



Advanced Topics
---------------


Customizing the schema
~~~~~~~~~~~~~~~~~~~~~~

THIS NEEDS TO BE WRITTEN

Importing Data Formats other than miniSEED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Obspy's generic file reader supports a long list of formats described
`here <https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html>`__.
Any of these formats are readily imported into MsPASS, but would require
writing a custom reader.  Our miniseed reader in :code:`mspasspy.preprocessing.seed`
provides a model to do this.  One version of such a custom algorithm could
be summarized in the following common steps:

#.  Run the obspy read function on a file.  It will return a Stream container
    with one or more Trace objects.
#.  Run the mspass Stream2TimeSeriesEnsemble function found in
    :code:`mspasspy.util.converter`.
#.  Run the loop as above containing :code:`db.save(d)` on the output of
    Stream2TimeSeriesEnsemble

If you need to import a format not on that list, the problem is much harder.
Our general recommendation is to replace the functionality of obspy's
reader with a custom python read function designed to crack that particular
format.  One could either convert the weird format data to an obspy Stream
object so it was plug compatible in obspy or convert the data directly to
TimeSeries or TimeSeriesEnemble objects with the mspass ccore api.
