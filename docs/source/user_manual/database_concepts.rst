.. _database_concepts:

Database Concepts
========================
Prof. Gary L. Pavlis
========================
Overview/roadmap
----------------------
Expect this section to be merger of several existing sections.
This section will need to guide reader on access points.
Need to use interal hyperlinks to keys section headings.
note it can be read linearly or nonlinearly with links but design is
a linear read.  Use the index for entry during use for reminders.



Fundamentals
--------------------------------------------
Wikepedia provides the following generic definition of a
"`database<https://en.wikipedia.org/wiki/Database>_":

| In computing, a database is an organized collection of data or a type
  of data store based on the use of a database management system (DBMS),
  the software that interacts with end users, applications,
  and the database itself to capture and analyze the data.

Prior to MsPASS two approaches were used in Seismology to define a
"Database" in this generic sense:

1.  The earliest systems for managing waveform data emerged from real-time
    monitoring and analysis systems developed in the later 1970s and early
    1980s.   Those systems used the then revolutionary concept
    of tree structure of a unix file system to manage waveform data.
    It is not commonly appreciated that that approach is an
    implementation of the oldest form of database called called
    a `hierarchic database model<https://en.wikipedia.org/wiki/Hierarchical_database_model>`__,
    which dates back to the concepts developed by IBM in the 1960s.
    The only reason that is important is that a large fraction of
    research code in seismology use files and file naming conventions
    to manage waveform data.   In addition, a relic still exists
    even to S3 object names Earthscope, SCEDC, and NCEDC all use
    that mirror unix path names.  Type example is day files for
    continuous data.
2.  The nuclear monitoring community of seismology was an early
    adopter (early 1980s) of the then revolutionary concept of
    `relational database systems<https://en.wikipedia.org/wiki/Relational_database>`__.
    Since then relational databases have become the foundation of a large
    fraction of information technology platforms across a wide range of
    fields.   Today relational databases are a workhorse in all seismology
    data centers and most operational seismic networks.  To most seismologists,
    however, the database is an engine under the hood of an information
    system we interact with only through a simplified interface.

MsPASS uses a more recent development with a package called
`MongoDB<https://www.mongodb.com/>`_.  MongoDB is an implementation of
what is called a "document database".   The name is misleading because
it has far more utility than providing a way to manage documents.
There is a lot more information about MongoDB in this manual and
and a truly vast amount of documentation on the web.   A few key
points, however, are worth emphasizing about MongoDB and MsPaSS:

- I would assert that MongoDB is a perfect database engine for
  a system like MsPASS designed for research applications. The reason is
  that MongoDB is so much more flexible than a relational database system.
  Relational databases demand data be organized as a table format that
  is quite rigid.   MongoDB documents, in contrast, are a loose
  collection of "key-value pairs" than can hold almost anything.
  That is extremely important in a research setting where a new idea
  may not mesh with an existing relational database tables.   In MongoDB
  all you need to do is add a new key to hold a new concept and you
  can mix it with older stuff.
- A MongoDB "document" maps almost exactly into a python dictionary.
  We exploit that in MsPASS to create a very flexible way to store
  auxiliary data ("metadata") in the same container as the waveform
  sample data.  The result is a generalization of the ancient
  (from the days of magnetic tape storage) concept of a data "header".
  We use MongoDB in MsPASS to store data that in a seismic reflection
  systems today are commonly stored as relational database tables.
  MongoDB documents are functionally the same, but I reiterate they
  are extensible and not frozen as they are in something like a
  file format like SEGY.
- MongoDB has a clean interface to python called
  `pymongo<https://pymongo.readthedocs.io/en/stable/>`__.
  Experienced python programmers will find the interface easy to use
  as it involves a small number of class methods and input and most of the
  output as python dictionaries or lists of python dictionaries or other
  things.

A final point in this overview is that most modern database systems,
including MongoDB, use a
`client-server model<https://en.wikipedia.org/wiki/Client%E2%80%93server_model>`_.
More about this
topic is found in the :ref:`mspass_component` section.   MsPASS
packages up components in a single client.  Most MsPASS jobs begin with a
version of the following stock incantation:

.. code-block:: python

   from mspasspy.client import Client
   mspass_client = Client()
   dbclient = mspass_client.get_database_client()
   db = mspass_client.get_database("mydb")

where the above sets the symbol `dbclient` to reference a handle to
the MongoDB client.   Many workflows don't need the client itself, but
most will need the line that sets the symbol `db` to reference a handle
used to interact with MongoDB components called "collections".

Parallel jobs face a challenge where multiple "workers" may need to
interact with the database server simultaneously.  Furthermore, in a modern
cluster each worker is independent.   Significant effort is required to
launch a client and initiate connections to the server.  Any parallel
application requiring database access would be very very slow if a new
client had to be instantiated for each new parallel task.   MsPASS solves
that problem by using a feature of dask called a
`worker plugin<https://docs.dask.org/en/stable/customize-initialization.html>`_.
We have found that parallel database access requires the
use of such a plugin.  Naive uses will either perform badly or
just fail.   For details about clients, in general, see :ref:`mspass_components`.
For this topic, there are two axioms:

1. If your workflow requires database access during the run, you must
   use the dask scheduler.   We have been unable to discover a comparable feature in
   pyspark to dask's worker plugin.
2. All parallel jobs require creating the worker plugin and pushing it to
   all workers using a variation of the following code fragment:

.. code-block:: python

   import mspasspy.util.db_utils as mdbu
   from mspasspy.client import Client

   mspass_client = Client()
   db_plugin = mdbu.MongoDBWorker(mspass_client)
   dask_client = mspass_client.get_scheduler()
   dask_client.register_plugin(db_plugin)

Seismology Data
---------------------
A database is an organized collection of information so the reader needs to first
understand what data MsPASS was designed to manage.  MsPASS considers
managing waveform data to be it's primary mission.  What we mean by "waveform data",
however, has some secondary classifications and nuances I discuss below.
MsPASS treats everything except waveform data as
"Metadata", which in the MsPASS perspective means any data not a required
element of a generic piece of waveform data.   MsPASS has well defined
procedure to manage three types of Metadata:  auxiliary waveform metadata,
receiver metadata, and source metadata.   I define what I mean by those
terms and expand on what I mean in subsections below.

Auxiliary waveform metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At the lowest level, single channel of a piece of waveform data is stored as a
vector of sample amplitudes.  That data is completely useless without
three required attributes:  (1) the sample interval/rate, (2) the number
of samples (length) of the data vector, and (3) the time standard and
what the time is of the first sample in that vector.   Anything else
you need to know about that waveform is "Metadata".   That includes
a very long list of seismology concepts like SEED net codes that aren't
required unless the parent data format is SEED.   Even then they aren't
required but a feature of that datum and best treated as Metadata.

The traditional way of handling seismology Metadata borrowed a concept
from seismic reflection processing commonly called a "header".   Many
readers are familiar with SAC files.  SAC files define a fixed data structure
with a fixed block of data (632 bytes to be precise)
at the beginning of the file that defined the "header".
In SAC the "header" is followed by the sample data in time order.
The "header" has a frozen set of attributes that must be located
in specific slots (byte offsets from 0 in computing jargon)
in the header.  MsPASS does not use a fixed header structure but
generalizes the concept to the `Metadata` container.  In object
oriented programming jargon all MsPASS seismic data objects "are" Metadata
(i.e. they inherit a data type called Metadata).  SAC uses that same
concept but the difference is that Metadata to SAC maps fixed memory
slots to a particular set of attributes.  In MsPASS we use a more
modern, generic approach with key-value pairs.   That is,
MsPASS Metadata is a container that allows access to attibute with
a key.  What that means, in practice, is that MsPASS data objects
can act like python dictionaries.   e.g. the following is a sample
code block to access SEED net codes from a seismic data object
defined with a generic symbol "d":

.. code-block:: python

   net = d["net"]
   sta = d["sta"]
   chan = d["chan"]

I inflicted all that on you to make two key points:

1.  The approach we developed for MsPASS is completely generic.
    The "header" can be minimal or expand to be huge depending on the needs
    of the processing workflow.
2.  The data objects loaded for processing in MsPASS do not mix up
    required data and Metadata.   Required data are attributes of the object.
    Everything else has to be stored as Metadata.   I know of no other
    implementation that does this cleanly.   Note commerical seismic reflection processing
    systems, although very clean and efficient, never have this feature.
    They are designed to handle a special class of
    seismic data - a seismic reflection experiment.  Many of us have had the
    bad experience of trying to adapt a seismic reflection package to handle
    earthquake data and found what can be called a collision of concept.
    i.e. there are metadata attributes needed for handling earthquake
    data for which a seismic reflection system has no concept what you mean.
    A case in point are the seed net codes like the example above.
    For decades IRIS-PASSCAL stuffed data into a modified SEGY format output
    and inserted SEED station codes in unused SEGY header slots.  That was
    functional, but produced code that was anything but generic.

The generic header (Metadata) was an early design feature of MsPASS.
We originally thought we could just stuff all Metadata into
a single MongoDB document and use MongoDB to manage the information held
in those documents.  Early design work, however, quickly reminded us that
that model would have created a huge inefficiency.   Two of the most
commonly required set of attributes for processing are highly redundant
if you have to store them as metadata for each waveform:   (a) receiver
attributes and (b) source attributes.   Anyone who has ever tried to
set station and source coordinates in a million SAC files can
immediately understand this problem.   For the rest of you, the point is
that a typical seismology data set a million waveforms would typically
be associated with tens or a few hundred stations.   That is many
orders of magnitude of redundancy and duplicating that same data for
every datum in storage is very inefficient and creates many potential
problems.   For that reason, MsPASS treats source and receiver data
specially as described in the next two sections.   A key point,
however, is that everything else is treated as Metdata and stored in
MongoDB documents.  A convention we adopted in MsPASS is that
collections that contain documents with data that can be used to
construct a valid seismic data object have a name that begins
with "wf".   (Standard ones are "wf_miniseed","wf_TimeSeries", and
"wf_Seismogram")   Auxiliary attributes stored their are open-ended
but each of the standard ones have a schema that defines the namespace
of what each key means for that collection.   We expand on that topic
below after discussing how we handle source and receiver metadata.

Source metadata
~~~~~~~~~~~~~~~~~
Any event-based seismology processing is guaranteed to need some
basic source information.   The standard MongoDB collection to hold that
data is referenced with the "source" collection.   Details about the
standard namespace for source metadata and tools for creating and
managing that data are described below.

Receiver metadata
~~~~~~~~~~~~~~~~~~~~~~~

I know of no scientifically useful example of a workflow that
does not require basic information about the sensors/stations/receivers
that collected a particular datum.   The most basic data is location
information, but many seismology applications require detailed
sensor response metadata.   No matter what is required, during processing
of a waveform most workflows require information about
receiver metadata.  As noted the receiver attributes, however, are
always redundant and best managed independently from waveform data.
Like the source problem, the details of how to create and manage
this information are below and elsewhere in this manual.   The key
point to recognize here is that like the source data receiver data is
managed outside waveform collections (those with a name starting with "wf")
and workflows need to link with that data through a different process
MongoDB calls "normalization".   Details of that topic are found
in :ref:`normalization`.

Managing Large Data objects
-----------------------------
A final generic concept in how MsPASS handles waveform data is that
like every other practical system we are aware of MsPASS manages
Metadata storage separately from the more voluminous sample data.
One way of thinking of this is that MsPASS has no intrinsic data format.
All standard formats are either an import detail or a data export problem.
The objective of all readers is to create a one or more valid
data objects in memory.  That can be done directly with a formatted
reader or indirectly by reading data previously loaded into system
and managed by the MsPASS database.   Once loaded into memory all data is the same
except for what Metadata is loaded with it.

Formatted data
~~~~~~~~~~~~~~~~
There are two ways one can manage formatted data:

1.  Read the files sequentially into memory, translate the reader's output
    to one or more MsPASS data objects, and save the result with a native
    MsPASS writer.  This, for example, is the recommended way to handle SAC
    files.  There is no simple function to do that, but the process is easily
    done with obspy's sac reader and a converter method to a MsPASS TimeSeries
    object.
2.  Index the formatted data and have the workflow read the and translate
    the raw data into MsPASS data objects.   That is, for example the
    recommended approach for starting a workflow with raw miniseed data.
    Numerous examples are found in the
    `MsPASS tutorials repository<https://github.com/mspass-team/mspass_tutorial>`_.
    That approach is particularly useful for data storage as most miniseed
    data uses compression to reduce data storage size by nearly a factor of 10.

External Data Storage within MsPASS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Most MsPASS processing workflows can be reduced to one or more blocks
of python code that can be summarized in three steps:

.. code-block:: python

   1. Read data from storage to create a seismic data object
   2. Run a series of algorithms on that data
   3. Save the results to external storage

Step 1 can be done by reading from formatted files as noted above, but
usually involves accessing data previously exposed to the system or
written by a version of step 3 in some previous processing run.
To clarify, an example of "being exposed to the system" is the standard
way of handling miniseed data.  Here is an example from the mspass tutorial
repository from the "PrecoursePreprocessing.ipynb" file used to import
a set of miniseed files into the MsPASS system:

.. code-block:: python

   import os
   import dask.bag as dbg

   current_directory = os.getcwd()
   dir = os.path.join(current_directory, 'wf')
   dfilelist=[]
   with os.scandir(dir) as entries:
       for entry in entries:
           if entry.is_file():
               dfilelist.append(entry.name)
   print(dfilelist)
   mydata = dbg.from_sequence(dfilelist)
   mydata = mydata.map(db.index_mseed_file,dir=dir)
   index_return = mydata.compute()

The key point here is work done by the `index_mseed_file`
method of the MsPASS Database object (referenced above with the symbol `db`).
It extracts the minimal Metadata stored with miniseed files and
writes one MongoDB document containing that metadata and file index
information that defines a range of bytes that can be used to
construct a single TimeSeries object.   The following example
should make that clearer.  It is an example of the content of a
document created with index_miniseed in json format:

.. code-block:: python

   {
    "sta": "S06",
    "net": "XX",
    "chan": "LHZ",
    "sampling_rate": 500.0,
    "delta": 0.002,
    "starttime": 1765652830.0,
    "last_packet_time": 1765749357.864,
    "foff": 862246686,
    "nbytes": 114463232,
    "npts": 48264000,
    "endtime": 1765749357.998,
    "storage_mode": "file",
    "format": "mseed",
    "dir": "/N/project/MtCarmel/MtCarmel_rawdata/453041443/20251214215554",
    "dfile": "seis009Z.MiniSeed",
    "time_standard": "UTC",
   }


If the above document were loaded into a python script
which we referenced with the symbol `doc`, the datum it references
could be read with the following:

.. code-block:: python

   d = db.read_data(doc,collection="wf_miniseed")

When that line completes `d` would contain a `TimeSeries` object
for the station "S06" and channel "LHZ" defined in `doc` and time range
defined by `starttime` to `endtime` stored as epoch times seed above.
The reader keys on the
`"format": "mseed"` which triggers a formatted read starting
at byte offset `"foff": 862246686` for a span of `"nbytes": 114463232`.   That loads
the sample data into the array defined by `d.data`.
The Metadata of `d` is copy of that above.  e.g. you
would find that after that read completed `d["sta"]` is "S06"
and `d["chan"]` is "LHZ".

The reader should note that the `"format": "mseed"` line
is not the default for data storage.   The default is `binary`
(note if `format` is not defined the default is `binary`).
`binary` tells the reader the to use the low level C function called
`fread<https://www.tutorialspoint.com/c_standard_library/c_function_fread.htm>`_
to load the `data` array in a TimeSeries or Seismogram objects being
constructed by the reader.  The reason for using a raw binary reader
as opposed to storage with something like miniseed is IO speed.
We know of no faster mechanism to read a block of array data that
is independent of system architecture.  Note you pay for speed in
external storage cost compared to miniseed.   That is, miniseed compression
reduces storage to approximately one byte per sample while internal
arrays in MsPASS all use 64 bit (8 byte) floating point numbers.   Hence,
data stored as `binary` require roughly 8 times as much storage as
the same data stored in miniseed.   The read/write times, however, are
much much less for binary data.  How much less is system dependent.

A second critical attribute in that sample document above is the
line:  `"storage_mode": "file"`.  When that line is found in a MongoDB
wf document, the reader will always look for three attributes that
are always required to be with it:   (1) `dir` defines the directory
where that data file (2) `dfile` is found in a file system.  `foff` (3) is
then the number of bytes to skip at the head of the file to get to
the relevant data.  If `storage_mode` is undefined or set to `gridfs`
it means the sample data are stored and managed by MongoDB.
To the reader that makes little difference in how it behaves.   It simply
references a different IO system to use to load the data array.  Some key
points noted elsewhere in this manual are important to emphasize about
gridfs storage:
1.  We have found for most systems gridfs storage is much slower than
    file-based IO.  There are multiple technical reasons for that which are
    a side issue for the topic here.
2.  The data stored on gridfs will live on the same file system as the data used to
    define the MongoDB database.  Large data sets require care to make sure
    the data fit in available external storage.  If you use gridfs storage
    you put "all your eggs in one basket".  Whether that is appropriate is
    dependent upon your local computing setup.  A common desktop configuration
    today is to have a mix of solid state disks and slower, but cheaper
    magnetic disks.   In that situation, the database is best kept on the
    solid state disk while the more voluminous sample data can be stored
    on the magnetic disk file system.   That type of split is not possible
    with gridfs.
3.  Large scale cleanup is often easier with files than gridfs storage.
    To understand the reasons see the section :ref:`CRUD_operations` of
    this manual.

The other thing to understand about MsPASS external storage is the
inverse of reading - writing  (step 3 in the pseudocode above).
A writer needs to create a MongoDB document that has all the information
a reader needs to construct a valid atomic data object.
That means the standard MsPASS writer (the `save_data` method of `Database`)
has to do the following:   (a) save all the sample data to gridfs
or a file name defined by some mechanism, (b) copy all Metadata
from the datum to a python dictionary, (c) load the required data
attributes (e.g. sample rate) into that same dictionary, (d) load
essential data needed to locate where the sample data was just written
(e.g. file name `dfile`), and (e) save that dictionary as a MongoDB document.

The most critical point to keep in mind about seismic data
storage in MsPASS is that sample data are handled independently from
Metadata and required data attributes (i.e. sample rate, npts, and start time).
Even with a format like miniseed or SAC that have headers that contain
some useful Metadata, a reader can be expected to largely ignore that
data stored externally on an headers.  The assumption is that a function
like the `index_mseed_file` method will have been used previously to
extract that Metadata and build one document for each atomic
data object in the dataset being analyzed.  The default `binary` data is the
most extreme as that format depends solely on the database documents
to supply the required attributes and Metadata.   That works because all
MsPASS readers use this pseudocode to create atomic data objects:

.. code-block:: python

   1) load document (dictionary) from database
   2) Use that dictionary to construct an template for the object.
      Specifically, TimeSeries and Seismogram objects have a
      constructor that is called directly to create the object valid
      except the data arrays are created but initialized to all zeros
   3) A sample reader is called to load sample data from external
      storage into the objects data array.

That complexity is a necessary evil to allow MsPASS to be very flexible
about how sample data is handled.  It also allows the Metadata stored on
in the MongoDB database to be altered as needed without having to touch
the sample data.   e.g. a nearly universal starting point with
miniseed data is to run the function
:py:func:`mspasspy.db.normalize.normalize_mseed` on the output of
:py:meth:`mspasspy.db.index_mseed_file`.
That modifies "wf_miniseed" documents creating cross-referencing keys
to the "site" and "channel" collections.   The attributes added are not
at all related to the SEED standard but are essential to utilize the
data effectively.   That strategy of adding to Metadata as needed is
an essential one for any functional seismic processing system.


Schema
------

Overview
~~~~~~~~

The above discussion of how seismic data are handled in MsPASS shows
the central role that Metadata stored as documents in MongoDB are to
how the system works.   One way to say that is if you don't build
the MongoDB documents correctly you can't do anything.
If the documents you create are flawed, you will get nothing but
errors when a reader tries to use them.
Because MongoDB documents are a container that reference a particular thing as a
key-value pairs, it means you must know what key to use to get what you want.
We thus enter the world of what is called a "schema" in database jargon.

Wikipedia defines a database schema as follows:

| The term "`schema <https://en.wiktionary.org/wiki/schema>`__"
  refers to the organization of data as a blueprint of how the database
  is constructed (divided into database tables in the case of `relational
  databases <https://en.wikipedia.org/wiki/Relational_databases>`__)
  the schema defines a set of attributes, tables (relations), and how
  they can be linked (joined).

As this definition states, in a relational database like CSS3.0 the
schema defines a set of attributes, tables (relations), and a how they are
linked (joined).   MsPASS uses a "nonSQL database", which means the interaction
is not with Structured Query Language (SQL).  As discussed above an
numerous other places in this manual the lowest common denominator
in MongoDB is a key-value pair.   A MongoDB *document* is a set of key-value
pairs, a *collection* is a set of documents, and "a database" is a set of
collections organized under a single name tag.  Readers familiar
with relatioal databases may find it convenient to think of a
collection as the equivalent of a table (relation) and a given document
is comparable to a database row (tuple).

I inflicted all that on you to help you understand that a "schema" is
an essential starting point for a relational database,
but it less essential with MongoDB.  With an RDBMS
the structure of those tables must be defined before they can be used.
MongoDB is much less restictive.  You can throw pretty much anything
into a MongoDB document and as long as it can be expressed as a key-value
pair you can save that attribute.
That means it is technically possible to create "documents"
with drastically different contents and store them in the same collection.
For something like MsPASS, however, that would be a really bad idea
as it would be like mixing English and Chinese text in the same book.
Hence, a way of saying what a schema in MsPASS is a set of keywords
you need to use to reference a concept the Metadata value it references defines.
The miniseed document example above provides some examples.
The keyword "delta" in MsPASS defines the time interval the
sample data in the waveform the document references.   All valid waveform
documents MUST contain that key-value pair.  Many of the other key-value
pairs in that document are necessary for that piece of data but not
always required.  e.g. "sta" and "chan" are SEED concepts that are
not really essential unless you are working with FDSN data.
The implication of that for a "schema" in MongoDB is that all you absolutely
have to agree on is what each key references and what the thing it returns
should be.  If you are familiar with the term "ontology" you might find it
helpful to think of a MongoDB schema as an implementation of a simple ontology and not
a rigid specification of a table structure that is required in
a relational database.

The details of the mspass schema definition are given in the
related section of this manual at :ref:`mspass_schema`.
That section includes tables that show the standard
metadata keys, the type of the datum with which they should be linked,
and the concept that the value should represent.
When you are developing a
workflow you may find it useful to have that table at your disposal.
Alternatively, in the modern world with AI agents integrated into
search engines, you can simply ask you favorite AI a question like
the following:
"What is the Metadata key in MsPASS to reference the data sample interval?".
For the most common cases you should get a reasonable response including
a summary longer than the cryptic summary in the tables of
:ref:`mspass_schema`.

We close this section by noting that a schema is not required by
MongoDB. As we discussed in detail in :ref:`data_object_design_concepts`
MsPASS data objects are implemented in C++.   Strong typing in C++
makes a schema a necessary evil to make the system more robust.
A schema also provides a necessary central location to define the
namespace of what kind of content is expected for a particular key.
The rest of this section defines the details of how that is applied in
MsPASS.

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
  An important feature of MsPASS is that the schema is more of a set of
  guidelines than a set of rigid rules.  MsPASS provides a mechanism for
  how rigidly any rules are enforced we will also describe below.

Some Key Concepts
~~~~~~~~~~~~~~~~~~~
ObjectId
:::::::::
MongoDB collections always utilize a unique identifier they call an
:code:`ObjectId` to provide a bombproof, unique identifier for a single document
in a collection.  MongoDB automatically generates one id with the special
name :code:`_id` whenever a new document is added to a collection.   That
attribute is also automatically indexed so queries using it as a key are
always fast.  An important
thing to realize is that two absolutely identical documents, which can readily
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
Metadata was better suited to make more use of what MongoDB's documentation
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
    about seismic sources that are associated with the signals to be
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
normalization.   That idea is described below in the
:ref:`normalization` section.   The key idea of the normalization
components of MsPASS is that in modern computers with massive memory
it is preferable to preload normalizing data into memory than
require millions of queries of small collections.

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
required to prepare the dataset.  What that means is data dependent
and dependent on what processing you need to do.   Standard MongoDB
tools and some Database extensions in MsPASS simplify this process,
but do not remove the burden.
We also would emphasize that preparing data for processing gets increasingly
complicated as the size of a dataset grows as the probability of an
unanticipated data problem increase with the size of a dataset.  Never underestimate the
universal concept of `Murphy's Law <https://www.dictionary.com/browse/murphy-s-law>`__.
For that reason we recommend all MsPASS processing workflows be broken
into at least two pieces:
(1) a preprocessing sequence that builds a clean database to drive (2) the
python code that defines your data processing workflow.

With that background, there are two collections used to manage waveform data.
They are called :code:`wf_TimeSeries` and :code:`wf_Seismogram`.
These two collection are the primary work areas to assemble a working data set.
We elected to keep data describing each of the two atomic data types in MsPASS,
:py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>`
and :py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>`,
in two different collections.  The
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
      1i)  Create a datum d from x
      1i)  Run a sequnce of functions on d
      2i)  Save the result


Parallel jobs are very similar but require creation of an RDD or Dask bag
to drive the processing.  Our parallel api, described elsewhere
(:ref:`parallel_processing`)
simplifies the conversion from a serial to parallel job.  In any case,
the equivalent parallel pseudocode logic is this::

  1) Create a memory resident database client on each worker
  2) Create a list of documents
  3) Create an RDD or dask bag from the list
  4) Run a parallel reader
  5) Run a sequence of map operators as in step 1i of the serial algorithm
  6) Save results with a parallel writer

Where step 1) is described in :ref:`components`.  More about what all
that means can be found in :ref:`parallel_processing`.

A simple perspective on the difference is that the loop for the serial
job becomes is implied in the parallel job.  The list of
documents drives a parallel pipeline job running sequence
read-process-write on each item.   More complex workflows are
possible, but that basic sequence is very common.

.. note::

   Although the simple sequence for parallel processing noted above is
   useful conceptually, we have found that with seismology data that
   approach works only for small datasets without a lot of difficult
   tuning of dask or spark.  The reason is that the input of a small
   volume of data defined by a list of documents misleads the scheduler
   about the memory requirements of the workflow.   Processing
   large data sets (meaning large enough that they can't fit is cluster memory)
   will almost always abort on a memory fault if a simple pipeline
   (series of map operators) like above is used.   A generic solution
   was developed for newer versions of MsPASS in a generic
   function called :py:func:`mspasspy.workflow.sliding_window_pipeline`.
   See the docstring and :ref:`parallel_processing` for more on this topic.


Waveform Data Storage
~~~~~~~~~~~~~~~~~~~~~~

.. note::

   The material in this subsection repeats more introductory versions
   of the same concepts earlier in this document.  It was retained
   during revisions of the manual because it is more-or-less a target
   for a "find out more about topic x" for waveform data storage.

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
in MsPASS is through a mechanism called :code:`gridfs` in MongoDB.  This
section expands on usage of these two mechanisms.

gridfs storage
:::::::::::::::
When data are saved to gridfs, MongoDB will automatically create two
collections it uses to maintain the integrity of the data stored there.
They are called :code:`fs.files` and :code:`fs.chunks`.   Any book on MongoDB and
any complete online source will discuss details of gridfs and these
two collections.  A useful example is this tutorial_.

   .. _tutorial: https://www.tutorialspoint.com/mongodb/mongodb_gridfs.htm

You, as a user, do not normally need to interact with these collections
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

A TimeSeries object stores data as vector of binary "double" values, which for
decades now has implied an 8 byte floating point number stored in the IEEE
format.  (Note historically that was not true.   In the early days of
computers there were major differences in binary representations of
real numbers.   We make an assumption in MsPASS that the machines in the
cluster used for processing have the same architecture and the content of
a binary doubles is
identical on all machines.)  Similarly, a Seismogram stores data in a
contiguous buffer of memory but the memory block is 3 x :code:`npts` doubles.
The buffer order in what numpy calls FORTRAN order meaning the matrix is
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
     Make sure any custom Metadata keys do not match existing MsPASS schema keys.
     If you change the meaning or data type stored with that key,
     you can create any range of downstream problems and could abort the
     final save of your results.

elog
~~~~~~

The elog collection holds log messages that should
automatically be posted and saved in a MsPASS workflow.  The elog
collection saves any entries in ErrorLogger objects that are
contained in all seismic data objects.   The
main idea of an ErrorLogger is a mechanism to post errors of any level
of severity to the data with which the error is associated, preserve a
record that can be used by the user to debug the problem, and allow
the entire job to run to completion even if the error made one or more data
invalid.  More details about this idea can be found in the :ref:`Data
Objects <data_object_design_concepts>` section.
Error log entries are automatically saved when any live datum is
handed to the :py:meth:`mspasspy.db.database.Database.save_data` method.
The documents saved in the elog collection have the attribute `wf_id` that contains the
:code:`ObjectId` of the saved waveform that contained that error.

A special case is data killed during processing.   Any datum from a MsPASS
processing module that was killed should contain an elog entry that the
level :code:`Invalid`.   The sample data in killed Seismogram or TimeSeries data
is not guaranteed to be valid, and may, in fact, be empty.   Hence, killed
data have to be handled specially.   The error logs from killed data
will appear in a different collection called the `cemetery` described
below.

history
~~~~~~~

.. note::

   Readers are warned that this section is based on a poorly tested component
   of MsPASS.  The history collection is the database storage area for a concept in
   MsPASS we developed to track object-level processing history.   The
   facility is integrated into the code base, but users are warned it
   has not be extensively tested on any large data set.

An important requirement to create a reproducible result from
data is a mechanism to create a full history that can be used to recreate
a workflow.  The same mechanism provides a way for you to know the sequence
of processing algorithms that have been applied with what tunable parameters
to produce results stored in the database.  The history collection stores this
information.   Most users should never need to interact directly with this
collection so we omit any details of the history collection contents from
this manual.  Users may, however, need to understand the concepts described
in :ref:`data_object_design_concepts`.

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
data and response data.   Both collections, however, mix in concepts CSS3.0 stores
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

Although those :code:`ObjectId` values can be thought of as primary keys, we provide
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
    index required to do that.   An example of how to do this can be found
    `here<https://github.com/mspass-team/mspass_tutorial/blob/master/Earthscope2024/Session2.ipynb>`_
    in our tutorial repository.

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
    the data set.   Unused documents are not a problem but waste space.
    Missing metadata is a problem as it will always lead to dropped data.
    Assembly of a working data set requires linking documents in :code:`site`
    and/or :code:`channel` to wf_Seismogram documents and channel to wf_TimeSeries
    using keys :code:`site_id` and :code:`chan_id` respectively.

MsPASS has some supported functions to aid in building the site and channel
collections and building links to wf collections.   The details are best
obtained from the docstrings for functions in :code:`mspasspy.db.database`.
The primary tool for FDSN data is
:py:meth:`mspasspy.db.database.save_inventory` which can be used to
save an obspy :code:`Inventory` object obtained from an FDSN server
via web services.   Experience has shown that data is best custom loaded
for each data set as needed to assure the most up-to-date metadata is loaded
from the original source.  You can find numerous examples of how to do that
in the mspass tutorials repository.

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
we handle this problem by storing a pickle image of the :code:`Response` object
related to that channel.

Finally, we emphasize that if your final processing workflow requires
metadata in :code:`site` and/or :code:`channel` you must complete preprocessing to
define linking ids in wf_Seismogram and/or wf_TimeSeries.  Any incomplete
entries will be dropped in final processing.
Conversely, if your workflow
does not require any receiver related Metadata (rare), these collections
do not need to be dealt with at all.
If your data source originated as miniseed, the simplest way to guarantee
the cross-references exist is to run the function
:py:func:`mspasspy.db.normalize.normalize_mseed` on your working database.

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
is data dependent.   With earthquakes are always estimated by an
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
Thus, for the same reason as above we state rule 4 which is very
similar to rule 3:

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
Any entries not associated will be likely be dropped.

cemetery
::::::::::

The *cemetery* collection organizes data "killed" and saved during
a processing workflow.   The kill concept is discussed in multiple
contexts in this manual, but the fundamental idea is a mechanism to
exclude data that cause problems or are otherwise "bad" the
results of the processing.  One normally wants to know what
data were "killed" and why.   The *cemetery* collection holds
that information.

*cemetery* documents have two parts:  (1) a set of key-value pairs
identical to those found in the *elog* collection, and (2)
a subdocument with the key "tombstone".   The "tombstone" content
is a dump of the entire Metadata container of the datum marked
dead.


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
   normal input for any parallel job.
*  The Database api simplifies the processing of reading and writing.
   We abstract the always complex process of reading and writing to :code:`save` and
   :code:`read` methods of the python class Database.  See the Python API section
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

Advanced Topics
---------------

Importing Data Formats other than miniSEED
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Overview
:::::::::

We use the MongoDB database to manage waveform data import.  Waveform data
import should always be understood as another component of preprocessing
needed to assemble a working data set.   The reason we are dogmatic on that
principle is that our TimeSeries and Seismogram containers were designed to
be totally generic, while every single data format we know of has
implicit assumptions about the nature of the data.   For example, SEGY
has intrinsic assumptions that the data are multichannel, seismic-reflection data and
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

The recommended way to handle miniseed data is to utilize the
:py:meth:`mspasspy.db.database.Database.index_mseed_file` method
similar to the example give above.   As noted, that builds an
queriable index that can be used to load raw data into a workflow
without altering that data at all.  I reiterate, however, that
mseed data has the bare minimum Metadata stored with it.
A workable wf_miniseed collection pretty much always needs to
have additional metadata added to every wf_miniseed document
for it to be useful as input to a processing workflow.

Other waveform formats
:::::::::::::::::::::::
MsPASS technically has support a long list of data formats
that are supported by the obspy
`read<https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html>`_
function.  MsPASS does not, however, have a simplified interface to
work with other formats comparable to
:py:meth:`mspasspy.db.database.Database.index_mseed_file`.   Instead,
the recommended approach is to use obspy's read function with the
desired format set for running the read function.   Here, for example,
is the skeleton of a serial script to read a directory of many SAC files and load
them into MsPASS:

.. code:: python

   import os
   import obspy
   import msapsspy.converters.Trace2TimeSeries
   from mspasspy.Client import Client

   mspass_client=Client()
   db = mspass_client.get_database("myproject")
   data_directory = os.getcwd("./wf_SAC")
   save_directory="./wf_TimeSeries"
   outfile="SACfileimages_raw.dat"
   with os.scandir(data_directory) as entries:
       for entry in entries:
           d = obspy.read(entry,format="SAC")
           # obspy.read always returns a Stream object
           # SAC files always have only one datum per file
           # used d[0] to extract that as a Trace object this function requires
           d = Trace2TimeSeries(d[0])
           #  this function needs to be supplied by user
           d = change_sac_metadata_keys(d)
           db.save_data(d,
                  collection="wf_TimeSeries",
                  storage_mode="file",
                  dir=save_directory,
                  dfile=outfile,
                  )

Note a few things about this script:
1.  I've used obspy to handle the formatted reading.
2.  I use the MsPASS converter :py:func:`mspasspy.converters.Trace2TimeSeries`
    to convert the Trace object obspy.read returns into a TimeSeries.
3.  Trace2TimeSeries handles conversion of required Metadata from the
    Trace object to those required by TimeSeries but the content of the
    Trace.stats dictionary are copied verbatim to the Metadata container
    of the TimeSeries it returns.  For that reason, most workflows
    will want to write a custom python function to implement the
    function I called `change_sac_metadata_keys`.  What that is depends on
    what metadata are stored in your sac files that you need to extract.

A related point about the above example indirectly highlights an
important detail about handling formatted data like SAC.   A fundamental
problem with SAC files in modern computing environments is that the
format is completely inappropriate for large data sets.  The reason
is that data stored as SAC file creates serious scalability problems
because of the rigid one file per single channel record requirement of the
format.
I had an example once where simply deleting a data set with the
order of a million files took days to complete on an HPC system.
The reason is that the virtual file systems defined on modern
HPC clusters are not the same as a real file system on a desktop.
They are actually very large arrays of file systems that provide a
programming interface to an application to support programming
io libraries. The simple operation of open, read, close of a file
in that environment can be very slow.   Do that a million times and
nothing works well.  There are also examples where people have crashed
an entire HPC cluster with a parallel job that flooded the io
system with request for too many different files at once.
For that reason an important feature of that template code above is
that all the data being converted are saved to only one file.
That has the further benefit of making it easier to clean up
your workspace when raw processing finishes.

Finally, formats other than SAC that obspy.read handles can be
imported by minor variants of the above template.
The obvious first thing is to change the "format" argument
for the format being loaded.  The more subtle issue is that many
formats are not like SAC and contain many pieces so
obspy.read returns a Stream object with many components.
The easiest way to handle that is to replace
`Trace2TimeSeries` with the related `Stream2TimeSeriesEnsemble`.
As the name implies the later returns a `TimeSeriesEnsemble`.
You would then need replace `change_sac_metadata_keys` to
point to a different function that handled the ensemble correctly
and did a similar metadata key conversion.  Since
:py:meth:`mspasspy.db.database.save_data` handles TimeSeriesEnsembles
directly the only issue on the last lines is if you want to
handle the external storage differently than as single file.
e.g. data organized by a set of large ensembles like event files
are often appropriate to store with one file per ensemble.
The details depend on the layout of data and how it will
be processed after being loaded.  See the sections :ref:`io`
and :ref:`parallel_io` for potentially useful guidance.
