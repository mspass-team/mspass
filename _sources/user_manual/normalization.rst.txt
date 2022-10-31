.. _normalization:

Normalization
=================================
Concepts
----------------
A universal property of any data amenable to storage in a database
is that some attributes are highly redundant.  For instance,
consider a typical seismic example.
A data set of 1 million waveforms recorded on the order of
1000 fixed seismic stations would have the same station coordinates repeated around
1000 times if stored with each waveform.
That redundancy problem was recognized decades ago as a fundamental
weakness of the use of static "headers" in seismic processing of any kind.
It was, in fact, one of the key motivations for the development of the
CSS3.0 relational database schema in the 1980s.
The standard CSS3.0
relational database schema handles this issue by defining
a set of tables that are linked to the waveform index (wfdisc)
using a relational database "join".  MongoDB is not relational
but handles the same issue by what they call the :code:`normalized`
versus the :code:`embedded` data model
(MongoDB's documentation on this topic can be found `here <https://www.mongodb.com/docs/manual/core/data-model-design/>`__).

Normalization is conceptually similar to a relational database join, but
is implemented in a different way that has implications on performance.
For small datasets these issues can be minor, but for very large
data sets we have found poorly designed normalization algorithms
can be serious bottleneck to performance.
A key difference all users need to appreciate
is that with a relational database a "join" is always a global operation done between all
tuples in two relations (tables or table subsets).  In MongoDB
normalization is an atomic operation made one document (recall a document
is analogous to a tuple) at a time.  Because all database operations are
expensive in time we have found that it is important to parallelize the normalization
process and reduce database transactions where possible.
We accomplish that in one of two ways described in the subsections
below:  (1) as part of the reader, and (2) with parallel normalization
functions that can be applied in a dask/spark map call.
A novel feature of the MsPASS normalization we discuss later is that
we have abstracted the process in a way that provides great flexibility
in how normalizing data is loaded.   e.g. receiver location information can
be loaded either through the MsPASS
:code:`site` or :code:`channel` collections using MongoDB or by
loading the data from a pandas Dataframe.
An example of the utility of a Dataframe is that all Antelope CSS3.0
tables are easily loaded into a Dataframe with one line of python code
(:code:`read_csv`).  That abstraction is possible because a MongoDB "collection"
is just an alternative way to represent a table (relation).

Before proceeding it is important to give a pair of definitions we used repeatedly
in the text below.   We define the :code:`normalizing` collection/table as the
smaller collection/table holding the repetitious data we aim to cross-reference.
In addition, when we use the term :code:`target of normalization`
we mean the thing into which data in the normalizing collection are to be copied.
The "target of the normalization" in all current examples is one of the
waveform index collections (wf_miniseed, wf_TimeSeries, or wf_Seismogram)
or, in the case of in-line normalization functions, the Metadata container of
one of the MsPaSS data objects.

Data reader normalization method
--------------------------------------

Overview
++++++++++++++

Almost all workflows begin with a set of initializations.   In a
production workflow using MsPASS that is normally followed immediately by one of
two MongoDB constructs:  (1) a query of one of the waveform collections
that returns a MongoDB :code:`cursor` object, or (2) a function call that
creates an RDD/bag of query strings.   The first is used if the processing
is limited to atomic level operations like filtering while the second is
the norm for ensemble processing.   In either case, :code:`normalization`
is best done through the readers that particular workflow uses to create the
working RDD/bag.  In both cases a key argument to the read functions is
:code:`normalize`.   In all cases :code:`normalize` should contain a
python list of strings defining mongodb collection names that should be
used to "normalize" the data being read.

Normalization during read operation has two important limitations
users must recognize:

#.  The cross-reference method to link waveform documents to normalizing
    data is fixed.   That is, in all cases the cross-reference is always
    based on the :code:`ObjectId` of the normalizing collection with a
    frozen naming convention.   For example, if we want to normalize Seismogram data
    with the :code:`site` collection, all MsPASS readers expect to find the
    attribute :code:`site_id` in :code:`wf_Seismogram` documents that
    define the (unique) :code:`ObjectId` of a document in the :code:`site`
    collection.  If a required id is not defined that datum is silently dropped.
#.  By default the readers load the entire contents of all documents in the normalizing
    collection.   That can waste memory.  For example, channel collection
    documents created from FDSN web service and saved with the
    :py:meth:`save_inventory <mspasspy.db.database.Database.save_inventory>` method always
    contain serialized response data that may or may not be needed.  If that
    is a concern, the easiest solution is to use the :code:`exclude`
    argument to pass a list of keys of attributes that should not be
    loaded.   An alternative is to use the inline normalization
    functions described later in this section.   They can provide more
    control on what is loaded and should normally be used as an
    argument to a map operator.

Defining Cross-referencing IDs
++++++++++++++++++++++++++++++++++

Because the readers use ObjectIds to provide the standard cross-reference
method for normalization, MsPASS has functions for the common matching
schemes.   The simplest to use is :py:func:`normalize_mseed <mspasspy.db.normalize.normalize_mseed>`.
It is used for defining :code:`channel_id`
(optionally :code:`site_id`) matches in the :code:`wf_miniseed` collection.
Use this function when your workflow is based on a set of miniseed files.
The actual matching is done by the using the complicated SEED standard of the
station name keys commonly called net, sta, chan, and loc codes and
a time stamp inside a defined time interval.  That complex match is, in fact,
a case in point for why we use ObjectIds as the default cross-reference.  The
:py:func:`normalize_mseed <mspasspy.db.normalize.normalize_mseed>`
function efficiently handles the lookup and
database updates by caching the index in memory and using a bulk update
method to speed update times.   We strongly recommend use of this function
for miniseed data as a simpler implementation was found to be as much as two
orders of magnitude slower than the current algorithm.  The data on that
development is preserved
`here on github <https://github.com/mspass-team/mspass/discussions/307>`__.

Normalizing source data is often a more complicated problem.   How difficult
the problem is depends heavily upon how the data time segmentation is
defined.   MsPASS currently has support for only two source association
methods:  (1) one where the start time of each datum is a constant offset
relative to an event origin time, and (2) a more complicated method based on
arrival times that can be used to associate data with start times relative
to a measured or predicted phase arrival time.  The later can easily violate
the assumption of the normalizing collection being small compared to the
waveform collection.  The number of arrivals can easily exceed the number of
waveform segments.
In both cases, normalization to set :code:`source_id` values are best
done with the mspass function :py:func:`bulk_normalize <mspasspy.db.normalize.bulk_normalize>`.
How to actually accomplish that is best understood by consulting the examples
below.

Here is a simple example of running normalize_mseed as a precursor to
reading and normalizing miniseed data:

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.database.normalize import normalize_mseed
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  retcodes = normalize_mseed(db)
  print("Number of wf_miniseed documents processed=",retcodes[0])
  print("Number of documents that normalize_mseed set channel_id=",retcode[1])

Examples of normalization while reading
+++++++++++++++++++++++++++++++++++++++++++

This is an example serial job that would use the result
from running normalize_mseed in the example above:

.. code-block:: python

  from mspasspy.client import Client
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # loop over all wf_miniseed records
  cursor = db.wf_miniseed.find({})
  for doc in cursor:
    d = db.read_data(doc,normalize=["channel"])
    # processing functions here
    # normally terminated with a save operation or a graphic display

Notice the use of the normalize argument that tells the reader to
normalize with the channel collection.   A parallel version of the
example above requires use of the function
:py:func:`read_distributed_data <mspasspy.db.database.read_distributed_data>`.
The following does the same operation as above in parallel with dask

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.database import read_distributed_data
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # loop over all wf_miniseed records
  cursor = db.wf_miniseed.find({})
  dataset = read_distributed_data(db,normalize=["channel"])
  # porocessing steps as map operators follow
  # normally terminate with a save
  dataset.compute()

Reading ensembles with normalization is similar.   The following is a
serial job that reads ensembles and normalizes each ensemble with data from
the source and channel collections.  It assumes not only
normalize_mseed has been run on the data but some version of bulk_normalize
was used to set the source_id values for all documents in wf_miniseed.

.. code-block:: python

  from mspasspy.client import Client
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # this assumes the returned list is not enormous
  sourceid_list = db.wf_miniseed.distinct("source_id")
  for srcid in sourceid_list:
    cursor = db.wf_miniseed.find({"source_id" : srcid})
    ensemble = db.read_ensemble_data(cursor,normalize=["channel","source"])
    # processing functions for ensembles to follow here
    # normally would be followed by a save


Normalization within a Workflow
-------------------------------
Concepts
++++++++++++++

An alternative to normalization during a read operation is to match records
in a normalizing collection/table on the fly and load desired attributes
from that collection.  We abstract that process to two concepts
that need to be implemented to make a concrete normalization procedure:

#.  We need to define an algorithm that provides a match of records in
    the normalizing collection with the target of the normalization.
    A matching algorithm may return a unique match (one-to-one) or
    multiple matches (one-to-many).
#.  After a match is found we need to copy a set of attributes
    from the normalizing collection to the target.  By definition a
    standard normalization operation requires the match be one-to-one.

We abstract both of these operations in a novel way in MsPASS
described in the two sections below.

Matchers
+++++++++++++++
A match can be defined by
something as simple as a single key string match or it
can be some arbitrarily complex algorithm. For example,
the standard seismology problem of matching SEED waveform data
to receiver metadata requires matching four
different string keys (station-channel codes) and a time interval.
Any matching operation, however, has a simple idea as the core concept:
matching requires an algorithm that can be applied to a collection/table with a boolean
outcome for each document/tuple/row.   That is, the algorithm returns
True if there is a match and a False if the match fails.
In MsPASS we define this abstraction in an object-oriented perspective
using inheritance and an abstract base class that defines the
core generic operation.  You can read the docstrings for this
class :py:class:`here <mspasspy.db.normalize.BasicMatcher>`
for details.
Note that the API requires a concrete instance of this base class to
implement two core methods:  :py:meth:`find <mspasspy.db.normalize.BasicMatcher.find>`
is used for a one-to-many match
algorithm while
:py:meth:`find_one <mspasspy.db.normalize.BasicMatcher.find_one>`
is the primary method for one-to-one matches.
Note we require even unique matchers to implement :py:meth:`find <mspasspy.db.normalize.BasicMatcher.find>` since one is
simply a special case of "many".

The choice of those two names (:py:meth:`find <mspasspy.db.normalize.DatabaseMatcher.find>` and :py:meth:`find_one <mspasspy.db.normalize.DatabaseMatcher.find_one>`) was not
arbitrary.  They are the names used to implement the same concepts in MongoDB
as methods of their database handle object.  In fact, as a convenience the
normalize module defines the intermediate class
:py:class:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher>`
that provides a layer to simply creating a matcher to work directly with
MongoDB.   That class implements :py:meth:`find <mspasspy.db.normalize.DatabaseMatcher.find>` and :py:meth:`find_one <mspasspy.db.normalize.DatabaseMatcher.find_one>` as
generic wrapper code that translates MongoDB documents into the (different)
structure required by the base class,
:py:class:`BasicMatcher <mspasspy.db.normalize.BasicMatcher>`.
To make the database matcher generic,
concrete implementations of :py:class:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher>`
are required to implement the method :py:meth:`query_generator <mspasspy.db.normalize.DatabaseMatcher.query_generator>`.
That approach allows the implementation to have a generic algorithm for
:py:meth:`find <mspasspy.db.normalize.DatabaseMatcher.find>` and :py:meth:`find_one <mspasspy.db.normalize.DatabaseMatcher.find_one>` with a series of matching classes
that are subclasses of :code:`DatabaseMatcher` with different implementations
of :code:`query_generator`.   The following table is a summary of concrete
matcher classes that are subclasses of :code:`DatabaseMatcher` with links
to the docstring for each class:

.. list-table:: Database Query-based Matchers
   :widths: 30 60
   :header-rows: 1

   * - Class Name
     - Use
   * - :py:class:`ObjectIdDBMatcher <mspasspy.db.normalize.ObjectIdDBMatcher>`
     - Match with MongoDB ObjectId
   * - :py:class:`MiniseedDBMatcher <mspasspy.db.normalize.MiniseedDBMatcher>`
     - Miniseed match with net:sta:chan:loc and time
   * - :py:class:`EqualityDBMatcher <mspasspy.db.normalize.EqualityDBMatcher>`
     - Generic equality match of one or more key-value pairs
   * - :py:class:`OriginTimeDBMatcher <mspasspy.db.normalize.OriginTimeDBMatcher>`
     - match data with start time defined by event origin time
   * - :py:class:`ArrivalDBMatcher <mspasspy.db.normalize.ArrivalDBMatcher>`
     - match arrival times to waveforms

As noted many times in this User's Manual database transactions are expensive
operations due to the inevitable lag from the time between issuing a query until
the result is loaded into your program's memory space.  The subclasses
derived from :py:class:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher>`
are thus most useful for one of two situations:  (1) the normalizing
collection is large and the matching algorithm can use an effective
MongoDB index, or (2) the dataset is small enough that the cost of the queries
is not overwhelming.

When the normalizing collection is small we have found a much efficient way
to implement normalization is via a cacheing algorithm.   That is, we
load all or part of a collection/table into a data area
(a python class :code:`self` attribute) "matcher" object
(i.e. a concrete implementation of
:py:class:`BasicMatcher <mspasspy.db.normalize.BasicMatcher>`.).
The implementation then only requires an efficient search algorithm
to implement the required
:py:meth:`find <mspasspy.db.normalize.BasicMatcher.find>`
and
:py:meth:`find_one <mspasspy.db.normalize.BasicMatcher.find_one>`
methods.   We supply two generic search algorithms as part of MsPASS
implemented as two intermediate classes used similarly to
:py:class:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher>`:

#.  :py:class:`DictionaryMatcher <mspasspy.db.normalize.DictionaryCacheMatcher>`
    uses a python dictionary as the internal cache.  It is most useful
    when the matching algorithm can be reduced to a single string key.
    The class implements a generic
    :py:meth:`find <mspasspy.db.normalize.DictionaryCacheMatcher.find>`
    method by using a python list to hold all documents/tuples
    that match the dictionary key.  Note the returned list is actually
    a list of Metadata containers as defined by the base class API.
    We do that for efficiency as Metadata containers are native to
    MsPASS data objects that are the target of the normalization.
#.  :py:class:`DataframeCacheMatcher <mspasspy.db.normalize.DataFrameCacheMatcher>`
    uses the more flexible
    `Pandas Dataframe API <https://pandas.pydata.org/docs/reference/index.html>`__.
    to store it's internal cache.   The Pandas library is robust and
    has a complete set of logical constructs that can be used to construct
    any query possible with something like SQL and more.  Any custom,
    concrete implementations of :py:class:`BasicMatcher <mspasspy.db.normalize.BasicMatcher>`
    that match the small normalizing collection assumption would be
    best advised to utilize this API.

These two intermediate-level classes have two features in common:

#.  Both can load the normalizing collection in one of two forms: (a)
    via a MongoDB database handle combined with a :code:`collection`
    name argument, or (b) a Pandas dataframe object handle.  The former,
    for example, can be used to load :code:`site` collection metadata from
    MongoDB and the later can be used to load comparable data from an
    Antelope :code:`site` table via the
    `Pandas read_csv method <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas.read_csv>`__
    or similar methods for loading a Dataframe from an SQL relational database.
#.  Both provide generic implementations of the :code:`find` and
    :code:`find_one` methods required by
    :py:class:`BasicMatcher <mspasspy.db.normalize.BasicMatcher>`.

These two classes differ mainly in what they require to make them
concrete.   That is, both have abstract/virtual methods that are required
to make a concrete implemntation.
:py:class:`DictionaryMatcher <mspasspy.db.normalize.DictionaryCacheMatcher>`
requires implementation of
:py:meth:`cache_id <mspasspy.db.normalize.DictionaryCacheMatcher.cache_id>`
and
:py:meth:`db_make_cache_id <mspasspy.db.normalize.DictionaryCacheMatcher.db_make_cache_id>`.
That minor complication was implemented to allow an implementation to use
different keys to access attributes stored in the database and
the equivalent keys used to access the same data in a workflow.
In addition, there is a type mismatch between a document/tuple/row
abstraction in a MongoDB document and the internal use by the matcher
class family.  That is, pymongo treats represents a "document" as a
python dictionary while the matchers require posting the same data to
the MsPASS Metadata container to work more efficiently with the C++
code base that defines data objects.

:py:class:`DataframeCacheMatcher <mspasspy.db.normalize.DataFrameCacheMatcher>`
requires only the method
:py:meth:`subset <mspasspy.db.normalize.DataFrameCacheMatcher.subset>`
used to select only the rows in the Dataframe that define a "match"
for the complete, concrete class.   For more details see the docstrings that
can be viewed by following the hyperlinks above.  We also discuss these
issues further in the subsection on writing a custom matcher below.

The following table is a summary of concrete
matcher classes that utilize a cacheing method.  As above each name
is a hyperlink to the docstring for the class:

.. list-table:: Cache-based Matchers
   :widths: 30 60
   :header-rows: 1

   * - Class Name
     - Use
   * - :py:class:`ObjectIdMatcher <mspasspy.db.normalize.ObjectIdMatcher>`
     - Match with MongoDB ObjectId as dictioary key for cache
   * - :py:class:`MiniseedMatcher <mspasspy.db.normalize.MiniseedMatcher>`
     - Miniseed match with net:sta:chan:loc and time
   * - :py:class:`EqualityMatcher <mspasspy.db.normalize.EqualityMatcher>`
     - Generic equality match of one or more key-value pairs
   * - :py:class:`OriginTimeMatcher <mspasspy.db.normalize.OriginTimeMatcher>`
     - match data with start time defined by event origin time

Noting currently all of these have database query versions that differ only
by have "DB" embedded in the class name
(e.g. the MongoDB version of :code:`EqualityMatcher` is :code:`EqualityDBMatcher`.)

Examples
++++++++++++++++++
Example 1:  ObjectId matching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The abstraction of defining matching through a python class allows the
process of loading normalizing data into a dataset through a single,
generic function called :py:func:`normalize <mspasspy.db.normalize.normalize>`.
That function was designed exclusively for use in map operations.  The
idea is most clearly seen by a simple example.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import ObjectIdMatcher,normalize
  from mspasspy.db.database import read_distributed_data
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to coordinates
  # Note these are defined when the matcher class is instantiated
  attribute_list = ['_id','lat','lon','elev']
  matcher = ObjectIdMatcher(db,collection="site",attributes_to_load=attribute_list)
  # This says load the entire dataset presumed staged to MongoDB
  cursor = db.wf_miniseed.find({})   #handle to entire data set
  dataset = read_distributed_data(cursor)  # dataset returned is a bag
  dataset = dataset.map(normalize,matcher)
  # additional workflow elements and usually ending with a save would be here
  dataset.compute()

This example loads receiver coordinate information from data that was assumed
previously loaded into MongoDB in the "site" collection.  It assumes
matching can be done using the site collection ObjectId loaded with the
waveform data at read time with the key "site_id".   i.e. this is an
inline version of what could also be accomplished (more slowly) by
calling :code:`read_distribute_data` with "site" in the normalize list.

Key things this example demonstrates in common to all in-line
normalization workflows are:

+  :code:`normalize` appears only as arg0 of a map operation (dask syntax -
   Spark would require a "lambda" function in the map call).
+  The "matcher" is created as an initialization before loading data.
   It is then used by passing it as an argument to the normalize
   function in the map operation.
+  Only the attributes defined in the constructor for the matcher are copied
   to the Metadata container of the data being processed.  In this example
   after running the normalize function the each datum for which a match
   was found will contain attributes with the following keys:
   :code:`site_id`, :code:`site_lat`, :code:`site_lon`, and :code:`site_elev`.
   Note these have the string "site\_" automaticaly prepended by default.
   That renaming can be disable by setting the :code:`prepend_collection_name`
   to False.  By default failures in matching cause the associated
   waveform data to be marked dead with an informational error log posted
   to the result.


Example 2:  miniseed matching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example illustrates the in-line equivalent of running the
normalization function for miniseed data noted above called
:py:func:`normalize_mseed <mspasspy.db.normalize.normalize_mseed>`.
This example would load and process an entire dataset defined in
the wf_miniseed collection of a database with the name "mydatabase".
It shows how a list of keys are used to limit what
attributes are extracted from the channel and site collections
and loaded into each datum.  These are defined by the
symbols :code:`channel_attribute_list` and :code:`site_atribute_list`.
As in example 1 creation of the matcher classes to match the
waveforms to site and channel collection documents is an initialization
step.  That is, we "construct" two concrete matchers we assign the symbols
:code:`channel_matcher` and :code:`site_matcher`.
As above these matches are passed as an argument to the :code:`normalize`
function in a map operator.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import MiniseedMatcher
  from mspasspy.db.database import read_distributed_data
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to coordinates and orientations
  channel_attribute_list = ['_id','lat','lon','elev','hang','vang']
  site_attribute_list = ['_id','lat','lon','elev']
  # These construct the channel a site normalizers
  channel_matcher = MiniseedMatcher(db,collection="channel",
     attributes_to_load=channel_attribute_list)
  site_matcher = MiniseedMatcher(db,collection="site",
     attributes_to_load=site_atribute_list)
  cursor = db.wf_miniseed.find({})   #handle to entire data set
  dataset = read_distributed_data(cursor)  # dataset returned is a bag/rdd
  dataset = dataset.map(normalize,channel_matcher)
  dataset = dataset.map(normalize,site_matcher)
  # additional processing steps normally would be inserted here
  dataset.compute()

Example 3:  source normalization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example shows an example of how to insert source data into
a parallel workflow.  As above use the dask syntax for a map operator.
This example uses the matcher called :code:`OriginTimeMatcher`
which works only for waveform segments where the start time of the
signal is a constant offset from the event origin time.
It illustrates another useful feature in the constructor
argument :code:`load_if_defined`.   This example uses one key, "magnitude",
for that list.  The use is that if a value is associated with the key
"magnitude" in the normalizing collection it will be loaded with the data.
If it is no defined it will be silently ignored and left undefined.  Note
that is in contrast to keys listed in "attributes_to_load" that are treated
as required.  As noted above if any of the attributes_to_load keys are
missing a datum will, by default, be killed.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import OriginTimeMatcher
  from mspasspy.db.database import read_distributed_data
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to source coordinates
  attribute_list = ['_id,''lat','lon','depth','time']
  # define source normalization instance assuming data start times
  # were defined as 20 s after the origin time of the event
  # origin time used to define the data time window
  source_matcher = OriginTimeMatcher(db,t0offset=20.0,
       attributes_to_load=attribute_list,load_if_defined=["magnitude"])
  cursor = db.wf_Seismogram.find({})   #handle to entire data set
  dataset = read_distributed_data(cursor)  # dataset returned is a bag/rdd
  dataset = dataset.map(normalize,source_matcher)
  # additional processing steps normally would be inserted here
  dataset.compute()

Example 4: ensemble processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example is a variant of example 3 immediately above but
implemented on ensembles.  That is, here the normalizing data
attributes are loaded in the SeismogramEnsemble's Metadata container
and not copied to the members of the ensemble.  This workflow is
a way to assemble what would be called "common-shot gathers"
in seismic reflection processing.
It uses a common
trick for ensemble processing building a dask bag from distinct source_id
values, constructing a ensemble-based query from the id, and then
calling the :py:meth:`read_ensemble_data <mspasspy.db.database.Database.read_ensemble_data>`
method within a parallel map call
to create the ensembles.  The bag of ensembles are then normalized.
Finally note that this example is a hybrid of database normalization and
in-line normalization.  The example assumes that the user has previously
run a function like :code:`bulk_normalize` to set the cross-referencing
id for the source collection :code:`source_id`.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import ObjectIdMatcher
  from mspasspy.db.database import read_ensemble_data

  def read_common_source_gather(db,collection,srcid):
    """
    Function needed in map call to translate a single source id (srcid)
    to a query, run the query, and load the data linked to that source_id
    """
    dbcol = db[collection]
    query = {"source_id" : srcid }
    # note with logic of this use we don't need to test for
    # no matches because distinct returns only not null source_id values dbcol
    cursor = dbcol.find(query)
    ensemble = db.read_ensemble(db, collection=collection)
    ensemble["source_id"] = srcid
    return ensemble

  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to source coordinates
  attribute_list = ['_id,''lat','lon','depth','time']
  source_matcher = ObjectIdMatcher(db,collection="source",
     attributes_to_load=attribute_list,load_if_defined=["magnitude"])
  # MongoDB incantation to find all unique source_id values
  sourceid_list = db.wf_Seismogram.distinct("source_id")
  dataset = dask.bag.from_sequence(sourceid_list)
  dataset = dataset.map(lambda srcid : read_common_source_gather(db, "wf_Seismogram", srcid))
  # dataset here is a bag of SeismogramEnsembles.  The next line applies
  # normalize to the ensemble and loading the attributes into the ensemble's
  # Metadata container.
  dataset = dataset.map(normalize,source_matcher)
  # additional processing steps normally would be inserted here
  dataset.compute()

Custom Normalization Functions
------------------------------------

If the current set of normalization algorithms are not sufficient for
your data, you may need to develop a custom normalization algorithm.
We know of three solutions to that problem:

#.  Think about what you are trying to match and see if it is possible to
    use header math functions :ref:`header_math`
    to construct a new Metadata attribute that can be
    used for a generic match like :py:class:`EqualityMatcher <mspasspy.db.normalize.EqualityMatcher>`.
    Similarly for string manipulation you may be able to create a special
    character string to define your match with a custom python function
    you could use in a map operation prior to using one or the MsPASS
    generic matchers.
#.  Write a custom python function for matching keys in a wf collection
    and a normalizing correction.  The recommended approach is to
    have the function set the
    ObjectId of the normalizing collection in the wf collection using
    the MsPASS naming convention for such ids (e.g. "source_id" to
    normalize source).  With this approach you would use the standard
    update methods of pymongo easily found from numerous web tutorials.
    You will also find examples in the MsPASS tutorials found
    `here <https://github.com/mspass-team/mspass_tutorial>`__.  Then
    you can use the :code:`normalize` argument with the readers to
    load normalizing data at read time or use the inline version
    :code:`ObjectIdDBMatcher` or :code:`ObjectIdMatcher`.
#.  Write an extension class to the intermediate level, subclasses of the base class
    :py:class:`BasicMatcher <mspasspy.db.normalize.BasicMatcher>`
    described above
    (:py:class:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher>`,
    :py:class:`DictionaryCacheMatcher <mspasspy.db.normalize.DictionaryCacheMatcher>`,
    and :py:class:`DataFrameCacheMatcher <mspasspy.db.normalize.DataFrameCacheMatcher>`).
    One could also build directly on the base class, but we can think of no
    example where would be preferable to extending one of the intermediate
    classes.  The remainder of this section focuses only on some hints for
    extending one of the intermediate classes.

We assume the reader has some familiarity with the general concept of inheritance
in object-oriented programming.  If not, some supplementary web research
may be needed to understand the concepts behind some of the terminology below
before an extension is attempted.  If you have a sound understanding of inheritance
in object oriented programming, you may want to just ignore the rest of this
section and see how we implemented concrete matcher classes in the
:code:`mspasspy.db.normalize` module and use one of them as a template
to modify.  You might, however, still find the following useful to understand the
concepts behind our design.

The syntax for inheritance is a standard python construct best illustrated
here by a simple example:

.. code-block:: python

  from mspasspy.db.normalize import DataframeCacheMatcher
  class MyCustomMatcher(DataframeCacheMatcher):
    # class implementation code

Any class needs a constructor as part of the API.   Most will
want to use the superclass constructor to simplify the setup.
Here is an example of the how the class :code:`MyCustomMatcher` above
could utilize the base class constructor to allow it to work
cleanly with the base class :code:`find` and :code:`find_one` methods:

.. code-block:: python

  class MyCustomMatcher(DataframeCacheMatcher):
    def __init__(
      self,
      db,
      # additional required arguments with o default would be defined here
      collection="site",
      attributes_to_load=["lat", "lon", "elev"],
      load_if_defined=None,
      aliases=None,
      prepend_collection_name=True,
      # additional optional arguments with defaults would added here
  ):
      super().__init__(
          db,
          collection,
          attributes_to_load=attributes_to_load,
          load_if_defined=load_if_defined,
          aliases=aliases,
          require_unique_match=True,
          prepend_collection_name=prepend_collection_name,
      )
      # any additional argument would be parse to set self variables here

The point of that somewhat elaborate construct is to cleanly construct the
base class, which here is :code:`DataframeCacheMatcher`, from the
inputs to a constructor.   An instance of the above using all defaults
could then be created with the following construct:

.. code-block:: python

   matcher = MyMatcher(db)

As the comments note, however, a typical implementation would usually
need to add one or more required or optional arguments to define constants
that define properties of the matching algoithm you are implementing.

Finally, as noted earlier each of the intermediate classes have one or more required
methods that the intermediate class declares to be "abstract" via
the :code:`@abstractmethod` decorator defined in the :code:`ABC` module.
The methods declared "abstract" are null in the intermediate class.
For an implementation to work it must be made "concrete", in the language used by the ABC
documentation, by implementing the methods tagged with the
:code:`@abstractmethod` decorator.  Requirement for each of the
intermediate classes you should use to build your custom matcher are:

-  The :py:class:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher>`
   requires implementing only one method called
   :py:meth:`DatabaseMatcher <mspasspy.db.normalize.DatabaseMatcher.query_generator>`.
   Tha method needs to create a python dictionary in pymongo syntax that is to
   be applied to the normalizing collection.  That query would normally be
   constructed from one or more Metadata attributes in a data object but
   time queries may also want to use the data start time and endtime available
   as methods in atomic data objects.  Consult the MongoDB documentation
   for guidance on the syntax of pymongo's query language based on
   python dictionaries.
-  The :py:class:`DictionaryCacheMatcher <mspasspy.db.normalize.DictionaryCacheMatcher>`
   requires implementing two methods.
   :py:meth:`cache_id <mspasspy.db.normalize.DictionaryCacheMatcher.cache_id>`
   is a function that needs to return a unique string that defines the
   key to the python dictionary used as to implement a cache in this
   intermediate class.
   The other method,
   :py:meth:`db_make_cache_id <mspasspy.db.normalize.DictionaryCacheMatcher.db_make_cache_id>`,
   needs to do the same thing and create identical keys.
   The difference being that
   :py:meth:`db_make_cache_id <mspasspy.db.normalize.DictionaryCacheMatcher.db_make_cache_id>`
   is used as the data loader to create the dictionary-based cache while
   :py:meth:`cache_id <mspasspy.db.normalize.DictionaryCacheMatcher.cache_id>`
   is used to construct the comparable key from a MsPASS data object.
-  The :py:class:`DataFrameCacheMatcher <mspasspy.db.normalize.DataFrameCacheMatcher>`
   requires subclasses to implement only one method called
   :py:meth:`subset <mspasspy.db.normalize.DataFrameCacheMatcher.subset>`.
   The :code:`DataframeCacheMatcher` defines its cache internally with the
   symbol :code:`self.cache`.  That symbol defines a pandas container.
   The subset method you implement can use the rich API of pandas to
   define the matching operation you need to build.  Pandas are so widely used
   there is an overwhelming volume of material you can use for a reference.
   `Here <https://pandas.pydata.org/docs/user_guide/indexing.html>`__ is
   a reasonable starting point.  In any case, a key point is that the
   :code:`subset` method you implement needs to fetch attributes from
   the input data object's Metadata (header) and/or the data objects
   internals (e.g. start time, end time, and orientation data) to construct
   a pandas query to select the rows of the cached dataframe that match
   that stored internally with the data.

We close this section by emphasizing that the value of using class inheritance
from the :code:`BasicMatcher` family is you can then utilize it in a
map operator to load attributes from a normalizating collection within a
workflow.  Here, for example, is a variant of example 1 using :code:`MyMatcher`:

.. code-block:: python

    from mspasspy.client import Client
    from mspasspy.db.database import read_distributed_data
    # import for MyMatcher would appear here
    dbclient = Client()
    db = dbclient.get_database("mydatabase")
    matcher = MyMatcher(db)
    cursor = db.wf_miniseed.find({})   #handle to entire data set
    dataset = read_distributed_data(cursor)  # dataset returned is a bag
    dataset = dataset.map(normalize,matcher)
    # additional workflow elements and usually ending with a save would be here
    dataset.compute()

If you compare this to example 1 you will see that the only difference is setting
the symbol :code:`matcher` to an instance of :code:`MyMatcher` instead of
an :code:`ObjectIdMatcher`.
