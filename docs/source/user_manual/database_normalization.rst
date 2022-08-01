.. _database_normalization:

Database Normalization
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
versus the :code:`embedded` data model.
(MongoDB's documentation on this topic can be found `here<https://www.mongodb.com/docs/manual/core/data-model-design/>`__.

Normalization is conceptually similar to a relational database join, but
is implemented in a different way that has implications on performance.
Those differences will be of interest to most users only
if you need to develop a custom normalization function as discussed in
the last subsection of this page.  A difference all users need to appreciate, however,
is that with a relational database a "join" is always a global operation done between all
tuples in two relations (tables or table subsets).  In MongoDB
normalization is an atomic operation made one document (recall a document
is analogous to a tuple) at a time.  Because all database operations are
expensive in time we have found that it is important to parallelize the normalization
process and reduce database transactions where possible.
We accomplish that in one of two ways described in the subsections
below:  (1) as part of the reader, and (2) with parallel normalization
functions that can be applied in a dask/spark map call.

Before proceeding it is important to give a pair of definitions we used repeatedly
in the text below.   We define the :code:`normalizing` collection as the
smaller collection holding the repetitious data we aim to cross-reference.
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
#.  The readers load the entire contents of all documents in the normalizing
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
schemes.   The simplest to use is :py:func:`normalize_mseed<mspasspy.db.normalize.normalize_mseed>`.
It is used for defining :code:`channel_id`
(optionally :code:`site_id`) matches in the :code:`wf_miniseed` collection.
Use this function when your workflow is based on a set of miniseed files.
The actual matching is done by the using the complicated SEED standard of the
station name keys commonly called net, sta, chan, and loc codes and
a time stamp inside a defined time interval.  That complex match is, in fact,
a case in point for why we use ObjectIds as the default cross-reference.  The
:py:func:`normalize_mseed<mspasspy.db.normalize.normalize_mseed>`
function efficiently handles the lookup and
database updates by caching the index in memory and using a bulk update
method to speed update times.   We strongly recommend use of this function
for miniseed data as a simpler implementation was found to be as much as two
orders of magnitude slower than our implementation.  The data on that
development is preserved
`here on github<https://github.com/mspass-team/mspass/discussions/307>`__.

Normalizing source data is often a more complicated problem.   How difficult
the problem is depends heavily upon how the data time intervals were
defined.   MsPASS currently has support for only two source association
methods:  (1) one where the start time of each datum is a constant offset
relative to an event origin time, and (2) a more complicated method based on
arrival times that can be used to associate data with start times relative
to a measured or predicted phase arrival time.  The later can easily violate
the assumption of the normalizing collection being small compared to the
waveform collection.  The number of arrivals can easily exceed the number of
waveform segments.
In both cases, normalization to set :code:`source_id` values are best
done with the mspass function :py:func:`bulk_normalize<mspasspy.db.normalize.bulk_normalize>`.
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

Here is a simple serial job that would use the result after the normalization
with normalize_mseed in the example immediately above completed:

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
:py:func:`read_distributed_data<mspasspy.db.database.read_distributed_data`.
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
-----------------------------------

Concepts
++++++++++++++

An alternative to normalization during a read operation is to match records
in a normalizing collection on the fly and load desired attributes
from that collection.   The abstraction of this process we use in MsPASS
makes a fundamental assumption that the normalizing collection is small
compared to the size of the wf collection that defines the working data set.
With that assumption we abstract normalization as two independent
operations:

#.  We need to define an algorithm that provides a one-to-one match of records in
    the normalizing collection with the target of the normalization.
#.  After a match is found we usually need to copy a set of attributes
    from the normalizing collection to the target.

Note item 1 is completely generic.  It can be as simple as a single key string match
or as complex as that used in :code:`normalize_mseed`.  Further, the
"algorithm" is generic.   It may be a database transaction but it is
not required to be such.   Similarly, item 2 may be a database transaction
but also doesn't have to be that.  An design object of our API was to
abstract that process.  The main reason was not programming elegance
but efficiency.  As noted earlier database transactions are expensive in
execution time and we have found it important to avoid unnecessary transactions.

Normalization API
++++++++++++++++++++++

MsPASS normalization is handled through a family of python
classes.  The family has a common root in the base class we
call :py:class:`NMF<mspasspy.db.normalize.NMF>`, which is an
abbreviation for "Normalization Match Function".  All
subclasses of this base class are required to
implement two methods that require concrete implementations of
the two concepts noted above.

#.  :py:meth:`get_document<mspasspy.db.normalize.NMF.get_document>`
    is used to retrieve the contents of a match to the normalizing
    collection using the pattern defined in a MsPASS data object
    passed as (required) arg0.
#.  :py:meth:`normalize<mspasspy.db.normalize.NMF.normalize>` goes
    a step beyond :py:meth:`get_document<mspasspy.db.normalize.NMF.get_document>`
    and copies the matching attributes to the :code:`Metadata` container
    of that data object.

All the implementations in MsPASS use the common standard
advice in all books on object-oriented programming that construction
is initialization.  That is, the instances we define below have
constructors that define the matching algorithm and what attributes
are to be loaded from the normalizing collection.
It is **very important** to recognize that all MsPASS normalization
classes by default **load the entire normalizing collection**.
That is the default because of the assumption noted above that the
normalizing collection is small compared to the data set.
We found cacheing the normalization data in this way dramatically improves
performance by eliminating all database transactions except those required to
load the required data.  The cost is an increase in the memory use during a
normalization operation.   All MsPASS normalization classes have an option
to revert to database transaction mode through the keyword argument
:code:`cache_normalization_data`.  If that argument is set False
all MsPaSS normalizers revert to database transaction mode.

The normalization classes currently available in MsPASS are
defined below with links to the docstrings that define their purpose:

.. list-table:: Normalization Operators
   :widths: 30 50 30
   :header-rows: 1

   * - Class Name
     - Use
     - Docstring
   * - ID_matcher
     - Generic ObjectId matching
     - :py:class:`ID-based normalization<mspasspy.db.normalize.ID_matcher>`
   * - mseed_channel_matcher
     - in-line version of normalize_mseed for channel
     - :py:class:`mseed_channel_matcher<mspasspy.db.normalize.mseed_channel_matcher>`
   * - mseed_site_matcher
     - in-line version of normalize_mseed for site
     - :py:class:`mseed_site_matcher<mspasspy.db.normalize.mseed_site_matcher>`
   * - origin_time_source_matcher
     - match data with start time defined by event origin time
     - :py:class:`origin_source_matcher<mspasspy.db.normalize.origin_time_source_matcher>`
   * - arrival_interval_matcher
     - match arrival times to waveforms
     - :py:class:`arrival_interval_matcher<mspasspy.db.normalize.arrival_interval_matcher>`

The model for using these python classes is to create a single instance of
the class and then apply the :code:`normalize` method in a spark/dask map
operator.   The examples below illustrate this more clearly than any prose.

In-line normalization example 1:
+++++++++++++++++++++++++++++++++++++

This example illustrates the in-line equivalent of running the
normalization function for miniseed data noted above called
:py:func:`normalize_mseed<mspasspy.db.normalize.normalize_mseed>`.
This example would load and process an entire dataset defined in
the wf_miniseed collection of a database with the name "mydatabase".
It shows how a list of keys are used to limit what
attributes are extracted from the channel and site collections
and loaded into each datum.  These are defined by the
symbols :code:`channel_attribute_list` and :code:`site_atribute_list`.
The example also show a required step of creation of an
instance of the two normalizing python classes for channel and site.
We assign the instance of each the symbols :code:`NMchan` and :code:`NMsite`
respectively.   Note the default behavior of the constructors for both
load what can be thought of as a table of attributes from channel and
site with the columns of the table defined by
:code:`channel_attribute_list` and :code:`site_atribute_list`.
The normalization is actually performed in the last two map calls.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import mseed_channel_matcher,mseed_site_matcher
  from mspasspy.db.database import read_distributed_data
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to coordinates and orientations
  channel_attribute_list = ['_id','lat','lon','elev','hang','vang']
  site_attribute_list = ['_id,''lat','lon','elev']
  # These construct the channel a site normalizers
  NMchan = mseed_channel_matcher(db,attributes_to_load=channel_attribute_list)
  NMsite = mseed_site_matcher(db,attributes_to_load=site_attribute_list)
  cursor = db.wf_miniseed.find({})   #handle to entire data set
  dataset = read_distributed_data(cursor)  # dataset returned is a bag/rdd
  dataset = dataset.map(NMchan.normalize)
  dataset = dataset.map(NMsite.normalize)
  # additional processing steps normally would be inserted here
  dataset.compute()

In-line normalization example 2:
+++++++++++++++++++++++++++++++++++++

This example shows an alternative to using the reader to normalize
source collection data stored as Seismogram objects and indexed with wf_Seismogram.
We use the same approach as example 1 immediately above to limit
what is loaded from the source collection to geographic coordinates.
We also show the use of the optional argument :code:`load_if_defined`.
Magnitudes are not universally available from source catalogs so we
make loading the generic magnitude (keyed by "magnitude") optional.
Note the default behavior is to kill any datum that does not
have any of the attributes listed in the :code:`attributes_to_load`
defined in the source collection.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import origin_time_source_matcher
  from mspasspy.db.database import read_distributed_data
  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to source coordinates
  attribute_list = ['_id,''lat','lon','depth','time']
  # define source normalization instance assuming data start times
  # were defined as 20 s after the origin time of the event
  # origin time used to define the data time window
  NMsource = origin_time_source_matcher(db,t0offset=20.0,
       attributes_to_load=attribute_list,load_if_defined=["magnitude"])
  cursor = db.wf_Seismogram.find({})   #handle to entire data set
  dataset = read_distributed_data(cursor)  # dataset returned is a bag/rdd
  dataset = dataset.map(NNsource.normalize)
  # additional processing steps normally would be inserted here
  dataset.compute()

In-line normalization example 3:
+++++++++++++++++++++++++++++++++++

This example is a minor variant of example 2 immediately above that
implements ensemble processing.  That is, here the normalizing data
attributes are loaded in the SeismogramEnsemble's Metadata container
and not copied to the members of the ensemble.   It uses a common
trick for ensemble processing building a dask bag from distinct source_id
values, constructing a ensemble-based query from the id, and then
calling the :py:meth:`read_ensemble_data<mspasspy.db.database.read_ensemble_data>`
method within a parallel map call
to create the ensembles.  The bag of ensembles are then normalized.
Finally note that this example is a hybrid of database normalization and
in-line normalization.  The example assumes that the user has previously
run a function like :code:`bulk_normalize` to set the cross-referencing
id for the source collection :code:`source_id`.

.. code-block:: python

  from mspasspy.client import Client
  from mspasspy.db.normalize import origin_time_source_matcher
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
    return ensemble

  dbclient = Client()
  db = dbclient.get_database("mydatabase")
  # Here limit attributes to be loaded to source coordinates
  attribute_list = ['_id,''lat','lon','depth','time']
  # define source normalization instance assuming data start times
  # were defined as 20 s after the origin time of the event
  # origin time used to define the data time window
  NMsource = origin_time_source_matcher(db,t0offset=20.0,
     attributes_to_load=attribute_list,load_if_defined=["magnitude"])
  sourceid_list = db.wf_Seismogram.distinct("source_id")
  dataset = dask.bag.from_sequence(sourceid_list)
  dataset = dataset.map(lambda srcid : read_common_source_gather(db, "wf_Seismogram", srcid))
  # dataset here is a bag of SeismogramEnsembles.  The next line applies
  # normalize to the ensemble and loading the attributes into the ensemble's
  # Metadata container.
  dataset = dataset.map(NNsource.normalize)
  # additional processing steps normally would be inserted here
  dataset.compute()

Custom Normalization Functions
------------------------------------

If the current set of normalization algorithms are not sufficient for
your data, you will need to develop a custom normalization algorithm.
We know of two solutions to that problem:

#.  Write a custom python function for matching keys in a wf collection
    and a normalizing correction.  The recommended approach is to
    have the function set the
    ObjectId of the normalizing collection in the wf collection using
    the MsPASS naming convention for such ids (e.g. "source_id" to
    normalize source).  With this approach you would use the standard
    update methods of pymongo easily found from numerous web tutorials.
    You will also find examples in the MsPASS tutorials found
    `here<https://github.com/mspass-team/mspass_tutorial>`__.  Then
    you can use the :code:`normalize` argument with the readers to
    load normalizing data at read time.
#.  Write an extension class to the base class found in
    the MsPASS normalize module (:code:`import mspasspy.database.normalize`).
    The remainder of this section discuss that approach.  We assume
    the reader has a basic understanding of object-oriented programming
    in general ahd how python implements objects (classes).

We assume the reader has some familiarity with the general concept of inheritance
in object-oriented programming.  If not, some supplementary web research
may be needed to understand the concepts behind some of the terminology below
before an extension is attempted.

We strongly advise any extensions to normalization classes be created
as subclasses of the base class we call :py:class:`NMF<mspasspy.db.normalize.NMF>`
that we discussed above.  At the time of this writing some of the details of the
API described the :py:class:`docstring for NMF<mspasspy.db.normalize.NMF>`
are subject to change.  The best advice is to look at the existing subclasses
of :code:`NMF` in the normalize module and use the existing subclasses as
a guide for writing a custom normalization class.
