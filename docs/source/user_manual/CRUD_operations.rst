.. _CRUD_operations:

CRUD Operations in MsPASS
=========================

Overview
~~~~~~~~~~~
The acronymn CRUD is often used as a mnemonic in books and online tutorials
teaching database concepts.  CRUD stands for Create-Read-Update-Delete.
It is a useful mnemonic because it summarizes the four main things any database system
must accomplish cleanly.  This section is organized into subsections on
each of the topics defined by CRUD.  At the end of this section we
summarize some common options in CRUD operations.

The most common database operations are defined as methods in a class
we give the obvious name Database.  Most MsPASS jobs need to the have following
incantation at the top of the python job script:

.. code-block:: python

    from mspasspy.db.client import DBClient
    dbclient = DBClient()
    db = dbclient.get_database('database_name')

where :code:`'database_name'`,
is, as implied, a name you chose for the dataset you are working with.
For the remainder of this section we will use the symbol "db" as
defined, but as in any programming language you need to recognize the
symbol can be anything you as the user find sensible. "db" is just our
choice.  Some may prefer "dbh" as short for database handle, as a
somewhat more descriptive name.

Unlike relational database systems, a schema is not required by
MongoDB.   However, for reasons outlined in the section
:ref:`data_object_design_concepts<data_object_design_concepts>` a schema feature
was added as a component of MsPASS.  We emphasize the
schema in this design, however, can be thought of as more
like guidelines than rigid rules.  The default schema
is called :code:`mspass`.   An alternative that is useful in working
with a workflow reading miniseed files is called :code:`mspass_lite`.
You can specify an alternative schema, like mspass_lite, with
this variant of the above:

.. code-block:: python

    from mspasspy.db.client import DBClient
    dbclient = DBClient()
    db = dbclient.get_database('database_name',schema="mspass_lite.yaml")

Users needing an alternative schema can use the existing yaml files as a
template and load their alternative in the data/yaml directory.
You would then simply substitute the file name you choose for the
schema argument.

In normal use the Database class sets the schema based on the
type of data you ask it to handle.

Create
~~~~~~~~~~

We tag all methods of the Database class that do "Create" operations with
the synonymous word "save".   Here we list all save methods with a brief
description of each method.  Consult the docstring pages for detailed
and most up to date usage:

#.  :py:meth:`save_data <mspasspy.db.database.Database.save_data>` is
    the standard method to save seismic data in serial jobs.  Parallel
    workflows should use the `write_distributed_data` function
    described below.  The
    first argument is one seismic data objects defined in MsPASS
    (Seismogram, TimeSeries, SeismogramEnsemble, or TimeSeriesEnsemble)
    that you wish to save.  Options are
    described in the docstring.  Here is an example usage:

    .. code-block:: python

        # Example assumes filename contains one channel of data in
        # miniseed format
        filename = 'testfile.msd'
        dstrm = read(filename, format='MSEED')
        # A stream is a list of obspy Trace objects - get the first
        dtr = dstrm[0]
        # This mspass function converts a Trace to our native TimeSeries
        d = Trace2TimeSeries(dtr)
        db.save_data(d)

    By default :py:meth:`save_data<<mspasspy.db.database.Database.save_data>>`
    stores all Metadata except those linked to
    normalized collections (:code:`source`, :code:`channel`, and/or :code:`site`) with no
    safety checks.  We discuss additional common options in a later section.
    Ensembles are groups of atomic data and the save is more or less a loop
    over the "member" array using this same method to save each atomic datum.
    The only detail is that the ensembles `Metadata` attributes will be copied
    to each member before they are saved.

#.  :py:meth:`save_catalog <mspasspy.db.database.Database.save_catalog>`
    should be viewed mostly as a convenience method to build
    the :code:`source` collection from QUAKEML data downloaded from FDSN data
    centers via obspy's web services functions.   :code:`save_catalog` can be
    thought of as a converter that translates the contents of a QUAKEML
    file or string for storage as a set of MongoDB documents in the :code:`source`
    collection.  We used obspy's :code:`Catalog` object as an intermediary to
    avoid the need to write our own QUAKEML parser.   As with save_data
    the easiest way to understand the usage would be this example derived from
    our *getting_started* tutorial.

    .. code-block:: python

        client = Client("IRIS")
        t0 = UTCDateTime('2011-03-11T05:46:24.0')
        starttime = t0-3600.0
        endtime = t0+(7.0)*(24.0)*(3600.0)
        lat0 = 38.3
        lon0 = 142.5
        minlat = lat0-3.0
        maxlat = lat0+3.0
        minlon = lon0-3.0
        maxlon = lon0+3.0
        minmag = 6.5
        cat = client.get_events(starttime=starttime, endtime=endtime,
                                minlatitude=minlat, minlongitude=minlon,
                                maxlatitude=maxlat, maxlongitude=maxlon,
                                        minmagnitude=minmag)
        db.save_catalog(cat)

    This particular example pulls 11 large aftershocks of the 2011 Tohoku
    Earthquake.

#.  :py:meth:`save_inventory <mspasspy.db.database.Database.save_inventory>`
    is similar in concept to :code:`save_catalog`, but instead of
    translating data for source information it translates information to
    MsPASS for station metadata.  The station information problem is slightly
    more complicated than the source problem because of an implementation
    choice we made in MsPASS.   That is, because a primary goal of MsPASS
    was to support three-component seismograms as a core data type, there
    is a disconnect in what metadata is required to support a TimeSeries
    versus a Seismogram object.   We handle this by defining two different,
    but similar MongoDB collections:  :code:`channel` for TimeSeries data and
    :code:`site` for Seismogram objects.  The name for this method contains the
    keyword "inventory" because like :code:`save_catalog` we use an obspy
    python class as an intermediary.  The reasons is similar; obspy had
    already solved the problem of downloading station metadata from
    FDSN web services with their
    `read_inventory function <https://docs.obspy.org/packages/obspy.core.inventory.html>`__.
    As with :code:`save_catalog` :code:`save_inventory` can be thought of as a translator
    from data downloaded with web services to the form needed in MsPASS.
    It may be helpful to realize that Obspy's Inventory object is actually
    a python translation of the data structure defined by the
    `FDSN StationXML <https://www.fdsn.org/xml/station/>`__
    standardized format defined for web service requests for station metadata.
    Like :code:`save_source` an example from the getting started tutorial
    should be instructive:

    .. code-block:: python

        inv = client.get_stations(network='TA', starttime=starttime, endtime=endtime, format='xml', channel='*')
        db.save_inventory(inv)

    This example extracts all stations with the "network code" of "TA"
    (the Earthscope transportable array).  A complication of station
    metadata that differs from source data is that station metatdata is
    time variable.  The reason is that sensors change, three-component sensors
    are reoriented, digitizers change, etc.  That means station metadata
    have a time span for which they are valid that has to be handled to
    assure we associate the right metadata with any piece of data.

    In MsPASS we translate the StationXML data to documents stored in two
    collections:  :code:`site` and :code:`channel`.  Both collections contain the
    attributes :code:`starttime` and :code:`endtime` that define the time interval for which
    that document's data are valid.  :code:`site` is simpler.  It mainly contains
    station location data defined with three standard attribute keys:
    :code:`lat`, :code:`lon`, and :code:`elev`.  We store all geographic coordinates (i.e. lat and lon)
    as decimal degrees and elevation (elev) in km.   The :code:`channel` collection
    contains station location information but it also contains two additional
    important pieces of metadata:  (1) orientation information defined by
    the keys :code:`hang` and :code:`vang`, and (2) full response information.
    We store response data in MongoDB as a pickle image of the data stored
    in the StationXML data as translated by obspy.   In the read section
    below we describe how to retrieve response data from :code:`channel`.

    Finally, we note a key feature of the :code:`save_inventory` method:
    it enforces a seed convention to avoid saving duplicate documents.
    As noted earlier the SEED standard uses the keys we call net, sta, chan,
    and loc along with a time interval to define a unique block of
    receiver metadata.   The :code:`save_inventory` method enforces
    the unique combination of these keys in a save.  It always will
    refuse to add an entry it interprets as a pure duplicate document.
    If you need to modify an existing site or channel
    collection that has invalid documents you will need to write a custom function to override that
    behavior or rebuild the collection as needed with web services.
    Note rebuilding these small collections takes seconds to a few minutes unless you
    are on a very slow internet connection.

#.  :py:meth:`write_distributed_data <mspasspy.db.database.Database.write_distributed_data>`
    should be used to save a dataset in a parallel environment.
    It parallelizes writes by separating database transactions from
    saving the sample data, which experience has shown improves write
    performance.  It also utilizes partitioning of the bag/RDD
    holding the dataset to parallelize the database transactions to the
    number of workers.   For ensembles each ensemble is saved with a
    call to the `save_data` method (item 1 above) as a dask/Spark
    "task".  That means, writes are performed in parallel by ensemble.

Note when saving seismic data, the `save_data` method, by default,
returns only the `ObjectId` of the document saved. Returning
a copy of the data is an option.
`write_distributed_data` is more dogmatic and always only returns
a python list of `ObjectID`s.
The default was found to be important to avoid
memory faults that can happen when a workflow computation is initiated in
the standard way (e.g. in dask calling the bag "compute" method.).
If the last step in the workflow is a save and the bag/RDD contains the
entire data set, a memory fault will occur when "compute" is called
as the scheduler will attempt to return the entire dataset to its
memory space.

There are three additional details about all the writers that User's
should recognize:

#.  As noted in many places in this User's Manual, in MsPASS all
    error messages during processing are posted to an internal
    class called `ErrorLogger` held in any datum (including ensembles)
    with the symbol name "elog"  (e.g if d is a TimeSeries the error
    log is stored in d.elog.)  When data are saved any elog entries for "live" data
    are posted to a collection called `elog`.  Each document in elog
    contain an id of the comparable waveform data with which that document is
    associated.
#.  Data killed during processing are handled differently.  Prior to version 2.0
    dead data were mixed with live data error messages in the `elog` collection.
    From version 2 forward, however, the bodies of data killed during
    a workflow are posted to
    a separate collection with the colorful name `cemetery`.  In addition,
    starting with version 2.0 we defined a special type of dead data with
    the another colorful/memorable name:  `abortion`.  An `abortion` in
    MsPASS is defined as a datum that was never born, meaning it was killed
    during construction by a reader.  MsPASS adopts a dogmatically pro-life
    stance and treats all abortions as a serious problem.  Any detected abortions
    are saved in seperate collection called "abortions".  The documents
    are the same structure as those in "cemetery", but they are isolated
    to a different collection to make the fact the error were internal and may
    not have been a fundamental data problem.   That is, most abortions
    can be avoided by using the `dbclean` command line tool.  They are also
    mainly created by running the readers with `mode="cautious"` or
    `mode="pedantic"`.   These concepts are described in detail below
    in the section on reading data.
#.  The writers all have a `save_history` to save the object-level history.
    That data is stored in a separate collection called `history`.
#.  Writers have a `mode` argument that must be one of "promiscuous",
    "cautious", or "pedantic".   Readers also use this argument, but
    writing this controls how much a schema is enforced on the output.
    The default ("promiscuous") does no schema enforcement at all.
    All modes, however, do dogmatically enforce one rule.  Any attribute
    key interpreted as loaded by normalization is erased before the save.
    In MsPASS normalization data are normally loaded by prepending a
    the name of the collection to the attribute.  e.g. the latitude of
    a station ("lat" in the MsPASS schema) stored in the channel collection
    would be loaded with the key "channel_lat".   Attributes with
    one of the standard collection names ("site", "channel", and "source")
    will always be erased before the wf document is saved.  When node
    is set to "cautious" the writer will attempt to correct any time mismatches
    and log an error if any issues are detected.  In "pedantic" mode any
    type mismatches will cause the datum to be killed before the save.
    "pedantic" is rarely advised for writing unless one is writing to a
    files with a format that is dogmatic about attribute types.


Read
~~~~~~~

The Read operation is the inverse of save (create).  The core readers were
designed to simplify the process of reading the core data types of MsPASS:  TimeSeries,
Seismogram, and ensembles of either.
As with the save operators we focus on key methods and refer the
reader to the sphinx documentation for full usage.

#.  :py:meth:`read_data <mspasspy.db.database.Database.read_data>`
    is the core method for reading any seismic data.  Prior to version 2.0
    there were separate methods for handling ensembles, but all are
    now handled through this single method.  The method has
    one required argument.  The expected form of that argument
    is completely different for ensembles and atomic data.
    For Atomic data that argument should
    be a "document" loaded from the appropriate MongoDB "wf" collection.
    The most common use
    is some variant of the following serial job:

    .. code-block:: python

        query = {...Some MongoDB query dict entry...}
        cursor = db.wf_TimeSeries.find(query) # Changed to wf_Seismogram for 3D data
        for doc in cursor:
            d = db.read_data(doc)  # Add option collection='wf_Seismogram' for 3C reads

    By default :code:`read_data` will use the waveform collection defined
    in the schema defined for the handle.  The default for the standard
    MsPASS schema is TimeSeries.   As the comment in the example states
    if you are trying to read from a different collection (i.e wf_Seismogram
    or wf_miniseed) you need to specify that alternative with the collection argument.

    Atomic reads are also possible by passing arg0 as a MongoDB
    `ObjectId`.   Be warned, however, that use is always slower than
    using a document (python dict) input as it is translated to a
    MongoDB `find_one` query using the object id to define the query.

    For ensembles, arg0 of `read_data` is expected to normally
    (There are options described in the sphynx documenation for this method.)
    be a MongoDB cursor like the one used to drive the loop above.
    A typical ensemble-based processing job would use `read_data`
    in the following variant of above.  This job reads data as
    "common source gathers" with the grouping defined by the "source_id"
    attribute:

    .. code-block:: python

        idlist = wf.TimeSeries.distinct("source_id")
        for id in idlist:
          query = {"source_id" : id}
          cursor = wf.TimeSeries.find(query)
          ensemble = db.read_data(cursor)

    The data objects in MsPASS are stored internally as C++ objects with
    multiple elements illustrated in the figure below.   Although these
    objects should be thought of as a single entity the individual
    parts are handled differently in reading because they define different concepts
    and are subject to different read, write, and storage rules.
    :numref:`CRUD_operations_figure1` illustrates this fragmentation:

    .. _CRUD_operations_figure1:

    .. figure:: ../_static/figures/CRUD_operations_figure1.png
        :width: 600px
        :align: center

        Schematic diagram of how different parts of a atomic data object are handled.
        The red box around the center of the figure shows a schematic of the
        data components when a data object is constructed in memory.  The
        boxes in the right-hand (black) box illustrate that the different
        pieces of the object are normally stored in different places.
        This example shows all the components stored within MongoDB
        (the black box), but we note sample data may also be stored as
        files in a file system or in cloud containers.

    The key point of this figure is that the waveform data is treated differently
    from the Metadata and two auxiliary items we call ProcessingHistory and the
    error log (elog).  Waveform data storage is abstracted to allow retrieval
    from drastically different implementations.   Currently, MsPASS has support
    to handle storage in MongoDB's "gridfs" storage, normal local files,
    cloud file systems, and a "URL" storage that utilizes web services.
    What handler is needed to retrieve the sample data is controlled by
    the `storage_mode` attribute readers expect to find in any
    "wf" collection.   `storage_mode` is a required attribute for any valid
    "wf" collection.

    Documents in the wf collection for
    the data type being read (`wf_TimeSeries` or `wf_Seismogram`) contain only
    data we store as Metadata.  A more extensive discussion of Metadata and
    how we use it can be found :ref:`here<data_object_design_concepts>`.
    That section also gives details about ProcessingHistory and the error
    log and the reasons they are part of MsPASS.

    By default :code:`read_data` reads Metadata in what we call "promiscuous" mode.
    That means it takes in all metadata stored in the python dict
    retrieved by pymongo and loads the results into the objects Metadata container
    with no type checking or filtering.
    (Note ensembles are conceptually more-or-less loops over the atomic construction
    processes loading each "member" with the same `read_data` method.  In
    reality ensembles readers have complexity to improve read performance
    described in the docstring for `Database.read_data`, but the conceptual
    model is still valid.)
    Alternatives are "cautious"
    and "pedantic".   Both of the later enforce the type and other constraints defined
    by the schema.   The difference is that in "pedantic" mode any
    conflicts with the schema will cause the
    return to be marked dead.  In "cautious" mode the reader will attempt
    to convert any mismatched types and mark the return dead only if the
    conversion is not possible (e.g. a string like "xyz" cannot normally
    be converted to an integer and a python list cannot be converted to
    a float.)  Guidelines for how to use these different modes are:

    #.  Use "promiscuous" mode when the wf collection to be read is known
        to be clean.  That mode is the default because it is faster to
        run because all the safeties are bypassed.  The potential cost is that
        some members of the data set could be killed on input.
        That potential problem can normally be eliminated by running the
        :code:`dbclean` command-line tool described in a section below.
    #.  Use "cautious" for data saved without an intervening :code:`dbclean`
        operation, especially if the workflow contains an experimental
        algorithm.
    #.  The "pedantic" mode is mainly of use for data export where a
        type mismatch could produce invalid data required by another package.

    Finally, note that fatal conditions flagged in "cautious" or "pedantic"
    mode are the most common source of dead data we define as "abortions"
    (see above).  The reader will refuse to construct a valid datum
    when any conditions are detected to make a datum invalid.  In serial
    jobs such data could be simply dropped, but in parallel jobs
    (see next below) the body has to be carried through the workflow.
    In either case, however, we emphasize that "abortions" should be
    considered a serious problem that should be fixed before running
    a large job and `dbverify` and `dbclean` need to become your friends.
    Note also there is a special case of abortions when reading ensembles.
    If the entire contents of an ensemble is a group of abortions the
    entire ensemble is marked dead.  The bodies, however, are retained
    in the container for diagnosic purposes.

#.  A workflow that needs to read and process a large data sets in
    a parallel environment should use
    the parallel equivalent of :code:`read_data` called
    :py:func:`read_distributed_data <mspasspy.io.distributed.read_distributed_data>`.
    MsPASS supports two parallel frameworks called
    SPARK and DASK.   Both abstract the concept of the parallel data set in
    a container they call an RDD and Bag respectively.   Both are best thought
    of as a handle to the entire data set that can be passed between
    processing functions.   The :code:`read_distributed_data` method is critical
    to improve performance of a parallel workflow.  Always use this function
    as the read step in a parallel workflow.

:code:`read_distributed_data` has a very different call structure than the
other seismic data readers.  It is not a method of Database, but a
separate function call.  A more important issue is that in order to
bundle parallel reads under this single function (`read_distributed_data`),
the call structure has some complexity.   That complexity is largely
driven by the fact that reading ensembles utilizing MongoDB as discussed
above is has complexities beyond that for reading atomic data.
In any case, the prime complexity is that the arguments to
`read_distributed_data` have some strong dependencies.  It might
be helpful to show a subset of the def line for this function:

.. code-block:: python

    def read_distributed_data(
      data,
      db=None,
      query=None,
      scratchfile=None,
      collection="wf_TimeSeries",
      mode="promiscuous",
    ):

There is a hierarchy of behavior driven by the type of
arg0 (`data` in the function definition).   The arguments
`db` and `query` have a dependency on the type of the `data` argument.
`data` is required to
be one of four python types with dependencies defined below for each item:

#. *An instance of a MsPASS `Database` object* (a subclass of MongoDB's
   Database class that is best thought of as a handle to interact with the
   database server).  When arg0 is a `Database` the `query` argument is
   always used explicitly on implicitly.  That is, if `query` is a python
   dictionary, which is used as a query operator in MongoDB, the contents
   are used to define the dataset with the MongoDB query:
   `cursor=db[collection].find(query)`.  If query is undefined (the default)
   the function will assume the entire content of the `wf[collection]`
   defines the data set.
#. *An implementation of a `DataFrame`*.   The most common of these is
   a `pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.
   The function also accepts the dask and pyspark implementations of
   the same conceptual container as both have nearly identical APIs to
   the pandas version.   A DataFrame, in this context, is best thought of
   as an abstraction of a table.  A single relation (table) in a relational
   database maps directly into a DataFrame and is a common way to create one.
   That includes the concept of a string that defines a human readable name
   for each column (attribute in relational database jargon) of the table.
   This interface exists as an alternative way to define input from MongoDB
   using an SQL database, spreadsheet, or an ascii table (csv or fixed field
   tables like those used in the flat-file database in seismology called
   `antelope <https://www.brtt.com>`__).  The interface will work provided the
   attribute names match several key concepts in the standard mspass schema.
   That is, each tuple in the DataFrame must contain key attributes that define
   how to construct a valid atomic data object.  That means universally
   essential attributes like `npts, delta,` and `starttime` as well as
   the long list of attributes that might be essential to describe
   your data.  That includes attributes like `dir`, `dfile`, and `foff` that
   are required when the sample data are stored in files.  To construct a
   valid DataFrame from outside a mspass `Database` will require care in
   assuring the attributes match the MsPASS schema.  Note, however, that
   a key reason this functionality exists is that within `read_distributed_data`
   we use a pandas DataFrame as an intermediary
   by mapping the set of documents that define the dataset in a wf collection
   as a DataFrame.  We found that approach is useful
   to avoid cursor timeouts that plagued our
   earler implementation for parallel reads.  The internal DataFrame
   usage, however, is a feature that is
   "under the hood".
#. *A list of python dictionaries*.  The dictionary content is
   required to be a set of valid pymongo queries that will resolve when
   applied to a wf collection (e.g. wf_TimeSeries) "find" operation.
   This interface exists for loading a dataset that is naturally handled
   as ensembles.  Reading ensembles in parallel present a
   fundamentally different problem from reading atomic data.   Ensembles
   are a collection of atomic data that have some relationship that defines
   the grouping.   e.g. common source gathers (ensembles) can be defined
   by data have a common `source_id`.  As discussed widely in online sources
   on dask and pyspark sorting and grouping atomic data can be very slow and
   very memory instensive.  Furthermore, reading huge datasets is subject to
   cursor timeout errors if ensembles are assembled inline by grouping.
   For these reasons we have found using the database
   to define ensemble grouping is a far superior approach as
   scalable is limited only by storage.

As with the other functions discussed in this section sample code can
help clarify the words.  We show examples below in the same order as the itemized
list immediately above.

*Example of loading dask bag with Database input*

.. code-block:: python

    from mspasspy.db.client import Client
    from mspasspy.db.database import read_distributed_data
    dbclient = Client()
    db = dbclient.get_database("testdb")
    query={'data_tag' : 'atomic_read_example'}
    mybag = read_distributed_data(db,query=query)

This example uses the default dask scheduler loading a dask bag, which
we reference here with the symbol `mybag`, with all data saved with the
`data_tag` argument set to "atomic_read_example".  The example uses
a MongoDB database with the name "testdb".

*Example of reading from a pandas DataFrame*

.. code-block:: python

    from mspasspy.db.client import Client
    from mspasspy.db.database import read_distributed_data
    import pandas as pd
    dbclient = Client()
    db = dbclient.get_database("testdb.csv")
    # note the file read here must contain attributes that mesh
    # with the appropriate schema for the data being constructed
    filename="examples_dbtable.csv"
    df = pd.read_csv("filename")
    mybag = read_distributed_data(df,db)

This code assumes the user has previously created a csv format file
with the name "testdb.csv" that defines a table of attributes with required
parameters needed to construct one `TimeSeries` object from each line
of the csv.  Each row of the file maps to one tuple the pandas DataFrame
created by `read_csv`.

*Example of reading enembles with query list*

.. code-block:: python

    def srcid_to_dict(srcid):
      return {"source_id" : srcid}
    from mspasspy.db.client import Client
    from mspasspy.db.database import read_distributed_data
    import pandas as pd
    dbclient = Client()
    db = dbclient.get_database("testdb")
    srcid_list = db.wf_TimeSeries.find({}).distinct("source_id")
    query_list = map(srcid_to_dict, srcid_list)
    mybag = read_distributed_data(query_list,db)

Note we used the small function `srcid_to_dict` defined at the top
to generate a list of python dict containers used to drive
`read_distributed_data`.  This examples uses a generic
python `map` function that is conceptually similar to dask/pyspark map.
The API is different and the operation is not done in parallel, but it
behaves the same; `map` converts the input list of `source_id` values to
a list of python dict containers that are MongoDB queries for finding
all waveforms matching the given id.

The default output of `read_distributed_data` is a dask bag
containing the content defined by
the wf_TimeSeries collection. If you are using Spark instead of Dask
you would add the optional
argument :code:`format='spark'` and you also need to pass a value
for the argument :code:`spark_context`.

Update
~~~~~~

Because of the way seismic data is stored in MsPASS (see figure above)
the concept of an update makes sense only for Metadata.
If the sample data are modified, the assumption is the result
would be saved as a new instance, not updated.   That constraint is
necessary for a long list of reasons.  In addition, the concept of an
update makes no sense for ProcessingHistory and error log data.
Hence, the history and elog collections, that hold that data, should never
be updated.   No MsPASS supported algorithms will do that, but we
emphasize that constraint because you as the owner of the dataset could
(incorrectly) modify history or elog with calls to MongoDB's api.

Metadata contained in a data objects in MsPASS can come
from three places:  (1) attributes loaded directly with the atomic data from
the unique document in a wf collection with which that data is originated,
(2) "normalized" data loaded from an auxiliary collection (currently
`channel`, `source`, and `site` and MsPASS standards but the concept is generic)
(3) new attributes created during processing.
In a waveform processing job (i.e. python driver script) the metadata
extracted from normalized collections should be treated as immutable.
In fact, when schema validation tests are enabled for save operations
(see above) any accidental changes to any normalized attributes will not be
saved but will be flagged with error log entries during the save.
In most cases regular attributes from normalized data (e.g. source_lat and
source_lon used for an earthquake epicenter) are silently ignored in an
update.  Trying to alter a normalization id field (i.e. source_id, site_id,
or channel_id) is always treated as a serious error that invalidates the
data.  The following two rules summarize these idea in a more concise form:

* **Update Rule 1**:  Processing workflows should never alter any database
  attribute marked readonly or loaded from a normalization collection.

* **Update Rule 2**:  Processing workflows must never alter a cross-referencing
  id field.   Any changes to cross-referencing ids defined in the schema will
  cause the related data to be marked dead.

These rules apply to both updates and writes.  How violations of the rules
are treated on writes or updates depends on the setting of the :code:`mode` argument
common to all update and write methods described in more detail in a section
below.

Delete
~~~~~~~~~
A delete operation is much more complicated in MsPASS than what you would
find as a type example in any textbook on database theory.  In a
relational system delete normally means removing a single tuple.
In MongoDB delete is more complicated because it is
common to delete only a subset of the contents of a given document (the equivalent
of a relational tuple).  The figure above shows that with MsPASS we have
the added complexity of needing to handle data spread across multiple MongoDB
collections and (sometimes) external files.  The problem with connected
collections is the same as that a relational system has to handle with
multiple relations that are commonly cross-referenced to build a
relational join.  The external file problem is familiar to any user
that has worked with a CSS3.0 relational database schema like Antelope.

In MsPASS we adopt these rules to keep delete operations under control.

* **Delete Rule 1**:  Normalization collection documents should never be
  delete any document during a processing run.  Creation of these collections should
  always be treated as a preprocessing step.
* **Delete Rule 2**:  Any deletions of documents in normalization collections should
  be done through one of the MongoDB APIs.  If such housecleaning is
  needed it is the user's responsibility to assure this does not leave
  unresolved cross-references to waveform data.
* **Delete Rule 3**:  Deletes of waveform data, wf collections, history,
  and error log data are best done through the mspass Database
  handle.  Custom cleanup is an advanced topic that must be handled
  with caution.

We trust rules 1 and 2 require no further comment.  Rule 3, however,
needs some clarification to understand how we handle deletes.
A good starting point is to look at the signature of the simple core delete
method of the Database class: :py:meth:`delete_data <mspasspy.db.database.Database.delete_data>`

As with the read methods id is the ObjectID of the wf collection document
that references the data to be deleted.
Similarly, the idea of the :code:`clear_history` and :code:`clear_elog`
may be apparent from the name.  When true all documents linked to the
waveform data being deleted in the history and elog collections (respectively)
will also be deleted.  If either are false, debris can be left behind
in the elog and history collections.  On the other hand, setting either
true will result in a loss of information that might be needed to address
problems during processing.  Furthermore, both are only relevant to
fully or partially processed data.   In general, we recommend the default
for any cleanups applied within a workflow.  Set clear_elog true only in
post processing cleanup after you are confident there are not serious
errors that need to be traced.  Set clear_history True only if you have
no interest in retaining the object level history.  The default is True
because we view object level history preservation as a last step to
provide a mechanism for others to reproduce your work.

The main complexity in this method is behind the boolean argument with the name
:code:`remove_unreferenced_files`.  First, recognize this argument is completely
ignored if the waveform data being referenced is stored internally in
MongoDB in the gridfs file system.  In that situation delete_data
will remove the sample data as well as the document in wf that id defines.
The complexity enters if the data are stored as external files.  The
atomic delete_data method of Database is an expensive operation that should be
avoided within a workflow or on large datasets.  The reason is that
each call for deleting an atomic object (defined by its id) requires a
second query to the wf collection involved to search for any other
data with an exact match to two attributes we used to define a
single data file:  :code:`dir` which is a directory name and :code:`dfile` which is the
name of the file at leaf node of the file system tree.  (CSS3.0 users
are familiar with these attribute names.  We use the same names as the concept here
is identical to the CSS3.0's use.)  Only when the secondary query finds
no matching values for :code:`dir` and :code:`dfile` will the file be deleted.
You should recognize that if, as is strongly advised, data are organized in
a smaller number of larger files deletes of this kind can leave a lot of
debris.   For example, it is easy to produce examples where deleting thousands of
files in a dataset with millions of files removes few if any files.  On the
other hand, the common old SAC model of one waveform per file is an abomination
for storing millions of waveforms on any HPC system.   If your application
requires frequent delete operations for cleanup during a workflow
we strongly advise you store all your data with the
gridfs option.

Key IO Concepts
~~~~~~~~~~~~~~~~~

MsPASS Chemistry
--------------------

In this section we expand on some concepts the user needs to understand
in interacting with the I/O system in MsPASS.  If we repeat things it means
they are important, not that we were careless in writing this document.

It might be useful to think of data in MsPASS with an analogy from
chemistry:  Ensemble data are analogous to molecules make up of a
chain of atoms, the atoms are our "Atomic" data objects (TimeSeries or
Seismogram objects), and each atom can be broken into a set of subatomic
particles.  The figure above illustrates the subatomic idea visually.
We call these "subatomic particles"
Metadata, waveform data, error log, and (processing) history.  The subatomic
particle have very different properties.

1.  *Metadata* are generalized header data.  Our Metadata concept maps closely
    to the concepts of a python dict.  There are minor differences described
    elsewhere.  For database interaction the most important concept is that
    Metadata, like a dict, is a way to index a piece of data with a name-value
    pair.   A fundamental reason MongoDB was chosen for data management in
    MsPASS is that a MongoDB document maps almost exactly into a python dict
    and by analogy our Metadata container.
2.  *waveform data* are the primary data MsPASS was designed to support.
    Waveform data is the largest volume of information, but is different in
    that it has a more rigid structure;  TimeSeries waveform data are universally
    stored in memory as a vector, and Seismogram data are most rationally (although not
    universally) stored as a matrix.  All modern computer systems have
    very efficient means of moving contiguous blocks of data from storage to
    memory so reading waveform data is a very different problem than
    reading Metadata when they are fragmented as in MsPASS. Note that
    traditional waveform handling uses a fixed format with a header and
    data section to exploit the efficiency of reading contiguous memory blocks.
    That is why traditional formats like SAC and SEGY have a fixed header/data
    sections that define "the data".   To make MsPASS generic that paradigm
    had to be broken so it is important to recognize in MsPASS
    waveform data are completely disaggregated from the other data components
    we use for defining our data objects.
3.  *error log* data has yet another fundamentally different structure.
    First of all, our normal goal in any processing system is to minimize
    the number of data objects that have any error log entries at all.
    After all, an error log entry means something may be wrong that
    invalidated the data or make the results questionable.  We structure
    error logs internally as a linked list.   There is an order because
    multiple errors define a chain in the order they were posted.   The order,
    however, is of limited use.  What is important in a processing workflow is
    that nonfatal errors can be posted to the error log and are accumulated
    as the data move through a processing chain.  That means all log entries
    must make it clear what algorithm posted the error.  We handle that
    by having all MsPASS supported processing functions post error messages
    that have a unique tag back to the process that generated them.
4.  *processing history* is an optional component of MsPASS processing that
    is designed to preserve the sequence of data processing steps required to
    produce a processed waveform saved by the system.  The details of the
    data structures used to preserve that history is a lengthy topic best
    discussed elsewhere.  For this section the key point is that preserving
    the history chain is an optional save parameter.  Whenever a save operation
    for history is initiated the accumulated history chain is dumped to
    the database, the history chain container is cleared, and then redefined
    with a hook back to the data that was just saved.

In MsPASS Metadata are further subdivided into three additional subsets
that are handled differently through the schema definition:

1.  An attribute can be marked read-only in the schema.   As the
    name implies that means they are never expected to be altered in a
    workflow.

2.  A special form of read-only attributes are attributes loaded by
    readers from normalized collections.  Such attributes are never saved
    by atomic object writers and the normalized collection (i.e. source, site,
    and channel) are always treated as strictly read only.

3.  Normalization requires a cross-referencing method.   In MsPASS we
    normally uses the ObjectID of the document in the normalizing collection
    and store that attribute using a key with a common structure:
    :code:`collection_id` where "collection" is a variable and "_id" is literal.
    (e.g. the linking key for the source collection is "source_id").
    We use that approach because in MongoDB an ObjectID is guaranteed to
    provide a unique index.   That allows the system to be more generic.
    Hence, unlike FDSN data centers that depend upon the SEED format in
    MsPASS net, sta, chan, loc (the core miniseed keys)
    are excess baggage in a processing workflow.  The default aims to
    only preserve the id that links a datum back to a site and/or channel
    collection.  We have functions for
    linking seed data with net, sta, chan, and loc keys to build links
    stored in the database or that can be used within a workflow.
    (See :ref:`Normalization<normalization>`)
    An example of why the approach we use
    is more generic is to contrast SEED data to something like a CMP
    reflection data set.  In a CMP survey geophone locations are never
    given names but are indexed by something else like a survey flag
    position.   We support CMP data with the same machinery as SEED
    data because the link is through the ObjectID.  The process of
    defining the geometry (site and/or channel) just requires a different
    cross-referencing algorithm. Because of their central role in
    providing such cross references a normalization id is treated
    as absolutely immutable in a workflow.  If a writer detects a linking
    id was altered the datum with which it is associated will be marked
    bad (dead) and the waveform data will not be saved. Finally, it is
    also important to note that MsPASS supports a generic matching
    feature for normalization discussed in detail in the section of
    this manual titled :ref:`Normalization<normalization>`.  In particular,
    normalization data that his highly redundant (i.e. many wf documents
    match to common normalization documents) are much faster to
    handle with cached matchers described in that section of this manual.

Save Concepts
----------------
Waveform save methods begin with this axiom:  a save operation should
never abort for anything but a system error.   That means the definition of
success is not black and white.  There are a range of known and probably
as yet unknown ways data can acquire inconsistencies that are problems of
varying levels of severity.  Here is the range of outcomes in increasing
order severity:

1.  No problems equals complete success.

2.  Problems that could be repaired automatically.  Such errors always
    generate error log entries, but the errors are judged to
    be harmless.   A good example is automatic type conversion from an
    integer to a floating point number.

3.  Errors that are recoverable but leave anomalies in the database.
    An example is the way read_only data and normalized attributes are handled if
    the writer detects that they have changed in the workflow.  When that
    happens the revised data are saved to the related wf collection with a
    an altered key and a more serious error is logged.

4.  Unrecoverable MsPASS errors that might be called an unforgivable sin.
    At present the only unforgivable sin is changing a cross-referencing id.
    If a writer detects that cross-referencing ObjectID has been altered the
    data will be marked dead and the Metadata document will be written to
    the `cemetery` collection.

4.  Unrecoverable (fatal) errors will abort a workflow.   At present that
    should only happen from system generated errors that throw an
    unexpected exception in python.   If you encounter any errors that
    causes a job to abort, the standard python handlers should post an
    informative error.  If you find the error should be recoverable, you
    can and should write a python error handler by surrounding the problem
    section with a *try-except* block.

Save operations by default apply only limited safeties defined by items 3-4
above.  Those are all required because if they were ignored the database
could be corrupted.   Safeties defined by item 2 are optional to make save
operations faster, although users are warned we may change that option
as we acquire more timing data.

In a save operation error log data is always saved.   The log entries are
linked to wf collections with another ObjectID with the standard naming
convention for cross-reference keys.  That is, wf_TimeSeries_id and
wf_Seismogram_id for TimeSeries and Seismogram data respectively.

Data marked dead are handled specially.  For such data the sample data will be
throw away.  The Metadata for dead data are saved in a separate collection
with the colorful name `cemetery`.  The
Metadata associated with the datum is saved as a subdocument accessible with the
key "tombstone".  That provides a simple query mechanism to
show only the most serious errors from a processing run.   Specifically,
this code fragment will print all error messages associated with
dead data with a crude tag of seed net, sta, starttime before each
list of elog messages:

  .. code-block:: python

    # This needs to be checked for correctness - done while off the grid
    query = {'$def' : 'tombstone'}
    cursor = db.cemetery.find(query)
    for doc in cursor:
      wfmd = doc['tombstone']
      print('Error log contents for this Seismogram marked dead:',
            wfmd['net'], wfmd['sta'], UTCDateTime(wfmd['startime']))
      err = doc['logdata']
      for e in err:
        print(e.message)

Note the above is minimal to be concise.  A good report would contain
additional entries from the tombstone contents and additional components of
the container defined the symbol "e".

Saving history data is optional.  When enabled the history chain contents
are dumped to this history collection, the history container is cleared, and
then initialized with a reference to the saved entry and the data
redefined as what we call an "ORIGIN".  The clear process is done because of
a concern that history data could, in some instances, potentially cause
a memory bloat with iterative processing.

Read concepts
-----------------
Reads have to construct a valid data object and are subject to different
constraints.  We believe it is most instructive to describe these in the order
they are handled during construction.

#.  Construction of TimeSeries or Seismogram objects are driven by
    document data read from a collection containing documents with
    key-value pairs that match at least a set of fundamental attributes
    required to construct any such datum.   For example, the number of points
    in the data vector of a TimeSeriers (columns in the Seismogram sample
    matrix) always requires a value to be associated with the key `npts`.
    By default the entire contents of each document
    are loaded into a Metadata container with no safety checks (defined
    above as "promiscuous mode").  Options allow Metadata type checks to be enabled
    against the schema.  In addition, one can list a set of keys that should
    be dropped in the read.  When running in "cautious" or "pedantic"
    mode attributes not defined in the schema will be dropped.   In "cautious"
    mode attributes that cannot be converted will be killed.   In "pedantic"
    mode any type mismatches will cause the datum to be killed.   Any datum
    killed in this will way will be returned as a "mummy", which is the
    husk of the object containing Metadata, error log data, and an empty
    sample container. Such data will also have the attribute `is_abortion`
    set true.

#.  The sample data are loaded.  How that happens depends on multiple
    attributes that are expected to be found in the input document or
    passed via optional parameters to `read_data`.  Currently, the top branch
    in the decision chain on how to handle sample data is controlled by
    the keyword `storage_mode`.  `storage_mode` is expected to be defined in
    all wf document, but if it is missing the value passed by a function
    argument with the same name will be used.   The current list of
    values for `storage_mode` are:  (1) "file" for conventional file storage,
    (2) "gridfs" for storage using a MongoDB feature with the same name, and
    (3) "url" to read from a web address
    (currently that means web services but the concept is more generic).
    Be aware this list may change in the future as IT systems evolve.
    The whole point of this approach is to provide a mechanism to abstract
    the reading process.  Using `storage_mode` in combination with other
    (method dependent) attributes.

#.  If the `normalize` argument is not empty the reader attempts to
    load attributes defined for the normalizing collection.  Since version 2
    of Database normalization can be defined one of two ways :
    (1) a list of subclasses of the `:class:BasicMatcher`, or
    (2) a list of strings defining collection names (e.g. "source").
    The feature of allowing the use of `:class:BasicMatcher` implementations
    was done for efficiency.
    (The developer discussion of this feature and why it improves performance
    can be found `here <https://github.com/mspass-team/mspass/discussions/303>`__.)
    Note that using the verson 1 construct of a list of collection names will
    always be much slower because it implies a Database Id matcher with
    the collection name.  Not only does that require an additional database query for each
    datum created but in the current implementation it also requires constructing
    the python class that implements the queries on each read call.
    Normalization failures will, by default, cause the datum to be killed and
    defined as an "abortion".

#.  If processing history is desired the :code:`load_history` option needs to be
    set true.  In a reader the only action this creates is initialization of the
    ProcessingHistory component of the data with a record providing a unique
    link back to the data just read.

#.  Any datum created by `read_data` and marked live should have an empty error log
    on return. It is good practice before running a large dataset to run a
    pure read only job that produces a report on the number of two key
    measures of reliability:  (1) the number of live entries for which the
    error log is not empty, and (2) the number of abortions.

Update Concepts
---------------
As noted above an update is an operation that can be made only to
Metadata.  In MsPASS Metadata map directly into MongoDB's document concept
of name-value pairs, while the waveform data are stored by some other
mechanism defined by the `storage_mode` attribute.
We know of two common application for a pure Metadata update
without an associated save of the waveform data.

1.  A processing step that computes something that can be conveniently
    stored as Metadata.  Examples are automated phase pickers,
    amplitude measurements and assorted QC metrics.

2.  Pure Metadata operations.  e.g. most reflection processing systems
    have some form of generic metadata calculator of various levels of
    sophistication.  The most flexible can take multiple Metadata (header)
    values and use them to compute a set a different value.   Such
    operations do not alter the waveform data but may require a
    database update to preserve the calculation.   An example is an
    active source experiment where receiver coordinates can often be
    computed from survey flag numbers or some other independent counter.
    In MsPASS Metadata calculations are particularly easy and thus likely
    because python is used as the job control language.   (Classical seismic
    reflection systems and programs like SAC use a custom interpreter.)

Updates to data that only involve Metadata changes should obey this rule:

* **Update Rule 3:**  Updates for Seismogram and TimeSeries object Metadata should be done
  through the :code:`update_metadata` method of :code:`Database`.  Updates to
  other collections should use the pymongo API.

As noted elsewhere numerous online and printed documentation exists for MongoDB
that you should refer to when working directly with database collections.
As the rule states when you need to save the results of a pure Metadata change
within a workflow (e.g. posting a phase pick) use the :code:`update_metadata`
method of :code:`Database`.   That method has two standard arguments already
discussed above:   (1) :code:`mode`, and (2) :code:`collection`.
Three others are important for controlling the behavior of updates:

1. **ignore_metadata_changed_test** is a boolean that is False by default.
   We know of no example where setting this argument True in a update would
   be advised (it exists as an option only to streamline create operations that
   are run through the same method.).  The Metadata container does bookkeeping
   that marks which, if any, key-value pairs in the container have been
   altered since the data was loaded (constructed).  The :code:`update_metadata`
   normally uses that feature to reduce the size of the update transaction by
   only submitting updates for key-value pairs marked changed.   Setting this
   argument True would most likely be harmless, but would also add inefficiency.
2. **exclude_keys** is an optional list of keys for the  Metadata container that the method
   should not try to update.   Use of this option is rare.   An example where it
   might be useful is if some function altered a Metadata value that is known
   to be incorrect.
3. **data_tag** was discussed above for save/create operations.  When the
   entire contents of a TimeSeries or Seismogram object are being saved the
   tag serves as a mark for saves to distinguish those data from the
   starting data or other intermediate saves.  In a pure update, however, the
   meaning is different.  The data_tag argument is used any data updated
   will have the associated tag in the database changed to the string
   specified in the call to :code:`update_metadata`.  The default is to
   do nothing to any existing tag (i.e. the tag is not updated).

Finally, we note that you should consider carefully if a database update is the
right algorithm for what you want to do.   Database updates are always
a slow operation relative to most calculations that can be posted as
Metadata.  Any operation we know of that can be reduced to operations
on Metadata attributes (e.g. computing epicentral distance from source
and receiver coordinates) can always be done faster inline with custom
python functions or one of the set of operators described in
the section of this manual titled `:ref:Header Math<_header_math>`.
