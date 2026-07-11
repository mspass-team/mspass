.. _CRUD_operations:

CRUD Operations in MsPASS
=========================

Overview
~~~~~~~~~~~
The acronym CRUD is often used as a mnemonic in books and online tutorials
teaching database concepts.  CRUD stands for Create-Read-Update-Delete.
It is a useful mnemonic because it summarizes the four main things any database system
must accomplish cleanly.  This section is organized into subsections on
each of the topics defined by CRUD.  At the end of this section we
summarize some common options in CRUD operations.

The most common database operations are methods of the
:py:class:`~mspasspy.db.database.Database` class.  Most MsPASS jobs need the
following initialization near the top of the Python job script:

.. code-block:: python

    from mspasspy.db.client import DBClient

    dbclient = DBClient()
    db = dbclient.get_database("database_name")

where :code:`"database_name"` is a name you chose for the dataset you are
working with.  Pass the MongoDB URI or host to :code:`DBClient` when the
server is not reachable through the local default.
For the remainder of this section we will use the symbol "db" as
defined, but as in any programming language you need to recognize the
symbol can be anything you as the user find sensible. "db" is just our
choice.

Unlike relational database systems, a schema is not required by
MongoDB.   However, for reasons outlined in the section
:ref:`data_object_design_concepts` a schema feature
was added as a component of MsPASS.  We emphasize the
schema in this design, however, can be thought of as more
like guidelines than rigid rules.  The default waveform collection
for most examples is :code:`wf_TimeSeries`.  Workflows that handle
three-component bundles can use the same :code:`Database` handle and
pass :code:`collection="wf_Seismogram"` to the read and write methods
where that collection is required.

Create
~~~~~~~~~~

We tag all methods of the :code:`Database` class that do "Create"
operations with the synonymous word "save".   Here we list the most
important save methods with a brief description of each method.  Consult
the docstring pages for detailed and most up to date usage:

1.  :py:meth:`~mspasspy.db.database.Database.save_data` is the primary
    waveform writer.  The first argument can be any of the core seismic
    data objects defined in MsPASS:  :code:`TimeSeries`,
    :code:`Seismogram`, :code:`TimeSeriesEnsemble`, or
    :code:`SeismogramEnsemble`.  Here is an example usage for a single
    miniseed trace:

    .. code-block:: python

        from obspy import read
        from mspasspy.util.converter import Trace2TimeSeries

        # Example assumes filename contains one channel of data in
        # miniseed format
        filename = 'testfile.msd'
        dstrm = read(filename, format='MSEED')
        # A stream is a list of obspy Trace objects - get the first
        dtr = dstrm[0]
        # This mspass function converts a Trace to our native TimeSeries
        d = Trace2TimeSeries(dtr)
        db.save_data(d)

    By default :code:`save_data` stores Metadata except :code:`_id` and
    attributes linked to normalized collections (:code:`source`,
    :code:`channel`, and/or :code:`site`) without schema name or type checks.
    We discuss additional common
    options in a later section.  With the default :code:`return_data=False`,
    the return is a Python dictionary containing :code:`is_live` and any
    available keys requested by :code:`return_list` (which defaults to
    :code:`["_id"]`).  For ensembles the default usually returns only
    :code:`is_live`, because member IDs are not ensemble Metadata.  Set
    :code:`return_data=True` when an intermediate save needs the edited data
    object returned to the workflow.

    When :code:`save_data` is passed an ensemble, it writes each live member
    as an atomic datum.  Ensemble Metadata are copied to each member before
    the member is saved.  A common pattern for miniseed data is to bundle
    single-component data into three-component seismograms before saving:

    .. code-block:: python

        from mspasspy.algorithms.bundle import bundle_seed_data

        # d is a TimeSeriesEnsemble loaded earlier in the workflow.
        d3c = bundle_seed_data(d)
        db.save_data(d3c, collection="wf_Seismogram")

    See :py:func:`~mspasspy.algorithms.bundle.bundle_seed_data` for details
    on the bundling step.  Older ensemble-specific write helpers are
    backward-compatibility wrappers; new code should use :code:`save_data`.

2.  :py:meth:`~mspasspy.db.database.Database.save_catalog` should be
    viewed mostly as a convenience method to build
    the :code:`source` collection from QUAKEML data downloaded from FDSN data
    centers via obspy's web services functions.   :code:`save_catalog` can be
    thought of as a converter that translates the contents of a QUAKEML
    file or string for storage as a set of MongoDB documents in the :code:`source`
    collection.  We used obspy's :code:`Catalog` object as an intermediary to
    avoid the need to write our own QUAKEML parser.   As with
    :code:`save_data`
    the easiest way to understand the usage would be this example derived from
    our *getting_started* tutorial.

    .. code-block:: python

        from obspy import UTCDateTime
        from obspy.clients.fdsn import Client

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

    This example requests magnitude 6.5 and larger events near the 2011 Tohoku
    earthquake during the following week.  Catalog contents can be revised by
    the data center, so code should not assume an exact result count.
    :code:`save_catalog` inserts every event and does not deduplicate the source
    collection.

3.  :py:meth:`~mspasspy.db.database.Database.save_inventory` is similar
    in concept to :code:`save_catalog`, but instead of
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
    `read_inventory function
    <https://docs.obspy.org/packages/autogen/obspy.core.inventory.inventory.read_inventory.html>`__.
    As with :code:`save_catalog` :code:`save_inventory` can be thought of as a translator
    from data downloaded with web services to the form needed in MsPASS.
    It may be helpful to realize that Obspy's Inventory object is actually
    a python translation of the data structure defined by the
    `FDSN StationXML <https://www.fdsn.org/xml/station/>`__
    standardized format defined for web service requests for station metadata.
    Like :code:`save_catalog` an example from the getting started tutorial
    should be instructive:

    .. code-block:: python

        inv = client.get_stations(
            network="TA",
            starttime=starttime,
            endtime=endtime,
            channel="*",
            level="response",
            format="xml",
        )
        db.save_inventory(inv)

    This example extracts all stations with the "network code" of "TA"
    (the Earthscope transportable array).  A complication of station
    metadata that differs from source data is that station metadata is
    time variable.  The reason is that sensors change, three-component sensors
    are reoriented, digitizers change, etc.  That means station metadata
    have a time span for which they are valid that has to be handled to
    assure we associate the right metadata with any piece of data.
    By default :code:`save_inventory` excludes the synthetic network code
    :code:`SY`; pass :code:`networks_to_exclude=None` to include it.

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
    in the StationXML data as translated by obspy.  Retrieve a response for a
    channel and time with
    :py:meth:`~mspasspy.db.database.Database.get_response`.

    Finally, we note a key feature of the :code:`save_inventory` method:
    it enforces a seed convention to avoid saving duplicate documents.
    Site uniqueness is based on net, sta, loc, and validity interval; channel
    uniqueness additionally uses chan.  The :code:`save_inventory` method
    refuses to add an entry it interprets as a pure duplicate document.
    If you need to modify an existing site or channel
    collection that has invalid documents you will need to write a custom function to override that
    behaviour or rebuild the collection as needed with web services.

4.  :py:func:`~mspasspy.io.distributed.write_distributed_data` is the terminal
    writer for a Dask bag or Spark RDD.  It initiates the lazy computation and
    returns ObjectIds for live waveform documents that were saved.  Atomic
    input produces a list of IDs; ensemble input produces a per-ensemble list
    of ID lists.  Set :code:`data_are_atomic=False` when the container holds
    ensembles.  For example:

    .. code-block:: python

        from mspasspy.io.distributed import write_distributed_data

        saved_ids = write_distributed_data(
            data,
            db,
            collection="wf_TimeSeries",
            data_are_atomic=True,
        )

    Use :code:`Database.save_data` inside a map operation instead when the
    workflow needs an intermediate save and continued processing.

Read
~~~~~~~

The Read operation is the inverse of save (create).  The core reader was
designed to simplify the process of reading the core data types of MsPASS:
:code:`TimeSeries`, :code:`Seismogram`, :code:`TimeSeriesEnsemble`, and
:code:`SeismogramEnsemble`.  As with the save operators we discuss here
the key methods, but refer the reader to the sphinx documentation for full
usage.

#.  :py:meth:`~mspasspy.db.database.Database.read_data` is the core
    waveform reader.  Its required first argument can be any of the
    following:

    1.  A MongoDB :code:`ObjectId`.  This reads one atomic datum from the
        waveform collection selected by the :code:`collection` argument.
        It is convenient but slower than cursor-driven reads because it
        requires an extra database query per datum.
    2.  A MongoDB document represented as a python :code:`dict` or
        :code:`Metadata` object.  This is the preferred serial pattern
        when iterating over a MongoDB cursor.
    3.  A :code:`pymongo.cursor.Cursor`.  This reads all documents returned
        by the cursor and constructs a :code:`TimeSeriesEnsemble` or
        :code:`SeismogramEnsemble`, depending on the selected collection.

    The most common serial pattern is to iterate over a cursor and pass
    each document to :code:`read_data`:

    .. code-block:: python

        query = {"data_tag": "raw"}
        cursor = db.wf_TimeSeries.find(query)
        for doc in cursor:
            d = db.read_data(doc, collection="wf_TimeSeries")

    Use :code:`collection="wf_Seismogram"` when reading three-component
    seismograms, or another waveform collection name such as
    :code:`wf_miniseed` when appropriate.

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

        Schematic diagram of how different parts of a data object are handled.
        The red box around the center of the figure shows a schematic of the
        data components when a data object is constructed in memory.  The
        boxes in the right-hand (black) box illustrate that the different
        pieces of the object are normally stored in different places.
        This example shows all the components stored within MongoDB
        (the black box), but we note sample data may also be stored as
        files in a file system or in cloud containers.

    The key point of this figure is that the waveform data is treated differently
    from the Metadata and two auxiliary items we call ProcessingHistory and the
    error log (elog).  Waveform data is currently stored either internally in
    MongoDB's gridfs storage or in external files.  Documents in the wf collection for
    the data type being read (wf_TimeSeries or wf_Seismogram) contain only
    data we store as Metadata.  A more extensive discussion of Metadata and
    how we use it can be found :ref:`here<data_object_design_concepts>`.
    That section also gives details about ProcessingHistory and the error
    log and the reasons they are part of MsPASS.

    By default :code:`read_data` reads Metadata in what we call "promiscuous" mode.
    That means it takes in all metadata stored in the wf collection at which
    it is is pointed and loads the results into the objects Metadata container
    with no type checking or filtering.  Alternatives are "cautious"
    and "pedantic".  Both of the latter enforce schema names and types and
    drop undefined attributes.  In "pedantic" mode any type mismatch causes
    the return to be marked dead.  In "cautious" mode the reader attempts to
    convert mismatched types; failure kills the datum only for a required
    attribute, while a nonconvertible optional attribute is retained and does
    not invalidate the datum.  Guidelines for these modes are:

    1.  Use "promiscuous" mode when the wf collection to be read is known
        to be clean.  That mode is the default because it is faster to
        run because all the safeties are bypassed.  The potential cost is that
        some members of the data set could still be killed by waveform
        construction or I/O errors.  Schema problems can be found beforehand
        with :py:meth:`~mspasspy.db.database.Database.verify` or repaired with
        :py:meth:`~mspasspy.db.database.Database.clean_collection`.
    2.  Use "cautious" when documents have not been checked against the
        schema, especially if the workflow contains an experimental algorithm.
    3.  The "pedantic" mode is mainly of use for data export where a
        type mismatch could produce invalid data required by another package.

    Passing a cursor directly to :code:`read_data` constructs an ensemble.
    The example code below assumes the variable :code:`source_id` was
    defined earlier, is a valid :code:`ObjectId` in the :code:`source`
    collection, and has been cross-referenced into :code:`wf_TimeSeries`.
    Notice we include a size check with the MongoDB function
    :code:`count_documents` to impose constraints on the query. That is
    always good practice.

    .. code-block:: python

        query = {"source_id": source_id}
        MAX_ENSEMBLE_SIZE = 10000
        ndocs = db.wf_TimeSeries.count_documents(query)
        if ndocs == 0:
            print("No data found for source_id = ", source_id)
        elif ndocs > MAX_ENSEMBLE_SIZE:
            print("Number of documents matching source_id=", source_id, " is ", ndocs,
                  "Exceeds the workflow limit on the ensemble size=", MAX_ENSEMBLE_SIZE)
        else:
            cursor = db.wf_TimeSeries.find(query)
            ensemble = db.read_data(cursor, collection="wf_TimeSeries")

#.  A workflow that needs to read and process a large data sets in
    a parallel environment should use
    the parallel equivalent of :code:`read_data` called
    :py:func:`~mspasspy.io.distributed.read_distributed_data`.  MsPASS
    supports two parallel frameworks, Spark and Dask.  Both abstract the
    concept of the parallel data set in
    a container they call an RDD and Bag respectively.   Both are best thought
    of as a handle to the entire data set that can be passed between
    processing functions.   The :code:`read_distributed_data` method is critical
    to improve performance of a parallel workflow.  The use of storage
    in MongoDB's gridfs in combination with SPARK or DASK
    are known to help reduce io bottlenecks
    in a parallel environment.  SPARK and DASK have internal mechanisms to schedule
    IO to optimize throughput, particularly with reads made through the gridfs
    mechanism we use as the default data storage.  :code:`read_distributed_data`
    provides the mechanism to accomplish that.

    :code:`read_distributed_data` is not a method of :code:`Database`; it
    is a separate function in :code:`mspasspy.io.distributed`.  Its first
    argument can be a :code:`Database` handle, a dataframe representation of
    a waveform collection, or a list of ensemble-defining queries.  The
    common pattern for reading a subset of a waveform collection is:

    .. code-block:: python

        from mspasspy.io.distributed import read_distributed_data

        query = {"data_tag": "raw"}
        data = read_distributed_data(
            db,
            query=query,
            collection="wf_TimeSeries",
            scheduler="dask",
        )

    The output is a Dask bag by default or a Spark RDD when
    :code:`scheduler="spark"`.  See :ref:`parallel_processing`,
    :ref:`parallel_io`, and
    :py:func:`~mspasspy.io.distributed.read_distributed_data` for more
    complete parallel workflow examples.

Update
~~~~~~

Most workflow updates change only Metadata and should use
:py:meth:`~mspasspy.db.database.Database.update_metadata`.  When sample data
must also be replaced, :py:meth:`~mspasspy.db.database.Database.update_data`
updates the Metadata, error log, history, and sample data for an atomic
:code:`TimeSeries` or :code:`Seismogram`.  Its sample-data update currently
uses GridFS; for file-backed input it writes replacement samples to GridFS and
updates :code:`storage_mode` rather than overwriting the external file in
place.  ProcessingHistory and error-log records should be managed through
these APIs instead of edited directly with PyMongo.

As noted elsewhere Metadata loaded with data objects in MsPASS can come
from one of two places:  (1) attributes loaded directly with the atomic data from
the unique document in a wf collection with which that data is associated,
and (2) "normalized" data loaded through a cross reference ID from one of the
standardized collections in MsPASS (currently :code:`site`, :code:`channel`,
and :code:`source`).  In a waveform processing job (i.e. Python driver script) the metadata
extracted from normalized collections should be treated as immutable.
Attributes with prefixes such as :code:`source_`, :code:`site_`, and
:code:`channel_` are removed from waveform documents during saves and updates
unless they are linking ID fields.  Other modified read-only attributes are
omitted under schema enforcement and generate a complaint in cautious or
pedantic mode; promiscuous mode intentionally bypasses those schema checks.

Cross-reference fields such as :code:`source_id`, :code:`site_id`, and
:code:`channel_id` are retained.  The update and save methods do not verify
that a changed ID resolves to a document in the referenced collection, so a
bad change can silently corrupt the association.  Use
:py:meth:`~mspasspy.db.database.Database.check_links` when link integrity is
in doubt.  The following rules summarize the safe workflow:

* **Update Rule 1**:  Processing workflows should never alter any database
  attribute marked readonly or loaded from a normalization collection.

* **Update Rule 2**:  Processing workflows must never alter a cross-referencing
  ID field unless intentionally relinking the datum and validating the target.

These rules apply to both updates and writes.  How violations of the rules
are treated on writes or updates depends on the :code:`mode` argument described
in more detail below.

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
  deleted during a processing run.  Creation of these collections should
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
A good starting point is the signature of
:py:meth:`~mspasspy.db.database.Database.delete_data`:

.. code-block:: python

  def delete_data(
      self,
      object_id,
      object_type,
      remove_unreferenced_files=False,
      clear_history=True,
      clear_elog=True,
  ):
      ...

The :code:`object_id` is the ObjectId of the waveform document, and
:code:`object_type` must be either :code:`"TimeSeries"` or
:code:`"Seismogram"`; it selects the corresponding waveform collection.
Similarly, the idea of the :code:`clear_history` and :code:`clear_elog`
may be apparent from the name.  When true all documents linked to the
waveform data being deleted in the history and elog collections (respectively)
will also be deleted.  If either are false, debris can be left behind
in the elog and history collections.  On the other hand, setting either
true will result in a loss of information that might be needed to address
problems during processing.  Both default to true to perform a complete
delete and avoid orphaned records.  Set either false only when intentionally
retaining those records and prepared to manage the broken waveform reference.

The main complexity in this method is behind the boolean argument with the name
:code:`remove_unreferenced_files`.  First, recognize this argument is completely
ignored if the waveform data being referenced is stored internally in
MongoDB in the gridfs file system.  In that situation delete_data
will remove the sample data as well as the document in wf that id defines.
The complexity enters if the data are stored as external files.  The
atomic :code:`delete_data` method of :code:`Database` is an expensive operation that should be
avoided within a workflow or on large datasets.  The reason is that
when :code:`remove_unreferenced_files=True`, each call for file-backed data
requires a second query to the waveform collection to search for any other
data with an exact match to two attributes we used to define a
single data file:  :code:`dir` which is a directory name and :code:`dfile` which is the
name of the file at leaf node of the file system tree.  (CSS3.0 users
are familiar with these attribute names.  We use the same names as the concept here
is identical to the CSS3.0's use.)  Only when that secondary query finds
no matching values for :code:`dir` and :code:`dfile` will the file be deleted.
You should recognize that if, as is strongly advised, data are organized in
a smaller number of larger files deletes of this kind can leave a lot of
debris.  For example, deleting thousands of waveform documents whose samples
share larger files may remove few if any files.  On the
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
in interacting with the io system in MsPASS.  If we repeat things it means
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
    provide a unique index.   That allows the system be more generic.
    Hence, unlike FDSN data centers that depend upon the SEED format in
    MsPASS net, sta, chan, loc (the core miniseed keys)
    are baggage for joining the site or channel
    collections to a set of waveform data.  We have functions for
    linking seed data with net, sta, chan, and loc keys to build links
    but the links are still best defined by ObjectIDs.   An example of why this
    is more generic is to contrast SEED data to something like a CMP
    reflection data set.  In a CMP survey geophone locations are never
    given names but are indexed by something else like a survey flag
    position.   We support CMP data with the same machinery as SEED
    data because the link is through the ObjectID.  The process of
    defining the geometry (site and/or channel) just requires a different
    cross-referencing algorithm. Because of their central role in
    providing such cross references, a normalization ID should normally be
    treated as immutable in a workflow.  The save and update methods retain
    these IDs but do not validate their targets; deliberately relink data only
    with an explicit integrity check.

Save Concepts
----------------
Waveform save methods begin with this axiom:  a save operation should
not abort an entire workflow for a recoverable problem in one datum.  Invalid
arguments and unexpected system errors can still raise exceptions.  Datum-level
schema problems are handled according to :code:`mode`:

1.  With :code:`mode="promiscuous"`, the default for
    :py:meth:`~mspasspy.db.database.Database.save_data`, schema name, type, and
    read-only checks are bypassed.  The writer still removes :code:`_id`, strips
    normalized attributes such as :code:`source_lat`, and handles dead data.

2.  In :code:`mode="cautious"`, defined values are checked against the schema
    and converted when possible.  Failure to convert a required value kills
    the datum.  Modified read-only values are omitted and logged.

3.  In :code:`mode="pedantic"`, any type mismatch kills the datum.  Modified
    read-only values use the same omission-and-log handling as cautious mode.

When :code:`save_data` saves a live datum with error-log entries, it writes
those entries to :code:`elog` and links them to the waveform document with
:code:`wf_TimeSeries_id` or :code:`wf_Seismogram_id`.

Data marked dead are handled specially; their sample data are not saved.  By default
:py:class:`~mspasspy.util.Undertaker.Undertaker` stores ordinary processing
deaths in :code:`cemetery` and read-time construction failures in
:code:`abortions`.  Each document contains Metadata under :code:`tombstone`
and, when present, a list of error dictionaries under :code:`logdata`.
For :code:`save_data`, setting :code:`cremate=True` skips burial and suppresses
both cemetery and abortion records.  This code prints a compact report from
the default collections:

  .. code-block:: python

    from obspy import UTCDateTime

    query = {"tombstone": {"$exists": True}}
    for collection_name in ("cemetery", "abortions"):
        for doc in db[collection_name].find(query):
            tombstone = doc.get("tombstone", {})
            starttime = tombstone.get("starttime")
            if starttime is not None:
                starttime = UTCDateTime(starttime)
            print(
                collection_name,
                tombstone.get("net", ""),
                tombstone.get("sta", ""),
                starttime,
            )
            for entry in doc.get("logdata", []):
                print(
                    entry.get("algorithm", ""),
                    entry.get("badness", ""),
                    entry.get("error_message", ""),
                )

The report is intentionally minimal; applications can select additional
tombstone fields or filter by :code:`data_tag`.

Saving history data is controlled by :code:`save_history` (currently true by
default for :code:`save_data`).  When enabled and the history container is not
empty, the chain is written to :code:`history_object`, cleared from the datum,
and reinitialized as an origin linked to the saved history.  This bounds memory
growth in iterative processing.

Read concepts
-----------------
Reads have to construct a valid data object and are subject to different
constraints.  We believe it is most instructive to describe these in the order
they are handled during construction.

1.  Construction of TimeSeries or Seismogram objects are driven by
    document data read from the wf_TimeSeries or wf_Seismogram collection
    respectively.   By default the entire contents of each document
    are loaded into Metadata with no safety checks (defined
    above as "promiscuous mode").  Options allow Metadata type checks to be enabled
    against the schema.  In addition, one can list a set of keys that should
    be dropped in the read.

2.  Normalized Metadata are loaded only when the :code:`normalize` or
    :code:`normalize_ensemble` options request one or more matchers.  Standard
    ID-based matchers use :code:`source_id`, :code:`site_id`, or
    :code:`channel_id`; other :class:`~mspasspy.db.normalize.BasicMatcher`
    implementations can use different matching rules.  Missing-match behavior
    belongs to the matcher and can log or kill a datum.

3.  The waveform data are read and the data object is constructed.  If that
    process fails, an atomic :code:`read_data` call returns a dead object of the
    requested type with an error-log entry, rather than :code:`None`.
    Construction and schema failures are normally marked with
    :code:`is_abortion=True`.

4.  For a cursor-driven ensemble read, documents rejected during Metadata
    conversion are omitted from the ensemble and recorded in the
    :code:`abortions` collection.  The returned ensemble is dead only when no
    valid members can be constructed.

5.  If the sample-data read is successful the error log will normally be empty.

6.  If processing history is desired the :code:`load_history` option needs to be
    set true.  In a reader the only action this creates is initialization of the
    ProcessingHistory component of the data with a record providing a unique
    link back to the data just read.

We reiterate that the overall behavior of all readers is controlled by the
:code:`mode=` argument common to all.  The current options are: :code:`promiscuous`,
:code:`cautious`, and :code:`pedantic`.   Detailed descriptions of what each mean are
given above and in the Sphinx documentation generated from docstrings.

Update Concepts
---------------
The most common update changes only Metadata.  In MsPASS Metadata map directly
into MongoDB's document concept of name-value pairs, while waveform samples are
stored separately.  We know of two common applications for a pure Metadata update
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
    because Python is used as the job control language.   (Classical seismic
    reflection systems and programs like SAC use a custom interpreter.)

Updates to data that only involve Metadata changes should obey this rule:

* **Update Rule 3:**  Updates for Seismogram and TimeSeries object Metadata
  should use :py:meth:`~mspasspy.db.database.Database.update_metadata`.
  Updates to other collections should use the PyMongo API.

As noted elsewhere numerous online and printed documentation exists for MongoDB
that you should refer to when working directly with database collections.
As the rule states, use :code:`update_metadata` to preserve a pure Metadata
change such as a phase pick.  The input must be a live atomic object with an
:code:`_id` identifying its waveform document; dead input returns
:code:`None`.  The Metadata container tracks which keys changed, and only
those keys are updated by default.  Important controls are:

1.  :code:`mode` defaults to :code:`"cautious"`; the three modes have the
    schema behavior described above.
2.  :code:`collection` selects a nondefault waveform collection when needed.
3.  :code:`exclude_keys` omits changed keys from the update.
4.  :code:`force_keys` includes listed keys even when they are not marked
    modified.
5.  :code:`normalizing_collections` controls which prefixed normalization
    attributes are stripped.  Change its default only for a custom schema.

If sample data also changed, use
:py:meth:`~mspasspy.db.database.Database.update_data`.  That method currently
updates sample data in GridFS and also accepts :code:`data_tag`; those features
are not arguments of :code:`update_metadata`.
