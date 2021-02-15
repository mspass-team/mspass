.. _CRUD_operations:

CRUD operations in MsPASS
==============================

Overview
~~~~~~~~~~~
The acronymn CRUD is often used as a mnemonic in books and online tutorials
teaching database concepts.  CRUD stands for Create-Read-Update-Delete.
It is usful because it summarizes the four main things any database system
must accomplish cleanly.  This section is organized into subsections on
each of the topics defined by CRUD.  At the end of this section we
summarize some common options in CRUD operations at the end of this section.

The most common database operations are defined as methods in a class
we give the obvious name Database.  Most MsPASS jobs need to the have following
incantation a the top of the python job script:

.. code-block:: python

   from mspasspy.db.client import Client
   from mspasspy.db.database import Database
   dbclient=Client()
   db=Database(dbclient,'database_name')

where the second argument on Database, 'database_name',
is a name you chose for the dataset you are working with.
For the remainder of this section we will use the symbol "db" as
defined, but as in any programming language you need to recognize the
symbol can be anything you as the user find sensible. "db" is just our
choice.

The default schema for a Database assumes the data set is a set of
TimeSeries object.  That implies the dataset is defined in the
collection we call *wf_TimeSeries*.   If the dataset is already
assembled into three component bundles (*Seismogram* objects)
the Database constructor needs to be informed of that through this
alternative construct:

.. code-block:: python

   from mspasspy.db.client import Client
   from mspasspy.db.database import Database
   dbclient=Client()
   db=Database(dbclient,'database_name',db_schema='wf_Seismogram')

If your workflow requires reading both TimeSeries and Seismogram
data, you should create two handles with one using the wf_TimeSeries
and the other using the wf_Seismogram schema.

Create
~~~~~~~~~~

We tag all methods of the Database class that do "Create" operations with
the synonymous word "save".   Here we list all save methods with a brief
description of each method.  We refer to the docstring pages for detailed
(and most up to date) usage:

1.  *save_data* is probably the most common method you will use.  The
    first argument is one of the atomic objects defined in MsPASS
    (Seismogram or TimeSeries) that you wish to save.  Options are
    described in the docstring.  Here is an example usage:

    .. code-block:: python

       from obspy.core.stream import read
       from mspasspy.util.converter import Trace2TimeSeries
       # Example assumes filename contains one channel of data in
       # miniseed format
       filename='testfile.msd'
       dstrm=read(filename,format='MSEED')
       # A stream is a list of obspy Trace objects - get the first
       dtr=dstrm[0]
       # This mspass function converts a Trace to our native TimeSeries
       d=Trace2TimeSeries(dtr)
       db.save_data(d)

    By default *save_data* stores all Metadata except those linked to
    normalized collections (*source, channel*, and *site*) with no
    safety checks.  We discuss the alternatives in a later section
    so the reader can first get a broader perspective.

2.  *save_ensemble_data*  is similar to *save_data* except the first argument
    is an Ensemble object.  There are currently two of them:  (1) TimeSeriesEnsemble
    and (2) SeismogramEnsemble.   As discussed in detail elsewhere an Ensemble
    is a generalization of the idea of a "gather" in seismic reflection processing.
    The *save_ensemble_data* method is a convenience class for saving Ensembles.
    Ensembles are containers of atomic objects.  *save_ensemble_data*
    is mostly a loop over the container saving the atomic objects it contains
    to the wf_TimeSeries (for TimeSeriesEnsembles) or wf_Seismogram
    (for Seismogram objects).

3.  *save_catalog* should be viewed mostly as a convenience method to build
    the *source* collection from QUAKEML data downloaded from FDSN data
    centers via obspy's web services functions.   *save_catalog* can be
    thought of as a converter that translates the contents of a QUAKEML
    file or string for storage as a set of MongoDB documents in the *source*
    collection.  We use obspy's *Catalog* object as an intermediary to
    avoid the need to write our own QUAKEML parser.   As with save_data
    the easiest way to the usage would be this example derived from
    our *getting_started* tutorial.

    .. code-block:: python

     from obspy import UTCDateTime
     from obspy.clients.fdsn import Client
     client=Client("IRIS")
     t0=UTCDateTime('2011-03-11T05:46:24.0')
     starttime=t0-3600.0
     endtime=t0+(7.0)*(24.0)*(3600.0)
     lat0=38.3
     lon0=142.5
     minlat=lat0-3.0
     maxlat=lat0+3.0
     minlon=lon0-3.0
     maxlon=lon0+3.0
     minmag=6.5
     cat=client.get_events(starttime=starttime,endtime=endtime,
         minlatitude=minlat,minlongitude=minlon,
         maxlatitude=maxlat,maxlongitude=maxlon,
         minmagnitude=minmag)
     db.save_catalog(cat)

    This particular example pulls 11 large aftershocks of the 2011 Tohoku
    Earthquake.

4.  *save_inventory* is similar in concept to *save_catalog*, but instead of
    translating data for source information it translates information to
    MsPASS for station metadata.  The station information problem is slightly
    more complicated than the source problem because of an implementation
    choice we made in MsPASS.   That is, because a primary goal of MsPASS
    was to support three-component seismograms as a core data type, there
    is a disconnect in what metadata is required to support a TimeSeries
    versus a Seismogram object.   We handle this by defining two different,
    but similar MongoDB collections:  *channel* for TimeSeries data and
    *site* for Seismogram objects.  The name for this method contains the
    keyword "inventory" because like *save_catalog* we use an obspy
    python class as an intermediary.  The reasons is similar; obspy had
    already solved the problem of downloading station metadata from
    FDSN web services with their
    `read_inventory function <https://docs.obspy.org/packages/obspy.core.inventory.html>`__.
    As with *save_catalog* *save_inventory* can be thought of as a translator
    from data downloaded with web services to the form needed in MsPASS.
    It may be helpful to realize that Obspy's Inventory object is actually
    a python translation of the data structure defined by the
    `FDSN STATIONXML<https://www.fdsn.org/xml/station/>`__
    standardized format defined for web service requests for station metadata.
    Like *save_source* an example from the getting started tutorial
    should be instructive:

    .. code-block:: python

       inv=client.get_stations(network='TA',starttime=starttime,endtime=endtime,format='xml',channel='*')
       db.save_inventory(inv)

    This example extracts all stations with the "network code" of "TA"
    (the Earthscope transportable array).  A complication of station
    metadata that differs from source data is that station metatdata is
    time variable.  The reason is that sensors change, three-component sensors
    are reoriented, digitizers change, etc.  That means station metadata
    have a time span for which they are valid that has to be handled to
    assure we associate the right metadata with any piece of data.

    In MsPASS we translate the STATIONXML data to documents stored in two
    collections:  *site* and *channel*.  Both collections contain the
    attributes *starttime* and *endtime* that define the time interval for which
    that document's data are valid.  *site* is simpler.  It mainly contains
    station location data defined with three standard attribute keys:
    *lat*, *lon*, and *elev*.  We store all geographic coordinates (i.e. lat and lon)
    as decimal degrees and elevation (elev) in km.   The *channel* collection
    contains station location information but it also contains two additional
    important pieces of metadata:  (1) orientation information defined by
    the keys *hang* and *vang*, and (2) full response information.
    We store response data in MongoDB as a pickle image of the data stored
    in the STATIONXML data as translated by obspy.   In the read section
    below we describe how to retrieve response data from *channel*.

Read
~~~~~~~

A *read* operation is the inverse of save (create).  The core readers were
design to simplify the process of reading the core data types of MsPASS:  TimeSeries
and Seismogram.  There are also convenience functions for reading ensembles.
As with the save operators we discuss here the key methods, but refer the
reader to the sphynx documentation for full usage.

1.  *read_data* is the core method to read atomic data.  The method has
    one required argument.  That argument can be either a MongoDB object
    id or a MongoDB document retrieved from one of the "wf" collections.
    The document version is actually only a convenience.  The actual
    database operation always uses the unique ObjectID for the document used
    to define the read operation.  (i.e. the first thing read_data does if
    if finds arg 0 is a document is to extract the "_id" attribute and use
    it to query MongoDB.  If arg 0 is an ObjectID the parsing of the doc is bypassed and
    the query is performed immediately with id passed.)

    *read_data* will use the waveform collection defined when the Database
    handle was constructed.  That is, a given Database handle should only
    be used for the same type of atomic object (TimeSeries or Seismogram).
    If a workflow uses both data types we recommend creating two handles with
    different names.  For interactive work one can also use the
    *set_database_schema* to switch.   Avoid frequent calls to
    *set_database_schema* for handling mixed data as it is a relatively
    expensive (in time) operation that involkes reading of a file defining
    the schema.

    The data objects in MsPASS are stored internally as C++ objects with
    multiple elements illustrated in the figure below.   Although these
    objects can conceptually be thought of as a single entity the individual
    parts are handled differently because they define different concepts
    and are subject to different read, write, and storage rules.  The
    following figure illustrates this fragmentation:

    insert figure here

    The key point of this figure is that the waveform data is treated differently
    from the Metadata and two auxiliary items we call ProcessingHistory and the
    error log (elog).  Waveform data is currently stored either internally in
    MongoDB's gridfs storage or in external files.  The wf collection for
    the data type being read (wf_TimeSeries or wf_Seismogram) stores only
    data we store as Metadata.  A more extensive discussion of Metadata and
    how we use it can be found `here <https://wangyinz.github.io/mspass/user_manual/data_object_design_concepts.html>`__.
    That section also gives details about ProcessingHistory and the error
    log and the reasons they are part of MsPASS.

    By default *read_data* reads Metadata in what we call "promiscuous" mode.
    That means it takes in all metadata stored in the wf collection arg 0
    defines with no type checking or filtering.  Alternatives are "cautious"
    and "pedantic".   Both of the later enforce the type and name constraints defined
    by the schema.   The difference is that in "pedantic" mode any
    conflicts in data type stored versus what is expected will cause the
    return to be marked dead.  In "cautious" mode the reader will attempt
    to convert any mismatched types and mark the return dead only if the
    conversion is not possible (e.g. a string like "xyz" cannot normally
    be converted to an integer and a python list cannot be converted to
    a float.)  Guidelines for how to use these different modes are:

    1.  Use "promiscuous" mode when the wf collection to be read is known
        to be clean.  That mode is the default because it is faster to
        run because all the safeties are bypassed.  The potential cost is that
        some members of the data set could be killed on input.
        That potential problem can normally be eliminated by running the
        *clean* method described in a section below.
    2.  Use "cautious" for data saved without an intervening *clean*
        operation, especially if the workflow contains an experimental
        algorithm.
    3.  The "pedantic" mode is mainly of use for data export where a
        type mismatch could produce invalid data required by another package.

2.  A closely related function to *read_data* is *read_ensemble_data*.  Like
    *save_ensemble_data* it is mostly a loop to assemble an ensemble of
    atomic data using a sequence of calls to *read_data*.  The sequence of
    what to read is defined by arg 0 that can be one of two things:  (a) a
    python list of ObjectIds of documents in a wf collection that define
    the ensemble, or (b) a MongoDB cursor object returned by a query that
    defines the ensemble.  The second option should be clearer with this
    example showing how to read a TimeSeriesEnsemble that is a generalized
    shot gather.  This fragment assumes the symbol source_id was set above
    by some other mechanism.  e.g. it could be extracted from a loop over
    the *source* collection (or query the source collection) aimed at
    processing all (or a subset) of the sources defined there.  Notice we
    also include a size check with the MongoDB function count_documents
    to impose constraints on the query. That is always good practice.

    .. code-block:: python

     query={"source_id" : source_id}
     ndocs=db.wf_TimeSeries.count_documents(query)
     if ndocs == 0:
       print("No data found for source_id=",source_id)
     elif ndocs > TOOBIG:
       print("Number of documents matching source_id=",source_id," is ",ndocs,
         "Exceeds the internal limit on the ensemble size=",TOBIG)
     else:
       cursor=db.wf_TimeSeries.find(query)
       ens=db.read_ensemble_data(cursor)

3.  A workflow that needs to read and process a large data sets in
    a parallel environment should use
    the parallel equivalent of *read_data* and *read_ensemble_data* called
    *read_distributed_data*.  MsPASS supports two parallel frameworks called
    SPARK and DASK.   Both abstract the concept of the parallel data set in
    a container they call an RDD and Bag respectively.   Both are best thought
    of as a handle to the entire data set that can be passed between
    processing functions.   The *read_distributed_data* method is critical
    to improve performance of a parallel workflow.  The use of storage
    in MongoDB's gridfs in combination with SPARK or DASK
    are known to help reduce io bottlenecks
    in a parallel environment.  SPARK and DASK have internal mechanisms to schedule
    IO to optimize throughput, particularly with reads made through the gridfs
    mechanism we use as the default data storage.  *read_distributed_data*
    provides the mechanism to accomplish that.

    *read_distributed_data* has a very different call structure than the
    other seismic data readers.  It is not a method of Database, but a
    separate function call.  The input to be read by this function is
    defined by arg 2 (C counting starting at 0).  It expects to be passed a
    MongoDB cursor object, which is the standard return from the database
    find operation.   As with the other functions discussed in this section
    a block of example code should make this clearer:

    .. code-block:: python

      from mspasspy.db.client import Client
      from mspasspy.db.database import Database,read_distributed_data
      dbclient=Client()
      # This is the name used to acccess the database of interest assumed
      # to contain data loaded previously.  Name used would change for user
      dbname='distributed_data_example'  # name of db set in MongoDB - example
      db=Database(dbclient,dbname)
      # This example reads all the data currently stored in this database
      cursor=db.wf_TimeSeries.find({})
      rdd0=read_distributed_data(dbclient,dbname,cursor)

    The output of the read is the SPARK RDD that we assign the symbol rdd0.
    If you are using DASK instead of SPARK you would add the optional
    argument *format='dask'*.

Update
~~~~~~~~

Because of the way we stored seismic data in MsPASS (see figure above)
the concept of an update makes sense only for Metadata.
The update concept makes no sense for ProcessingHistory and error log data.
Hence, the history and elog collections, that hold that data, should never
be updated.   No MsPASS supported algorithms will do that, but we
emphasize that constraint because you as the owner of the dataset could
(incorrectly) modify history or elog with calls to MongoDB's api.

As noted elsewhere Metadata loaded with data objects in MsPASS can come
from one of two places:  (1) attributes loaded directly with the atomic data from
the unique document in a wf collection with which that data is associated,
and (2) "normalized" data loaded through a cross reference ID from one of the
three standardized collection in MsPASS (*site, channel*, and *source*).
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
are treated on writes or updates depends on the setting of the *mode* argument
common to all update and write methods described in more detail in a section
below.

Delete
~~~~~~~~~
A delete operation is much more complicated in MsPASS that what you would
find as a type example in any textbook an database theory.  In a
relational environment delete normally means removing a single tuple.
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
  needed it is the user's responsibility to assure this does not leaving
  unresolved cross-references to waveform data.
* **Delete Rule 3**:  Deletes of waveform data, wf collections, history,
  and error log data are best done through the mspass Database
  handle.  Custom cleanup is an advanced topic that must be handled
  with caution.

We trust rules 1 and 2 require no further comment.  Rule 3, however,
needs some clarification to understand how we handle deletes.
A good starting point is to look at the signature of the simple delete
method of the Database class:

.. code-block:: python

  def delete(self,id,remove_unreferenced_files=False,
                      clear_history=True,clear_elog=True):)

As with the read methods id can be either an ObjectID referencing a
document in one of the relevant wf collection or a document that contains
that ObjectID.  Similarly, the idea of the *clear_history* and *clear_elog*
may be apparent from the name.  When true all documents linked to the
waveform data being deleted in the history and elog collections (respectively)
will also be deleted.  If either are false debris can be left behind
in the elog and history collections.

The main complexity in this method is behind the boolean argument with the name
*remove_unreferenced_files*.  First, recognize this argument is completely
ignored if the waveform data being referenced is stored internally in
MongoDB (the default) in the gridfs file system.  In that situation delete
will remove the sample data as well as the document in wf that id defines.
The complexity enters if the data are stored as external files.  The
simple delete method of Database is an expensive operation that should be
avoided within a workflow or on large datasets.  The reason is that
each call for deleting an atomic object (defined by id) requires a
second query to the wf collection involved to search for any other
data with an exact match to two attributes we used to define a
single data file:  *dir* which is a directory name and *dfile* which is the
name of the file at leaf node of the file system tree.  (CSS3.0 users
are familiar with these attribute names.  We use the same names as the concept here
is identical to the CSS3.0's use.)  Only when the secondary query finds
no matching values for *dir* and *dfile* will the file be deleted.

For large datasets stored in external files we provide a secondary cleanup
mechanism through a special class we call the *staged_file_manager*.
The constructor for a *staged_file_manager* requires the Database handle
that it should use to manage file data.  The delete method for
the *staged_file_manager* runs a four step algorithm:

1.  Extract the *dir* and *dfile* fields from the document from wf to
    be staged for file deletion.
2.  Build the path description for the referenced file as dir+"/"+dfile.
3.  Push the result to a set container cached in the *staged_file_manager*
    class instance.  A set only adds a new element if the string that path
    defines differs from an existing element.
4.  Delete the document in the wf collection defined by the input id.

To use this feature you will need to write a custom cleanup function in
python.  That cleanup function should call the delete method of *staged_file_manager*
in a loop.   When the function should call
the *delete_unreferenced_files* method of *staged_file_manager*.   That method
queries for any residual matches for *dir* and *dfile*.  Only if there are
no remaining matches will the referenced file be deleted.   Once removed the
entry for that file is removed from the internal set container.
This algorithm allows for interwoven calls to the *delete* method and
the *deleted_unreferenced_files* method.  That allows for multiple cleanup
stages during a workflow to help manage memory.

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
Metadata, sample data, error log, and (processing) history.  The subatomic
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
    that it has a more rigid structure;  TimeSeries wavformdata are universally
    stored as a vector, and Seismogram data are most rationally (although not
    universally) stored as a matrix.  All modern computer systems have
    very efficient means of moving contiguous blocks of data from storage to
    memory so reading waveform data is a very different problem than
    reading Metadata when they are fragmented as in MsPASS. Note that
    traditional waveform handling uses a fixed format with a header and
    data section to exploit the efficiency of reading contiguous memory blocks.
    That is why traditional formats like SAC and SEGY have a fixed header/data
    sections that define "the data".   To make MsPASS generic that paradigm
    had to be broken so it is important to recognize in MsPASS
    waveform data are completely disaggreted from the other data components
    we use for defining TimeSeries and Seismogram objects.
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
that are handled differently:
1.  An attribute can be marked *read_only* in the schema.   As the
    name implies that means they are never expected to be altered in a
    workflow.
2.  A special form of *read_only* attributes are attributes loaded by
    readers from normalized collections.  Such attributes are never saved
    by atomic object writers and the normalized collection (i.e. source, site,
    and channel) are always treated as strinctly read only.
3.  Normalization requires a cross-referencing method.   In MsPASS we
    always uses the ObjectID of the document in the normalizing collection
    and store that attribute with a key with a common structure:
    *collection_id* where "collection" is a variable and "_id" is literal.
    (e.g. the linking key for the source collection is "source_id").
    We use that approach because in MongoDB an ObjectID is guaranteed to
    provide a unique index.   That allows the system be more generic.
    Hence, unlike FSDS data centers that depend upon the SEED format in
    MsPASS net, sta, chan, loc are baggage for joining the site or channel
    collections to a set of waveform data.  We have functions for
    linking seed data with net, sta, chan, and loc keys to build links
    but the links are still defined by ObjectIDs.   An example of why this
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
    bad (dead) and the waveform data will not be saved.

Save Concepts
----------------
Waveform save methods begin with this axiom:  a save operation should
never abort for anything but a system error.   That means the definition of
success is not black and white.  There are a range of known and probably
as yet unknown ways data can acquire inconsistencies that are problems of
varying levels of severity.  Here is the range of outcomes in increasing
order severity:
1.  No problems equal to complete success.
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
    a special collection called "tombstone".
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

Data marked dead are handled specially.  For such data the sample will be
throw away.  The Metadata will be written to a separate collection called
"tombstone".  Error log data linked to dead data are written to the elog
collection along with living data, but the cross-referencing id is
tagged as "tombstone_id".  That provides a simple query mechanism to
show only the most serious errors from a processing run.

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
1.  Construction of TimeSeries or Seismogram objects are driven by
    document data read from the wf_TimeSeries or wf_Seismogram collection
    respectively.   By default the entire contents of each document
    are loaded into Metadata with no safety checks (defined
    above as "promiscuous mode").  Options allow Metadata type checks to be enabled
    against the schema.  In addition, one can list a set of keys that should
    be dropped in the read.
2.  By default normalized Metadata can only be loaded through cross-referencing id
    keys (currently source_id, site_id, and/or channel_id but more may be added).
    The set of which collections are to be loaded are controlled by optional
    parameters in each reader.  An important constraint is that for all
    normalized collections defined as required, if the cross-referencing
    key is no defined a reader will ignore that datum.  *read_data* silently
    signals that condition by returning a None.  *read_ensemble_data* and
    *read_distributed_data* normally silently skip such data.   That model
    is intentional because it allows initial loading of a large data set with
    unresolvable anomalies that prevent one or more of the cross-referencing
    ids from being defined.
3.  The waveform data is read and loaded.  If that process fails the data
    will be marked dead and an error log posted with the reason (e.g. a
    file not found message).
4.  If the sample date read is successful the error log will normally be empty
    after any read.
5.  If processing history is desired the *load_history* option needs to be
    set true.  On read the only action this creates is initialization of the
    ProcessingHistory component of the data with a record providing a unique
    link back to the data just read.

We reiterate that the overall behavior of all readers are controlled by the
"mode=" argument common to all.  The current options are: *promiscuous,
cautious*, and *pedantic*.   Detailed descriptions of what each mean are
give above and in the sphynx documentation generated from docstrings.

Update Concepts
-----------------
As noted above an update is an operation that can be made only to
Metadata saving the image to the related wf collection.  We know of
two common needs for a pure Metadata update without an associated
save of the waveform data.
1.  A processing step that computes something that can be conveniently
    stored as Metadata.  Examples are automated phase pickers and
    amplitude measurements.
2.  Pure Metadata operations.  e.g. most reflection processing systems
    have some form of generic metadata calculator of various levels of
    sophistication.  The most flexible can take multiple Metadata (header)
    values and use them to compute a set a different value.   Such
    operations do not alter the waveform data but may require a
    database update to preserve the calculation.   An example is an
    active source experiment where receiver coordinates can often be
    computed from survey flag numbers or some other independent counter.

Many database updates are standalone operations such as preprocessing to
create entries for attributes like the source_id cross-reference to 
define a link to the right source for each waveform.
