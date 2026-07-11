.. _importing_data:

Importing Data
=================
Overview
~~~~~~~~~~~~
MsPASS is a data processing system, so the first step is to assemble the data
set you want to process.  The focus of our initial development was on tools
for assembling data acquired from the `FDSN (Federated Digital
Seismic Network) <https://www.fdsn.org/>`__
data centers.  FDSN adopted the
`Standard for the Exchange of Earthquake Data (SEED) <https://www.fdsn.org/seed_manual/SEEDManual_V2.4.pdf>`__
format in the 1980s.  A data-record subset of SEED, commonly called miniSEED,
became the standard waveform exchange format at FDSN data centers.  miniSEED
is a compressed data format with
minimal metadata that has proven useful for data distribution because it
creates compact files with lossless compression.

FDSN data centers have largely shifted from distributing files by FTP to web
services.  FDSN web services
distribute three fundamentally different data types:
(1) single-channel sample data delivered as miniSEED,
(2) station metadata delivered with a standard format called
`StationXML <https://www.fdsn.org/xml/station/>`__, and
(3) earthquake source data delivered through a standard format called
`QuakeML <https://quake.ethz.ch/quakeml/>`__.
Fortunately for our development efforts a well-developed solution for
acquiring all three data types from FDSN sources was already available in
ObsPy.  The sections below assume you can consult ObsPy's
documentation to design the details of how you acquire these data from
FDSN data centers.   The focus here is how files stored locally from a
web service request can be assimilated into MsPASS and validated for
processing.

This section also includes a brief discussion of importing data with
formats other than SEED/miniSEED.  Current capability is largely controlled
by the extensive set of readers that already exist in ObsPy.  The primary
issue you face in importing other formats is handling the metadata name
mismatch that is inevitable in dealing with other formats.

FDSN data
~~~~~~~~~~~~~

Importing miniSEED Data
----------------------------
From the MsPASS perspective, the best-supported mechanism to obtain data from
the FDSN federation of data centers is to use ObsPy.
Two common ObsPy interfaces can fetch miniSEED data from FDSN data centers.

#.  The simplest tool ObsPy provides is the :code:`get_waveforms`
    method of its FDSN web-service client.  The documentation for that
    function can be found
    `here <https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.client.Client.get_waveforms.html>`__.
    :code:`get_waveforms` retrieves waveforms selected by station codes and a
    time range.  It is suitable for small requests or for a custom acquisition
    program that explicitly manages many requests.
#.  For larger, multi-provider acquisitions, ObsPy supplies the
    :code:`MassDownloader` class.  Its :code:`download` method writes miniSEED
    waveform files and StationXML metadata files to caller-selected storage.
    An overview of the mass downloader is given
    `here <https://docs.obspy.org/tutorial/code_snippets/retrieving_data_from_datacenters.html>`__
    and its API is documented
    `here <https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.mass_downloader.mass_downloader.MassDownloader.html>`__.

The ObsPy tools are well documented and relatively easy to use.  Users
are referred to the above pages and examples to build a workflow to
retrieve a working data set.   We do, however, think it useful to
give a few warnings.

#.  Each :code:`get_waveforms` call incurs a network request and server work.
    Thousands of small calls can therefore be much slower and less robust than
    a batched acquisition.
#.  :code:`MassDownloader.download` makes large acquisitions practical, but
    elapsed time still depends on provider load, requested volume, retry
    behavior, and network throughput.  Large data sets can take days to
    acquire.
#.  No web-service acquisition should be assumed complete merely because the
    client returned.  Requests can fail or return no data for individual time
    ranges.  Retain the downloader logs and verify the local files against the
    intended request before indexing them into MsPASS.

Data-center services and recommended access paths change.  Check the current
provider and ObsPy documentation when building an acquisition workflow.  MsPASS
also contains specialized cloud and FDSN indexing methods; consult their
current API documentation before using them.  Historical examples of the
project's S3/AWS work
can be found `here <https://github.com/mspass-team/mspass/tree/master/scripts/aws_lambda_examples>`__.
Those examples and the cloud-oriented methods should be treated as specialized
interfaces rather than substitutes for the local-file workflow below.

Indexing, reading, and optionally converting miniSEED
------------------------------------------------------

Downloading a miniSEED file does not by itself expose its waveforms to MsPASS.
The standard local-file import path begins with
:py:meth:`index_mseed_file<mspasspy.db.database.Database.index_mseed_file>`.
This method scans each file and writes one or more index documents to the
``wf_miniseed`` collection.  It does **not** copy or convert the waveform
samples; the documents retain the absolute directory, file name, byte offset,
byte count, channel codes, and time span needed to read the original file.

For example, this serial loop indexes every ``.mseed`` file in one directory:

.. code-block:: python

    from pathlib import Path

    from mspasspy.db.client import DBClient
    from mspasspy.db.database import Database

    db = Database(DBClient(), "mydatabase")
    data_directory = Path("/absolute/path/to/mseed")

    for path in sorted(data_directory.glob("*.mseed")):
        db.index_mseed_file(
            path.name,
            dir=str(data_directory),
            collection="wf_miniseed",
        )


Files should contain miniSEED packets ordered by network, station, location,
channel, and time.  With the current default ``segment_time_tears=True``, a
time discontinuity also starts a new index document.  For distributed work,
the absolute path recorded by the index must resolve to the same file on every
worker that reads it.

An index document can be read as a MsPASS ``TimeSeries`` without first
rewriting the compressed samples:

.. code-block:: python

    query = {}  # Replace with a MongoDB query to select a subset.
    for doc in db["wf_miniseed"].find(query):
        datum = db.read_data(doc, collection="wf_miniseed")
        if datum.live:
            # Process datum here, or persist a native MsPASS copy:
            db.save_data(datum, collection="wf_TimeSeries")


:py:meth:`read_data<mspasspy.db.database.Database.read_data>` uses the stored
index to read only the referenced byte range.  Calling
:py:meth:`save_data<mspasspy.db.database.Database.save_data>` is optional: use
it when the workflow should persist a converted/native copy, and omit it when
processing can begin directly from ``wf_miniseed``.  Normalization with
receiver and source metadata is a separate step described in
:ref:`normalization`.

.. warning::

   In the current implementation, ``index_mseed_file(normalize_channel=True)``
   computes a channel match after inserting the index records but does not
   persist the resulting ``channel_id`` to those records.  Leave
   ``normalize_channel=False`` (the default) and normalize the
   ``wf_miniseed`` collection separately.

Assembling Receiver Metadata
----------------------------------

Receiver metadata from FDSN sources is easily obtained and assimilated
into MsPASS using a combination of ObsPy functions and methods of the
:py:class:`Database<mspasspy.db.database.Database>` class.

FDSN receiver metadata is obtainable through one of two fundamentally different
approaches:  (1) the older "dataless SEED" file format, and (2) the
XML-based format transmitted through web services called StationXML.
Both are FDSN standards.  Dataless SEED is a legacy format for which MsPASS
does not provide a dedicated reader.  ObsPy's :code:`read_inventory` supports
legacy inventory formats as well as StationXML, but StationXML is the
recommended path for a new workflow.

We recommend using ObsPy to assemble receiver metadata from FDSN
data centers.  There are two common routes; choose the one that matches the
way the waveform data were acquired.

#.  The FDSN client has a method called :code:`get_stations` that is
    directly comparable to :code:`get_waveforms`.  It uses web
    services to download metadata for one or more channels of data.  It has
    a set of search parameters used to define what is to be retrieved that
    is usually comprehensive enough to fetch what you need in no more than
    a few calls.  It retrieves the results into memory as a
    custom ObsPy data object called an :code:`Inventory`.  The docstring
    for an :code:`Inventory` object can be found
    `here <https://docs.obspy.org/packages/autogen/obspy.core.inventory.html?highlight=inventory>`__.
    An :code:`Inventory` object can be viewed as more or less a
    StationXML file translated into a Python data structure with a
    few added decorations (e.g. plotting).   We return to this point
    below when we discuss how these data are imported to MsPASS.
#.  When you use :code:`MassDownloader.download`, you have the option of
    having that method download station metadata and save the
    actual StationXML data files retrieved from web services.  The resulting
    files can then be read with the :code:`read_inventory` function
    described `here <https://docs.obspy.org/packages/autogen/obspy.core.inventory.inventory.read_inventory.html>`__.

Both approaches create an ObsPy
:code:`Inventory` object in Python:  for (1) that is the return of the
function while for (2) it is the output of a call to :code:`read_inventory`
with :code:`format="STATIONXML"`.  We use the ObsPy :code:`Inventory` object
as an intermediary for storing receiver metadata in a MongoDB database.
The :code:`Database` method
:py:meth:`save_inventory<mspasspy.db.database.Database.save_inventory>`
translates an :code:`Inventory` object into documents stored in
what we call the :code:`channel` and :code:`site` collections.   As noted
many other places in our documentation :code:`channel` contains receiver
metadata for :code:`TimeSeries` data while :code:`site` contains a smaller
subset of the same information appropriate for :code:`Seismogram`
data.   A typical application of :code:`save_inventory` can be seen in the
following code fragment:

.. code-block:: python

    from obspy import UTCDateTime
    from obspy.clients.fdsn import Client

    from mspasspy.db.client import DBClient
    from mspasspy.db.database import Database

    client = Client("IRIS")
    db = Database(DBClient(), "getting_started")
    starttime = UTCDateTime("2015-01-01")
    endtime = UTCDateTime("2015-01-02")
    inventory = client.get_stations(
        network="TA",
        starttime=starttime,
        endtime=endtime,
        channel="BH?",
        level="response",
    )
    counts = db.save_inventory(inventory, verbose=False)
    print("save_inventory returned values=", counts)


The returned four-integer tuple reports site documents saved, channel
documents saved, distinct sites processed, and distinct channels processed.
By default, ``save_inventory`` excludes the synthetic network code ``SY``;
pass ``networks_to_exclude=None`` if that network should be imported.

As noted above, an :code:`Inventory` object
is more or less an image of a StationXML file.   StationXML is complete, but
often contains a lot of baggage that is not necessary for most workflows and
would unnecessarily bloat a MongoDB database.  For that reason, in MsPASS
we do not extract the entire contents of the StationXML file image.
As noted in the documentation for :code:`save_inventory`, MsPASS saves
receiver locations, component orientations, and serialized channel/response
data.  If your application requires additional data from the
StationXML image you will need to extract that information from the
:code:`Inventory` object and use the update functions of MongoDB to
add what you need.  MongoDB permits additional key-value pairs, but choose
names and types that do not collide with fields defined by the active MsPASS
schema.

Source Metadata
-------------------

Source metadata are a substantially more complicated problem than receiver
metadata.   The following is a litany of the complexity we needed to
deal with in a generic framework like MsPASS that could support all
forms of data seismologists deal with.

#.   What defines source metadata is as wildly variable as anything
     we can think of.   Some methods like noise correlations or
     studies of noise do not require any source information.
     Even when source information is required the attributes
     required are not fixed.   Some data require only coordinates,
     but the coordinates may be geographic or some local coordinate
     system.   Some, but not all data need moment tensor estimates.
     The list continues.  The complete flexibility of MongoDB in
     defining what attributes are loaded as the source "document"
     effectively solves this problem.
#.   With some data there is one and only one source estimate for
     each datum.   The type example is seismic reflection data
     where the shot coordinates are defined with standard "geometry"
     attributes.   Natural source data often have multiple, competing
     estimates of source metadata for the same "event".  The CSS3.0
     schema, for example, handles this issue by defining two relational
     database tables called *event* and *origin* with the concept that
     an *event* is a unique source while an *origin* is one of multiple
     possible source estimates for a given *event*.   Although the
     flexibility of MongoDB could provide a workable solution
     for the multiple origin problem (the likely solution would involve subdocuments)
     we chose to not add that complexity to MsPASS.  At present we
     assume that when using the source collection to define source
     metadata a given waveform will be associated with one and only one
     source document.
#.   There are large variations in the complexity of the problem of
     associating a seismic datum to a set of (document) source
     metadata.   That problem is trivial with seismic reflection data
     compared to most natural source data.  Until recently all
     seismic reflection data was naturally collected as "common shot (source) gathers".
     Most seismic reflection geometry definitions simply require an
     ordered list that defines the order of gathers in a linear data file.
     The same issue is much more complex with passive recording. A partial
     list includes:  (1) irregular sample rate, (2) irregular start times,
     (3) there may or may not be a need to compute or use a set of
     phase arrival times, and (4) overlapping, duplicate copies of the same
     data in multiple input data files.   Because of the complexity of this
     problem we provide only a partial set of tools for associating
     waveforms with source data:  MongoDB normalization described in the section
     of this user's manual titled
     :ref:`normalization`.

MsPASS directly supports one primary mechanism for loading FDSN source
metadata.  It is similar to the way we handle station data: use ObsPy to
download data from FDSN web services and translate the ObsPy Python
data structure, which in this case is called a :code:`Catalog`, into
MongoDB source documents.

ObsPy provides two relevant operations for retrieving or loading source
metadata.

#.  :code:`get_events` is an ObsPy FDSN-client method that is very similar to the
    receiver equivalent :code:`get_stations` noted above.
    Their documentation on this function can be found
    `here <https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.client.Client.get_events.html>`__.
    Like the receiver equivalent it has search criteria to yield a set of source data
    based on some spatial, time, magnitude, and/or other criteria.
    In addition, like :code:`get_stations`, :code:`get_events` returns the
    result in a Python data structure called a :code:`Catalog`.
    The :code:`Catalog` class is more or less an image of the FDSN standard
    for web service source data in XML format they call :code:`QuakeML`.
    The biggest issue with this approach for many workflows is that
    it is too easy to create a collection of source data that is much
    larger than the number of events actually in the data set.

#.  :code:`read_events` reads a saved event file, including QuakeML, into a
    :code:`Catalog`.  Contrary to an older version of this page,
    :code:`MassDownloader.download` does not create QuakeML files; its storage
    outputs are miniSEED and StationXML.  Query the event service separately
    with :code:`get_events`, and optionally persist that catalog as QuakeML.

The procedure is comparable to the StationXML import described above: an
ObsPy object is the intermediary.  :code:`get_events` returns a
:code:`Catalog` directly, while :code:`read_events` creates one from a local
file.  The latter is documented
`here <https://docs.obspy.org/packages/autogen/obspy.core.event.read_events.html>`__.
A :code:`Catalog` instance can then be saved to a MongoDB source collection
using
:py:meth:`save_catalog<mspasspy.db.database.Database.save_catalog>`.
For a local QuakeML file, the complete import is:

.. code-block:: python

    from obspy import read_events

    catalog = read_events("events.xml", format="QUAKEML")
    number_saved = db.save_catalog(catalog, verbose=False)
    print("source documents saved=", number_saved)


``save_catalog`` inserts every event; it does not deduplicate the ``source``
collection.  The current implementation also requires each event to have both
a preferred origin and a preferred magnitude.  Validate those values before
calling it, especially for catalogs assembled from non-FDSN sources.

An alternative for which we provide limited support is importing catalog
data exported from an Antelope/CSS3.0 database.  The module
:code:`mspasspy.preprocessing.css30.dbarrival` remains in the source tree but
should be treated as prototype code.  It includes
:py:func:`load_css30_sources<mspasspy.preprocessing.css30.dbarrival.load_css30_sources>`
and related arrival helpers.  The approach used in the prototype
is independent of the relational database system used for managing the
source data.   That is, the approach is to drive the processing with a
table defined as a text file.  The same conceptual approach could be used as the
export of a query of any relational database that is loaded internally
as a pandas DataFrame.


Importing Other Data Formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently MsPASS relies on ObsPy for importing most waveform formats other
than miniSEED.  The following example converts every ObsPy ``Trace`` in each
input ``Stream`` to a member of a MsPASS ``TimeSeriesEnsemble`` and saves the
members to ``wf_TimeSeries``:

.. code-block:: python

    from pathlib import Path

    from obspy import read

    from mspasspy.db.client import DBClient
    from mspasspy.db.database import Database
    from mspasspy.util.converter import Stream2TimeSeriesEnsemble

    db = Database(DBClient(), "mydatabasename")
    data_directory = Path("/absolute/path/to/sac_files")
    for filename in sorted(data_directory.glob("*.sac")):
        stream = read(str(filename), format="SAC")
        ensemble = Stream2TimeSeriesEnsemble(stream)
        db.save_data(ensemble, collection="wf_TimeSeries")

Replace :code:`SAC` and the file pattern with another keyword and naming
convention from the list of ObsPy supported formats found
`here <https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html#supported-formats>`__
and
:py:func:`Stream2TimeSeriesEnsemble<mspasspy.util.converter.Stream2TimeSeriesEnsemble>`
performs the one-Trace-to-one-TimeSeries conversion.  For a format whose
headers need special treatment, wrap that converter in a format-specific
function that validates and repairs Metadata before saving.  If the intended
output is a three-component ``Seismogram`` rather than three independent
``TimeSeries`` objects, use the three-component conversion/bundling workflow
described in :ref:`obspy_interface`.
See :ref:`data_object_design_concepts` for the corresponding differences
between the ObsPy and MsPASS data containers.

There are two different issues one faces in converting an external format to
the instance of an implementation of a seismic data object like TimeSeries or
TimeSeriesEnsemble:

#.  The sample-data vectors may require conversion from various binary
    structures to the internal vector format (in our case IEEE doubles).
    The approach we advocate here solves that problem by not reinventing a
    wheel already implemented by ObsPy.  If you need to convert a large
    quantity of data in an external format it may prove necessary to
    measure and optimize that step, but first establish with profiling that
    the ObsPy reader is the bottleneck.
#.  Every format has a different header structure with few, if any, overlaps in
    namespace (the keys and value types defining each concept).  ObsPy readers
    expose format-specific headers differently.  That variance is why a
    format-specific validation/translation function is often needed around
    the generic converter shown above.

The Metadata schema definitions provide tools for translating names.  In
particular,
:py:meth:`apply_aliases<mspasspy.db.schema.SchemaDefinitionBase.apply_aliases>`
renames canonical MsPASS keys to caller-specified aliases and records those
aliases in that schema object.  The inverse
:py:meth:`clear_aliases<mspasspy.db.schema.SchemaDefinitionBase.clear_aliases>`
restores registered aliases to canonical names.  For importing external
headers, define the external names as aliases in the YAML schema (or register
them with ``add_alias``), then use ``clear_aliases`` on the converted Metadata.

We close this section by emphasizing that MsPASS does not provide dedicated,
fully tested import pipelines for every format supported by ObsPy.  This is an
area where the user community can help expand MsPASS.  If you develop
a robust format-specific conversion function, we encourage you to contribute
the implementation and tests to the MsPASS repository.

Validating an Imported Data Set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
After importing data into MsPASS, especially through a format-specific
conversion, run the installed ``mspass-dbverify`` command on the resulting
collection.  Run one test per invocation.  For a native waveform collection,
for example:

.. code-block:: console

    mspass-dbverify mydatabase -c wf_TimeSeries -t required
    mspass-dbverify mydatabase -c wf_TimeSeries -t schema_check


The ``required`` test has built-in defaults for ``wf_TimeSeries`` and
``wf_Seismogram``.  For a raw ``wf_miniseed`` index, supply the required keys
explicitly, then run the schema check:

.. code-block:: console

    mspass-dbverify mydatabase -c wf_miniseed -t required -r npts delta starttime dir dfile foff nbytes
    mspass-dbverify mydatabase -c wf_miniseed -t schema_check


Resolve missing required values and type mismatches before launching a large
workflow.  The command also supports a ``normalization`` test after receiver
or source cross-references have been added; run ``mspass-dbverify --help`` for
the current options.
