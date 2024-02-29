.. _importing_data:

Importing Data
=================
Overview
~~~~~~~~~~~~
MsPASS is a data processing system so the first step in using
the package for anything is to assemble the data set that you
want to process.   The focus of our initial development has been
tools to assemble data acquired from `FDSN (Federated Digital
Seismic Network) <https://www.fdsn.org/>`__
data centers.  Decades ago FDSN adopted the
`Standard for the Exchange of Earthquake Data (SEED)<http://www.fdsn.org/pdf/SEEDManual_V2.4.pdf>`__
format.   Since then a subset of SEED, commonly called miniseed,
has become the universal tool for distributing earthquake data through
all FDSN data centers.   Miniseed is compressed data format with
minimal metadata that has proven useful for data distribution because it
creates compact files with lossless compression.

In the past decade FDSN data centers have largely shifted from distributing data
by ftp transfers of files to web services.  FDSN web services
distribute three fundamentally different data types:
(1) single channel, sample data delivered as images of miniseed files,
(2) station metadata delivered with a standard format called
`StationXML<https://www.fdsn.org/xml/station/>`__, and
(3) earthquake source data delivered through a standard format called
`QuakeML<https://earthquake.usgs.gov/earthquakes/feed/v1.0/quakeml.php>`__.
Fortunately for our development efforts a well-developed solution for
acquiring all three data types from FDSN sources was already available in
obspy.   The sections below assume you can consult obspy's
documentation to design the details of how you acquire these data from
FDSN data centers.   The focus here is how files stored locally from a
web service request can be assimilated into MsPASS and validated for
processing.

This section also includes a brief discussion of importing data with
formats other than SEED/miniseed.  Current capability is largely controlled
by the extensive set of readers that already exist in obspy.  The primary
issue you face in importing other formats is handling the metadata name
mismatch that is inevitable in dealing with other formats.

FDSN data
~~~~~~~~~~~~~

Importing Miniseed Data
----------------------------
From the MsPASS perspective,
at this point in time the best mechanism to obtain data from
the FDSN confederation of data centers is to use obspy.
Obspy has two different python functions that can be used to
fetch miniseed data from one or more FDSN data centers.

#.  The simplest tool obspy provides is their :code:`get_waveform`
    method of the fdsn web service client.  The documentation for that
    function can be found
    `here<https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.client.Client.get_waveforms.html>`__.
    :code:`get_waveform` retrieves a relatively small number of waveform
    at a time using station codes with wildcards and a time range.
    It is suitable only for small datasets or as a custom agent
    to run for weeks acquiring data driven by some large list.
#.  For most MsPASS users the obspy tool
    of choice is :code:`bulk_download`.
    An overview of the concepts of :code:`bulk_download` are given
    `here<https://docs.obspy.org/tutorial/code_snippets/retrieving_data_from_datacenters.html>`__
    and the detailed docstring for the function can be found
    `here<https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.mass_downloader.html>`__.

The obspy tools are well documented and relatively easy to use users
are referred to the above pages and examples to build a workflow to
retrieve a working data set.   We do, however, think it useful to
give a few warnings.

#.  Never use :code:`get_waveform` to retrieve more than a few thousand
    waveforms unless you are designing an agent that will run for weeks.
    It is intrinsically very slow because of the inevitable
    internet delays that are unavoidable because of the design of that
    function.   Each call to :code:`get_waveform` initiates a query request
    transmitted by the internet to the targeted fdsn web-service server,
    the server has to do it's work and respond, a back connection to the
    caller has to be set up, and then the data transmitted via the internet.
    The built-in, multiple delays make that process too slow for large
    data requests.
#.  The authors of obspy developed their :code:`bulk_download` function
    because their early experience, like ours, showed :code:`get_waveform`
    was not feasible as a tool to download large data sets.
    :code:`bulk_download` makes it feasible to download large data sets.
    Be warned that feasible, however, does not mean quick.   In our experience
    :code:`bulk_download` can sustain a transfer rate around a few Mb/s.
    That is outstanding performance for internet data transfer,
    but keep in mind 1 Tb of data at that rate
    will require of the order of one week to download.
#.  A fundamental flaw of the web service model for data downloading is
    there is no guarantee a request will succeed.  We have seen many examples
    where a large request will have missing data that we know are present
    at the data center.   The reason is that web service is, by design,
    a one way request with no conversation between the client and server
    to guarantee success.  The authors of obspy's :code:`bulk_download`
    method seem to have done some tricks to reduce this problem but
    it is not clear to us if it has been completely solved.

We endorse strongly the following statement in obspy's documentation
on this topic:
"Keep in mind that data centers and web services are constantly changing
so this recommendation might not be valid anymore at the time you read this. "
In particular, we are aware all the FDSN data centers are in the process of
transitioning to a cloud file service model as the next generation of
data access.   It is a current development effort of MsPASS to
provide a simple reader for cloud systems.   Examples of
using our implementation for S3 on AWS
can be found `here<https://github.com/mspass-team/mspass/tree/master/scripts/aws_lambda_examples>`__.
As the word "prototype" implies that api
is likely to change.  Check the docstring api for more recent changes if
you are interested in this capability.

Assembling Receiver Metadata
----------------------------------

Receiver metadata from FDSN sources is easily obtained and assimilated
into MsPASS using a combination of obspy functions and import functions
that are part of the :code:`Database` class (MongoDB handle) of MsPASS.

FDSN receiver metadata is obtainable through one of two fundamentally different
approaches:  (1) the older "dataless SEED" file format, and (2) the
XML-based format transmitted through web services called StationXML.
Both are now FDSN standards.   It is our opinion that dataless SEED is
archaic and we have not devoted development time to supporting the format.
A key reason is that there are multiple, existing solutions to reading dataless
SEED files.   Most notably dataless SEED files can be theoretically be read and
translated with obspy as a format option to the :code:`read_inventory`
function we discuss in more detail below.  We have not tested that
approach, however, and would recommend the much simpler and cleaner
StationXML format.

We recommend users utilize obspy to assemble receiver metadata from FDSN
data centers.   There are two different tools obspy provides.  We have found
both are usually necessary to assemble a complete suite of receiver metadata.

#.  The fdsn client has a method called :code:`get_stations` that is
    directly comparable to :code:`get_waveform`.   It uses web
    services to download metadata for one or more channels of data.  It has
    a set of search parameters used to define what is to be retrieved that
    is usually comprehensive enough to fetch what you need in no more than
    a few calls.   Be warned it retrieves the results into memory into a
    custom obspy data object they call an :code:`Inventory`.  The docstring
    for an :code:`Inventory` object can be found
    `here<https://docs.obspy.org/packages/autogen/obspy.core.inventory.html?highlight=inventory>`__.
    An :code:`Inventory` object can be viewed as more or less a
    StationXML format file translated into a python data structure with a
    few added decorations (e.g. plotting).   We return to this point
    below when we discuss how these data are imported to MsPASS.
#.  When you use the :code:`mass_downloader` you have the option of
    having that function download the station metadata and save the
    actual StationXML data files retrieved from web services.  The resulting
    files can then be read with their :code:`read_inventory` method
    described `here<https://docs.obspy.org/packages/autogen/obspy.core.inventory.inventory.read_inventory.html>`__.

Both of the approaches above can be used to create an obspy
:code:`Inventory` object in python:  for (1) that is the return of the
function while for (2) it is the output of a call to :code:`read_inventory`
with :code:`format="STATIONXML"`.  We use the obspy :code:`Inventory` object
as an intermediary for storing receiver metadata in a MongoDB database.
The :code:`Database` class has a method we call :code:`save_inventory`
described `here<https://www.mspass.org/python_api/mspasspy.db.html#module-mspasspy.db.database>`__.
That method translates an :code:`Inventory` object into documents stored in
what we call the :code:`channel` and :code:`site` collections.   As noted
many other places in our documentation :code:`channel` contains receiver
metadata from :code:`TimeSeries` data while :code:`site` contains a more
subset of the same information more appropriate for :code:`Seismogram`
data.   A typical application of :code:`save_inventory` can be seen in the
following code framgment extracted from our tutorials:

.. code-block:: python

  from mspasspy.db.database import Database
  from mspasspy.db.client import DBClient
  dbclient=DBClient()
  db=Database(dbclient,'getting_started')
  inv=client.get_stations(network='TA',starttime=starttime,endtime=endtime,
                      format='xml',channel='BH?',level='response')
  ret=db.save_inventory(inv,verbose=False)
  print('save_inventory returned values=',ret)

As noted above an :code:`Inventory` object
is more or less an image of a StationXML file.   StationXML is complete, but
often contains a lot of baggage that is not necessary for most workflows and
would unnecessarily bloat a MongoDB database.  For that reason, in MsPASS
we do not extract the entire contents of the StationXML file image.
As noted in the documentation for :code:`save_inventory` we save
receiver locations, component orientations, and a serialized version of the
response data.  If your application requires additional data from the
StationXML image you will need to extract that information from the
:code:`Inventory` object and use the update functions of MongoDB to
add what you need.  As noted many times in this manual MongoDB is
completely cavalier about what is stored in any given document so
adding additional key-value pairs will not break any MsPASS algorithms.

Source Metadata
-------------------

Source metadata is a vastly more complicated problem that receiver
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
     ordered list defines the order of gathers in a linear data file.
     The same issue is much more complex with passive recording. A partial
     list includes:  (1) irregular sample rate, (2) irregular start times,
     (3) there may or may not be a need to compute or use a set of
     phase arrival times, and (4) overlapping, duplicate copies of the same
     data in multiple input data files.   Because of the complexity of this
     problem we provide only a partial set of tools for associating
     waveforms with source data:  MongoDB normalization described in the section
     of this user's manual titled
     :ref:`database_normalization`.

MsPASS currently supports directly only one mechanism for loading source
metadata.  That method is a similar in approach to the way we handle
FDSN station data.   That is, we use obspy for the machinery to
download the data from FDSN web services and translate the obspy python
data structure, which in this case is called a :code:`Catalog`, into
MongoDB source documents.

Like the receiver problem, obspy has two comparable functions for
retrieving source metadata.

#.  :code:`get_events` is an obspy function that is very similar to the
    receiver equivalent :code:`get_stations` noted above.
    Their documentation on this function can be found
    `here<https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.client.Client.get_events.html>`__.
    Like the receiver equivalent it has search criteria to yield a set of source data
    based on some spatial, time, magnitude, and/or other criteria.
    In addition, like :code:`get_stations`, :code:`get_events` returns the
    result in a python data structure that in this case they call a :code:`Catalog`.
    The :code:`Catalog` class is more or less an image of the FDSN standard
    for web service source data in XML format they call :code:`QuakeML`.
    The biggest issue with this approach for many workflows is that
    it is too easy to create a collection of source data that is much
    larger than the number of events actually in the data set.

#.  If you use the obspy :code:`mass_downloader` driven by source
    queries (see example titled "Earthquake Data" on the
    mass_downloader page found `here<https://docs.obspy.org/packages/autogen/obspy.clients.fdsn.mass_downloader.html>`__)
    that function will create QuakeML data files defining the unique source data for
    all the waveforms downloaded with each call to that function.

The procedure to load source data for a MsPASS workflow derived from
one of the obspy methods is comparable to that described above for FDSN
StationXML data.  That is, we use an obspy python data structure as the
intermediary for the import.  :code:`get_events` returns the
obspy :code:`Catalog` class directly while the output QuakeML files from
the :code:`mass_downloader` are easily created by calling the
obspy function :code:`read_events` described
`here<https://docs.obspy.org/master/packages/autogen/obspy.core.event.read_events.html>`__.
A :code:`Catalog` instance can then be saved to a MongoDB source collection
using the :code:`Database` method called :code:`save_catalog`.
The following is a fragment of a workflow doing this with the output of
:code:`mass_downloader`.

.. code-block:: python

   paste in portion of 2012 usarray workflow

An alternative for which we provide limited support is importing catalog
data from an Antelope database.   We have a prototype implementation in
the module :code:`mspasspy.preprocessing.css30.dbarrival` but emphasize
that code is a prototype that is subject to large changes.   The actual
:code:`dbarrival.py` prototype will almost certainly eventually be
depricated.  We include it in our initial release as a starting point for
users who may need this functionality.  The approach used in the prototype
is independent of the relational database system used for managing the
source data.   That is, the approach is to drive the processing with a
table defined as a text file.  The same conceptual approach could be used as the
export of a query of any relational database that is loaded internally
as a pandas dataframe.


Importing Other Data Formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently MsPASS depends completely on obspy for importing waveforms in
a format other than miniseed.   The following is pseudocode with a
pythonic flavor that illustrates how this would be done for a list of data file
to be processed:

.. code-block:: python

  from obspy import read
  from mspasspy.db.database import Database
  from mspasspy.db.client import DBClient
  dbclient=DBClient()
  db=Database(dbclient,'mydatabasename')
     ...
  for fname in filelist:
    st = obspy.read(fname,format="SOMEFORMAT")
    d = converterfunction(st)
    db.save_data(d)

where :code:`SOMEFORMAT` is a keyword from the list of obspy
supported formats found
`here<https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html#supported-formats>`__
and :code:`converterfunction` is a format-specific python function you would need to
write.  The function :code:`converterfunction` needs to handle
the idiosyncrasies of how obspy handles that format and convert the stream
:code:`st` to a TimeSeriesEnsemble using the MsPASS converter function
:code:`Stream2TimeSeriesEnsemble` documented
`here<https://www.mspass.org/python_api/mspasspy.util.html#module-mspasspy.util.converter>`__.
That is a necessary evil because as the authors of the obspy write in
their documentation some formats have concepts incompatible with
obspy's design.   Although we cannot provide unambiguous proof we have
confidence the same is not true of MsPASS because the TimeSeries container
is more generic than those used in obspy as we discuss in
the section :ref:`data_object_design_concepts`.

There are two different issues one faces in converting an external format to
the instance of an implementation of a seismic data object like TimeSeries or
TimeSeriesEnsemble:

#.  The sample data vector(s) may require a conversion from various binary
    structures to the internal vector format (in our case IEEE doubles).
    The approach we advocate here solves that problem by not reinventing a
    wheel already invented by obspy.   If you face the need to convert a large
    quantity of data in an external format it may prove necessary to
    optimize that step more than what obspy supplies as we have no
    experience on the efficiency of their converters.   Don't enter that
    gate to hell, however, unless it is essential as you may face a real-life
    example of the Dante quote:  "abandon hope all ye who enter here".
#.  Every format has a different header structure with few, if any overlaps in
    the namespace (i.e. the key-value pair defining a concept).   That means
    both the string used in the api to refer to an attribute and the
    type of the value.  The obspy readers handle this issue differently for
    different formats.   That variance is why we suggest any conversion
    will require developing a function like that we call :code:`converterfunction`
    above.

The MsPASS schema class
(see `this page<https://www.mspass.org/python_api/mspasspy.db.html#module-mspasspy.db.schema>`__ for details)
has tools we designed to aid conversion of Metadata
(i.e. item 2 above) from external representations
(format) of data to MsPASS.   In particular, the :code:`apply_aliases` and
the inverse :code:`clear_aliases` were designed to simplify the mapping for
key-value pairs in one namespace to another.   To utilize this feature for
a given format you can either create a yaml file defining the aliases or
hard code the aliases into a python dict set as key:alias.

We close this section by emphasizing that that at this time we have intentionally not
placed a high priority on development of complete tools for importing
formats other than SEED/miniseed.   We consider this one of the first
things our user community can do to help expand MsPASS.   If you develop
an implementation of one of the functions we gave the generic name
:code:`converterfunction` above we encourage you strongly to contribute
your implemetation to the MsPASS repository.

Validating an Imported Data Set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
After importing any data to MsPASS (miniseed included but especially any
specialized import function output) you are advised strongly to run the
MsPASS command line tool :code:`dbverify` on the imported collection.
We advise you run both the :code:`required` test
(-t required) and the :code:`schema_check` test (-t schema_check)
before running a significant workflow on an imported data set.
