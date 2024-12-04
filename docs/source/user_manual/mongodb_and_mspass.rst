.. _mongodb_and_mspass:

Using MongoDB with MsPASS
==============================
*Prof. Gary L. Pavlis*
------------------------
Overview and Roadmap
-----------------------
There are a huge number of internet and printed resources on the
Database Management system used in MsPASS called `MongoDB`.
The reason is that MongoDB is one of the most heavily used open source
packages in the modern software ecosystem.  For that reason in earlier
versions of our User's Manual we simply punted the ball and told User's
to consult online sources.  It became clear, however, that the
firehose of information that approach creates is not for everyone.
Hence, we created this section to reduce the firehose to a, hopefully,
manageable stream of information.

The first section, which is titled "Concepts", is introductory material.
The material, however, is broken into subsections directed at people
with specific backgrounds.   If the title matches you, start there.  If
you fit none of the descriptions, read them all.  The sections after that
are organized by the letters of the standard acronymn used in
many sources on database system:  CRUD (Create, Read, Update, and Delete),
although not that order for pedagogic reasons.
The final section covers an auxiliary issue of MongoDB: indexes.
Indexes are critical for query performance, but are not required.

Concepts
-------------
MongoDB for RDBMS Users
~~~~~~~~~~~~~~~~~~~~~~~~~
Users who are familiar with Relational DataBase Management Systems (RDBMS)
will find MongoDB has many similar concepts, but also is fundamentally
different from any RDBMS.  In an RDBMS the conceptual model of the data
structure the system manages is a table (A "relation" in database theory
is synonymous with "table".) That is, however, better thought of as an
implementation detail for what the system is aiming to do.
Computer science textbooks on database theory make heavy use of set theory
and treat a database as an abstract set of "attributes" that each define
some quantity of interest and have some relationship.   Some kinds of data fit the
tabular model well, but some do not fit the concept at all.
Seismic data objects are a case in point;  traditional "header" attributes
like those in SAC or SEGY map well into the RDBMS mode.
On the other hand, it is well known that things like
the sample data and response information are awkward, at best, to handle
that way.   If you are a user of a system like Antelope that uses CSS3.0
you will know what I mean.  The other fundamental weakness of the
tabular model is handling the "null" problem.   That is, if an attribute
is optional the table has to define what defines a null value.
MongoDB suffers neither of those issues, but has its own "implementation
details" that we will discuss later.

A relation (table) indexes a given piece of data by row and column.
Although a system like Antelope actually uses integers to efficiently
access cells in a table, the normal abstraction is that both
the columns (attributes) and rows (tuples) are defined by some
index.   Columns are always indexed by an "attribute name"
that is a keyword associated with the concept that column of
data define.
The rows (tuples) in any RDBMS have
an index defined by the "primary keys" of the relation.
(They sometimes have alternate keys, but that is a detail.)
The primary key always has some indexing scheme to speed lookup
by that key.   One way to think of MongoDB's data model is to
think of it a collection of binary blobs (a list) with one or more
fast indexing methods that allow random retrieval of any of the
things it stores by the index.   It may be useful to think of
a MongoDB collection as a relation with only 1 attribute per tuple,
which MongoDB calls a "document".   That one attribute is itself
a more complex data structure that maps into a python dictionary.
That is in contrast to more traditional RDBMS tables where the
cells are usually only allowed to be simple types
(i.e. string, integers, real numbers, or boolean values).
The tuple-level indexing is also very different as any key(s) in the document can
serve as an index.  The more profound difference is that a "document"
is infinitely more flexible than an RDBMS table.   In an RDBMS table
every cell needs an entry even if the value is Null.   In MongoDB
a "document" can contain anything that can be reduced to a
name-value pair (the data structure of a python dictionary).
The "value" associated with the key has few restriction, but
the approach makes no sense unless the "value" can be translated into
something that matches the concept that key is supposed to reference.
For example, a seismic station "name" is a unique tag seismologists
use describe a particular observatory/site.   Earthquake seismologists
nearly always use human-generated names stored as a string.
Multichannel seismic data managment systems (e.g. Seismic Unix)
commonly use an integer index to define a given position because
survey flags are commonly defined by sequential numbers defining positions
along a line/profile.   Conceptually, however, both approaches require the
same thing.  An index (station name or station index number) is used
to define a particular geographic location.  Different keywords (keys)
are used to define particular concepts.   For the case in point,
in MsPASS we use
"lat", "lon", and "elev" as keys to define an instruments geographic
position. Fetching all three from a particular document will produce all
the data required to define a unique position.  The same thing is done
in an RDBMS, but the table model is used to define the model of how
to fetch a particular attribute by a similar key.   e.g. in the CSS3.0
schema the same three  attributes have the same keys.
That is, the CSS3.0 standard defines the keys "lat", "lon", and "elev"
as latitude, longitude, and elevation.

In MongoDB the equivalent of a table (relation) is called a "collection".
A more technical definition of a "collection" is a set of "documents"
accessible with a random access iterator.   What that means, in practice,
is that a collection act like an RDBMS table in the sense that the
contents can be sorted into any order or subsetted with a query.

On the other hand, The thing that makes
a relational database "relational" is not done well by MongoDB.
That is, various forms of a "join" between two tables are a fundamental
operation in most, if not all operational relational database systems.
MongoDB has the equivalent in what it calls "normalization", but
our experience is it is much slower than comparable operations with
an RDBMS.   We discuss normalization at length in the
related section of the User's Manual titled :ref:`Normalization<normalization>`.
The results of a query
can also be "grouped", but require very different programming constructs than
SQL.

Perhaps the most important common construct used by all RDBMS systems I
know of that is also a part of MongoDB is the concept of a "cursor".
In an RDBMS a cursor
is a forward-iterator (i.e. it can only be incremented)
that loops over the set of tuples returned by a query.
In MongoDB is is more-or-less the same thing with different words.
A MongoDB cursor is a forward-iterator that can be used to work through
a set of documents returned by a query.  You will see numerous examples
of using cursors in the MsPASS User's manual and any source discussing
MongoDB.

MongoDB for Python programmers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
All python programmers will be familiar with the container
commonly called a "dictionary" (dict).   Other sources call the same concept
an "associative array" (Antelope) or "map container" (C++ and Java).
Why that is important for MongoDB is simple:  the python bindings for
MongoDB (`pymongo`) used a dict to structure the content of what
MongoDB calls a "document" AND pymongo uses dict containers as the base of
its query language.   Two simple examples from the related tutorial notebook
illustrate this.

*Document output example:*

.. code-block:: python

  from bson import json_util
  doc = db.wf_minised.find_one()
  print(json_util.dumps(doc,indent=2))

Produces:

.. code-block:: python

  {
    "_id": {
      "$oid": "65f6e45e4f0f9fe8183675eb"
    },
    "sta": "IUGFS",
    "net": "2G",
    "chan": "BHE",
    "sampling_rate": 20.0,
    "delta": 0.05,
    "starttime": 1355020458.049998,
    "last_packet_time": 1355024042.649848,
    "foff": 0,
    "npts": 72000,
    "storage_mode": "file",
    "format": "mseed",
    "dir": "/N/slate/pavlis/usarray/wf/2012",
    "dfile": "event70.mseed",
    "time_standard": "UTC",
  }

Here is the first of several example queries in this section:

*Query example:*

.. code-block:: python

  query=dict()
  query['sta' : 'AAK']
  query['chan'] : 'BHZ'
  query['loc'] = '00'
  print("query content in pretty form")
  print(json_util.dumps(doc,indent=2))
  doc=db.wf_miniseed.find_one(query)
  print("output of query")
  print(json_util.dumps(doc,indent=2))


MongoDB for Pandas Users
~~~~~~~~~~~~~~~~~~~~~~~~~~
Most users who have had any significant experience with python will
likely have encountered pandas.   The name "pandas" is
one of those strained acronyms.   Multiple online sources indicate the
name comes from "panel data", which is basically a stretch of a synonym for
a table.  That insight is fundamental,however, as pandas can be thought of as
little more than python version of a spreadsheet.   In addition, more
elaborate features of the panda API can be used to mimic much of
an RDBMS functionality.

Since pandas are little more than an API for manipulating tables,
linking pandas to MongoDB differs little from linking an RDBMS table
to MongoDB.  What I mean by that is perhaps best illustrated by
an example.  The `Antelope software <https://brtt.com/software/>`__
used by many seismologists is a "flat-file" RDBMS.  It stores tabular
data in simple text files that can be viewed with standard unix tools.
(Note most RDBMS systems hide data behind the API like MongoDB does and
the data are stored in some binary set of files accessible only through
a server.)  Antelope uses the CSS3.0 schema.   One of the way pandas can
be used with MsPASS is to import CSS3.0 tables.   With Antelope
files that can be done with the `read_fsf` function in pandas.  The
following illustrates an alternative way to create a `site` collection
from an Antelope `site` table.

.. code-block:: python

  import pandas
  from obspy import UTCDateTime
  keys = ['sta','ondate','offdate','lat','lon','elev','statype','refsta','dnorth','deast','lddate']
  widths = [6,8,8,9,9,9,50,4,6,9,9,17]  # need the antelope schema file to get these
  df = pandas.read_fwf('demo.site',names=keys,widths=widths)
  doclist = df.to_dict('records')
  # This loop is needed to convert ondate and offdate to starttime and
  # endtime used in MsPASS.
  for doc in doclist:
    # In CSS3.0 these are integers for year day.  UTCDateTime
    # converts correctly ONLY if it is first converted to a string
    ondate=str(doc['ondate'])
    offdate=str(doc['offdate'])
    starttime=UTCDateTime(ondate).timestamp()
    enddate=UTCDateTime(offdate).timestamp()
    doc['starttime']=starttime
    doc['endtime']=endtime
  # this script assumes db is a MongoDB Database handle set earlier
  db.site.insert_many(doclist)

A few details worth noting about this example:

-  The list of keywords assigned to the symbol `keys` is needed because
   Antelope wfdisc fles do not have attribute names as the first line of
   the file.   The list used above uses CSS3.0 attribute names.  The order
   is significant as the names are tags on each column of data loaded
   with `read_fsf`.

-  The `widths` symbol is set to a list of fixed field widths.  They ere
   derived from the antelope schema file.

-  The call to the pandas `to_dict` method converts the pandas table to
   a list of python dictionaries.

-  The for loop after the call to `to_dict` is not strictly necessary.
   It is used in this example to produce a "site collection" consistent
   with the MsPASS namespace.   This is an example of a disconnect in
   concept between two database systems.  CSS3.0 is an older standard and
   the committee that developed it elected to store the "ondate" and "offdate"
   fields as integers that specified time to the nearest day.  The SEED
   standard changed the equivalent to a time stamp normally specified as
   a unix epoch time or a date string.  Here we convert the time to a
   unix epoch time through obspy's UTCDateTime class.

-  The last line is the only MongoDB component of this script.  More examples
   like this are seen below.  A key point here is that `insert_many` can
   handle any number of documents defined in doclist.   It is, of course,
   memory limited because pandas and `doclist` are all in memory.  The
   del call in the script demonstrates good practice to release potentially
   large memory objects like `df` after they are no longer needed.

The above example works for the special case of Antelope text-based
database files.   The pandas API, as experienced pandas users know,
has a rich set of readers that can read nearly any imaginable
tabular data format from files, sql servers, and online sources.  These are documented
`here <https://pandas.pydata.org/docs/reference/io.html>`__ and include
Excel, csv, and json formatted files, SQL servers, and jargon most of
us have never seen.  I have found that for research problems the fact that MongoDB
documents are completely agnostic about content can be very helpful.
For a new problem it is trivial to create a new collection and start putting
things (documents) into it and have the data available by MongoDB queries.
Readers should realize the schema we imposed on seismic waveform collections
was imposed to provide a standardized namespace for keys to allow the
framework to be extended without breaking lower level functionality.
For the exploration stages of a research problem having now schema
constraints is a very useful feature of MongoDB. Importing data through
pandas is a particularly simple way to import many forms of data
you may acquire from internet sources today.

A final key point about pandas is that both dask and pyspark
have a parallel equivalent.  Both refer to the
equivalent of a pandas data structure as
a `DataFrame`.   A large fraction of the pandas API are available
in the dask and pyspark DataFrame API.  Experienced pandas users
may find it helpful in handling large tabular data sets to develop
applications with MsPASS that use the DataFrame API to manipulate
the tabular data.  With dask or pyspark most pandas operations
can be parallelized.

Queries (Read of CRUD)
-----------------------
Query language
~~~~~~~~~~~~~~~~
In my experience the single most important usage of a database like
MongoDB in MsPASS research data processing is defining queries to
select a subset of data holdings or to define groupings (ensembles)
to be processed together.  A barrier to usage, however, is that
MongoDB uses a unique and rather strange query language that users
familiar with a language like SQL will find foreign.   Furthermore,
the biggest weakness I've seen in any
online source I've found on MongoDB usage is a failure to
address the fundamental syntax of the query language.
All sources seem to think the best way to understand the
language is from examples.  That is somewhat true, but many of us find it
easier to remember a few basic rules than a long list of
incantations.   This section is an attempt to provide some
simple rules that can, I hope, help you better understand the
MongoDB query language.  Here are what seem to me to be the
fundamental rules:

1.  All queries use a python dictionary to contain the instructions.
2.  The key of a dictionary used for query normally refers to an attribute
    in documents of the collection being queried.  There is an exception
    for the logical OR and logical AND operators (discussed below).
3.  The "value" of each key-value pair is normally itself a python
    dictionary.   The contents of the dictionary define a simple
    language (Mongo Query Language) that resolves True for a match
    and False if there is no match.  The key point is the overall
    expression the query dictionary has to resolve to a boolean condition.
4.  The keys of the dict containers that are on the value side of
    a query dict are normally operators.  Operators are defined with
    strings that begin with the "$" symbol.
5.  Simple queries are a single key-value pair with the value either
    a constant or a dictionary with a single operator key.  e.g.
    to a test for the "sta" attribute being the constant "AAK" the
    query could be either `{"sta" : "AAK"}` or `{"sta" : {"$eq" : "AAK"}}`.
    The form with constant value only works for "$eq".
6.  Compound queries (e.g. time interval expressions) have a value
    with multiple operator keys.
7.  There is an implied logical AND operation
    between multiple key operations.  An OR must be specified differently
    (see below).

In the examples below, refer back to these rules to help you remember
these fundamentals.

Query methods
~~~~~~~~~~~~~~~~
Querying (read) is again a "collection operation".   That is, if we set
the symbol `db` to a MsPASS or MongoDB `Database` object, the query
functions are "methods" of a collection object.   (see longer discussion
above in the "Create" section)  There are three standard methods for
the "Read" part of CRUD.  We will show examples of all three below.

1.  `find_one` returns a document that is the first document found matching
    a query operator.
2.  `find` returns a MongoDB
    `Cursor object <https://www.mongodb.com/docs/v3.0/core/cursors/>`__
    that can be used to iterate through query that returns many documents.
3.  `count_documents` is a utility function used to bound how many documents
    match a particular query.

Examples of the use of each of the three functions above:

.. code-block:: python

  query={'sta' : 'AAK'}  # shorthand for {'sta' : {'$eq' : 'AAK'}}
  doc = db.site.find_one(query)
  print("First matching document in site collection for query=",query)
  print(doc)
  print("All documents in site collection for query=",query)
  cursor = db.site.find(query)
  for doc in cursor:
    print(doc)
  n_matches = db.site.count_documents(query)
  print("Number of documents matching query=",query," is ",n_matches)

`find` and `find_one` are the basic document-level fetching methods.
The examples above show the most common, simple usage.
Both, however, actually have three positional arguments with defaults
you should be aware of.

1.  `arg0` defines the query operator.  The default is an empty dictionary
    that is interpreted as "all".
2.  `arg1` defines a "projection" operator.  That means it is expected to
    be a python dictionary defining what attributes are to be retrieved or
    excluded from the returned value(s).   For RDBMS users a "projection"
    in MongoDB is like the SELECT clause in SQL.  That idea is best
    illustrated by examples below.
3.  `arg2` is an "options" operator.   I have personally never found a
    use for any of the listed options in the MongoDB documentation.  I can't
    even find an online example so "options" are clearly an example of
    "an advanced feature" you can ignore until needed.

Note also that a `find_one` returns only a single "document", which
pymongo converts to a python dictionary.   The `find` method, in
contrast returns a pymongo `Cursor` object.  A `Cursor` is, in the
the jargon of data structures, a "forward iterator".  That means it can
only be traversed in one direction from the first to last document retrieved
by the MongoDB server.  There is a `rewind` method for the cursor object
but it is of use largely for interactive debugging.

We will next consider a series of increasingly complicated examples.

Simple (single key) queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Single key queries are always of the form:
`{key : expression}` where `key` is the attribute that is to be tested
by the query and `expression` is either: (1) another dictionary or
(2) a single value.  An example is the same one we used above.

.. code-block:: python

  query={'sta' : 'AAK'}
  query2={'sta' : {'$eq' : 'AAK'}}

`query1 and `query1` are completely equivalent.
Both are equality tests for the attribute with
the key "sta" matching a particular, unique name "AAK".

A similar inequality test for waveforms having `starttime` values
after a particular date is the following:

.. code-block:: python

  from obspy import UTCDateTime
  t_cutoff = UTCDateTime('2012-07-28T00:00:00.00')
  # query here needs to convert to a unix epoch time (timestamp method)
  # for numerical comparison to work
  query = {'starttime' : {'$gt' : t_cutoff.timestamp()}}
  cursor = db.wf_miniseed.find(query)

MQL has a rich collection of operators.
`This page <https://www.mongodb.com/docs/manual/reference/operator/query/>`__
of the MongoDB documentation has the complete list. A particularly useful
one for most seismologists that is typically omitted from introductory
tutorials is the
`$regex <https://www.mongodb.com/docs/manual/reference/operator/query/regex/#mongodb-query-op.-regex>`__
operator.  $regex can be used to apply a unix regular expression in
a query operation.   Most seismologists are familiar with the regular
expression syntax from using the unix shell.   The following, for
example, could be used to select only PASSCAL temporary experiments
from a site collection:

.. code-block:: python

  query={'net' : {'$regex' : 'X.'}}

Note that works because of an FDSN convention that net codes starting
with "X" are shorter term deployments.   Regular expressions are a rich
language for text-based filtering.  See the link above or do a web
search for more examples.

Multiple key queries
~~~~~~~~~~~~~~~~~~~~~~~
A query to test the value of more than one attribute uses a dictionary
with multiple keys.  In most cases each key
defines an attribute to be each tested for matches by the query operation.
The key can, however, also sometimes be an operator, in which case
the dictionary would be called a "compound query" (see example below).
For a normal example, the following can be used to find all documents for
all channels for station with net code "II" and station code "PFO":

.. code-block:: python

  query = dict()
  query['net'] = 'II'
  query['sta'] = 'PFO'
  cursor = db.find(query)
  for doc in cursor:
    print(doc)

I used an explicit code to set the query dict container for instructional
purposes.  That form emphasizes that `query` is a python dictionary
and the query uses 'net' and 'sta' attributes.
Most online sources use the inline form for defining a python
dictionary.  That is, the following could be used to replace the query definition
above:

.. code-block:: python

  query = {'net' : 'II', 'sta' : 'PFO'}

For simple queries the inline form is generally easier.  I  have found,
however, that for complex queries like some examples below the form using
key-value setting pairs is less error prone.  Complex inline expressions
can easily get confused by confusions about which curly backet belongs where.

A final important point about multiple attribute queries is that
there is an implied "AND" operations between the dictionary components.
For example, the example query above could be stated in works as:
`net attribute is 'II' AND sta attribute is 'PFO'`.  A logical "OR"
query equivalent requires a compound query (next section).

Compound queries
~~~~~~~~~~~~~~~~~~~
Compound queries mean multiple conditions applied to one or more attributes.
A type example is a very common one in seismology.  That is, waveform
data are always stored as segments with each segment having a start time
(`starttime` in the stock MsPASS namespace) and ending time
(`endtime` in the MsPASS namespace).   We often want to extract
a waveform segment with a particular time span from a database indexing
an entire dataset.   That dataset may be larger windows downloaded
previously or an archive of continuous data commonly stored as day files.
The problem is complicated by the fact that a requested time window
may span the artificial gap at day boundaries in continuous data or
data gaps that are marked in the set of wf documents as a break
with a particular time window.

With that long introduction, here is an example for a single channel
request.  In particular, this example reads a month of "LHZ"
channel data for station "PFO" and loads the results into a
`TimeSeriesEnsemble`:

.. code-block:: python

  from obspy import UTCDateTime
  # Example to select the month of June of 2012
  tsutc = UTCDateTime('2012-06-01T00:00:00.0')
  teutc = UTCDateTime('2012-07-01T00:00:00.0')
  ts=tsutc.timestamp()
  te=teutc.timestamp()
  query = dict()
  query['net'] = 'II'
  query['sta'] = 'PFO'
  query['chan'] = 'LHZ'
  query['loc'] = '00'
  query['$and'] = [
     {'starttime' : {'$lte' : te} },
     {'endtime' : {'$gte' : ts} }
  ]
  cursor = db.wf_miniseed.find(query)
  ens = db.read_data(cursor,collection='wf_miniseed')

That is a complex query by any definition, but it illustrates several
features of MQL, some of which are new and some of which were discussed earlier:

1.  The dictionary of this key uses both attribute names
    ('net','sta','chan', and 'loc') and an operator ('$and').
2.  The four attribute keys defined implied == (equality) matches on the
    seed channel code keywords.  As noted above there is an implied logical
    AND between the four seed station code matching components. (The "$and"
    is different.)
3.  Notice the subtle detail that the '$and' operator key is associated with
    a python list (implied by the [] symbols) instead of a python dict
    or simple value like all previous examples.   The logical AND is
    applied to all components of the list.  This example has two components
    but it could be as many as needed.   The components of the list are
    MQL dictionary expressions that resolve True or False.
4.  This example shows the application of a query to create a cursor
    passed to the `read_data` method of `Database`.   That is the standard
    way in MsPASS to get a bundle of data we call a `TimeSeriesEnsemble`.
    In this case, the ensemble will contain all waveform segments for the LHZ
    channel of the IRIS-Ida station PFO (loc code 00) that have any samples
    recorded in the month of June 2012.

A final point for this section is another shorthand allowed in the MQL
language.   That is, the "$and" operator above is not actually required.
The same query as above could, in fact, have been written as follows:

.. code-block:: python

  query = {
    'net' : 'II',
    'sta' : 'PFO',
    'chan' : 'LHZ',
    'loc' : '00',
    {'starttime' : {'$lte' : te} },
    {'endtime' : {'$gte' : ts} }
  }

In this case I used the inline syntax because it more clearly shows
the point.  That is, a query defined by a series of expressions has
an implied "AND" logical operator for all separate expressions.
For this example, you would say that in words as:
net code is II AND sta code is PFO AND channel code is LHZ AND ...
For that reason the $and opertor above is not actually
required.  Note, however, if a query logical expression involves
an OR clause the list of expressions syntax is required.  Here,
for example, is a similar query to above with an OR clause.
This query would always retrieve horizontal components and handle the
obnoxious channel code variation of E,N,Z naming versus 1,2,Z naming.
It also drops the "loc" matching and would thus ignore the loc code
and retrieve data from all sensors at PFO.

.. code-block:: python

  query = {
    'net' : 'II',
    'sta' : 'PFO',
    '$or' : ['chan' : 'LHE', 'chan' : 'LHN', 'chan' : 'LH1', 'chan' : 'LH2'],
    {'starttime' : {'$lte' : te} },
    {'endtime' : {'$gte' : ts} }
  }

Finally, the previous example also can be used to illustrate a
clearer solution with the `$regex` operator.   Most $or clauses I've
encountered are easier to express with a regular expression.
The above could thus be express equivalently with this one:

.. code-block:: python

  query = {
    'net' : 'II',
    'sta' : 'PFO',
    'chan' :  {'$regex' : 'LH.'},
    {'starttime' : {'$lte' : te} },
    {'endtime' : {'$gte' : ts} }
  }

Geospatial queries
~~~~~~~~~~~~~~~~~~~~~
MongoDB has a fairly sophisticated geospatial querying feature.
A first order thing you must realize about geospatial indexing is that
to be useful two things are required:

1.  The attribute(s) you want to query should be structured into a
    special data type called a
    `GeoJSON object <https://www.mongodb.com/docs/manual/geospatial-queries/#std-label-geospatial-geojson>`__.
    The only example packaged that way by MsPASS is the coordinates of
    seismic instruments stored in the "site" and"channel" collections
    and source spatial coordinates defined in the standard "source" collection.
    For all three the "lat" and "lon" keys define the latitude
    and longitude directly, but copies are stored in a GeoJSON point object
    with the key `location` in "site" and "channel" and "epicenter" in "source".
    A limitation of MongoDB's geospatial query engine is it is much like
    ArcGIS and is tuned to coordinate-based queries.  To add a depth
    constraint requires a compound query mixing geospatial and a range
    query over depth.
2.  All geospatial queries REQUIRE creating a
    `geospatial index <https://www.mongodb.com/docs/manual/core/indexes/index-types/index-geospatial/#std-label-geospatial-index>`__.
    Most MsPASS users will ALWAYS want to use what MongoDB calls a
    "2dsphere" index.   Their "2d" index uses a map projection and is
    designed only for local scale software apps at a city scale.
    The "2d" index is not accurate for the scale of most seismology
    research problems.  An exception is that UTM coordinates may work
    with a "2d" index, but I have no direct experience
    with that approach.  That could be useful with active source data
    where survey coordinates often use UTM coordinates.

The most common usage for geespatial queries I know in seismology is
limiting the set of seismic instruments and/or sources based on a
geographical area.   MQL implements geospatial queries as
a special type of operator.  i.e. the definitions of the query
are used like '$gt', '$eq', etc., but use different keywords.

Here is a simple example to retrieve documents from the site collection
for all stations within 500 km of my home in Bloomington, Indiana.
It is a minor variant of a similar example in the tutorial linked to
this page.

.. code-block:: python

  query = {"location":{
        '$nearSphere': {
            '$geometry' : {
                'type' : 'Point',
                'coordinates' : [-86.5264, 39.1653]
            },
            '$maxDistance' : 500000.0,
        }
      }
    }
    cursor = db.site.find(query)
    for doc in cursor:
      print(doc)

Note the complex, nested operators that characterize all MongoDB
geospatial queries.   I trust the verbose names make clear how this
query works provided you realize the location of Bloomington is
around 39 degrees latitude and the distance parameters have to
be defined in meters.   Note a few key details:

1.  MQL's geospatial query language is best done with
    `geoJSON <https://geojson.org/>`__.  This example defines a
    geoJSON point
    and a search radius.  In all cases, the key at the top level of
    the query is an MQL operator.   In this case the operator is
    "$nearSphere".  Note the first character "$" that is universally
    used to define a key as an operator in MQL. This example is
    a typical geospatial query made up of a multi-level document/dictionary
    with multiple operators at different levels.
2.  The distance specification is in meters and the geographical
    coordinate data are in degrees.  As far as I can tell that is the
    norm for MongoDB.  (Some older sources suggest some operators
    once used radian units, but that seems to be the distant past.)
3.  Once constructed the query is used like any other dictionary
    passed to find.  This example doesn't use any projection to
    keep the example simple, but it could have.

The set of spatial query operators are document in
`this page <https://www.mongodb.com/docs/manual/reference/operator/query-geospatial/>`__
of the MongoDB documentation.  Most of the complexity is in the
second level of attributes passed to the operator specified in geoJSON.
That is, for spherical geometry, which I again stress is the only thing
you should use, there are only three operator:
(1) `nearSphere` that I illustrated above, and (2) `geoWithin`
used to search inside a specified geometric shape (e.g. a polygon
but can be other things), and (3) `geoIntersects` that
"selects documents whose geospatial data intersects with a specified GeoJSON object ...".

Although the spatial query operators are a powerful tool to allow
geospatial queries comparable to some elements of a GIS system, there
are some major caveats and warnings:

1.  It is quite clear that the geospatial features of MongoDB
    have evolved significantly in recent years.
    Why that matters is I find a lot of online sources
    contain out-of-date information.
    To make matters worse, MongoDB's documentation on the topic
    does a poor job of describing this evolution
    and older documentation has examples that I found wouldn't work.
    That may change, but be warned you are likely in for some hacking.
2.  From what I can glean from fighting with this feature, the
    current problem was created by a evolution of MongoDB that seems to
    have begun around 2020.   It appears the earliest attempts to add
    geospatial queries to MongoDB used a "legacy" format to define
    coordinates.  e.g. a specific lon-lat can be specified in "legacy"
    format like this:`{ "coords" : [-102.7724, 33.969601]}`.   The same
    information defined in geoJSON is:

    .. code-block:: python

      { "coords" :
          {
            "type": "Point",
            "coordinates": [
              -102.7724,
              33.969601
            ]
          }
      }

    From my experience you should avoid the legacy format and only use
    geoJSON specifications in MongoDB documents.  To make that easier
    there is a convenience function in the `mspasspy.db.database`
    module called `geoJSON_doc`.   It can be used to create the obscure
    document structure MongoDB requires for simple lat,lon point data.
3.  A limitation of the (current) MongoDB implementation is the
    `count_documents` method does not seem to work for any valid
    query I can construct.  Internet chatter suggests that is the norm.
    I have found `count_documents` a useful tool to test a query while
    developing a workflow script.  Since all geospatial queries are complex by
    almost any definition that is problematic.  I find that to debug
    a geospatial query it is helpful to isolate the query in a
    jupyter notebook box run it until the query runs without an error.
    The example code block immediately above is a good model.
    Use the same structure, but remove the print loop until you get the
    query to work.

I would stress that in spite of these caveats, the integration of
geospatial query functions in the MongoDB are an important functionality
that can simplify a lot of research workflows.  If your work requires
any kind of geospatial grouping, it is worth investing the effort to
understand MongoDB's geospatial operators and how we use them in MsPASS.

Sorting
~~~~~~~~~~
There are many situations where an algorithm using input from
a MongoDB query requires a list sorted by one or more keys.
Defining a sort is straightforward but a little bit weird
until you understand the logic.   It will be easier to
explain that with a simple example.   Here is a query that returns
a cursor to retrieve documents defining LHZ waveforms from
station PFO (it uses a duplicate of one of the compound queries
from above) but this time we sort the result by starttime
(a type example of a sort requirement):

.. code-block:: python

  # ts and te are epoch times defing the time range as above
  query = {
    'net' : 'II',
    'sta' : 'PFO',
    'chan' : 'LHZ',
    'loc' : '00',
    {'starttime' : {'$le' : te} },
    {'endtime' : {'$ge' : ts} }
  }
  cursor = db.wf_miniseed.find(query).sort("starttime",1)
  ens = db.read_data(cursor,collection='wf_miniseed')

There are two key points this example illustrates:

1.  `sort` is defined as a "method" of the "Cursor" object returned by find.
    That is more than a little
    weird but a common construct in python which is an object-oriented language.
    Most of us can remember it better by just thinking of it as a clause
    added after find and separated by the "." symbol.  Because it is a method
    of cursor the sort clause could have been expressed as another statement
    after the find operator done like this:  `cursor = cursor.sort("starttime,1)")`
2.  The sort expression for a single key can be thought of as calling a
    function with two arguments.  The first is the key to use for the
    sort and the second defines the direction of the sort. Here I
    used "1" which means sort into an ascending sequence.  When the result is
    passed to the `read_data` it guarantees the waveforms in the
    ensemble created by `read_data` will be in increasing starttime order.
    You would use -1 if you wanted to sort in descending order.
    (Note:  some sources will use the verbose symbols `pymongo.ASCENDING`
    instead of 1 and `pymongo.DESCENDING` instead of -1.  For me 1 and -1
    are a lot easier to remember.)   In a typical python way there is also
    a default for the sort order of 1.  i.e. in the sort call above
    we could have omitted the second argument.

Sorting on multiple keys requires a slightly different syntax.   Again, an
example will make this clearer. This code segment prints a report for
the entire contents of the channel collection sorted by seed channel code:

.. code-block:: python

  sort_clause = [
    ("net",1),
    ("sta",1),
    ("chan",1),
    ("starttime",1)
  ]
  cursor = db.channel.find()
  cursor = cursor.sort(sort_clause)
  print("net sta chan loc starttime")
  for doc in cursor:
    # conditional to handle common case with loc undefined
    if 'loc' in doc:
      print(doc['net'],doc['sta'],doc['chan'],doc['loc'],UTCDateTime(doc['starttime']))
    else:
      print(doc['net'],doc['sta'],doc['chan'],'UNDEFINED',UTCDateTime(doc['starttime']))

The main thing to notice is that when using multiple keys for a sort they
must be defined as a python list of python tuples (arrays defined with [] will
also work).  That usage is potentially confusing for two reasons you should
be aware of:

1.  Most examples you will see of a single key sort use just the key name
    (ascending order is the default) or two arguments version like that I used
    above.   Multiple key sorts require a completely different type for arg0;
    a python list of tuples.
2.  Most examples you will find in a routine internet search with a phrase
    like "mongodb sort with mutiple keys" will show the syntax you can use
    with the "mongo shell".   The problem is that the mongo shell speaks
    a different language (Javascript) that uses a syntax that looks like
    it is defining an inline python dictionary definition, but it is not.
    That is, with
    the mongo shell the sort above could be written as:
    `{'net':1, 'sta':1, 'chan':1, 'starttime':1}`.  That is not a python
    dictionary, however, even though the syntax is exactly the same.
    In Javascript that is a list where the order of the list means something.
    If that were translated to a python dictionary it would not work
    because order of input is not preserved in a python dictionary.  Hence,
    the pymongo API has to use a list to preserve order.

Report generators
~~~~~~~~~~~~~~~~~~~~
One of the important applications of queries in MsPASS is to generate
a human readable report on the content of a database that is to be used
as input for processing.  An option for experienced programmers familiar with the
incantations of detailed formatting of text output is to create a
custom formatting function to generate a report from a cursor input.
For mere mortals, however, there are two much simpler options:

1.  For small numbers of documents the `json_util` package can be useful.
2.  Pandas are your friend for producing output visualized well with a table.

The examples in this section show how to set up both.  The related tutorial
notebook for this section of the User's Manual provide hands on examples
and why raw output can be ugly.

A typical example of `json_util` is that you might want to look at the gross
structure of one or more documents created by running something like the
MsPASS `index_mseed_file` method of `Database`.   Something like the
following can be useful to use in a python notebook run interactively
to work out data problems:

.. code-block:: python

  from bson import json_util
  doc = db.wf_miniseed.find_one()
  print(json_util.dumps(doc,indent=2))

The `indent=2` argument is essential to create an output that is
more readable than what would be otherwise produced by the much
simpler to write expression `print(doc)`.

Many quality control reports are conveniently visualized with a well
formatted table display.  As noted above pandas are your friend in
creating such a report.  Here is an example that creates a report of all
stations listed in the site collection with coordinates and the time
range of recording.  It is a variant of a code block in our
`mongodb_tutorial <https://github.com/mspass-team/mspass_tutorial/tree/master/notebooks>`__

.. code-block:: python

  import pandas
  cursor=db.site.find({})
  doclist=[]
  for doc in cursor:
    # Not essential, but produces a more readable table with date strings
    doc['starttime']=UTCDateTime(doc['starttime'])
    doc['endtime']=UTCDateTime(doc['endtime'])
    doclist.append(doc)
  df = pandas.DataFrame.from_dict(doclist)
  print(df)

Saves (Create of CRUD)
------------------------
The first letter of CRUD is the save operation.   A save of some kind
is usually the first thing one does in building a seismic dataset
as there needs to be some what to populate the database collections.
Our "getting_started" tutorial illustrates the most common
workflow:  populating the "site", "channel", "source", and (usually)
"wf_miniseed".   This section focuses more on the general problem of
loading some other data that doesn't match the standard mspass schema.
An example, which we use for the hands on supplement to this section
in our notebook tutorials, is downloading and loading the current CMT
catalog and loading it into a nonstandard collection we all "CMT".
In this manual we focus on the fundamentals of the pymongo API for
saving documents.  See the
`mongodb_tutorial <https://github.com/mspass-team/mspass_tutorial/tree/master/notebooks>`__
for the examples.

There are two methods of `Database.collection` that you can use to
save "documents" in a MongoDB collection.  They are:

1.  `insert_one` as the name implies is used to save one and only
    one document.   It is usually run with one argument that is assumed
    to be a python dictionary containing the name-value pairs that
    define the document to be saved and subsequently managed by
    MongoDB.
2.  `insert_many` is used to insert multiple documents.  It expects
    to receive a python list of dictionaries as arg0 each of which is
    like input sent to `insert_one`.

An important thing to realize is that `insert_many` is not at all
the same thing as running a loop like this:

.. code-block:: python

  # Wrong way to do insert_many
  for doc in doclist:
    db.CMT.insert_one(doc)

The reason is that the loop above does an independent transaction for
each document and the loop blocks until the MongoDB server acknowledges
success.   `insert_many`, in contrast, does a bulk update.   It automatically
breaks up the list into chunk sizes it handles as one transaction.
With a large save `insert_many` can be orders of magnitude faster
than the same number of one-at-a-time transactions.

Here is a partial example of save from the related tutorial notebook:

.. code-block:: python

  doclist = ndk2docs(fname)
  print("Number of CMT records in file=",fname,' is ',len(doclist))
  r=db.CMT.insert_many(doclist)
  n=db.CMT.count_documents()
  print("Number of documents in CMT collection is now ",n)


Updates (U of CRUD)
--------------------
Updates have a similar API to the insert/create API.  That is, there
are again two different collection methods:

1.  `update_one` is used to replace all or some of the data in one document.
2.  `update_many` is used to replace all or some attributes of many documents
    in a single transaction.

There is, however, a special feature called a `bulk_write` that can be useful
in some situations.   I cover that more specialized function at the end of
this section.

Although they have options, both `update_one` and `update_many`
are usually called with two arguments. *arg0* is an MQL matching query and
*arg1* defines what is to be changed/added.   The only real difference
between `update_one` and `update_many` is that `update_one` will only
change the first occurrence it finds if the match query is not unique.
For that reason, `update_one` is most commonly used with an `ObjectId`
match key.  For example, this segment would be a (slow) way to add
a cross-reference id link to wf_TimeSeries documents with
documents from a channel collection
created by loading from an Antelope sitechan table.  It uses the foreign
key "chanid" in CSS3.0 to find a record and then uses `update_one` to
set the MsPASS standard cross-reference name `channel_id` in wf_TimeSeries.

.. code-block:: python

  # some previous magic has been assumed to have set chanid in
  # wf_TimeSeries (feasible with a CSS3.0 wfdisc table)
  # This examplei is for illustration only and is not of direct use
  cursor = db.wf_TimeSeries.find({})
  for doc in cursor:
    if 'chanid' in doc:
      chandoc = db.channel.find_one({'chanid' : doc['chanid']})
      # find_one failures return None so this is a test for a valid return
      if chandoc:
        cid = chandoc['_id']
        wfid = doc['_id']
        db.wf_TimeSeries.update_one({'_id' : wfid},{'channel_id' : cid })

The `update_many` method is more commonly used with more complex queries
to set a constant for the group of documents.  The example below uses
`update_many` to build source cross-reference ids for a dataset created
by extracting waveforms using event origin times as the start times.
We can then do a match (arg0) using a range test with a small tolerance
around the origin time.  This fragment, unlike the `update_one` example,
is a useful prototype for a common organization of a dataset that initiates
from common source gathers:

.. code-block:: python

  # define a +- range relative to origin time for starttime
  ot_range=1.0
  # loop over all source records to drive this process
  cursor = db.source.find({})
  for doc in cursor:
    otime = doc['time']
    ts = otime - ot_range
    te = otime + ot_range
    match_query = {
      'starttime' : {
        {'$gte'  ts, '$lte' : te}
      }
    }
    srcid = doc['_id']  # ObjectId of this source document
    update_doc = {'source_id' : srcid}
    db.wf_TimeSeries.update_many(match_query,update_doc)

Finally, a more advanced approach that is useful for large numbers of
random updates with a large data set is the pymongo collection method
called `bulk_write <https://pymongo.readthedocs.io/en/stable/examples/bulk.html>`__.
An example of how to use this method can be found in the MsPASS function
`bulk_normalize <https://github.com/mspass-team/mspass/blob/master/python/mspasspy/db/normalize.py>`__.
Briefly, the idea is to manually build up blocks of atomic-level updates.
That approach is necessary only in the case where there is no simple
recipe for creating smaller number of matching operators like my
`insert_many` example above.  That is, the `bulk_normalize` function uses
does unique matches for each wf document with an object_id.   It makes
sense in that context because it assumes the matching was done externally
with one of the MsPASS matchers.

Delete (D of CRUD)
--------------------
As noted elsewhere, we take the position that for most design uses
of MsPASS delete operations should be rare.  To reiterate, the reason
is that MsPASS was designed with the idea that the database is used
to manage an assembled data set.  Appending more data is expected to
be the norm, but deleting data unusual.  For most cases, for example,
it is easier to rebuild something like a `wf_miniseed` collection
than design a deletion operation to selectively remove some data.
Nonetheless, that statement motivates the example we give below.

The API for deletion has a strong parallel to insert and update.  That is,
there are two basic method:  `delete_one` deletes one document and
`delete_many` can be used to delete a set of documents matching a
query defined with arg0.

As an example, suppose we have a large dataset assembled into Seismogram
objects indexed with `wf_Seismogram` and we find station "XYZ" had
something fundamentally wrong with it for the entire duration of the
dataset.   Assume "XYZ" doesn't require a "net" qualifier to be unique
this code fragment could be used to delete all entries for "XYZ".
It is complicated by a bit by the fact that multiple site entries can
occur for the same station due to time-dependent metadata:

.. code-block:: python

  cursor = db.site.find({'sta': 'XYZ'})
  for doc in cursor:
    sid = doc['_id']
    query = {'site_id' : sid}
    n = db.wf_Seismogram.count_documents(query)
    print("Number of documents to be deleted for station XYZ =",n)
    print("for time period ",UTCDateTime(doc['starttime']),UTCDateTime(doc['endtime']))
    db.wf_Seismogram.delete_many(query)

Indexes
--------------
An index is desirable in any database system to improve read performance.
Without an index a query requires a linear search through the entire
database to find matching records.  As a result read and update performance on
any database system can be improved by orders of magnitude with a properly
constructed index.   On the other hand, an indexes can slow write performance
significantly.  I have found that in data processing with MsPASS
there are two primary uses of indices:  (1) normalizing collections, and
(2) ensemble processing (index is the key(s) used for grouping).
Both fit the
constraint above.  In particular, the model you should use is to build the
index(indices) as the last phase before running the workflow on an assembled
data set.

A first point to recognize is that MongoDB ALWAYS defines an index on the
magic attribute key "_id" for any collection.   Any additional
index needs to be created with the collection method called
`create_index <https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html>`__.
The index can be defined for one or more keys and set to define an
increasing or decreasing sequence.   e.g. the following will create an
index on the channel collection appropriate for miniseed metadata
that require at least the four keys used to provide a unique match:

.. code-block:: python

  db.channel.create_index(
   [
    "net",
    ("sta", pymongo.DESCENDING),
    "chan",
    "time"
   ]
  )

Note arg0 is a list of attribute names and/or 2-element tuples.
Tuples are needed only if the default ascending order is to be
switch to descending.  I did that for illustration above for "sta",
but it wouldn't normally be necessary.

There are two utility functions for managing indexes in a collection:

1.  `list_indexes` returns a `Cursor` that can be iterated
    to show details of all indexes defined for a collection.  e.g. the
    above section to create a special index for channel might be
    followed by this line to verify it worked:

.. code-block:: python

  cursor = db.channel.list_indexes()
  for doc in cursor:
    print(json_utils.dumps(doc,indent=2))

2. There is a `delete_index` that can be used to remove an index from
   a collection.  It uses the index name that is returned on creation
   by `create_index` or can be obtained by running `list_indexes`.

A final point about indexes is special case of "geospatial indexes"
discussed above.   A geospatial index is a very different thing than
a "normal" index on one or more keys.   Consult online sources if
you need to learn more about that topic.
