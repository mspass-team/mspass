.. _importing_tabular_data:

Importing Tabular Data
=========================
*Prof. Gary L. Pavlis*
~~~~~~~~~~~~~~~~~~~~~~~~


Concepts
~~~~~~~~~~~~

Few seismologists and surprising few "IT professionals" appreciate
the full significance of the
generic concept encapsulated by the term :code:`Tabular Data` in the title of
this section.  Tabular data are ubiquitous in the modern IT world
largely due to the intimate connection with relational databases.
In fact, it is worth stressing that a "relation" in database jargon
means "table".

MsPASS supports mechanisms to import/export the following
types of tabular data:

- A large range of file-based formats.   Common exampes include:
  comma separated value (csv) files, Excel speadsheets,
  most white-space separated text files, and various legacy fixed format
  tabular data files.
- The standard query language for relational database servers is the
  language called "Structured Query Language" that is commmonly referred to
  by the jargon term "SQL server".  SQL queries return tabular data that
  is readily handled in MsPASS with common python packages.
- A special form of relational database in the seismology community is
  what is most properly called a "Datascope database" but which is commonly
  referred to as a "Antelope database".   Technically "Datascope" is a
  component of the larger "Antelope" software suite.   Those tables are important
  as a number of regional seismic networks use Antelope as their database
  engine.  Furthermore, the Array Network Facility of the Earthscope project
  used Antelope and the database tables are still accessible at
  the AUG web site `here <https://anf.ucsd.edu/tools/events/>`__.
  In any case, the key concept to remember is that Antelope/Datascope
  is just a different implementation of a relational database.  Below we
  describe a special interface in MsPASS to import data from Datascope tables.

In MsPASS the central concept used to unify these diverse sources is the
concept of a :code:`DataFrame` that is THE data structure that is the
focus of the commonly used
`pandas <https://pandas.pydata.org/docs/>`__ python package.
The name does not come from a fuzzy animal but is apparently
a acronymn derived from "PANel DAta".  "panel" is, in fact,
an odd synonym for "table".  Pandas and their extension in
dask and pyspark are an API to manipulate tabular data.
Because both dask and pyspark have DataFrame implementations
it is particular natural that the way we suggest to best manipulate
tabular data in MsPASS is to use the DataFrame API.
All "imports" below can be thought of as ways to convert a table
stored some other way to a DataFrame.

Import/Export Tables Stored in Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Importing data from any common tabular data format I know if is
essentially a solved problem via the :code:`DataFrame` API.

- The documentation on reading tabular data files can be found
  `here <https://pandas.pydata.org/pandas-docs/stable/reference/io.html>`__.
  There are also writers for most of the same formats documented on that
  same page.
- Dask has a more limited set of readers described
  `here <https://examples.dask.org/dataframes/01-data-access.html>`__.
  The reason is that the large data model of DataFrame for a dask
  workflow is most applicable when the table is large compared to
  the memory space of a single node. Hence, something like an
  Excel spreadsheet can never be expected to hold gigantic tables.
  In constrast, a huge cvs file is easy to construct from many identical
  pieces using the standard unix "cat" command.
- Pyspark has similar functionallity, but a very different API than
  pandas and dask.  The documentation for the read/write interface can be found
  `here <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html>`__.
  The list of formats pyspark can read or write is similar to pandas.

The most common application for reading tabular data is importing
some nonstandard data from a research application stored in one of the
standard formats.  For example, here an example extracted from our jupyter notebook
tutorial on MongoDB.  It shows how one can import the output of
`PhaseNets <https://github.com/AI4EPS/PhaseNet>`__
with it's output structured as a csv file.
It also shows how the results can be written to MongoDB in a collection it
creates called "arrival":

.. code-block:: python

  import pandas as pd
  df = pd.read_csv('./data/picks.csv')
  doclist=df.to_dict('records')
  for doc in doclist:
    station_id = doc['station_id']
    slist=station_id.split('.')
    net=slist[0]
    sta=slist[1]
    doc['sta']=sta
    doc['net']=net

  insert_many_output=db.arrival.insert_many(doclist)

Noting the complexity using the python string `split` method is a necessary
evil for the file format.  See the tutorial for a more extensive
explanation of this example.

Import/Export Data from an SQL server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Importing data from an SQL server or writing a DataFrame to an
SQL server are best thought of different methods of the
DataFrame implementation.  e.g. if the data in the example above
had been stored on an SQL server you would change the line
:code:`df = pd.read_csv('./data/picks.csv')` to use the variant
for interacting with an SQL server.   See the links above the to
io sections for pandas, dask, and pyspark for details on what the
work out the correct incantation for your needs.

Import/Export of Data from Datascope
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Although the Datascope (Antelope) database is an implementation of
a relational database, it does not work with the SQL API of
pandas, dask, and pyspark because Datascope does not speak SQL.
On the other hand, Datascope is heavily used in seismology as a
means to create earthquake catalogs from local earthquake networks.
It also has a set of useful tools that complement MsPASS.   Therefore,
there are a lot of known ways that the Antelope software can be used
in combination with MsPASS to handle novel research problems.
As a result, we created a special API to interact with data
managed by an Antelope database.   The MsPASS API aims to
loosely mimic the SQL functionality using an pandas
DataFrame as the intermediary.

A python class called
:py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`
is used as the agent for interacting with an
Antelope/Datascope database.   Unlike all common SQL
database of today, Datascope does not use a client-server model
to manage the data it contains.   It is an example of what is
commonly called a "flat-file database" because the tables it manages
are stored in text files.  That is relevant for this discussion
only because of how those files are defined.   That is, in
Datascope the file names are special and take the form
`dbname`.`table`.   `dbname` is the name of the collection
of tables that is the "database":  the "database name".  As the
word implies `table` is the schema name for a particular table
that that file contains.   For example, if one sees an
Datascope table with the file name "usarray.arrival" that file
is the "arrival" table in a database someone chose to call "usarray".
With that background, an instance of a
:py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`
can be created with a variant of the following code fragment:

.. code-block:: python

  dsdb = DatascopeDatabase("usarray")

where "usrray" is the "name" used for this database instance.
Experienced Datascope users will know that Datascope has a useful,
albeit confusing, feature that allows the collection of
files that define the database to be spread through multiple directories.
That features is nearly always exploited, in practice, by placing
more static tables in a separate directory.   For that reason
:py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`
has an optional `dir` argument to point the constructor to read
data files from a different directory.   e.g. a variant of the
above example to access files in a "dbmaster" (common practice)
directory is the following:

.. code-block:: python

  dsdbm = DatascopeDatabase("usarray",dir="~/dbmaster")

Once, an instance of
:py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`
is created that points to the directory from which you want to import
one or more tables, the usage to fetch the data that table contains
is similar to that for the pandas SQL readers.   Use the
:py:meth:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase.get_table`
method of
:py:class:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase`
to retrieve individual tables from the Datascope database
as a pandas DataFrame.   An important option descibed in the
docstrng is a python list passed via the optional argument
with key `attributes_to_load`.  The default loads the entire css3.0
schema table.  Use a list to limit what attributes are retrieved.
As an example of a typical use of the
:py:meth:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase.get_table`
method the following would retrieve the coordinate data from
the usarray "site" tables using the handle `dsdbm` created with
the code line above:

.. code-block:: python

  coordlist = ['sta','ondate','offdate','lat','lon','elev']
  df = dsdbm.get_table("site",attributes_to_load=coordlist)

The result could be used for normalization to load coordinates by
station name.  (In reality there are some additional complications
related to the time fields and seed station codes.   Those, however are a side issue
that would only confuse the topic of discussion so ignore it here.)

The inverse of the
:py:meth:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase.get_table`
method is the
:py:meth:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase.df2table`
method.  As it's name implies it attempts to write a pandas
DataFrame to a particular Datascope table, which means it will attempt to
write a properly formatted text file for the table name passed to the
method function.

Finally, the :code:`datascope.py` module also contains two
convenience methods that simply two common operations with
Datascope database tables:

#.  :py:meth:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase.CSS30Catalog2df`
    creates the standard "catalog-view" of CSS3.0.   In seismology a
    "catalog" is a image of what in ancient times was distributed as book
    tabulating earthquake hypocenter estimates and arrival time data used
    to create those estimates.   CSS3.0 standardized a schema for creating
    a "catalog" as the join of four tables that this method creates:
    event->origin->assoc->arrival where "->" symbolizes a right database
    join operator.  It returns a pandas DataFrame that is the "catalog".
    Usage details can be gleaned from the docstring.
#.  :py:meth:`mspasspy.preprocessing.css30.datascope.DatascopeDatabase.wfdisc2doclist`
    can be thought of as an alternative to the MsPASS
    :py:meth:`mspasspy.db.database.Database.index_mseed_file` method.
    It returns a list of python dictionaries (documents) that are roughly equivalent to
    documents created by
    :py:meth:`mspasspy.db.database.Database.index_mseed_file`.
    The main application is to use the alternative miniseed indexer
    of Antelope.   There are many ways that raw miniseed files from
    experimental data (i.e. data not sanitized for storage in the archives)
    can be flawed.   The Antelope implementation has more robust handlers
    for known problems than that in MsPASS.  For most uses we would encourage
    immediately dumping the output to the standard MsPASS collection
    :code:`wf_miniseed` using a variant of this example.

.. code-block:: python

  client = DBClient()
  db = client.get_database("myproject")
  dsdb = DatascopeDatabase("usarray")
  doclist = dsdb.wfdisc2doclist()
  db.wf_miniseed.insert_many(doclist)
