.. _importing_tabular_data:

Importing Tabular Data
=========================
*Prof. Gary L. Pavlis*
---------------------------


Concepts
----------

Few seismologists and surprisingly few "IT professionals" appreciate
the full significance of the
generic concept encapsulated by the term :code:`Tabular Data` in the title of
this section.  Tabular data are ubiquitous in the modern IT world
largely due to the intimate connection with relational databases.
In fact, it is worth stressing that a "relation" in database jargon
means "table".

MsPASS supports mechanisms to import/export the following
types of tabular data:

- A large range of file-based formats.  Common examples include
  comma-separated value (CSV) files, Excel spreadsheets,
  most white-space separated text files, and various legacy fixed format
  tabular data files.
- Relational database servers commonly use Structured Query Language (SQL).
  SQL queries return tabular data that
  is readily handled in MsPASS with common Python packages.
- A special form of relational database in the seismology community is
  what is most properly called a "Datascope database" but which is commonly
  referred to as an "Antelope database".  Technically, Datascope is a
  component of the larger Antelope software suite.  Those tables are important
  as a number of regional seismic networks use Antelope as their database
  engine.  Furthermore, the Array Network Facility of the EarthScope USArray
  project used Antelope.  Its final CSS3.0 event database—including event
  catalogs, phase arrivals, and waveform metadata—is preserved in the
  `ANF CSS Event Database archive <https://doi.org/10.17611/DP/EB.1>`__.
  In any case, the key concept to remember is that Antelope/Datascope
  is just a different implementation of a relational database.  Below we
  describe a special interface in MsPASS to import data from Datascope tables.

In MsPASS the central concept used to unify these diverse sources is the
concept of a :code:`DataFrame`.  A :code:`DataFrame` is the central data
structure of the commonly used
`pandas <https://pandas.pydata.org/docs/>`__ Python package.
The name does not come from a fuzzy animal but is derived from
"panel data".  Pandas, Dask, and PySpark each provide DataFrame APIs for
manipulating tabular data, although their APIs are not identical.
Because both Dask and PySpark have DataFrame implementations,
it is particularly natural to manipulate tabular data in MsPASS through
the appropriate DataFrame API.
All "imports" below can be thought of as ways to convert a table
stored some other way to a DataFrame.

Import/Export Tables Stored in Files
---------------------------------------
Importing data from common tabular formats is
essentially a solved problem through the pandas, :code:`DataFrame` API.

- The documentation on reading tabular data files can be found
  `here <https://pandas.pydata.org/docs/reference/io.html>`__.
  There are also writers for most of the same formats documented on that
  same page.
- Dask's readers and writers are described
  `here <https://docs.dask.org/en/stable/dataframe-create.html>`__.
  The large-data model of a DataFrame in a Dask
  workflow is most applicable when the table is large compared to
  the memory space of a single node. Hence, something like an
  Excel spreadsheet is not normally an appropriate format for a gigantic
  table.  In contrast, a huge CSV data set is easy to construct from many
  identically structured pieces with tools such as the Unix ``cat`` command.
- PySpark has similar functionality, but a different API from pandas and
  Dask.  The documentation for the read/write interface can be found
  `here <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/io.html>`__.
  The list of formats PySpark can read or write is similar to pandas.

The most common requirement for reading tabular data is needing to import
nonstandard data from a research application.  There are
common examples in seismology that do not work with a standard reader, such as
the CMT catalog.  Most tabular data downloadable on the internet today,
however, is stored in one of the
standard formats.  For example, the following example is extracted from our
Jupyter notebook tutorial on MongoDB.  It shows how one can import
`PhaseNet <https://github.com/AI4EPS/PhaseNet>`__ output structured as a CSV file.
It also shows how the results can be written to MongoDB in a collection it
creates called "arrival":

.. code-block:: python

  import pandas as pd

  from mspasspy.db.client import DBClient

  db = DBClient().get_database("myproject")
  df = pd.read_csv("./data/picks.csv")
  doclist = df.to_dict("records")
  for doc in doclist:
      station_id = doc["station_id"]
      fields = station_id.split(".")
      doc["net"] = fields[0]
      doc["sta"] = fields[1]

  insert_many_output = db.arrival.insert_many(doclist)

The use of the Python string ``str.split`` method is necessary because the
PhaseNet ``station_id`` field combines multiple codes.  See the tutorial for
a more extensive explanation of this example.

The example retains the direct PyMongo ``insert_many`` operation used by the
tutorial.  Once the needed columns have been added to a DataFrame, the MsPASS
:py:meth:`save_dataframe<mspasspy.db.database.Database.save_dataframe>` method
is a higher-level alternative that performs the DataFrame-to-document
conversion and insertion.

Import/Export Data from an SQL Server
----------------------------------------

Importing data from an SQL server or writing a DataFrame to an
SQL server are best thought of as different methods of the
DataFrame implementation.  For example, if the data in the example above
had been stored on an SQL server you would change the line
:code:`df = pd.read_csv('./data/picks.csv')` to use the appropriate SQL
reader.  Pandas provides
`pandas.read_sql <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html>`__
and
`DataFrame.to_sql <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html>`__;
Dask provides SQL readers and a ``to_sql`` method; and PySpark reads and
writes database tables through JDBC.  See the I/O documentation linked above
for connection and engine requirements.

Import/Export of Data from Datascope
----------------------------------------

Although the Datascope (Antelope) database is an implementation of
a relational database, it does not work with the SQL API of
pandas, Dask, and PySpark because Datascope does not speak SQL.
On the other hand, Datascope is heavily used in seismology as a
means to create earthquake catalogs from local earthquake networks.
It also has a set of useful tools that complement MsPASS.  Therefore,
there are a lot of known ways that the Antelope software can be used
in combination with MsPASS to handle novel research problems.
As a result, we created a special API to interact with data
managed by an Antelope database.  The MsPASS API aims to
loosely mimic core SQL functionality using the pandas
:code:`DataFrame` implementation as the intermediary.

A Python class called
:py:class:`DatascopeDatabase<mspasspy.preprocessing.css30.datascope.DatascopeDatabase>`
is used as the agent for interacting with an
Antelope/Datascope database.  Unlike most SQL databases in use today,
Datascope does not use a client-server model
to manage the data it contains.  It is an example of what is
commonly called a "flat-file database" because the tables it manages
are stored in text files.  That is relevant for this discussion
only because of how those files are defined.  In
Datascope the file names are special and take the form
``dbname.table``.  ``dbname`` is the name of the collection
of tables that is the "database":  the "database name".  As the
word implies, ``table`` is the schema name for the table that the file
contains.  For example, if one sees a Datascope table with the file name
``usarray.arrival``, that file
is the "arrival" table in a database someone chose to call "usarray".

With that background, an instance of a
:py:class:`DatascopeDatabase<mspasspy.preprocessing.css30.datascope.DatascopeDatabase>`
can be created with a variant of the following code fragment:

.. code-block:: python

  from mspasspy.preprocessing.css30.datascope import DatascopeDatabase

  dsdb = DatascopeDatabase("usarray")

where ``usarray`` is the database name (and file-name prefix).  The constructor
also accepts an optional ``pffile`` argument defining the table layouts; when
it is omitted, MsPASS loads the bundled ``DatascopeDatabase.pf`` through
``$MSPASS_HOME``.

Experienced Datascope users will know that Datascope has a useful,
albeit confusing, feature that allows the collection of
files that define the database to be spread through multiple directories.
That feature is often used to place more static tables in a separate
``dbmaster`` directory.  The MsPASS adapter does **not** implement Datascope's
file-alias mechanism or search multiple directories from one handle.  Its
``dbname`` argument may, however, include a directory.  Create a separate
handle for each database prefix.  For example, to access ``usarray.*`` files
in ``~/dbmaster``, use:

.. code-block:: python

  import os

  dsdbm = DatascopeDatabase(os.path.expanduser("~/dbmaster/usarray"))

Once an instance of
:py:class:`DatascopeDatabase<mspasspy.preprocessing.css30.datascope.DatascopeDatabase>`
is created for the database prefix from which you want to import
one or more tables, fetching a table's data
is similar to that for the pandas SQL readers.  Use the
:py:meth:`get_table<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.get_table>`
method of
:py:class:`DatascopeDatabase<mspasspy.preprocessing.css30.datascope.DatascopeDatabase>`
to retrieve individual tables from the Datascope database
as a pandas DataFrame.  An important option described in the
docstring is a Python list passed through the optional
``attributes_to_use`` argument.  The default loads every attribute in the
CSS3.0 table schema.  Use a list to limit which attributes are retrieved.
That is frequently desirable as all CSS3.0 tables have attributes that
are often or nearly always null.

The following example shows a typical use of the
:py:meth:`get_table<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.get_table>`
method.  This example retrieves the coordinate data from
the ``usarray.site`` table using the ``dsdbm`` handle created with
the code line above:

.. code-block:: python

  coordlist = ["sta", "ondate", "offdate", "lat", "lon", "elev"]
  df = dsdbm.get_table("site", attributes_to_use=coordlist)

The result could be used for normalization to load coordinates by
station name.  (In reality there are some additional complications
related to the time fields and SEED station codes.  Those are separate from
the basic import operation shown here.)

The inverse of the
:py:meth:`get_table<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.get_table>`
method is the
:py:meth:`df2table<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.df2table>`
method.  As its name implies, it writes a pandas DataFrame as a properly
formatted Datascope table.  The ``db`` argument selects the output database
prefix, ``table`` selects the schema, and ``append`` controls whether an
existing table is extended (the default) or overwritten.  Missing schema
columns are filled with Datascope null values, and the returned DataFrame has
the table's column order.  For example, the following writes
``site_subset.site`` in the current directory, replacing that file if it
already exists:

.. code-block:: python

  df_written = dsdbm.df2table(
      df,
      db="site_subset",
      table="site",
      append=False,
  )

``df2table`` also has a ``dir`` argument for selecting an existing output
directory.  That argument belongs to this writer; it is not a constructor
argument.

Finally, the :code:`datascope.py` module also contains two
convenience methods that implement two common operations with
Datascope database tables:

#.  :py:meth:`CSS30Catalog2df<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.CSS30Catalog2df>`
    creates the standard "catalog-view" of CSS3.0.  In seismology a
    "catalog" is an image of what was once distributed as a book
    tabulating earthquake hypocenter estimates and arrival time data used
    to create those estimates.  CSS3.0 standardized a schema for creating
    a "catalog" as the join of four tables that this method creates:
    event->origin->assoc->arrival where "->" symbolizes a right database
    join operator.  It returns a pandas DataFrame that is the "catalog".
    Usage details can be gleaned from the docstring.
#.  :py:meth:`wfdisc2doclist<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.wfdisc2doclist>`
    can be thought of as an alternative to the MsPASS
    :py:meth:`index_mseed_file<mspasspy.db.database.Database.index_mseed_file>` method.
    It returns a list of Python dictionaries (documents) that are roughly
    equivalent to documents created by
    :py:meth:`index_mseed_file<mspasspy.db.database.Database.index_mseed_file>`.
    The main application is to use the alternative miniSEED indexer
    of Antelope.  There are many ways that raw miniSEED files from
    experimental data (i.e. data not sanitized for storage in the archives)
    can be flawed.  The Antelope implementation has more robust handlers
    for known problems than that in MsPASS.  For most uses we would encourage
    immediately writing the output to the standard MsPASS collection
    :code:`wf_miniseed` using a variant of this example.

.. code-block:: python

  from mspasspy.db.client import DBClient
  from mspasspy.preprocessing.css30.datascope import DatascopeDatabase

  client = DBClient()
  db = client.get_database("myproject")
  dsdb = DatascopeDatabase("usarray")
  doclist = dsdb.wfdisc2doclist()
  if doclist:
      db.wf_miniseed.insert_many(doclist)

.. note::

  Be warned that
  :py:meth:`wfdisc2doclist<mspasspy.preprocessing.css30.datascope.DatascopeDatabase.wfdisc2doclist>`
  only works with a ``wfdisc`` table that indexes miniSEED data.  It does not
  currently support other formats defined by CSS3.0.
