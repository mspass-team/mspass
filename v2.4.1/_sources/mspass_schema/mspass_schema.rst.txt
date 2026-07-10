.. _mspass_schema:

MsPASS Schema
-------------

Many seismologists think of a database schema as the specification of tables
and attributes in a relational database.  The standard relational schema in
seismology is CSS3.0, which is used by systems such as Antelope.  In computer
science, however, the term has a broader meaning: it describes the structure,
types, and constraints of data.  MsPASS uses that broader meaning because its
primary database backend is MongoDB rather than a relational database.

This section documents the MongoDB collections and metadata keys recognized by
MsPASS.  Each table is generated from ``data/yaml/mspass.yaml`` during the
Sphinx build, so the published tables track the same schema used by the Python
database layer.

The tables below are organized in three ways:

1. The database collection schemas defined in the YAML ``Database`` section.
2. A complete alphabetical catalog of valid metadata keys and aliases.
3. Groups of keys that are normalized into master MongoDB collections, such as
   receiver and source coordinates.
4. Groups of keys that have a practical or conceptual relationship, such as
   ObsPy ``Trace`` attributes, three-component orientation metadata, and file
   storage metadata.

Database Collections
~~~~~~~~~~~~~~~~~~~~

These tables describe the MongoDB collections defined by the YAML schema.  They
include operational collections such as ``elog``, ``cemetery``, and
``abortions`` that are populated by the database and dead-data handling code.

.. include:: database_collections.inc

All Valid Attributes
~~~~~~~~~~~~~~~~~~~~

The table below defines all attributes MsPASS should handle without special
treatment.  They are arranged in alphabetical order. (Note all keys are
case sensitive.  In UTF-8 alphabetical order capitals appear before lower case.)

.. csv-table:: **All MsPASS Attributes**
   :file: all.csv
   :widths: 12,18,8,10,12,50
   :header-rows: 1

The key in the master list above is always used as the index to locate an
attribute for a particular concept in MongoDB.  However, MsPASS also implements a generic
alias mechanism that allows a function to fetch an attribute internally
(i.e. when the data are in memory for processing) using an alternative name (alias).
The table below defines all valid aliases for unique keys with the alias keys
separated by colons:

.. csv-table:: **All MsPASS aliases**
   :file: aliases.csv
   :widths: 20,60
   :header-rows: 1

Master Documents for Normalized Attributes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Site Collection (Receiver coordinates)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This set of attributes defines the physical location of a seismic sensor on the
Earth.  It also lists related attributes used for cross-referencing documents
in the ``site`` collection.

.. csv-table:: **Receiver coordinate (site collection) Attributes**
    :file: site.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

Source Collection (Seismic Source coordinates)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This set of attributes defines location estimates or measured locations of
seismic sources.  MsPASS does not support the concept of
multiple location estimates for the same seismic events that is a major
complication in the relational database schema CSS3.0 used, for example,
in Antelope.  Each source is defined by the unique id ``source_id`` listed in
the table below.

.. csv-table:: **Source coordinate (source collection) Attributes**
    :file: source.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

Attributes with Conceptual Relationship
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

ObsPy Trace Object
^^^^^^^^^^^^^^^^^^

The ObsPy package on which MsPASS leans heavily defines a ``Trace`` object with
a fixed set of required parameters.  The names can be aliased but the
unique keys of all of the attributes below are identical to the name defined
for the ObsPy ``Trace`` object.

.. csv-table:: **Required Attributes for obspy Trace object**
    :file: obspy_trace.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

Channel Attributes
^^^^^^^^^^^^^^^^^^

.. csv-table:: **Channel attributes**
    :file: channel.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

Three-Component Data Attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These attributes describe the orientation and transformation matrix used by
three-component seismic data objects.

.. csv-table:: **Three-component data attributes**
    :file: 3Cdata.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

Seismic Phase Related Attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. csv-table:: **Phase attributes**
    :file: phase.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

External File Attributes
^^^^^^^^^^^^^^^^^^^^^^^^

.. csv-table:: **File attributes**
    :file: files.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1

Indexing Attributes for MongoDB
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. csv-table:: **MongoDB attributes**
    :file: MongoDB.csv
    :widths: 12,18,8,10,12,50
    :header-rows: 1
