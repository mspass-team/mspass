*********************************
MsPASS Schema
*********************************
May seismologists think of a database schema as the specification of a set of
tables and attributes in a relational database schema.  The standard one in
seismology is that defined decades ago by the Center for Seismic Studies
commonly referred to as CSS3.0 in, for example, Antelope.   In computer
science, however, the term has a more abstract definition.   For a readable
description follow `this link <https://en.wikipedia.org/wiki/Database\_schema>`_
to a short article in Wikipedia.  We use schema in the broader sense defined
in that article because MsPASS utilizes the nonSQL (not relational) database
called MongoDB.

This section addresses two key topics about the MsPaSS schema:

1.  The atomic object in MongoDB is a name-value pair.   This section contains
a table of all recognized keys, the data types that we assume are associated
with each key, and the short verbal description of the concept that the value
associated with the key defines.   The document also contains a set of useful
subsets of the full table that contain attributes that have an association.
For example, one table defines attributes related to a sensors position
on the Earth.  (Note "table" is this context is what normal people would
understand as a table and has nothing to do with relation in a relational
database)

2. As noted above some name-value pairs have a conceptual relationship like
sensor location attributes.  That idea plays a fundamental role in
defining constraints on relations in a relational database.  In MsPASS
such associations can be either conceptual or imposed as a loose a constraint.
An example of a conceptual constrain is the table below showing attributes that
define a transformation matrix for three-component seismic data.  The constraint
there is conceptual because the user needs to understand what they are and how
they are used, but the database itself simply treats them as just one of
many possible name-value pairs.  In contrast, source and receiver
location attributes have internal constraints in MongoDB.  In particular, some
sets of attributes like source and receiver coordinates use what
MongoDB calls the
`normalized data model <https://docs.mongodb.com/manual/core/data-model-design/>`_ .
Normalized data have a central home in a separate collection from the wf
collection that links one-to-one with each waveform data object that defines
your data set.  All normalized attributes are defined in separate tables below.

Finish later here with a roadmap with links to tables below.

#####################
All Valid attributes
#####################
The table below defines all attributes MsPaSS should handle without special
treatment.  They are arranged in alphabetical order. (Note all keys are
case sensitive.  In UTF-8 alphabetical order capitals appear before lower case.)

.. csv-table:: **All MsPASS Attributes**
   :file: all.csv
   :widths: 12,5,5,50
   :header-rows: 1

The key in the master list above is always used the index to locate a attribute
for a particular concept in MongoDB.   However, MsPaSS also implements a generic
alias mechanism that allows a function to fetch an attribute internally
(i.e. when the data are in memory for processing) using an alternative name (alias).
The table below defines all valid aliases for unique keys with the alias keys
separated by colons:

.. csv-table:: **All MsPASS aliases**
   :file: aliases.csv
   :widths: 20,60
   :header-rows: 1

###########################################
Master Documents for Normalized Attributes
###########################################

=======================================
Site Collection (Receiver coordinates)
=======================================

This set of attributes are used to define the physical location of a seismic sensor
on the Earth.   Also listed are related attributes used for cross referencing
with a collection with the special name 'site'.

.. csv-table:: **Receiver coordinate (site collection) Attributes**
    :file: site.csv
    :widths: 12,5,5,50
    :header-rows: 1

===============================================
Source Collection (Seismic Source coordinates)
===============================================

This set of attributes are used to define location estimates or measured
locations of seismic sources.   MsPASS does not support the concept of
multiple location estimates for the same seismic events that is a major
complication in the relational database schema CSS3.0 used, for example,
in Antelope.   Each source defined by two alternative unique ids:  evid and
oid_source defined in the table below.

.. csv-table:: **Source coordinate (source collection) Attributes**
    :file: source.csv
    :widths: 12,5,5,50
    :header-rows: 1

###############################################################
Attributes with Conceptual Relationship
###############################################################

=============================
obpsy Trace object
=============================

The obspy package on which MsPASS leans heavily defines a Trace object with
a fixed set of required parameters.  The names can be aliased but the
unique keys of all of the attributes below are identical to the name defined
for the obspy Trace object.

.. csv-table:: **Required Attributes for obspy Trace object**
    :file: obspy_trace.csv
    :widths: 12,5,5,50
    :header-rows: 1

==============================
Channel (sitechan) attributes
==============================
.. csv-table:: **Channel attributes**
    :file: sitechan.csv
    :widths: 12,5,5,50
    :header-rows: 1

=================================
Seismic phase related attributes
=================================
.. csv-table:: **Phase attributes**
    :file: phase.csv
    :widths: 12,5,5,50
    :header-rows: 1

=============================
External File Attributes
=============================
.. csv-table:: **File attributes**
    :file: files.csv
    :widths: 12,5,5,50
    :header-rows: 1
    
================================
Indexing Attributes for MongoDb
================================
.. csv-table:: **MongoDB attributes**
    :file: MongoDB.csv
    :widths: 12,5,5,50
    :header-rows: 1
