.. _database_concepts:

Database Concepts
========================

NonSQL Database
------------------------

| MsPASS uses a NonSQL database called MongoDB.   NonSQL is a generic
  name today for a database system that does not utilize the structure
  query language (SQL).  SQL is the standard language for interacting
  with relational database systems like Oracle, MySQL, PostGRES, etc.
  One type of NonSQL database is a "document database".  MongoDB is
  classified as a document database.   Readers unfamiliar with the
  concept of a document database are referred to online sources which
  are far better than anything we could produce.   A good starting point
  is the `MongoDB tutorial
  introduction <https://docs.mongodb.com/manual/introduction/>`__.
  Another good source is `this
  one <https://www.tutorialspoint.com/mongodb/index.htm>`__ on
  tutorialspoint.

Schema
---------------

Overview
~~~~~~~~~~

Wikepedia defines a database schema as follow:

| The term "`schema <https://en.wiktionary.org/wiki/schema>`__"
  refers to the organization of data as a blueprint of how the database
  is constructed (divided into database tables in the case of `relational
  databases <https://en.wikipedia.org/wiki/Relational_databases>`__)
  the schema defines a set of attributes, tables (relations), and how
  they can be linked (joined).

As this definition states in a relational database like CSS3.0 the
schema defines a set of attributes, tables (relations) and a how they are
linked (joined).   MsPASS uses a "nonSQL database", which means the interaction
is not with Structured Query Language (SQL).   We use a particular
form of nonSQL database called a "document database" as implemented in
the open source package `MongoDB <https://www.mongodb.com/>`__.
The top-level concept for understanding what MongoDB is name-value pairs.
One way of thinking of MongoDB is that it only implements each attribute
as a name-value pair:  the name is the key that defines the concept and
the attribute is the thing that defines that concept.  The thing can
be something as simple as an integer number or as elaborate as any python
object.  Table (relations) are replaced with that concept of a "document"
that is conceptually similar but operationally very different.
A document is a collection of name-value pairs that is conceptually
similar to a single tuple in a table (relation).

Design Concepts
~~~~~~~~~~~~~~~~~

A properly designed database schema needs to prioritize the problem it
aims to solve.   The schema for MsPASS was aimed to address the
following design goals:

#. *Efficient flow through Spark.* A key reason MongoDB was chosen as
   the database engine for MsPASS was that it is cleanly integrated with
   Spark.   Nonetheless, the design needs to minimize database
   transaction within a workflow.   Our aim was to try to limit database
   transaction to reading input data, saving intermediate results, and
   saving a final result.
#. *KISS (Keep It Simple Stupid).* Experience has shown clearly that
   complex relational schemas like CSS3.0 have many, sometimes subtle,
   issues that confound beginners.  A case in point is that large
   organizations commonly have a team of database managers to maintain
   the integrity of their database and optimize performance.   An
   important objective of our design is to keep it simple so scientists
   do not have to become database managers to work with the system.
#. *Efficient and robust handling of three-component seismograms.*
   Although MsPASS supports :ref:`scalar seismic
   data, <data_object_design_concepts>` our view is that the
   greater need in the community is an efficient system for handling 3C
   data.   In reality, our schema design ended up completely neutral on
   this point and scalar and 3C data are handled identically.  The only
   differences is what attributes (Metadata) are required.
#. *Provide a clean mechanism to manage static metadata.* MsPASS is a
   system designed to process a "data set", which means the data are
   preassembled, validated, and then passed into a processing chain.
   The first two steps (assembly and validation) are standalone tasks
   that require assembly of waveform data and a heterogenous collection
   of metadata from a range of sources.   Much of that problem has been
   the focus of extensive development work by IRIS and the FDSN.   We
   thus provide tools for importing datasets through web services that
   have become the current, standard exchange for data and Metadata.
   The complexity of the Metadata is the reason that development effort
   has been far from trivial.   A primary issue is that some attributes
   change with nearly every seismogram (e.g. the start time of a
   waveform), while many others either never change (e.g. a station name
   almost never changes) or change rarely (e.g. a change in sensor
   orientation).  We aimed for a balance to efficiently manage static
   data while avoiding all the things that can go wrong that have to be
   handled by network operators and the PIs of shorter term
   deployments.
#. *Extensible.* A DBMS cannot be too rigid, or it will create
   barriers to progress.  This is especially important to MsPASS as our
   objective is to produce a system for seismic research, not a
   production system for repetitive processing of the similar data.
   (Seismic reflection processing and seismic network catalog
   preparation are two examples of repetitive processing in
   seismology.)  A goals was to provide a mechanism for users to extend
   the database with little to no impact on the core system.

| On the other hand, we have explicitly avoided worrying about problems
  we concluded were already solved.  These are:

#. *Catalog preparation.*   At this time a primary job of most
   operational seismic networks of all scales is preparation of a
   catalog of seismic events and linking that information to data used
   to generate the event location and source parameters.  There are
   multiple commercial and government supported systems for solving
   this problem.   We thus treat catalog data as an import problem.
#. *Real time processing*.   Although there are elements of MsPASS that
   are amenable to near real time processing of streaming data, we view
   real time processing as another solved problem outside the scope of
   this system.

Collections
~~~~~~~~~~~~~
*Overview*.  In MongoDB a *collection* is roughly equivalent to a table (relation)
in a relational database.  Each collection holds one or more *documents*.
A single document is roughly equivalent to a tuple in a relational database.
In this section we describe how we group documents into collections defined
in MsPASS.   These collections and the attributes they contain are the
*schema* for MsPASS.

*wf*.  The wf collection is THE core table in
MsPASS.  All seismogram read operations access ONLY the wf collection.
Writers are more complicated because they may have to deal with
newly generated attributes and potentially fundamental changes in the
nature of the waveform we want to index.  *e.g.*, a stack can become
completely inconsistent with the concept of a station name and may
require creation of a different set of attributes like an index back to
the parent seismograms that created the stack.   We thus define this
rule that all users need to recognize in designing a MsPASS workflow:

| **Rule 1**.  All required attributes to run a workflow must exist in the
  wf collection before starting the job.

| An abstract way of saying this is that at the start of processing the
  input data must obey what MongoDB calls an
  `embedded <https://docs.mongodb.com/manual/core/data-model-design/>`__
  data model.  To many seismologists a less abstract way of saying this is
  that the database contents must define all required header attributes
  needed for the processing. On the other hand, the full system has support for
  *normalized* data managed by collections like site and source (see
  below - note the list of normalization collections is expected to
  evolve as MsPASS is developed).

| This model was chosen exactly due to design issues 1 and 2 above:
  data processing is efficient because database transactions are limited
  to the initial loading of data into the system, and the model
  satisfies the KISS principle because within a processing chain
  attributes look like header data accessible by simple name:value pair
  getters and putters.

| Users must also realize that the sample data in Seismogram or TimeSeries objects
  be constructed from *wf* documents in one of two ways.  First, the sample data
  can be stored in the more conventional method of CSS3.0 based systems
  as external files.   In this case, we use the same construct as CSS3.0 where
  the correct information is defined by three attribures:  *dir*, *dfile*, and
  *foff*.   Unlike CSS3.0 MsPASS currently requires external file data to be
  stored as native 64 bit floating point numbers.   We force that restriction
  for efficiency as the Seismogram *u* variable and the TimeSeries *s*
  variable can then be read and written with fread and fwrite respectively from
  the raw buffers.  The alternative (second) method for storing sample data
  in MsPASS is through a mechanism called *gridfs* in MongoDB.  When this
  method is used (it is the default) the waveform sample data are managed
  by file system like handles inside MongoDB.  That process is largely hidden
  from the user, but the most important thing the user must recognize is
  that when this method is used the sample data are stored in the same
  disk area where MongoDB stores it's other data.  Details about the
  interaction this method requires with the gridfs_wf collection are given below.

| *gridfs_wf*.  This collection is best thought of as an auxiliary
  collection that comes into play when the (default) *gridfs* approach
  is used to store waveform sample data.  Each document in *gridfs_wf*
  has a one-to-one relation with a related document in *wf*.  The
  entry in *wf* is treated as the master since all processing in MsPASS is
  driven by the *wf* collection.   The gridfs attributes needed to load
  waveform data stored through *gridfs_wf* are linked to *wf* through
  a MongoDB object id.  Specifically, *wf* documents with data stored in
  gridfs use the attribute name *gridfs_wf_id* to hold the ObjectID of the
  document in *gridfs_wf* that defines the waveform data.  Readers
  thus need to either join gridfs_wf and wf with the gridfs_wf_id key or
  run a large number find transactions to connect the proper wf and gridfs_wf
  documents.

| *elog*.   The elog collection holds log messages that should
  automatically be posted and saved in a MsPASS workflow.  The elog
  collection saves any entries in ErrorLogger objects that are
  components of all data objects handled internally by MsPASS.   The
  main idea of an ErrorLogger is a mechanism to post errors of any level
  of severity to the data with which the error is associated, preserve a
  record that can be used by the user to debug the problem, and allow
  the entire job to run to completion even if the error made the data
  invalid.  More details about this idea can be found in the :ref:`Data
  Objects <data_object_design_concepts>` section.

| *site*. The site collection is intended as a largely static table
  that can be used to
  `normalize <https://docs.mongodb.com/manual/core/data-model-design/>`__
  a wf collection.   The name is (intentionally) identical to the CSS3.0
  site table.   It's role is similar, but not identical to the CSS3.0
  table.  There are two primary differences.  First, the (unnecessary in
  our view) requirement of a station name tag is not required in the
  documents the collection contains.  It is optional.  Second, the link
  to data is not defined through a station name tag, but by a form of
  spatial query.  The use of a spatial query was a design decision based
  on the author's experiences using CSS3.0's site table as implemented
  in Antelope.   Antelope uses the station name and a time period as a
  key to find location information for a waveform.   That model works
  well for bulletin preparation but creates a dilemma for processed
  waveforms;  the concept of a "station name" is meaningless for many
  types of processed waveform.  Two type examples, are a phased array
  beam and Common Conversion Point (CCP) stacks of receiver functions.
  On the other hand, many such processed waveforms have a space concept
  that needs to be preserved.  Hence, the location information in the
  collection may relate to some more abstract point like  piercing point
  for a CCP stack.   Each document is keyed with a unique integer with
  tag (siteid) as well as the ObjectId that is automatically generated
  (and required) by MongoDb.   wf documents can index a location in site
  either through the siteid, or the ObjectId of an entry in the
  collection (the choice is implementation dependent).

| A spatial query to link anything to a point in the site collection has
  two complexities:  (1) all spatial queries require a uncertainty
  specification that are data and implementation dependent, and (2)
  sometimes, but not always, a vertical position (site_elev) needs to be
  defined.  The first is readily solved with the geospatial indexing
  capabilities of MongoDB.   Geospatial queries can define a radius of
  uncertainty to efficiently find one or more documents linked to a
  circle defined relative to a query point.  The size of such a circle
  is always a data dependent choice;  a scientist working with free
  oscillations of the earth require station coordinates with minimal
  precision, while an active source experiment often requires submeter
  location precision.   We treat vertical positions differently.  The
  common key to define vertical position is *site_elev*.   How to handle
  vertical position is application dependent.  *e.g.* to look up the
  location of an operational GSN station, it may be necessary to
  distinguish borehole and vault instruments that are deployed at many
  stations.   In contrast, a point defined by piercing points for a CCP
  stack would normally be assumed referenced to a common, fixed depth so
  site_elev may not even be needed.  We deal with this complexity by a
  defining another rule that user's need to recognize and abide by:

| **Rule 2**. The site collection only contains points in space relevant to
  the data set.   Assembly of a working data set requires linking
  required points in site to wf documents as required and defining the
  coordinates with the proper wf keys.

| As an example, to begin processing on a set of raw waveforms imported
  from the FDSN the wf collection would normally need to be normalized
  with data from site to set geographic locations of the instrument that
  generated each wf entry:  *site_lat, site_lon,* and *site_elev*.
  Partially processed wf entries may require the definition of
  additional geospatial points in site.

| Managing response information for seismic instruments is a related problem.
  We handle it through site, but recognize that response information has
  issues similar to that discussed above for the simpler concept of a
  station name.  That is, response data is not always required or even
  necessary for the workflow (e.g. in reflection processing most algorithms
  assume all data have a common instrument response and algorithm only cares
  that they are matched.).  We treat response data as a raw waveform attribute
  that can optionally be utilized by obspy tools.  The site collection
  can contain an optional pickled version of the obspy Inventory object
  that can be used by algorithms to implement response corrections.

| *source*. The source collection has much in common with site, but
  has two fundamental differences:  (1) the origin time of each source
  needs to be specified, and (2) multiple estimates are frequently
  available for the same source.

| The origin time issue is a more multifaceted problem that it might
  first appear.  The first is that MongoDB, like ArcGIS, is map-centric
  and stock geospatial queries lack a depth attribute, let alone a time
  variable.   Hence, associating a waveform to a source position defined
  in terms of hypocenter coordinates (*source_lat, source_lon,
  source_depth*, and *source_time*) requires a multistage query that can
  potentially be very slow for a large data set.   Hence, Rule 2 could
  be restated as Rule 3 with "site collection" replaced everythere by
  "source collection".

| The other issue that distinguishes origin time is that it's accuracy
  is data dependent.   With earthquake it is always estimated by an
  earthquake location algorithm, while with active source it normally
  measured directly.  The complexity with active source data is a
  classic case distinguishing "precision" from "accuracy".   Active
  source times relative to the start time of a seismogram may be very
  precise but not accurate.  A type example is multichannel data where
  time 0 of each seismogram is defined by the shot time, but the
  absolute time linked to that shot may be poorly constrained.   We
  address this problem in MsPASS through the concept of UTC versus
  "Relative" time definined in all MsPASS data objects.  See the :ref:`Data
  Object section <data_object_design_concepts>` on BasicTimeSeries
  for more on this topic.

| A final point about the source table is the issue of multiple
  estimates of the same event.   The CSS3.0 has an elaborate mechanism
  for dealing with this issue involving three closely related tables
  (relations):  event, origin, assoc, and arrival.   The approach we
  take in MsPASS is to treat that issue as somebody else's problem.
  Thus, for the same reason as above we state rule 3 which is very
  similar to rule 2:

| **Rule 3**.  The source collection should contain any useful source
  positions that define locations in space and time (attributes
  *source_lat, source_lon, source_depth*, and *source_time*).  Linking
  each document in a wf collection to the desired point in the source
  collection is a preprocessing step to define a valid dataset.

| The consequences to the user is that associating each document in *wf* with
  the correct source information must be understood as a processing step.
  Once a MsPASS workflow is initiated it can and should assume the source
  information loaded is the best available.   We also emphasize that for
  efficiency preprocessing needs to load source coordinates (or any other
  required source information like uncertainties) as attributes in the
  *wf* collection copied from *site*.  That is required because algorithms
  in a MsPASS workflow can and should always avoid database transactions
  within the workflow other than saving intermediate or final results to
  the *wf* collection.

| *history*. | An important requirement to create a reproducible result from
  data is a mechanism to create a full history that can be used to recreate
  a workflow.  The same mechanism provides a way for you to know the sequence
  of processing algorithms that have been applied with what tunable parameters
  to produce results stored in the database.  The history collection stores this
  information.   Most users should never need to interact directly with this
  collection so we omit any details of the history collection contents from
  this manual.  Users should, however, understand the concepts described
  in - link to new document in this manual on ProcessingHistory concepts --

| *global*.  Not yet implemented, but something we need.  Should be a
  place to hold global attributes.  Examples might be unit definitions,
  space tolerance for site information, space-time tolerance for events,
  and an alternative to yaml storage of data stored now in mspass.yaml.
