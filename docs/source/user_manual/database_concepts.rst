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

| The term "`schema <https://en.wiktionary.org/wiki/schema>`__" refers
  to the organization of data as a blueprint of how the database is
  constructed (divided into database tables in the case of `relational
  databases <https://en.wikipedia.org/wiki/Relational_databases>`__). 
  As this definition states in a relational database like CSS3.0 the
  schema defines a set of tables, attributes, and a how they are
  linked.   MsPASS uses a "nonSQL database", which means the interaction
  is not with Structured Query Language (SQL).   We use a particular
  form of nonSQL database called a "document database" as implemented in
  the open source package `MongoDB <https://www.mongodb.com/>`__. 

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
   preparation are the two examples of repetitive processing in
   seismology.)  A goals was to provide a mechanism for users to extend
   the database with little to no impact on the core system. 

| On the other hand, we have explicitly avoided worrying about problems
  we concluded were already solved.  These are:

#. *Catalog preparation.*   At this time a primary job of most
   operational seismic networks of all scales is preparation of a
   catalog of seismic events and linking that information to data used
   to generate the event location and source parameters.  There are
   multiple commericial and government supported systems for solving
   this problem.   We thus treat catalog data as an import problem.
#. *Real time processing*.   Although there are elements of MsPASS that
   are amenable to near real time processng of streaming data, we view
   real time processing as another solved problem outside the scope of
   this system.  

Collections
~~~~~~~~~~~~~

*wf*.  The wf collection (reminder:  a "collection" is roughly
equivalent to a table in a relational database and "document" is roughly
equivalent to a row (tuple) in a relation/table) is THE core table in
MsPASS.  All seismogram read operations access ONLY the wf collection. 
Writers can and are more complicated because they may have to deal with
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
  data model.  On the oher hand, the full system has support for
  *normalized* data managed by collections like site and source (see
  below - note the list of normalization collections is expected to
  evolve as MsPASS is developed). 

| This model was chosen exactly due to design issues 1 and 2 above: 
  data processing is efficient because database transactions are limited
  to the initial loading of data into the system, and the model
  statisfies the KISS principle because within a processing chain
  attributes look like header data accessible by simple name:value pair
  getters and putters.

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
  
| *history*. | An important requirement to create a reproducible result from data is a mechanism to create a full history that can be used to recreate a workflow.  The same mechanism provides a way for you to know the sequence of processing algorithms that have been applied with what tunable parameters to produce results stored in the database.  

A properly constructed algorithm to be used in MsPASS will register itself at the start of the (python) script that drives the workflow.   The registration process will create an document in the history collection for each algorithm whether the workflow is actually run or not.  The structure of an individual history document is easiest to see from this example:

::

'_id': ObjectId('5e3fecc021d9d7571de83241')
'jobid': 5
'testsimple':

  'algorithm': 'testsimple'
  
  'param_type': 'dict'
  
  'params': 
  
    'foo': 'bar'
    
    'testint': 10
    
    'testfloat': 2.0
    
'testpfalg': 

  algorithm': 'testpfalg'
  
  'param_type': 'AntelopePf'
  
  'params': {'simple_string_parameter': 'test_string', 'simple_real_parameter': 2.0, 
  ...} 

::

The example shows that each history document is indexed by a jobid that is assigned to the run when the registration process is completed.  The '_id' key is an alternative created by MongoDB that provides a unique key for any document.  That is followed in this example by two subdocuments posted in the order in which they were registered.  The key to each subducument ('testsimple' and 'testpfalg' for this example) are the names of the algorithm applied.   In this case they are artificial but a more typical might be a sequence like: 'filter','WindowData', 'deconvolution'. The subdocument keyed by each algorithm name has three field: 

1.  algorithm - duplicates the subdocument key
2. param_type defines the format of the params section
#. params is a block that is a dump of the parameters passed to the algorithm.

param_type can currently be either 'dict' or 'AntelopePf' although other formats may eventually be supported.  Use 'dict' to define simple command line arguments.  The normal expectation is they define the arguments passed to a function that is called in the processing script.  In python all arguments have a name keyword which would be the key to the dict and the value passed would be the other half of the key:value pair.  The 'AntelopePf' format is much more complicated.  We translate the structure of the text file that would be the originating Pf file into a (possibly nested) dict that is saved as the value attached to 'params'.  The user should not normally be concerned about this detail as utility functions are provided to translate this complicated dict structure to the structure of the original antelope pf file. 


| *global*.  Not yet implemented, but something we need.  Should be a
  place to hold global attributes.  Examples might be unit definitions,
  space tolerance for site information, space-time tolerance for events,
  and an alternative to yaml storage of data stored now in mspass.yaml.   
