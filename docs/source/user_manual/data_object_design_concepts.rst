.. _data_object_design_concepts:

Data Object Design Concepts
===========================

Overview
~~~~~~~~

| The core data objects in MsPASS were designed to encapsulate the most
  atomic objects in seismology waveform process:  scalar (i.e. single
  channel) signals, and three component signals.   The versions of these
  you as a user should normally interact with are two objects defined in
  MsPASS as *TimeSeries* and *Seismogram* respectively.  

| These data objects were designed to simply interactions with MongoDB. 
  MongoDB is completely flexible in attributes names handled by the
  database.  We manage the attribute names and data types through an
  interfacing object we call *MetadataDefinitions* that is a required in
  most python database interactions.

| Data objects are grouped in memory with a generic concept called an
  *Ensemble*.   The implementation in C++ uses a template to define a
  generic ensemble.   A limitation of the current capability to link C++
  binary code with python is that templates do not translate directly.  
  Consequently, the python interface uses two different names to define
  Ensembles of TimeSeries and Seismogram objects:  *TimeSeriesEnsemble*
  and *ThreeComponentEnsemble* respectively. *  ThreeComponentEnemble*
  is a bit of a mismatch in a naming convention, but we felt the name
  was long enough already that the name seems clearer than the
  alternative of SeismogramEnsemble. 

| The C++ objects have wrappers for python that hide the details from
  the user.   All MongoDB operations implemented with the pymongo
  package using these wrappers.   Numerical operations on the sample
  data should either be written in C/C++ with their own wrappers or
  exploit numpy/scipy numerical routines.   The later is possible
  because the wrappers make the data arrays look like numpy arrays.  

History
~~~~~~~

| It might be helpful for the user to recognize that the core data
  objects in MsPASS are the second generation of a set of data objects
  developed by one of the authors (Pavlis) over a period of more than 15
  years.   The original implementation was developed as a component of
  Antelope.  It was distribured via the open source additions to
  Antelope distributed through the `antelope user's
  group <antelopeusersgroup>`__ and referred to as SEISPP.   The bulk of
  the original code can be found
  `here <https://github.com/antelopeusersgroup/antelope_contrib/tree/master/lib/seismic/libseispp>`__
  in github, and doxygen generated pages comparable to those found with
  this package can be found
  `here <http://www.indiana.edu/%7Epavlab/software/seispp/html/index.html>`__. 

| To build the core data objects from this older library we followed
  standard advice and mostly burned the original keeping only the most
  generic with features the authors had found essential over the
  years.   The revisions followed these guidelines:

-  Make the API as generic as possible.
-  Use inheritance more effectively to make the class structure more
   easily extended to encapsulate different variations in seismic data.
-  Divorce the API completely from Antelope to achieve the full open
   source goals of MsPASS.  Although not implemented at this time, the
   design will allow a version 2 of SEISPP in Antelope, although that is
   a "small matter of programming" that may never happen.
-  Eliminate unnecessary/extraneous constructors and methods developed
   by the authors as the class structure evolved organically over the
   years.  In short, think through the core concepts more carefully and
   treat SEISPP as a prototype.
-  Extend the Metadata object (see below) to provide support for more
   types (objects) than the lowest common denominator of floats, ints,
   strings, and booleans handled by SEISPP.  

| MsPASS has hooks to and leans heavily on
  `obspy <https://github.com/obspy/obspy/wiki>`__.   We chose, however,
  to diverge some from obspy in some fundamental ways.   We found two
  fundamental flaws in obspy for large scale processing that required
  this divergence:

#. obspy handles what we call Metadata through set of python objects
   that have to be maintained separately and we think unnecessarily
   complicate the API.   We aimed instead to simplify the management of
   Metadata much as possible.  Our goal was to make the system more like
   seismic reflection processing systems that manage the same problem
   through a simple namespace wherein metadata can be fetched with
   simple keys.   We aimed to hide any hierarchic structures (e.g
   relational database table relationships, obspy object hierarchies, 
   or MongoDB normalized data structures) behind the
   *MetadataDefinitions* object to reduce all Metadata to pure
   name:value pairs. 
#. obspy does not handle three component data in a native way, but mixes
   up the concepts we call *Seismogram* and *Ensemble* in to a common
   python object they define as a
   `Stream <http://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.html#obspy.core.stream.Stream>`__.  
   We would argue our model is a more logical encapsulation of the
   concepts that define these ideas. 

Core Concepts
~~~~~~~~~~~~~

Overview - Inheritance Relationships
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

| The reader needs to first see the big picture of how TimeSeries and
  Seismogram objects are defined to understand the core concepts
  described in sections that follow.  We assume the reader has some
  understanding of the concepts of inheritance in object oriented
  code.   The inheritance structure use we best understood as derived
  from the SEISPP prototype (see history above).  We aimed to rebranch
  and prune SEISPP  based on the experience from 15 years of development
  for SEISPP.

| The (admittedly) complicated inheritance diagrams for TimeSeries and
  Seismogram objects generated by doxygen are illustrated below
| |TimeSeries Inheritance|

| |Seismogram Inheritance|

| Notice that both CoreSeismogram and CoreTime series have a common
  inheritance from three base classes:  *BasicTimeSeries,
  BasicMetadata,* and *MsPASSCoreTS*.   Python supports multiple
  inheritance and the wrappers make dynamic casting within the hierarchy
  automatic.  e.g. a *Seismogram* object can be passed directly to a
  python function that does only Metadata operations and it will be
  handled seamlessly because python does no enforce type signatures on
  functions.  CoreTimeSeries and CoreSeismogram should be though of a
  core data that is independent of MsPASS.   Common features need by
  both objects to interact with MsPASS are inherited from
  MsPASSCoreTS.    A key point is that future users could chose to prune
  the MsPASSCoreTS component and build on CoreTimeSeries and
  CoreSeismogram and have no dependence upon MsPASS to build a
  completely different framework. 

| The remainder of this section discusses the individual components in
  the class hierarchy.

:ref:`BasicTimeSeries<mspass.BasicTimeSeries>` - Base class of common data characteristics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This base class object can be best viewed as an answer to this
questions:  What is a time series?   Our design answers this question by
saying all time series data have the following elements:

#. We define a time series as data that has a **fixed sample rate**.  
   Some extend this to arbitrary x-y data, but we view that as wrong. 
   Standard textbooks on signal processing focus exclusively on
   uniformly sampled data.  With that assumption the time of any sample
   is virtual and does not need to be stored.  Hence, the base object
   has methods to convert sample numbers to time and the inverse (time
   to sample number).
#. Data processing always requires the time series have a **finite
   length**.   Hence, our definition of a time series directly supports
   windowed data of a specific length (public attribute *ns* - mnemonic
   abbreviation for number of samples).  This definition does not
   preclude an extension to modern continuous data sets that are too
   large to fit in memory, but that is an extension we don't currently
   support. 
#. We assume the data has been cleaned and **lacks data gaps**.  Real
   continuous data today nearly always have gaps at a range of scale
   created by a range of possible problems that create gaps:  telemetry
   gaps, power failures, instrument failures, time tears, and with older
   data gaps created by station servicing.  MsPASS has stub API
   definitions for data with gaps, but these are currently not
   implementations.   Since the main goal of MsPASS is to provide a
   framework for efficient processing of large data sets, we pass the
   job of finding and/or fixing data gaps to other packages or
   algorithms using MsPASS with a "when in doubt throw it out" approach
   to editing.   The machinery to handle gap processing exists in both
   obpsy and Antelope and provide possible path to solution for users
   needing more extensive gap processing functionality.

| BasicTimeSeries uses public attributes to define the base properties
  discussed in the points above and has methods that are common to any
  data with these properties.  (e.g. a time(n) method returns the
  computed time for sample number n.)   An unusual attribute borrowed
  from reflection processing is the boolean variable with the name
  *live*.   Data not marked live (live == false) should normally be
  passed through a processing chain, but will always be dropped by
  database writers.  Other public attributes are public for convenience,
  but changing any of them must be done with caution.  

Handling Time
^^^^^^^^^^^^^

| MsPASS uses a generalization to handle time that is the same as a
  novel method used in the original SEISPP library.   The concept can be
  thought of as a generalized, but yet simplified version of how SAC
  handles time.   The time standard is defined by an enum class in C++
  called tref which is mapped to fixed names in python.   There are
  currently two options: 

#. When tref is TimeReferenceType::Relative (TimeReferenceType.Relative
   in python) the computed times are some relatively small number from
   some well defined time mark.   The most common relative standard is
   the implicit time standard used in all seismic reflection data:  shot
   time.   SAC users will recognize this ideas as the case when
   IZTYPE==IO.   Another important one used in MsPASS is an arrival time
   reference, which is a generalization of the case in SAC with
   IZTYPE==IA or ITn.  We intentionally do not limit what this standard
   actually defines as how the data are handled depends only on the
   choice of UTC versus Relative.  The ASSUMPTION is that if an
   algorithm needs to know the detail of "relative to what?" means, that
   detail will be defined in a Metadata attribute.
#. When tref is TimeReferenceType::UTC (TimeReferenceType.UTC in python)
   all times are assumed to be an absolute time standard defined by
   coordinated universal time (UTC).   We follow the approach used in
   Antelope and store ALL times defined as UTC with `unix epoch
   times. <https://en.wikipedia.org/wiki/Unix_time>`__  We use this
   simple approach for two reasons:  (1) storage (times can be stored as
   a simple double precision (64 bit float) field), and (2) efficiency
   (computing relative times is trivial compared to handling calendar
   data).   This is in contrast to obspy which require ALL start times
   (t0 in mspass data objects) be defined by a python class they call
   `UTCDateTime <https://docs.obspy.org/packages/autogen/obspy.core.utcdatetime.UTCDateTime.html#obspy.core.utcdatetime.UTCDateTime>`__. 
   Since MsPASS is linked to obspy we recommend you use the UTCDateTime
   class in python if you need to convert from epoch times to one of the
   calendar structures UTCDateTime can handle.

| A more concise summary of what these two time standard mean is this: 
  active source data always use Relative time and earthquake data are
  always stored in raw form as UTC time stamps (e.g. see the SEED
  standard).  UTC is a fixed standard while Relative could have other
  meanings.
| BasicTimeSeries defines two methods to convert between these two time
  standards:  rtoa (Relative to Absolute) and ator (Absolute to
  Relative).  Be aware the library has internal checks to avoid an
  invalid conversion from relative to absolute with the rtoa() method. 
  This was done to avoid errors from trying to convert active source
  data to an absolute time standard when the true time is not well
  constrained. 

Metadata and MetadataDefinitions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

| All data objects used by the MsPASS C++ library inherit a Metadata
  object.  A *Metadata* object is best thought of through either of two
  concepts well known to most seismologists:  (1) headers (SAC), and (2)
  a dictionary container in python.   Both are ways to handle a general,
  modern concept of
  `metadata <https://en.wikipedia.org/wiki/Metadata>`__ commonly defined
  as "data that provides information about data".  Packages like SAC use
  fixed (usually binary fields) slots in an external data format to
  define a finite set of attributes with a fixed namespace.   obspy uses
  a python dictionary like container they call
  `Stats <https://docs.obspy.org/packages/autogen/obspy.core.trace.Stats.html>`__
  to store comparable information.   That approach allows metadata
  attributes to be extracted from a flexible container addressable by a
  key word and that can contain any valid data.   For example, a typical
  obspy script will contain a line like the following to fetch the station 
  name from a Trace object :code:`d`. 

.. code-block:: python

  sta=d.Stats["station]

| In MsPASS we use a similar concept building on Pavlis's SEISPP library
  developed originally a number of years before obspy.   The Metadata
  object in MsPASS, however, has additional features not in the older
  SEISPP version.  

| The mspass::Metadata object has a container that can hold any valid
  data much like a python dictionary.   The current implementation uses
  the `any <https://theboostcpplibraries.com/boost.any>`__ library that
  is part of the widely used boost library.   In a C++ program Metadata
  can contain any data that, to quote the documentation, is "copy
  constructable".  The python interface, however, is much more
  restrictive for a number of reasons.  The most important, however, is
  that to interact cleanly with MongoDB we elected to limit the set of
  allowed types for Metadata attributes to those supported as distinct
  types in the python MongoDB API.   That list is defined
  `here <https://docs.mongodb.com/manual/reference/bson-types/>`__.  In
  principle, MongoDB can support generic "array" and "object" types that
  could contain serialized containers, but currently MsPASS only
  supports core types in all database engines:  real numbers (float or
  double), integers (32 or 64 bit), strings (currently assumed to be
  UTF-8), and booleans.   This creates some rigidity in the python API
  to a Metadata.   There are four "getters" seen in the following
  contrived code segment:

.. code-block:: python

   # Assume d is a Seismogram or TimeSeries which automatically casts to a Metadata in the python API use here
   x=d.get_double("t0")   # example fetching a floating point number - here a start time
   n=d.get_int("nsamp")   # example fetching an integer
   s=d.get_string("sta")  # example fetching a UTF-8 string
   b=d.get_bool("LPSPOL") # boolean for positive polarity used in SAC

| There are parallel "putter":

.. code-block:: python

   d.put_double("to",x)
   d.put_int("nsamp",n)
   d.put_string("sta",s)
   d.put_bool("LPSPOL",True)

| Mapping the C++ Metadata container to python was a challenge because
  of a fundamental difference in an axiom of the two languages:   python
  has a loose definition of "type" while C/C++ are "strongly typed".  
  To understand the difference note that all C/C++ code REQUIRES all
  variables to be declared before use with a type specification while
  python has no concept of "declaration" in the language at all.  In
  python the same variable name can change from a simple integer to some
  much more complicated type like an obspy Trace object.  Similar usage
  in a C program will always fail to compile.   To assure consistency on
  this issue the Metadata container will throw an exception
  (RuntimeError in python and MsPASSError in C++) if a user tries to
  extract a parameter with the wrong type.   For example:

.. code-block:: python

   d.put("sta","AAK")
   s=d.get_string("sta")  # this succeeds because sta was set a string
   x-d.get_double("sta")  # this will throw an exception because "sta" was not set as a real number.

| This effectively creates a strong typing layer between python and the
  C libraries to prevent type collisions that would otherwise be too
  easy to create.   A related feature in MsPASS described in the next
  section, which we call MetadataDefinitions, can be thought of as a
  referee that can be used to guarantee type consistency of any
  key:value pair that is to be read from or written to MongoDB. 

MetadataDefinitions and MongoDBConverter objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

| A MetadataDefinitions object is required by all MongoDB functions that
  perform CRUD operations to MongoDB with data objects.   It has two
  critical purposes when interacting with MongoDB:

#. It manages type properties and enforces decisions about whether a
   Metadata attribute is mutable in writes and updates.  A typical
   example would be station properties like the location of the sensor,
   instrument response data, etc.  Such parameters are expected to be
   read once by a reader and passed through a processing workflow until
   a write operation.  They also are normally expected to be
   `normalized <https://docs.mongodb.com/manual/core/data-model-design/>`__
   with the master copy in a separate collection from waveform data.  
#. It is used by readers to sort out potentially ambiguous keys.  A
   typical example would be instrument characteristics of a seismic
   observatory station.   Sensors are changed, channel codes are
   changed, sensors can change orientation when swapped, etc.   This can
   make critical metadata like response information time variable.  
   (e.g. asking for the response data for station AAK channel BHZ is
   ambiguous for multiple reasons.)   MetadataDefinitions was designed
   to abstract such information and front load the process of resolving
   such ambiguities to readers.   More details on this interaction are
   given in the description (WILL NEED A LINK HERE) of the MongoDB
   python API.   

| For most users the practical issue is that most processing workflows
  will need to include these lines near the top of any python script:

.. code-block:: python

   from mspasspy import MetadataDefinitions
   mdef=MetadataDefinitions()

| This loads the default namespace.   Alternatives are possible, but
  should be used only for specialized applications algorithms that
  require a different namespace.  For example, in principle it should be
  possible to build a specialized configuration to build a
  MetadataDefinitions object that could be used to translate between the
  SAC or SEGY namespaces and mspass. 
| A closely related object has the name *MongoDBConverter*. The
  *MongoDBConverter* caches a copy of the *MetadataDefinitions* it
  loads (usually behind the scenes).  It has methods that provide an
  interface between the C++ objects and python that simplify database
  interactions with MongoDB.   Most MongoDB CRUD operations functions
  require a  *MongoDBConverter* as an argument.  

Scalar versus 3C data
^^^^^^^^^^^^^^^^^^^^^

| MsPASS currently supports two different data objects:   TimeSeries is
  used to store single channel data while Seismogram is used to store
  data from three component instruments.  TimeSeries objects are based
  on the standard concept for storing scalar data that has been around
  since the earliest days of digital seismic data in the oil and gas
  industry.  That is, the sample values are stored in a continuous block
  of memory that we abstract as an array/vector.   The index for the
  array serves as a proxy for time (*time* method in BasicTimeSeries).  
  We use a C++ `standard template library vector
  container <http://www.cplusplus.com/reference/vector/vector/>`__ to
  hold the sample data accessible through the public variable s.  The
  python API makes the vector container look like a numpy array that can
  be accessed in same way sample data are handled in an obspy Trace
  object in the "data" array.   They can similarly be processed with the
  wide variety of operations available in scipy (e.g. `simple bandpass
  filters <https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.iirfilter.html#scipy.signal.iirfilter>`__). 

| Although scalar time series data are treated the same (i.e. as a
  vector) in every seismic processing system we are aware of, the
  handling of three component data is not at all standardized.   There
  are several reasons for this created by some practical data issues:

#.  Most modern seismic reflection systems provide some support for
   three-component data.   In reflection processing scalar, multichannel
   raw data are often conceptually treated as a matrix with one array
   dimension defining the time variable and the other index defined by
   the channel number. When three component data are recorded the
   component orientation can be defined implicitly by a component index
   number.   A 3C shot gather than can be indexed conveniently with
   three array indexes.  A complication in that approach is that which
   index is used for which of the three concept required for a gather of
   3C data  is completely undefined.   Furthermore, for a generic system
   like mspass the multichannel model does not map cleanly into passive
   array data because a collection of 3C seismograms may have irregular
   size, may have variable sample rates,  and may come from variable
   instrumentation.  Hence, a simple matrix or array model would be very
   limiting.
#. Traditional multichannel data have synchronous time sampling.  
   Seismic reflection processing always assumes during processing that
   time computed from sample numbers is accurate to within one sample.  
   Furthermore, the stock assumption is that all data have sample 0 at
   shot time;  that assumption allows the conceptual model of a matrix
   to represent scalar, multichannel data.  That is not necessarily true
   in passive array data and raw processing requires efforts to make
   sure the time of all samples can be computed accurately and time
   aligned.  Alignment for a single stations is normally automatic
   although some instruments have measurable, constant phase lags at the
   single sample level.  The bigger issue for all modern data is that
   the raw data are rarely stored in a multiplexed multichannel format,
   although the SEED format allows that.   Most passive array data
   streams have multiple channels stored as compressed miniSEED packets
   that have to be unpacked and inserted into something like a vector
   container to be handled easily by a processing program.   The process
   becomes more complicated for three-component data because at least
   three channels have to be manipulated and time aligned.   The obspy
   package handles this issue by defining a Stream object that is a
   container of single channel Trace objects.  They handle three
   component data as Stream objects with exactly three members in the
   container.  

| We handle three component data in MsPASS by using a matrix, which we
  define with the symbol "u" following the convention in Aki and
  Richards, to store the data for a given *Seismogram*.   There are two
  choices of the order of indices for this matrix.  A *Seismogram*
  defines index 0(1) as the channel number and index 1(2) as the time
  index.  The following python code section illustrates this more
  clearly than any words:

.. code-block:: python

   from mspasspy import Seismogram
   d=Seismogram(100)  # Create an empty Seismogram with storage for 100 time steps initialized to all zeros
   d.u(0,50)=1.0      # Create a delta function at time t0+dt*50 in channel 0

| Note we use the C (an python) convention for indexing starting at 0.  
  In the C++ API the matrix u is defined with a lightweight
  implementation of a matrix as the data object.   That detail is
  largely irrelevant to python programmers as the matrix is equivalenced
  to a numpy matrix by the wrappers.   Hence, python programmers
  familiar with numpy can manipulate the data in the u matrix with all
  the tools of numpy. 
| The Seismogram object has a minimal set of methods that the authors
  consider core concepts defining a three component seismogram.  We
  limit these to coordinate transformations of the components.   There
  are multiple methods for rotation of the components (overloaded rotate
  method), restoring data to cardinal directions at the instrument
  (rotate_to_standard), Kennett's free surface transformation, and a
  general transformation matrix.   We use a pair of (public) boolean
  variables that are helpful for efficiency: 
  *components_are_orthogonal* is true after any sequence of orthogonal
  transformations and *components_are_cardinal* is true when the
  components are in the standard ENZ directions.    
| FIX BEFORE RELEASE:   ENSEMBLE WRAPPERS HAVE NOT YET BEEN DEFINED OR
  TESTED
| Ensembles of TimeSeries and Seismogram data are handled with a more
  elaborate standard template library container.   For readers familiar
  with C++ the generic definition of an Ensemble is the following class
  definition created by stripping the comments from the definition in
  Ensemble.h:

.. code-block:: c++

   template <typename Tdata> class Ensemble : public Metadata
   {
   public:
     vector<Tdata> member;
     // ...
     Tdata& operator[](const int n) const
     // ...
   }

| where we omit all standard constuctors and methods to focus on the key
  issues here.  First, an Ensemble is little more than a vector of data
  objects with a Metadata object to store attributes common to the
  entire ensemble.  Hence, the idea is to store global attributes in the
  Ensemble Metadata field.   There is a "dismember" algorithm in MsPASS
  (NOT YET IMPLEMENTED by already present in seispp and easy to
  implement) that takes this structure apart and copies the Metadata
  components into each member.  The vector container makes it simple to
  handle an entire group (Ensemble) with a simple loop.   e.g. here is a
  simple loop to work through an entire Ensemble (defined in this code
  segment with the symbol d) in order of the vector index:

.. code-block:: python

   n=d.member.size()
   for i in range(n):
     somefunction(d.member[i])    # pass member i to somefunction

MsPASSCoreTS and Core versus Top-level Data Objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

| The class hierarchy diagrams above show there are CoreTimeSeries and
  CoreSeismogram objects that are parents of TimeSeries and Seismogram
  respectively.   That design was aimed to make the Core objects more
  readily extendible to other uses than MsPASS.   We encourage users to
  consider using the core objects as base for other ways of handling
  seismic data.  

| All mspass specific elements of our implementation are in MsPASSCoreTS
  which is a parent for both TimeSeries and Seismogram objects.  
  MsPASSCoreTS has two elements:

#. In MsPASS we use MongoDB for data management.   In MongoDB the lowest
   common denominator to identify a particular "document" in the
   database is the
   `ObjectID <https://docs.mongodb.com/manual/reference/method/ObjectId/>`__. 
   We store a representation of the ObjectID that was used to create any
   data object read from the database.   The *Metadata* object has a
   mechanism that keeps track of which attributes have been altered from
   the original.   That feature can be exploited for pure *Metadata*
   operations to only update the changed attributes and retain the
   original data.   When the sample data are altered the user is
   responsible for deciding if the original waveform data are to be
   retained and the new data added or updated in place.   The ObjectID
   is critical for managing any update.  
#. MsPASSCoreTS contains an error logging object.   The purpose of this
   object is to contain a log of any errors or informative messages
   created during the processing of the data.  All processing modules
   need to be designed with global error handlers so that they never
   abort, but in worst case post a log message that tags a fatal
   error.   More details on this feature are given in the next section.

Error Logging Concepts
^^^^^^^^^^^^^^^^^^^^^^

| When processing large volumes of data errors are inevitable and
  handling them clearly is an essential part of any processing
  framework.   This is particularly challenging with a system like Spark
  where a data set gets fragmented and handled by (potentially) many
  processors.   A poorly designed error handling system could abort an
  entire workflow if one function on one piece of data threw some kinds
  of "fatal" errors.  

| To handle this problem MsPASS uses a novel *ErrorLogger* object.  Any
  data processing module in MsPASS should NEVER exit on any error
  condition except one from which the operating system cannot recover. 
  All C++ and python processing modules need to have appropriate error
  handles (i.e. try/catch in C++ and try/except in python) to keep a
  single error from prematurely killing a large processing job.   We
  recommend all error handlers in processing functions post a message
  that can help debug the error.   Error messages should be registered
  with the data object's elog object.   Error messages should not
  normally be just posted to stdout (i.e. print in python) for two
  reasons.  First, stream io is not thread safe and garbled output is
  nearly guaranteed unless the log message are rare.  Second, with a
  large dataset it can become a nearly impossible to find out which
  pieces of data created the errors.  Proper application of the
  *ErrorLogger* object will eliminate both of these problems.

| Multiple methods are available to post errors of severity from fatal
  to logging messages that do not necessarily indicate an error.   A
  small python code segment may illustrate this more clearly.

.. code-block:: python

  try:
    d.rotate_to_standard()
    d.elog.log_verbose("rotate_to_standard succeed for me")
    # ...
  except RuntimeError:
    d.elog.log_error("rotate_to_standard method failure - transformation matrix may be singular",
      ErrorSeverity.Invalid)
    d.live=False   # note in python just be False not false

| To understand the code above assume the symbol d is a *Seismogram*
  object with a singular transformation matrix created, for example, by
  incorrectly building the object with two redundant east-west
  components.   The rotate_to_standard method tries to compute a matrix
  inverse of the transformation matrix, which will generate an
  exception.   This code catches that exception with a python
  RuntimeError.  In this simple case we compose our own error message
  and post it to the *ErrorLogger* attached to this data (d.elog).  The
  ErrorSeverity.Invalid implies the data are bad so the last line sets
  the live boolean false.   In contrast, the call to log_verbose, like
  the name suggests, writes a pure informational message.  
| All that would be usless baggage except the MongoDB database writers
  (Create and Update in CRUD) automatically save any elog entries in a
  separate database collection called elog.   The saved messages can be
  linked back to the data with which they are associated through the
  ObjectID of the data in the wf collection. 

.. |TimeSeries Inheritance| image:: /doxygen/html/classmspass_1_1_time_series.png
   
.. |Seismogram Inheritance| image:: /doxygen/html/classmspass_1_1_seismogram.png
   
