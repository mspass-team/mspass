.. _obspy_interface:

Using ObsPy with MsPASS
=======================

Role of ObsPy in MsPASS
~~~~~~~~~~~~~~~~~~~~~~~
The home page for `Obspy <https://docs.obspy.org/>`__ defines the package as follows:

    ObsPy is an open-source project dedicated to provide a Python framework for processing seismological data.
    It provides parsers for common file formats and seismological signal processing routines which allow the manipulation of seismological time series (see Beyreuther et al. 2010, Megies et al. 2011, Krischer et al. 2015)

Python is sometimes called a "glue language" because of its capability for joining different software packages.
Our integration of ObsPy in MsPASS is a case in point.
Some user's may find a good starting point for working with MsPASS is to treat it as a supplement to ObsPy that provides data management and parallel processing support.
All feasible processing algorithms in ObsPy have MsPASS "wrappers" that provide that support.
On the other hand, to interact correctly with the MsPASS framework requires frequent conversion between native ObsPy data objects and MsPASS data objects.
We have implemented fast algorithms to do those operation, but the performance cost is not zero.
We describe more details on converters below and how ObsPy processing algorithms can be used in parallel workflows.

Converters
~~~~~~~~~~
To understand converters and their limitations it is helpful to describe details of ObsPy and MsPASS data objects.
They have similarities but some major differences that a software engineer might call "a collision in concept".
ObsPy defines two core data objects:

#.  ObsPy :py:class:`Trace <obspy.core.trace.Trace>` containers hold a single channel of seismic data.
    An ObsPy :code:`Trace` maps almost directly into the MsPASS atomic object we call a :py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>`.
    Both containers store sample data in a contiguous block of memory that implement the linear algebra concept of an N-component vector.
    Both store the sample data as Python float values that always map to 64 bit floating point numbers (double in C/C++).
    Both containers also put auxiliary data used to expand on the definition of what the data are in an indexed container that behaves like a Python dict.
    ObsPy calls that container a :py:class:`Stats <obspy.core.trace.Stats>` attribute.
    MsPASS defines a similar entity we call :code:`Metadata`.
    Some users may find it convenient to think of both as a generalized "header" that defines a fixed namespace like that in `Seismic Unix <https://wiki.seismic-unix.org/doku.php>`__.
    From a user's perspective the "headers" in ObsPy and MsPASS behave the same but the API is different.
    In ObsPy the header parameters are stored in an attribute with a different name (Stats) while in MsPASS the dict behavior is part of the TimeSeries object.
    (For those familiar with Object Oriented Programming generic concepts ObsPy views metadata using the concept that a Trace object "has a Stats" container while in MsPASS we say a TimeSeries "is a Metadata".)
#.  ObsPy :py:class:`Stream <obspy.core.stream.Stream>` containers are little more than a Python list of :code:`Trace` objects.
    A :code:`Stream` is very similar in concept to the MsPASS data object we call a :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.TimeSeriesEnsemble>`.
    Both are containers holding a collection of single channel seismic data.
    In terms of the data they contain there is only one fundamental difference;
    a :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.TimeSeriesEnsemble>` is not just a list of data but it also contains a :code:`Metadata` container that contains attributes common to all members of the ensemble.

There are some major collisions in concept between ObsPy's approach and that we use in MsPASS that impose some limitations on switching between the packages.

#.  First, ObsPy's authors took a different perspective on what defines data than we did.
    ObsPy's :code:`Trace` and :code:`Stream` objects both define some processing functions as methods for the objects.
    That is a design choice we completely rejected in MsPASS.
    An axiom of our design was data is data and processing is processing and they should intersect only through processing functions.
    A technical detail, however, is that methods in an object never represent a conversion problem.
    They only add overhead in constructing ObsPy data objects.
#.  ObsPy's support for three-component data is mixed in the concept of a :code:`Stream`.
    A novel feature of MsPASS is support for an atomic data class we call a Seismogram that is a container designed for consistent handling of three component data.
#.  In MsPASS we define a second type of "Ensemble" we call a :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.SeismogramEnsemble>` that is a collection of :py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>` objects.
    It is conceptually identical to a :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.TimeSeriesEnsemble>` except the members are :py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>` objects instead of :py:class:`TimeSeries<mspasspy.ccore.seismic.TimeSeries>` objects.

The concept collision between Seismograms objects and any ObsPy data creates some limitations in conversions.
A good starting point is this axiom:  converting from MsPASS Seismogram objects or SeismogramEnsemble to ObsPy Stream objects is simple and robust;
the reverse is not.

With that background the set of converters are:

- :code:`Trace2TimeSeries` and :code:`TimeSeries2Trace` are, as the names imply, converters between the ObsPy Trace and the MsPASS TimeSeries objects.
  As noted above that process is relatively straightforward.
- :code:`Seismogram2Stream` and :code:`Stream2Seismogram` are the converters related to Seismogram objects.
  The Stream produced by Seismogram2Stream has some predictable restriction.
  First, the output Trace objects will all have exactly the same start time and number of samples even if raw data from which the data originated had subsample time differences or had irregular lengths.
  More significant is that not all channel-dependent metadata will be retained.
  Currently the only retained channel properties are orientation information (:code:`hang` and :code:`vang` attributes).
  For most users the critical information lost in the opposite conversion (:code:`Stream2Seismogram`) is any system response data.
  A corollary that follows logically is that if you need to do response corrections for your workflow you need to do so equally on all three components before converting the data to Seismogram objects.
  Because of complexities in converting from :code:`Stream` to :py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>` objects we, in fact, do not recommend using :code:`Stream2Seismogram` for that purpose.
  If the parent data originated as miniSEED from an FDSN data center, a more reliable and flexible algorithm is the :code:`BundleSEEDGroup` function.
- The MsPASS ensemble data converters can be used to convert to and from ObsPy :code:`Stream` objects, although with side effects in some situations.
  As with the other converters the (verbose) names are mnemonic for their purpose.
  :code:`TimeSeriesEnsemble2Stream` and its inverse :code:`Stream2TimeSeriesEnsemble` convert :code:`TimeSeriesEnsembles` to a :code:`Stream` and vice-versa.
  The comparable functions for :code:`SeismogramEnsembles` are :code:`SeismogramEnsemble2Stream` and :code:`Stream2SeismogramEnsemble`.
  A complication in the conversion both directions is handling of the ensemble Metadata.
  As noted, that concept does not exist in the Stream object so some compromises were necessary.
  There are tradeoffs in complexity (increasing execution time) and the odds of unexpected changes.
  One way to explain this is to describe the two algorithms in pseudocode.
  First, the Ensemble to Stream algorithm is the following:

  #. Copy all ensemble metadata to every live member
  #. Save the keys for ensemble metadata to a Python list
  #. Post the list to each member with an internal (hidden) key
  #. Run the converter for each atomic member and push to the Stream result

  Reversing the conversion (Stream to Ensemble) then follows this algorithm:

  #. Convert all Trace objects in the stream to build the Ensemble result
  #. Extract the Python list of keys from the first live member
  #. Copy the Metadata defined with the ensemble keys to the Ensemble's Metadata
  #. Erase the list of ensemble keys field from the Metadata of all members

  This has two side effects of which you should be conscious.

  #. When an Ensemble is converted to a Stream and back to an Ensemble, which is the norm for applying an ObsPy algorithms to an entire Ensemble, a copy of the Ensemble's Metadata will be present in every live member of the Ensemble after the to and from conversion.
     That is a side effect to the double conversion if the input did not have the same property (i.e. all members having a copy of the Ensemble Metadata).
     That was, however, a design decision as having the only copy of Metadata in the Ensemble is considered an anomaly that needs to be handled anyway.
     The reason is our definition of "Atomic" that appears repeatedly in this User's Manual.
     Atomic data are saved and read as the single entity.
     An Ensemble, in contrast, is like a molecule that can be dismembered into atoms.
     Ensemble Metadata are like valence electrons that have to be balanced when saved as atoms.
  #. The converters do not test for consistency of member Metadata and the Ensemble Metadata.
     If the member Metadata are different from those of the Ensemble the Ensemble version will silently overwrite that of the members when the data are converted to a Stream.
     That shouldn't happen if the Ensemble Metadata are what they are asssumed to be - attributes that are the same for all members of the group.

A final critical issue about using ObsPy converters is handling of extra concepts that MsPASS data objects contain that are not part of ObsPy.
That means two elements of atomic data in MsPASS that have no related concept in ObsPy.
That is, what we call :code:`ErrorLogger` and :code:`ProcessingHistory`.
Decorators described in the next section are used to make this conversion happen automatically for ObsPy algorithms applied to MsPASS objects.
If the converters are used in isolation (e.g. one could easily run several ObsPy algorithms between converters from mspass to ObsPy and back) these extra components will be lost without custom coding to preserve them.
For this reason we recommend only running ObsPy algorithms through the decorators described in the next section.

Decorated ObsPy Functions
~~~~~~~~~~~~~~~~~~~~~~~~~
The decorated ObsPy functions can be thought of as a way to run ObsPy's functions on mspass data objects.
That means both atomic data and Ensembles.
This should be clearer from an example.

Consider this small code fragment to apply a bandpass filter to an ObsPy :code:`Trace` object:

.. code-block:: python

   from obspy import read
   d = read('mydatafile')
   d.filter('bandpass', freqmin=0.05, freqmax=2.0)

This little fragment uses the typical ObsPy approach of reading data from a file and applying an algorithm in a construct that makes the algorithm look like a method for the data class.
That model does not mesh well with parallel schedulers that are a core component of MsPASS.
The normal application of the map and reduce operations, which are a core idea of the parallel schedulers, requires the algorithm be cast in a function form.
Hence, a comparable algorithm in MsPASS to the above is the following:

.. code-block:: python

  # in mspass all jobs normally start with an incantation similar to
  # this to create a database handle which we here link to the symbol db
  import mspasspy.algorithms.signals as signals
  from mspasspy.db.client import Client
  from mspasspy.db.database import Database
  dbclient = Client()
  db = Database(dbclient, 'mydata')

  # These three lines are comparable to ObsPy example above
  doc = db.wf_TimeSeries.find_one()
  d = db.read_data(doc['_id'])
  ed = signals.filter(d, 'bandpass', freqmin=0.05, freqmax=2.0)

We include the top section of code to emphasize that building a database handle, which above is set to the symbol db, is comparable in some respects to opening a data file.
That step is hidden in the ObsPy read function behind several layers of functions to make their reader generic.
In this example data in the file 'mydatafile' is conceptually the same as what we fetch in mspass with the :code:`db.read_data` method call for "doc".
The :code:`filter` function applied above is an example of one of the ObsPy wrappers.
It applies exactly the same algorithm as the ObsPy example but automatically handles the conversions from mspass to ObsPy and back again after the function is applied.
All the ObsPy algorithms found in the :code:`mspasspy.algorithms.signals` module use the same concept.
All accept any mspass data object for processing.
Some require multiple input data objects and are more restricitve.
For example, the :code:`correlate` function requires two TimeSeries inputs.
See the :py:mod:`mspasspy.algorithms.signals` documentation for details.

ObsPy Processing in Parallel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ObsPy decorator allow ObsPy operators to be applied in parallel.
For example, the following is a variant of filter algorithm but this example uses the Dask scheduler to process the entire data set:

.. code-block:: python

   # Assume db is created as above
   from mspasspy.db.database import read_distributed_data
   cursor = db.wf_TimeSeries.find({})
   data = read_distributed_data(db, cursor, format='dask')
   data = data.map(signals.filter, "bandpass", freqmin=0.05, freqmax=2.0).compute()
   data.compute()

The key thing to note here is that the basic algorithm is identical to above: :code:`read_distributed_data` and :code:`filter`.
The difference is that the entire data set is read and filtered instead of one TimeSeries/Trace.
The added incantations are needed to translate the function call to the :code:`map` method of the parallel API, but the basic structure is the same.
For more on parallel processing constructs see :ref:`parallel_processing`.
