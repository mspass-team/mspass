.. _algorithms:

Algorithms
===============================
Overview
-----------
This page is a curated quick reference to commonly used algorithms available in MsPASS.
The functions described here are all algorithms for handling MsPASS
seismic data objects.   Utility functions for other objectives are described
elsewhere.

The :doc:`generated Python algorithms API <../python_api/mspasspy.algorithms>`
is the complete reference for additional specialized routines and current
signatures.

Most high-level Python processing functions use MsPASS decorators to provide a
common API.  Low-level C++ bindings and helper functions can differ, so the
linked docstring remains authoritative.  Key conventions are:

#.  Most high-level processing functions take a seismic data object as arg0.
    Multi-input algorithms and the nonstandard functions listed below are
    exceptions.
#.  Some functions can handle all data types and some are more restrictive.
    The tables below provide a quick reference in addition to the docstrings.
#.  Wrapped functions validate arg0 either in the decorator or in the function.
    Invalid usage normally raises :code:`TypeError` or :code:`ValueError`.
#.  High-level processing functions implement the dead/live concept.  A dead
    input normally returns without numerical processing.  Consult the linked
    docstring for type-changing and multi-input functions.
#.  Decorated functions that are intrinsically atomic (i.e. operate on
    :code:`TimeSeries` and/or :code:`Seismogram` objects) can also be applied to
    ensembles.   In MsPASS an
    ensemble is just a container with one or more atomic data stored under the
    common symbol :code:`member`.  (e.g. if e is a :code:`TimeSeriesEnsemble`,
    then e.member[i] is the ith component :code:`TimeSeries` object.) Atomic
    algorithms applied to ensemble are always done as a loop over the
    :code:`member` array.
#.  High-level wrappers normally convert recoverable per-datum failures into a
    killed datum with an error-log entry.  Usage errors, fatal failures, and
    direct calls to low-level bindings can still raise exceptions.  When an individual datum has
    issues that don't allow the algorithm to work with the specified
    inputs (e.g. windowing when the window range is outside the time range of
    the data) the datum will be killed with a message posted to the object's
    error log.

This document is organized as a set of tables with hyperlinks to the docstrings
for each algorithm function.

Processing Functions
--------------------------
These algorithms have a function form to allow their use in parallel map
operations.   They are functions that have one or more seismic data
objects as inputs and return the output of the algorithm.  The output
may or may not be the same type.   Some are wrappers
for obspy functions, some are written in C++ with python wrappers, and
some are pure C++ functions with python bindings.   Note the name field in the
table below is a hyperlink to the docstring for that function.

The input and output columns describe each row explicitly.  "Any" means any of
the four MsPASS seismic data types.  Some decorated atomic functions also loop
over the matching ensemble type; confirm that behavior in the linked docstring.

.. list-table:: Processing Functions
   :widths: 10 70 10 10
   :header-rows: 1

   * - Name
     - Synopsis
     - Inputs
     - Outputs
   * - :py:func:`correlate<mspasspy.algorithms.signals.correlate>`
     - Cross-correlate pair of signals (obspy)
     - TimeSeries, TimeSeries
     - numpy.ndarray
   * - :py:func:`correlate_stream_template<mspasspy.algorithms.signals.correlate_stream_template>`
     - obspy correlation of many with a template
     - Seismogram or TimeSeriesEnsemble, plus template
     - Seismogram or TimeSeriesEnsemble
   * - :py:func:`correlate_template<mspasspy.algorithms.signals.correlate_template>`
     - obspy single channel correlation against a template
     - TimeSeries, TimeSeries template
     - numpy.ndarray
   * - :py:func:`detrend<mspasspy.algorithms.signals.detrend>`
     - obspy detrend algorithm
     - Any
     - Same as input
   * - :py:func:`filter<mspasspy.algorithms.signals.filter>`
     - obspy time-invariant filter function
     - All
     - Same as input
   * - :py:func:`interpolate<mspasspy.algorithms.signals.interpolate>`
     - obspy function to resample using interpolation
     - Any
     - Same as input
   * - :py:func:`WindowData<mspasspy.algorithms.window.WindowData>`
     - Cut a smaller window from a larger segment
     - All
     - Same as input
   * - :py:func:`merge<mspasspy.algorithms.window.merge>`
     - Combine a set of segments into one
     - list of TimeSeries
     - TimeSeries
   * - :py:func:`scale<mspasspy.algorithms.window.scale>`
     - scale data to specified level with a specified amplitude metric
     - Any
     - Same as input
   * - :py:func:`broadband_snr_QC<mspasspy.algorithms.snr.broadband_snr_QC>`
     - Computes a series of snr-based amplitude metrics and posts them to output Metadata
     - TimeSeries or Seismogram
     - Same as input (Metadata altered)
   * - :py:func:`arrival_snr<mspasspy.algorithms.snr.arrival_snr>`
     - Computes a series of snr-based amplitude metrics relative to an arrival time
     - TimeSeries
     - TimeSeries (Metadata altered)
   * - :py:func:`FD_snr_estimator<mspasspy.algorithms.snr.FD_snr_estimator>`
     - Computes a set of broadband snr estimates in the frequency domain
     - TimeSeries
     - [dict,ErrorLogger]
   * - :py:func:`ArrivalTimeReference<mspasspy.ccore.algorithms.basic.ArrivalTimeReference>`
     - Shifts time 0 to arrival time defined by a Metadata key
     - Seismogram or SeismogramEnsemble
     - Same as input
   * - :py:func:`agc<mspasspy.ccore.algorithms.basic.agc>`
     - Applies an automatic gain control operator with a specified duration
     - Seismogram
     - TimeSeries of gains, Seismogram input altered in place
   * - :py:func:`repair_overlaps<mspasspy.ccore.algorithms.basic.repair_overlaps>`
     - Repair overlapping data segments
     - list of TimeSeries
     - list of TimeSeries
   * - :py:func:`seed_ensemble_sort<mspasspy.ccore.algorithms.basic.seed_ensemble_sort>`
     - Sorts an ensemble into net:sta:chan:loc order
     - TimeSeriesEnsemble
     - None (input reordered in place)
   * - :py:func:`splice_segments<mspasspy.ccore.algorithms.basic.splice_segments>`
     - Splice a list of TimeSeries objects into a continuous single TimeSeries
     - list of TimeSeries
     - TimeSeriesWGaps
   * - :py:func:`bundle_seed_data<mspasspy.algorithms.bundle.bundle_seed_data>`
     - Bundle a TimeSeriesEnsemble into a SeismogramEnsemble
     - TimeSeriesEnsemble
     - SeismogramEnsemble
   * - :py:func:`BundleSEEDGroup<mspasspy.algorithms.bundle.BundleSEEDGroup>`
     - Bundle a list of TimeSeries into a Seismogram object
     - list of TimeSeries
     - Seismogram
   * - :py:func:`ExtractComponent<mspasspy.algorithms.basic.ExtractComponent>`
     - Extract one component from a Seismogram or SeismogramEnsemble
     - Seismogram or SeismogramEnsemble
     - TimeSeries or TimeSeriesEnsemble
   * - :py:func:`ator<mspasspy.algorithms.basic.ator>`
     - Change from UTC to relative time standard
     - any
     - same as input
   * - :py:func:`rtoa<mspasspy.algorithms.basic.rtoa>`
     - Change from relative to UTC time standard
     - any
     - same as input
   * - :py:func:`rotate<mspasspy.algorithms.basic.rotate>`
     - Generic coordinate rotation
     - Seismogram or SeismogramEnsemble
     - same as input
   * - :py:func:`rotate_to_standard<mspasspy.algorithms.basic.rotate_to_standard>`
     - Restore data to cardinal directions
     - Seismogram or SeismogramEnsemble
     - same as input
   * - :py:func:`transform<mspasspy.algorithms.basic.transform>`
     - Apply a general transformation matrix to 3C data
     - Seismogram or SeismogramEnsemble
     - same as input
   * - :py:func:`linear_taper<mspasspy.algorithms.basic.linear_taper>`
     - Apply a one-sided, linear taper
     - any
     - same as input
   * - :py:func:`cosine_taper<mspasspy.algorithms.basic.cosine_taper>`
     - Apply a one-sided, cosine taper
     - any
     - same as input
   * - :py:func:`vector_taper<mspasspy.algorithms.basic.vector_taper>`
     - Apply a taper defined by a vector of samples
     - any
     - same as input
   * - :py:func:`erase_metadata<mspasspy.algorithms.edit.erase_metadata>`
     - Clear all values associated with specified Metadata keys
     - Any
     - Same as input

Nonstandard Processing Functions
-----------------------------------
The next table is similar to above, but the inputs or outputs are not seismic
data objects.   They are used internally by some functions and can have
utility for writing custom functions that use them inside another algorithm.

.. list-table:: Nonstandard Processing Functions
   :widths: 10 70 10 10
   :header-rows: 1

   * - Name
     - Synopsis
     - Inputs
     - Outputs
   * - :py:func:`BandwidthStatistics<mspasspy.ccore.algorithms.amplitudes.BandwidthStatistics>`
     - Compute statistical summary of snr in a passband returned by EstimateBandwidth
     - Two PowerSpectrum objects and BandwidthData
     - Metadata
   * - :py:func:`EstimateBandwidth<mspasspy.algorithms.snr.EstimateBandwidth>`
     - Estimate signal bandwidth estimate of power spectra of signal and noise
     - PowerSpectrum objects for signal and noise
     - BandwidthData - input for BandwidthStatistics
   * - :py:func:`MADAmplitude<mspasspy.ccore.algorithms.amplitudes.MADAmplitude>`
     - Calculate amplitude with the MAD metric
     - TimeSeries or Seismogram
     - float
   * - :py:func:`PeakAmplitude<mspasspy.ccore.algorithms.amplitudes.PeakAmplitude>`
     - Calculate amplitude with the peak absolute value
     - TimeSeries or Seismogram
     - float
   * - :py:func:`PercAmplitude<mspasspy.ccore.algorithms.amplitudes.PercAmplitude>`
     - Calculate amplitude at a specified percentage level
     - TimeSeries or Seismogram
     - float
   * - :py:func:`RMSAmplitude<mspasspy.ccore.algorithms.amplitudes.RMSAmplitude>`
     - Calculate amplitude with the RMS metric
     - TimeSeries or Seismogram
     - float
   * - :py:func:`snr<mspasspy.algorithms.snr.snr>`
     - Compute one of several possible time-domain signal-to-noise ratio metrics
     - TimeSeries or Seismogram
     - float

Processing Objects
-------------------------------------
This collection of things are "processing objects" meaning they implement
processing using a C++ or python class that has a method that runs
an algorithm on seismic data.  All are effectively functions that
take inputs and emit an output.  The only difference is the syntax
of a "method" compared to a simple function.  They also have the
same API guarantees listed at the beginning of this section
(i.e. the list of items in the first section).

.. list-table:: Processing Objects
   :widths: 10 70 10 10 10
   :header-rows: 1

   * - Class Name
     - Synopsis
     - Processing method
     - Inputs
     - Outputs
   * - :py:class:`Add<mspasspy.algorithms.edit.Add>`
     - Add a constant to a metadata value
     - apply
     - any
     - Edited version of input
   * - :py:class:`Add2<mspasspy.algorithms.edit.Add2>`
     - Add two metadata values
     - apply
     - any
     - Edited version of input
   * - :py:class:`ChangeKey<mspasspy.algorithms.edit.ChangeKey>`
     - Change key associated with a value
     - apply
     - any
     - Edited version of input
   * - :py:class:`Divide<mspasspy.algorithms.edit.Divide>`
     - Divide a metadata value by a constant
     - apply
     - any
     - Edited version of input
   * - :py:class:`Divide2<mspasspy.algorithms.edit.Divide2>`
     - Divide one metadata value by another
     - apply
     - any
     - Edited version of input
   * - :py:class:`IntegerDivide<mspasspy.algorithms.edit.IntegerDivide>`
     - Apply integer divide operator to a metadata value
     - apply
     - any
     - Edited version of input
   * - :py:class:`IntegerDivide2<mspasspy.algorithms.edit.IntegerDivide2>`
     - Apply integer divide operator to two metadata values
     - apply
     - any
     - Edited version of input
   * - :py:class:`Mod<mspasspy.algorithms.edit.Mod>`
     - Change a key to value mod constant
     - apply
     - any
     - Edited version of input
   * - :py:class:`Mod2<mspasspy.algorithms.edit.Mod2>`
     - Set a field as mod division of a pair of values
     - apply
     - any
     - Edited version of input
   * - :py:class:`Multiply<mspasspy.algorithms.edit.Multiply>`
     - Change a key to value times constant
     - apply
     - any
     - Edited version of input
   * - :py:class:`Multiply2<mspasspy.algorithms.edit.Multiply2>`
     - Set a field as the product of a pair of values
     - apply
     - any
     - Edited version of input
   * - :py:class:`SetValue<mspasspy.algorithms.edit.SetValue>`
     - Set a field to a constant
     - apply
     - any
     - Edited version of input
   * - :py:class:`Subtract<mspasspy.algorithms.edit.Subtract>`
     - Change a key to value minus a constant
     - apply
     - any
     - Edited version of input
   * - :py:class:`Subtract2<mspasspy.algorithms.edit.Subtract2>`
     - Set a field as difference of a pair of values
     - apply
     - any
     - Edited version of input
   * - :py:class:`MetadataOperatorChain<mspasspy.algorithms.edit.MetadataOperatorChain>`
     - Apply a chain of metadata calculators
     - apply
     - any
     - Edited version of input
   * - :py:class:`FiringSquad<mspasspy.algorithms.edit.FiringSquad>`
     - Apply a series of kill operators
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataDefined<mspasspy.algorithms.edit.MetadataDefined>`
     - Kill if a key-value pair is defined
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataUndefined<mspasspy.algorithms.edit.MetadataUndefined>`
     - Kill if a key-value pair is not defined
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataEQ<mspasspy.algorithms.edit.MetadataEQ>`
     - Kill a datum if a value is equal to constant
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataNE<mspasspy.algorithms.edit.MetadataNE>`
     - Kill a datum if a value is not equal to constant
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataGE<mspasspy.algorithms.edit.MetadataGE>`
     - Kill if a value is greater than or equal to a constant
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataGT<mspasspy.algorithms.edit.MetadataGT>`
     - Kill if a value is greater than a constant
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataLE<mspasspy.algorithms.edit.MetadataLE>`
     - Kill if a value is less than or equal to a constant
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataLT<mspasspy.algorithms.edit.MetadataLT>`
     - Kill if a value is less than a constant
     - kill_if_true
     - any
     - Edited version of input
   * - :py:class:`MetadataInterval<mspasspy.algorithms.edit.MetadataInterval>`
     - Kill for value relative to an interval
     - kill_if_true
     - any
     - Edited version of input

Deconvolution algorithms
----------------------------
MsPASS has a specialized algorithms module on "deconvolution".  Users
should recognize that currently the module has algorithms for "deconvolution"
in form of estimation of so called "receiver functions".  Receiver functions
are a special type of deconvolution useful only, at present anyway, for
application to teleseismic body wave phase data.

A suite of conventional scalar and generalized iterative methods is available
through two different mechanisms:

1.  The wrapper function :py:func:`mspasspy.algorithms.RFdeconProcessor.RFdecon`
    is a functional form that can be used directly in map operators.
2.  By default, `RFdecon` instantiates an instance of the class
    :py:class:`mspasspy.algorithms.RFdeconProcessor.RFdeconProcessor`
    in each call to the function.  It is more efficient (i.e. faster)
    to instantiate a single processor and pass it through the ``engine``
    argument on repeated calls, especially for methods with significant setup
    costs.  The processor's lower-level
    :py:meth:`mspasspy.algorithms.RFdeconProcessor.RFdeconProcessor.apply`
    method is also available for specialized workflows.

See the docstrings with links above for usage.

A different, unpublished (aka experimental) algorithm called
"Colored Noise Regularized Three Component Decon " is
implemented in the C++ class
:py:class:`mspasspy.ccore.algorithms.deconvolution.CNRDeconEngine`.
There are two python functions that utilize the "engine":

#.  :py:func:`mspasspy.algorithms.CNRDecon.CNRRFDecon` uses the
    CNR algorithm to estimate conventional "receiver functions" where
    each 3C station is deconvolved independently.
#.  The same engine
    is used for an array version called
    :py:func:`mspasspy.algorithms.CNRDecon.CNRArrayDecon`
    
See :doc:`Deconvolution Operators <deconvolution>` for conventional, CNR, and
generalized iterative deconvolution guidance.
