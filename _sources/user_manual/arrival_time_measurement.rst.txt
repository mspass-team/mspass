.. _arrival_time_measurement:

================================================
Arrival Time Measurement Techniques in MsPASS
================================================
*Gary L. Pavlis*
--------------------
Overview
---------
Arrival time measurements are the oldest and most fundamental data of
seismology.  They are the fundamental data used to produce all
current generation earthquake catalogs and were used in one
way or the other in everything we know about earth structure
from seismic waves.  There are two fundamentally different
problems a research computing framework like MsPASS needs to address
to support the near universal dependence of seismology research on
arrival time data:

#. Data management tools to efficiently handle all types of timing data.
#. Tools to measure timing relationships from waveform data.
   Note there are two classes of timing measurements that we
   need to handle:  (a) absolute time tags like traditional
   phase picks, and (b)relative timing relationships between parts of the
   same waveform or between two or more different waveforms.

For item 1, the fact that timing data of one kind or another are a universal
concept of seismology means that every scientist has a set of both
specialized and generic tools they use for handling their data.
For that reason the role MsPASS can play for most people is as a tool
for low-level data handling used in combination with one or more
other tools they use as a component of their research.   For that reason
much of the issue with item 1 is an import/export problem.  One of
the most important features of the MongoDB database in MsPASS is the
complete flexibility it provides in what it stores.  It provides a
generic way to store any format of timing data we have yet encountered.
Every source we know of stores such data either as a set of text
files or as tables in a relational database.   Import/export of such
data is discussed in the :ref:`Importing Tabular Data <importing_tabular_data>`
section of the MsPASS documentation.

This section is focused mainly on item 2.  That is, it describes tools in MsPASS
for creating arrival time measurements from waveform data.  We emphasize,
on the other hand, that there are multiple commerical and open-source systems for
processing and data management of traditional earthquake monitoring
networks.  We view the problem of earthquake catalog generation a solved
problem best done by one those systems that were designed specifically
to do that job well.   MsPASS is a system for more specialized research
problems.  The tools currently available are:

#.  A multichannel correlation algorithm building on earlier work by
    `Pavlis and Vernon (2011) <https://doi.org/10.1016/j.cageo.2009.10.008>`_.
    That algorithm can be used for estimating travel time residuals
    for teleseismic phases.  It also computes a robust stack of the
    inputs after they are time aligned by cross-correlation.  It differs
    from the original application as it is designed for automated
    processing while the original application was used with an integrated
    graphical user interface.
#.  Obspy has several picking method described
    `here <https://docs.obspy.org/tutorial/code_snippets/trigger_tutorial.html>`_.
    Below we discuss how they can be used with data managed by MsPASS.
#.  TODO:  depending on progress note new phasenet implementation.

The rest of this section has subsections on each of the items above.

Teleseismic Arrival Time Measurement
----------------------------------------
Fundamentals
^^^^^^^^^^^^^

There are some fundamental properties of teleseismic body-wave data
that any algorithm for working with such signals has be aware:

#. Teleseismic body-wave signals vary in space at spatial scales far larger than
   signals from local and regional events.  The reasons are a fundamental
   property of the earth and a topic outside the scope of this manual.
   The key fact, however, is that coherent processing of signals from local
   and regional events can only be done over the scale of one or two
   wavelengths.  In constrast, many studies demonatrate coherent processing of
   teleseismic P and S waves are possible over distances of thousands of
   kilometers, which correspond to hundreds of wavelengths.
#. The combination of source spectrum dependence on magnitude and
   the strong frequency dependence of the microseisms
   create variations in data spectra that can create
   large errors in measurements if not handled
   properly.  One perspective is that a large fraction of traditional signal
   processing algorithms inherited from oil and gas processing have explicit
   or implicit assumptions that noise is "white".   Modern broadband
   data noise is anything but white.   It is very "colored" by
   microseisms.  Similarly, traditional algorithms from oil and gas
   processing have an implict assumption the source is constant for
   all data.  That is rarely true with earthquake data (the only
   exception is repeating earthquakes).  As a result a fundamental
   thing teleseismic processing must handle is that the
   the optimal bandwidth for signal processing
   is always dependent on earthquake magnitude and the noise
   spectrum of the seismic station being analyzed.

A corollary of item 2 is that any any automated
processing algorithm needs to be "robust".  In this case that means
it will automatically handle a mix of data of variable quality
and extreme outliers.  In my experience automated arrival time
estimation of any type can be treated as two different problems
best handled by different processing algorithms:

#.  Automated discarding of the lost causes.  Data can be a lost
    cause for a long list of reasons from the completely dead channel
    or always noisy channel to a one-up problem created by some
    local noise source the overwhelms the signal you want to see
    during a time window of interest.
#.  Robust handling of data with signals of variable quality.  For the largest
    earthquakes this is a minor concern.  The issue is fundamental, however,
    for the smallest events that may have usable signals only on a fraction
    of the receivers being processed.   Unfortunately, due the magnitude
    frequency relationship there are always far more of the marginal
    signals to handle than the larger, easier ones.

The approach in MsPASS is shaped by my experience in developing and using
the *dbxcor* program described in
`Pavlis and Vernon (2011) <https://doi.org/10.1016/j.cageo.2009.10.008>`_.
In *dbxcor* we addressed both of these issues with the graphical user
interface.  The key idea was to iteratively improve a stack of the
array data by repeating the steps of (a) compute a robust stack,
(b) sort by some quality metric, (c) kill the junk, and repeat until
satisified.   MsPASS provides tools for accomplishing that same
process in an automated workflow.  Examples showing how the tools
can be linked together to accomplish that are found in mspass tutorials.
I would emphasize, however, the tools there are not the last word on this
problem and creative solutions are needed to improve performance and
efficiency.   In particular, using machine learning to auto-edit
data is an obvious and likely valuable way to accomplish auto-editing.

MsPASS Multichannel Correlation Method
-----------------------------------------
Data Preparation
^^^^^^^^^^^^^^^^^

An assumption of the multichannel correlation algorithm in MsPASS is that the
data have been preprocessed through some variation of the following
steps:

#.  The working dataset is a version of a common-source gather.  By that
    I mean the data set is indexed in a way that all the waveforms linked to
    a particular seismic source can be assembled into a set of ensembles.
    For parallel processing that means the data set is an RDD/bag of ensembles.   The
    members of the ensembles are assumed to span a time range around the
    seismic phase that is to be analyzed.
#.  All waveforms are :ref:`normalized<normalization>` that will allow load source and
    receiver coordinates to be loaded from the database during
    the initial read operation or within the workflow when needed.
#.  Although not required, I have found that in practice all
    waveforms in the ensemble should normally have some basic low-level processing.
    For high quality data like USArray data that can be as simple as
    demean and scaling the data by a constant to compensate for gain
    variations.  For more heterogenous data a more sophisticated response
    correction may be necessary to assure the data are all normalized to
    a common response.
#.  The data are required to be resampled to a common sample rate to
    run the main processor called
    :py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`.
    The generic MsPASS function to accomplish this task is
    called :py:func:`resample<mspasspy.algorithms.resample.resample>`.
#.  Time tags need to be defined in the Metadata container of
    each waveform to define at least
    an initial estimate of the arrival time of the phase of interest.
    These can be previously measured arrivals that are to be refined or
    model-based estimates computed from source coordinates, receiver coordinates,
    and an earth model.  The MsPASS tutorial notebooks contain
    many examples of how to do this using obspy's tau-p travel time
    calculator.
#.  The data should be shifted from UTC times to what in the
    docstrings we call the "arrival time refernence" frame.   That is,
    we expect the data have been shifted with the function
    :py:func:`ator<mspasspy.alorithms.basic.ator` with the shift
    time as the initial arrival time estimate set previously.
    That means a plot using the `time_axis_method` for each atomic
    datum will have 0 as the initial arrival time estimate.
#.  The working ensembles are
    :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.TimeSeriesEnsemble`
    objects containing a component of data appropriate for the phase
    being analyzed.   For P data that can be simple vertical components
    while for S it always demands the data were processed to
    :py:class:`Seismogram<mspasspy.ccore.seismic.Seismogram>` objects
    and oriented to radial or transverse components.  For most modern
    data I would recommend all data be assembled to begin with as
    :py:class:`SeismogramEnsemble<mspasspy.ccore.seismic.SeismogramEnsemble`
    objects, rotated to LQT or with the
    :py:func:`free_surface_transformation<mspasspy.algorithms.basic.free_surface_transformation>`
    operator, and then the approprate component extracted using the
    :py:func:`ExtractComponent<mpsasspy.algorithms.basic.ExtractComponent`
    function.
#.  The data should be passed through the algorithm called
    :py:func:`broadband_snr_QC<mspasspy.algorithms.snr.broadband_snr_QC>`.
    That function computes Metadata attributes that
    are required for two purposes described below:
    (a) a single value that can be used as he "best" signal used as a
    seed for the multichannel algorithm, and (b) a pair of attributes that
    can be used to filter the data to an optimal frequency band for
    each ensemble.   Plug in replacements are possible but would
    require careful looks at the python functions that utilize those
    attributes in the `MCXcorStacking` module.

Algorithm Background
^^^^^^^^^^^^^^^^^^^^^^
The top-level function for processing teleseismic body waves is
:py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`.
As the name suggests, it does two distinctly different tasks:

#.  Aligns a set of input waveforms by cross-correlation so all the waveforms
    match as closely as possible when plotted on a common, relative time base.
    (see e.g. Figure 1 of
    `Pavlis and Vernon (2011) <https://doi.org/10.1016/j.cageo.2009.10.008>`_)
#.  Produce a robust stack of the time-aligned data.  After the first
    alignment stage the robust stack is used as the correlation reference.
    The algorithm name contains "MCXcor" which is shorthand for
    "Multichannel X(cross-)correlation" to contrast it with pairwise
    cross-correlation algorithms following the older work of
    `VandeCarr and Crosson (1990)<TODO:LOOK UP DOI>`__.

Any user who needs to use the
:py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`
function of MsPASS should plan to have a copy of the
`Pavlis and Vernon (2011) <https://doi.org/10.1016/j.cageo.2009.10.008>`_
paper for reference.   The primary theory behind
:py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`
is documented in that paper.
There are, however, some large differences between the MsPASS
implementation and the publication.
`Pavlis and Vernon's paper(2011) <https://doi.org/10.1016/j.cageo.2009.10.008>`_
describes an analyst tool for measuring teleseismic body wave phase arrival times using
multichannel cross-correlation they called *dbxcor*.   *dbxcor* has an integrated
grapical user interface that allows the user to set some key processing
parameters interactively and graphically edit the data to discard
bad and unacceptably noisy data.
:py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`,
in contrast, was designed as a purely automated tool that could be
applied to large data sets and produce quality results without any human
intervention.  The next section documents the current algorithms used
to automate what was done interactively in *dbxcor*.   Note this topic
is a research area that could be improved with alternative algorithms.
For example, it is an obvious candidate for machine learning.

MsPASS Automation
^^^^^^^^^^^^^^^^^^^
Adapting the algorithm of *dbxcor* to work as an automated tool required
developing algorithms to define three key parameters *dbxcor* required
the user to set via the graphical user interface:

#.  The algorithm used for cross-correlation ultimately uses the
    array stack to correlate with each signal in an input
    :py:class:`TimeSeriesEnsemble<mspasspy.ccore.seismic.TimeSeriesEnsemble`.
    However, to start the interative sequence the input ensemble has
    to be time aligned to first order or the stack will be grossly distorted.
    *dbxcor* solved that issue by requiring the user to select the signal
    in the ensemble that would be used for initial correlations.
    The MsPASS implementation uses a python function in the module
    `mspasspy.algorithms.deconvolution.MCXcorStacking` called
    :py:func:`extract_initial_beam_estimate<mspasspy.algorithms.deconvolution.MCXcorStacking.extract_initial_beam_estimate>`.
    That function is designed to use the output of the MsPASS function
    :py:func:`broadband_snr_QC<mspasspy.algorithms.snr.broadband_snr_QC>`, which
    creates a "subdocument" (aka python dictionary) with a specified key
    containing a suite of waveform signal-to-noise estimate attributes.
    :py:func:`extract_initial_beam_estimate<mspasspy.algorithms.deconvolution.MCXcorStacking.extract_initial_beam_estimate>`
    uses the datum with the largest value of specified attribute as the
    initial stack estimate.
#.  The algorithm requires a definition of what I call the
    *correlation time window*.   As noted above, the first stage of the
    multichannel algorithm is to align the data by cross-correlation
    with the current estimate of the stack (aka *beam* - a jargon
    term in array processing).  For automated processing it is never
    a good idea to use a fixed window that is the duration of the
    input signals for two reasons:  (a) variations in source properties
    (size, location, near source structure, and source complexity) drastically vary the
    optimal duration for correlation, and (b) interference from
    secondary phases (e.g. P or pP or even S for teleseismic P) is
    a nearly universal problem unless the initial data time span was
    carefully trimed previously.
    The MsPASS solution to setting this time window uses a coda duration
    estimation algorithm appropriate only for teleseismic P wave data.
    That function is used within the higher level function called
    :py:func:`MCXcorPrepP<mspasspy.algorithms.deconvolution.MCXcorStacking.MCXcorPrepP>`
    discussed below.  The algorithm used there automatically avoids
    interference from pP or P phases phases and sets the end time of the
    correlation window passed on a well established coda decay algorithm.
    Specifically, the envelope of each signal is computed and the algorithm
    searches backward in time until the envelop exceeds an amplitude
    threshold based on a specified signal-to-noise ratio.  In
    :py:func:`MCXcorPrepP<mspasspy.algorithms.deconvolution.MCXcorStacking.MCXcorPrepP>`
    the correlation window is derived from an average
    (multiple estimates of center are supported) coda duration from all
    ensemble members.
#.  The algorithm requires a different time indow that in our original
    paper we called the *robust time window*.   The *robust time window*
    is the time window used to define the robust stack I discuss below.
    The idea of the robust stack is to create a stack that automatically
    discards outliers and focuses on the better data while
    reducing the impact of low signal-to-noise data.
    Experience with *dbxcor* has shown that for reliable results the
    *robust time window* needs to be smaller than the time window
    defined for the *correlation time window*.  A rule of thumb I
    always suggested for interactive processing is to define the robust
    window as the first 2 or 3 cycles of the dominate frequency of the
    phase being analyzed.   For P wave data
    :py:func:`MCXcorPrepP<mspasspy.algorithms.deconvolution.MCXcorStacking.MCXcorPrepP>`
    standardizes this rule of thumb in an internal function called
    :py:func:`compute_default_robust_window<mspasspy.algorithms.deconvolution.MCXcorStacking._compute_default_robust_window>`.
    That algorithm is more-or-less a generalization of the "rule of thumb"
    I noted for dbxcor.  It sets the *robust window* as specified number of
    cycles of an input frequency.  I recommend that normally be set as the
    low frequency band edge computed by
    :py:func:`broadband_snr_QC<mspasspy.algorithms.snr.broadband_snr_QC>`.
    An alternative appropriate for most data is a fixed time window spanning
    a time range from a small negative number large enough to exceed the
    maximum residual from earth model time estimates (typically from -2 to -1 s)
    to a few seconds (2 or 3 times the duration of the center frequency of the
    traditional short-period band).  A fixed window preforms well on smaller
    events that have significant signal only in the short period band but will
    work badly on large events.   As a result an alternative is to process the
    data with different robust window lengths for different magnitude ranges.

Teleseismic P-wave automatation
-----------------------------------
For teleseismic P wave data the above automated algorithms are encapsulated in a single
function called
:py:func:`MCXcorPrepP<mspasspy.algorithms.deconvolution.MCXcorStacking.MCXcorPrepP>`.
It provides a top-level interface for automatically setting the required
inputs to run the
:py:func:`align_and_stack <mspasspy.algorithms.deconvolution.align_and_stack>`
function.
The parameters this function defines are discussed in the section above.
See the docstring for the function for guidance on use and examples
below and in the mspass tutorial repository.

Robust stacking
------------------
:py:func:`align_and_stack <mspasspy.algorithms.deconvolution.align_and_stack>`
has several tuneable parameters that were a fixed constants in the original
*dbxcor* program.   To understand the context it might be helpful
show here the full function signature for `align_and_stack`:

.. code-block:: python

  def align_and_stack(
    ensemble,
    beam,
    correlation_window=None,
    correlation_window_keys=["correlation_window_start", "correlation_window_end"],
    window_beam=True,
    robust_stack_window=None,
    robust_stack_window_keys=["robust_window_start", "robust_window_end"],
    robust_stack_method="dbxcor",
    use_median_initial_stack=True,
    output_stack_window=None,
    robust_weight_key="robust_stack_weight",
    time_shift_key="arrival_time_correction",
    time_shift_limit=2.0,
    abort_irregular_sampling=False,
    convergence=0.01,
    residual_norm_floor=0.1,
    ) -> tuple:

The parameters of note are:

#. *use_median_initial_stack* sets what the rather verbose name implies.
   That is, by default the initial stack for each robust stack is
   the median stack of the ensemble time aligned to the beam computed
   in the previous iteration.  The default is known to be dead stable
   and is still recommended. When False the beam computed in the
   previous iteration is used as the initial stack estimate.
#. *residual_norm_floor* implements a concept not recognized when
   we developed the original *dbxcor* program.
   It relates to a
   subtle feature of the robust weighting scheme we discussed in
   the original paper but we didn't realize then how the concept this
   parameter implements would impact the results.  In *dbxcor* it was
   fixed constant.  To understand its use this is the weight formula
   used in `align_and_stack` that is enabled when *robust_stack_method*
   is set to the (default) of "dbxcor":

.. math::

    w_i = \frac{1}{\| \mathbf{r}_i \|}
    \frac{\mid \mathbf{b} \cdot \mathbf{d}_i \mid}
    { \| \mathbf{d}_i}

where :math:`\mathbf{r}_i = \mathbf{d}_i - (\mathbf{b} \cdot \mathbf{d}_i )\mathbf{b}`.
As we noted in the original paper there can be an issue for very consistent
data if :math:`\mid \mathbf{r}_i \mid` gets too small.   A floor on that
value is required, for example, with simulation data with no variance at all
divide by zero floating point error.  There is a more subtle issue that we now
know has a major impact on the result.  To understand why it is helpful to
note something we didn't recognize when the *dbxcor* paper was published.
That is, the second term in the weight formula,
:math:`\frac{\mid \mathbf{b} \cdot \mathbf{d}_i \mid}{ \| \mathbf{d}_i}`,
should be understood as the peak value of the cross-correlation function
between the :math:`i^{th}` datum
with the beam (stack).   Hence, the *dbxcor* weight function is the
product of the a cross-correlation weight and inverse of the residual norm term.
As we noted in the original paper
the residual term makes the weighting more aggressively
downweight any datum that differs significantly from the stack
That is desirable and a reason this algorithm can handle wildly variable
quality data.  The dark side to recognize, however, is that it makes
the result strongly history dependent.   The default behavior enabled by
having *use_median_initial_stack* set True, is to produce a stack that
is close to the median stack.   How "close" is controlled by the
setting of *residual_norm_floor*.   When *residual_norm_floor* is
1.0 the residual weighting term is disabled.  As you make the floor smaller
and smaller the result will approach a pure median stack.  The default 0.1
is appropriate for quality data.   For ensembles with a large fraction of
marginal signals a smaller value may be appropriate.  More on this topic
can be found in the (HYPERLINK) notebook that addresses this topic.  Finally,
note that if *use_median_initial_stack* is set False and *residual_norm_floor*
is small the stack will tend to converge to the signal the datum used as
the initial beam (normally that selected by
:py:func:`MCXcorPrepP<mspasspy.algorithms.deconvolution.MCXcorStacking.MCXcorPrepP>`).

Examples
^^^^^^^^^
This example shows the skeleton of a
serial job reading from data that were previously
preprocessed to bundle data into `Seismogram` objects.
It is a "skeleton" as it shows the typical steps to process
data with the multichannel correlation algorithm, but is far
from complete and untested.  The idea is you can use this as a
starting point to work with your data set:


.. code-block:: python

  def prep_ensemble(e):
    """
    Does requires preprocessing of an input SeismogramEnsemble.
    This function is specialized to this workflow with
    function call arguments fixed.   A more generic function
    would use kwargs to change some arguments.
    """
    qcnw = TimeWindow(-120.0,-5.0)
    qcsw = TimeWindow(-5.0,60.0)
    for i in range(len(e.member)):
      # d is shorthand used for readability of this example
      d = e.member[i]
      if d.dead():
        continue
      d = rotate_to_standard(d)
      # assume default handling of input slowness using Metadata
      # attributes ux an uy set previously
      d = free_surface_transformation(d,vp0=5.0,vs0=3.5)
      d = broadband_snr_QC(d,
        noise_window=qcnw,
        signal_window=qcsw,
        use_measured_arrival_time=True,
        measured_arrival_time_key="Ptime",
      )
      e.member[i]=d
  return e

  ######################### MAIN ############
  # assume db is Database handle object
  site_matcher=ObjectIdMatcher(db,
    collection='site',
    attributes_to_load=['lat','lon','elev','_id'])
  source_matcher=ObjectIdMatcher(db,
    collection='source',
    attributes_to_load=['lat','lon','depth','time'])

  srcids = db.wf_Seismogram.distinct('source_id')
  nw = TimeWindow(-100.0,-5.0)
  # code above would define a query and run find to generate cursor
  for sid in srcids:
    cursor = db.wf_Seismogram.find({'source_id' : sid})
    ens = db.read_data(cursor,
      collection='wf_Seismogram',
      normalize=[site_matcher,source_matcher],
      )
    ens = prep_ensemble(ens)
    ens = ExtractComponent(ens,2)
    ens = MCXcorPrepP(ens,
      nw,
      station_collection='site',
    )
    beam = extract_initial_beam_estimate(ens)
    ens,beam = align_and_stack(ens,beam)
    db.save_data(ens,collection='wf_TimeSeries',data_tag='MCXcorProcessed')
    db.save_data(beam,collection='wf_TimeSeries',data_tag='MCXcorProcessed')

Local Earthquake Arrival Time Measurement
=============================================
Fundamentals
---------------
Processing local earthquake data has similarities to processing
teleseismic data, but there are some fundamental ones that
require very different handling:

#. Local earthquake signals can only be processed by coherent signal
   processing methods like that used in
   :py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`
   for dense arrays of stations.   For most of the Earth that means
   a group of instruments with an aperture of the order of 1 km or less.
#. Common receiver gathers of sources located in a small volume of the
   order of a few square km can sometimes be processed with coherent
   processing like that used in
   :py:func:`align_and_stack <mspasspy.algorithms.MCXcorStacking.align_and_stack>`.
   In practice that tends to work only for events of a similar size and
   focal mechanism.

An under-appreciated, in my opinion, fundamental property of the Earth
is that local earthquake signals are so fundamentally different from
teleseismic signals.   The reason is not actually known but the
prevailing model is that waves with frequencies over around 1 Hz
are more strongly scattered than the lower frequency signals
that define teleseismic body wave phases.   There is indirect evidence
that the crust and near-surface are the main source of the stronger
scattering but that may be an observational gap due to the fact that
no seismic sources have been observed from deeps deeper than around 600 km.
In any case, from a data processing perspective, coherent
wavefield processing of local earthquake data is rarely feasible and
other approaches have proven more successful.  The methods currently
available can be grouped into three broad categories:

#.  Traditional analyst-based picking.  In the dark ages of seismology
    that means times measured from paper records.  In the digital data era that
    has always meant manual picks made from a computer screen with a
    graphical user interface.  That approach has been the bread-and-butter
    of seismic network operations worldwide for decades.
#.  Automated waveform processing methods using some for of *detector* and
    *associator*.   By *detector* I mean an algorithm like the tried and true
    sta/lta detector that has been the standard since the earliest days of
    digital seismic network operations.   A detector flags a signal transient
    based on looking at one and only one channel of data.  An *associator*
    is an algorithm that collects a group of *detector* outputs and
    makes a decision on which detections are possible seismic events
    that should be processed to estimate a possible location.   All modern
    associators have an integrated, fast preliminar earthquake location
    algorithm.   The net output is a preliminary earthquake location and
    the set of detections that are consistent with that location.  In
    almost all cases a large fraction of detections are discarded as
    spurious by a good associator.

The detector/associator paradigm are the core functions of local and
regional seismic network operations.  As stated multiple times in this
User Manual, MsPASS was not designed to address the seismic network operation
problem.  Multiple, robust software systems exist to address this problem
both in the private and open-source worlds.  MsPASS views this a solved
problem best handled by other tools if done on a large scale.  On the other hand,
I know from experience a typical research problem may need to do some
form of manual or automated picking of local earthquake data.  The
remainder of this section addresses how MsPASS can be used in combination
with some other packages for addressing that issue.

Obspy Picking
--------------
Obspy has no graphical analyst workstation for manual picks.
They do support a fairly extensive set of *detector* functions and a
crude *associator*.  Those are best understood by reading their
`Tigger/Detector Tutorial <https://docs.obspy.org/tutorial/code_snippets/trigger_tutorial.html>`_.
In general obspy, like MsPASS, was not designed to handle network
operations and is suitable only for handling small data sets with a
lot of manual intervention to handle the deficiency of their
associator.

Monitoring Network Software
-----------------------------
A large fraction of research problems in seismology may need to
use one of the specialized packages used for seismic network operations.
At present the ones I know of are:

#.  In the US scientists at academic institutions can obtain a license
    for the commercial package called
    `antelope <https:www.brtt.com>`_ provided they are not the operators
    of an operational seismic network.   Antelope has a full suite of
    network processing capabilities.  For research applications a particularly
    valuable feature is its capability to operate the real-time system on
    previously recorded data.   Antelope uses a nonstandard relational
    database called `Datascope` that uses ascii files to hold the
    relational database tables.   Because I am a long term user of
    Antelope I developed a specialized python class for handling
    these tables called
    :py:class:`DatascopeDatabase<mspasspy.preprocessing.css30.datascope.DatascopeDatabase>`.
    It can be used to import and internally manage antelope database
    tables.  How to do that is discussed in the
    :ref:`Importing Tabular Data <importing_tabular_data>`
    section of this manual.  Using Antelope for processing is a larger
    topic addressed in Antelope's documentation.
#.  `earthworm <http://www.earthwormcentral.org/>`__ is an open-source
    earthquake monitoring system used by most earthquake monitoring
    networks in the U.S. that are supported by the U.S. Geological Survey.
    Earthworm builds on several applications originally developed by
    the U.S. Geological Survey that have been around for decades.
    A type example is
    `Hypoinverse <http://www.earthwormcentral.org/documentation4/USER_GUIDE/hypoinverse.html>`__.
    It also includes a suite of real-time applications maintained by a commercial
    company called `ISTI <https://www.isti.com/products-offerings/earthworm>`__,
    who also serve as a contractor to maintain the package.   Although I
    have limited and old experience with Earthworm it appears the package
    still does not use a database but uses a file system hierarchy to
    manage data.  If that is correct, using data managed by an
    Earthworm system will require developing custom file readers for
    Metadata.  Miniseed waveform data can be indexed as normal for MsPASS.
    This is a type example of a place someone in the Earthworm community
    could help by contributing import/export tools for Earthworm.
#.  `seiscomp <https://www.seiscomp.de/doc/>`__ is more-or-less the European
    equivalent of Earthworm.   It has similar functionality for
    real-time data acquisition, detection, event association, and location.
    It also has a collction of graphical user interface tools useful
    for network operations.  I have zero experience with seiscomp but
    the same statement I made about Earthworm applies:   this is a place
    someone from the Seiscomp community could help MsPASS development by
    producing import/export functions from that system.

MsPASS Phasenet Picking
-------------------------
THIS PACKAGE IS UNDER DEVELOPMENT.   LOOK FOR FUTURE UPDATES IN THIS SECTION
WHEN THAT CODE IS RELEASED.
