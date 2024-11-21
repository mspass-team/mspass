.. _signal_to_noise:

Signal to Noise Ratio Estimation
===================================
Concepts
_____________
When analyzing passive array data most seismology analysis focuses
on "transient signals" that are the wiggly lines that are the core
data of seismology.   One of the most basic quantitative measures of
"goodness" of a particular signal is what is universally called
signal-to-noise ratio.  Robust estimates of signal-to-noise ratio are
an essential component of any workflow that doesn't resort to brute
force interactive trace editing by a human.  A fundamental problem we
recognized in building this component of MsPASS is that there is no
standard definition of how to measure signal-to-noise ratio.   The reason is
that not all metrics perform equally on all signals.  For that reason we
decided it was necessary to supply a range of options for signal-to-noise
estimation to allow anyone to develop a "robust" set of automated
criteria for data editing.

It is useful to first give a generic definition of signal-to-noise
ratio.  Sections below describe specific metrics based on the following
generic formula:

.. math::

  snr = \frac{signal\_metric}{noise\_metric}

i.e. we divide some measures of the signal amplitude by some measure of
noise amplitude.  There are complexities in defining what "the signal"
and "noise" mean and what measure is used for each.  We can address the
first in a common way for all MsPASS implementations.  Choices for the
second are discussed below.

All MsPASS algorithms are driven by definitions of the time windows that
define the part of an input datum is signal and noise.   All current
implementations are driven by a simple data object we call a `TimeWindow`.
You can read the docstring but everything useful about this object
can be gleaned from running this code fragment in the mspass container:

.. code-block:: python

  from mspasspy.ccore.algorithms.basic import TimeWindow
  win = TimeWindow(-5.0,20.0)
  print("Time window range:  ",win.start,"<=t<=",win.end)

The time window here would be appropriate if the datum it used for
had been converted to "relative time".   The example could be appropriate
to define a signal window if the data had been converted to "relative time"
(see section :ref:`time_standard_constraints`)
with zero defined as a measured or predicted phase arrival time.

All algorithms to estimate *snr* require a `TimeWindow` defining what section of
data should be used for computing some metric for signal and noise to
compute the ratio.  The metric applied to each window may or may not always
be the same.

Time Window Amplitude Metrics
______________________________

The most common measure of amplitude is the root mean square error
that is a scaled version of the L2 vector norm:

.. math::

  | \bf{x} \|_{rms} = \sqrt{\frac{\sum\limits_{i=n_s}^{n_e} x_i^2 }{n_e - n_s -1}}

where :math:`n_s` is the data index for the start time of a time window and
:math:`n_e` is the index of the end time.  In the snr docstrings this metric
is given the tag `rms`.

The traditional metric for most earthquake magnitude formulas is
peak amplitude. In linear algebra language peak amplitude in a time
window is called the :math:`L_\infty` norm defined as:

.. math::

  | \bf{x} |_{\infty} = max ( \mid x_{n_s}\mid ,  \mid x_{n_s + 1}\mid , \cdots , \mid x_{n_e}\mid )

For those unfamiliar with this jargon it is little more than
a mathematical statement that we measure the largest sample amplitude.
In the snr docstrings this metric is given the tag `peak`.

MsPASS also provides a facility to calculate a few less common metrics based
on ranked (sorted) lists of amplitudes.  All create a vector of
:math:`N=n_s - n_e -1` absolute values of each sample in the time window
(:math:`\mid x_i \mid`) and sort the values in increasing order.
The MsPASS snr module supports two metrics based on a fully sorted list
of amplitudes:

1.  The tag `median` is used to compute the standard quanity called the
    median, which is defined the center (50% point) of the sorted list of amplitudes.
2.  The generic tag `perc` is used for a family of metrics using sorted
    amplitudes.   Users familiar with seismic unix will recognize this
    keyword as a common argument for plot scaling in that package.  MsPASS,
    in fact, adapted the perc metric concept from seismic unix.
    If used it always requires specifying a "percentage"
    describing the level it should define.   e.g the default of 95 means
    return the amplitude for which 95% of the samples have a lower amplitude.
    Similarly, perc of 50 would be the same as the median and 100 would
    be the same as the peak amplitude.

Low-level SNR Estimation
________________________________
The basic function to compute signal-to-noise ratios has the
simple name :py:func:`snr<mspasspy.algorithms.snr.snr>`.  The docstring
describes the detailed use, but you will normally need to specify or
default four key parameters:  (1) signal time window, (2) noise time window,
(3) metric to use for the signal window, and (4) metric to use for the
noise window.   The metric choice is made by using the tag strings
defined above:  `rms`, `peak, `mad`, or `perc`.   Note the `perc`
option level defaults but can be set to any level that makes sense.
If `perc` is used for both signal and noise the same level will be used
for both.  The following is a typical usage example.  The example uses
the `peak` metric for the signal and rms for noise and returns the estimate
in the symbol `dsnr`.

.. code-block:: python

  swin = TimeWindow(-2.0,10.0)
  nwin = TimeWindow(-120.0,-5.0)
  # assume d is a TimeSeries object defined above
  dsnr = snr(d,noise_window=nwin,signal_window=swin,
                 noise_metric="rms",signal_metric="peak")

Broadband SNR Estimation
____________________________
MsPASS implements a set of more elaborate signal-to-noise metrics
designed for quality control editing of modern broadband data.
An issue not universally appreciated about all, modern, passive array
data recorded with broadband instruments is that traditional measures of
signal-to-noise ratio are usually meaningless if applied to raw data.
From a signal processing perspective the fundamental reason is that
broadband seismic noise and earthquake signals are both strongly
"colored".  Broadband noise is always dominated by microseisms and/or
cultural noise at frequencies above a few Hz.   Earthquake's have
characteristic spectra.  The "colors" earthquakes generate are commonly
used, in fact, to measure source properties.  As a result most earthquake records
have wildly variable signal-to-noise variation across the recording
band of modern instruments.   MsPASS addresses this issue through
a novel implementation in the function
:py:func:`FD_snr_estimator<mspasspy.algorithms.srn.FD_snr_estimator>`
and two higher level functions that use it internally called
:py:func:`arrival_snr<mspasspy.algorithms.srn.arrival_snr>`
and :py:func:`arrival_snr_QC<mspasspy.algorithms.srn.arrival_snr_QC>`.
The focus of this section is the algorithm used in
:py:func:`FD_snr_estimator<mspasspy.algorithms.srn.FD_snr_estimator>`.
The others should be thought of as convenient wrappers to run
`FD_snr_estimator`.

The "FD" in `FD_snr_estimator` function is short for "Frequency Domain"
emphasizing that FD is the key idea of the algorithm.  Specifically,
the function computes the
power spectrum of the data inside the signal and noise windows.
The spectral estimates are used to compute a series of
snr metrics you can use to sort out signals worth processing further.
The algorithm makes a fundamental assumption that the optimal frequency band
of a given signal can be defined by a high and low corner frequency.
In marginal snr conditions that assumption can easily be wrong.
A type example is teleseismic P waves that have signals in the
traditional short period and long period bands, but the signal level does
not exceed the microseism peak.  Nonetheless, the single passband assumption
is an obvious first order approach we have found useful for automated
data winnowing of teleseismic body wave signals.   The algorithm
is nearly guaranteed to work badly on local earthquake data where the
single passband assumption is often violated.

The function attempts to determine the data passband by a process illustrated in the
Figure below.  The algorithm is a bidirectional search initiated from
an initial starting frequency defined by the parameter `f0`.
The algorithm first search upward (increasing f) from f0 until
it detects the first point where the snr drops below a threshold
defined by the argument `band_cutoff_snr`.   That frequency is marked
as the upper band edge.  The process is then repeated searching in the
opposite direction (i.e. incrementally testing snr at frequencies below f0)
until low frequency band edge is found where the snr drops below
`band_cutoff_snr`.   There are two complications the algorithm needs to
handle:

#.  If the snr at `f0` is less than `band_cutoff_srn` the initial
    search works backward to lower and lower frequency until a point with
    with snr above the threshold is reach.  If none is ever found the
    datum will be killed with an error message noting no signal was detected.
#.  There are multiple complications with the backward search to find the
    low-frequency corner.   First, the definition of 0 frequency is dependent
    upon the spectral estimation method.   Our implementation, however, uses
    the multitaper method to compute spectral estimates.  Because of that
    a key property is the so called time-bandwidth product,
    :math:`W = T \Delta f` where :math:`T` is the window length in seconds
    and :math:`\Delta f` is the desired frequency resolution of a
    spectral estimate in Hz.   It is convenient to think of :math:`\Delta f`
    in terms of the number of Rayleigh bins, :math:`\Delta f_R = \frac{1}{T}`
    which is the frequency resolution of the discrete Fourier
    transform for a signal of duration :math:`T`.
    A key point is that increasing the time-bandwidth products causes the
    spectral estimates to progressively smoother.   For this application
    we found using `tbp=4` with 8 tapers or `tbp=5` and 10 tapers
    are good choices as they produce
    smoother spectra that produces more stable results.
    Readers unfamiliar with
    multitaper spectral methods may find it useful begin with
    the `matlab help file in their multitaper estimator <https://www.mathworks.com/help/signal/ref/pmtm.html>`__.
    There is also a large literature applying the technique to a range of
    practical problems easily found by any library search engine.
    We chose to use the multitaper because it always produces a more
    stable estimator for this application because of its reliable smoothing
    properties.  The low-frequency edge detection is a particular case.
    With multitaper spectra the lowest frequency indistinguishable from 0
    is :math:`W \Delta f_R`.  The low band edge is treated as 0 if the
    low frequency band edge drops below 2 times that frequency.  The
    factor of 2 is a fudge factor to assure the set of metrics we
    describe next are reliable for large events.

Once, `FD_snr_estimator` has an estimate of the bandwidth of the data
it does one of two things:  (1) computes a filtered version of the signal
in that passband, or (2) if the bandwidth is below a threshold specified
by the parameter `signal_detection_minimum_bandwidth` the datum is killed
and an error message is posted to the return.

`FD_snr_estimator` also has a set of "optional" metrics it can compute.
`FD_snr_estimator` should be viewed as a lower level function that
is generic.   Most applications will likely prefer the two higher
level functions called
:py:func:`mspasspy.algorithms.snr.arrival_snr` or
:py:func:`mspasspy.algorithms.snr.broadband_snr_QC`.  Both
enable all or a subset of the "optional metrics" by default.
The optional metrics that :py:func:`mspasspy.algorithms.snr.FD_snr_estimator`
can compute are defined by any a set of keywords passed via the
function argument `optional_metrics`.  These are:

#. If "snr_stats" appears in `optional_metrics` it triggers the
   computation of five attribute with keys:  *snr_band_maximum*, *snr_band_minimum*, *snr_band_1/4*,
   *snr_band_3/4*, and *snr_band_median*. All are computed directly
   from the discrete spectral estimates.  Specifically, a vector of
   signal-to-noise values is computed on the frequency grid defined by
   the signal spectrum of all values exceeding the signal-to-noise
   threshold value passed to the function.  That vector is used to compute
   the stock statistics many may recognize as what is used in a "box plot".
   As the names imply "maximum" and "minimum" are the smallest and largest values
   respectively, "1/4" and "3/4" are the 25% and 75% points respectively, and
   "median" is the 50% point.
#. A second family of snr metrics can be computed.  They are a family because
   they are computed from a common algorithm.   Although they can be specified
   independently note that the computational effort to compute one is not
   significantly different from all.   The common algorithm is that the data
   passed to the function are first filtered by the bandwidth defined
   by the estimates saved as *low_f_band_edge* to *high_f_band_edge*.  A
   detail is that if the *low_f_band_edge* is detected as indistinguishable
   from zero a lowpass filter is applied.  Otherwise a zero phase Butterworth
   filter is applied with the band edge estimates as the corner frequencies.
   The following are computed from that filtered data:
   - *filtered_L2* computes the RMS value of the filtered signal
   - *filtered_Linf* computes the peak absolute value of the signal
   - *filtered_MAD* computes the median absolute difference of the filtered signal vector
   - *filtered_perc* computes the amplitude as a specfied percentage value
     (default is 95%).   "perc" may be mysterious to anyone not familiar with
     Seismic Unix.  perc is commonly used in Seismic Unix plotting to normalize
     amplitudes.   Note perc of 100% is identical to the maximum value.
   - *filtered_envelope* uses the numpy envelope function and the result is
     the maximum value of the filtered data passed through the envelope.
     The result for most data differs little from the largest absolute value.
  Note all of the above are implemented using the amplitude functions discussed
  at beginning of this section 
