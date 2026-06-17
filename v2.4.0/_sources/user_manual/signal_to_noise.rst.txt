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
power spectrum of the signal and noise windows tha are used to compute a series of
broadband snr metrics you can use to sort out signals worth processing further.
The algorithm makes a fundamental assumption that the optimal frequency band
of a given signal can be defined by a high and low corner frequency.
In marginal snr conditions that assumption can easily be wrong.
A type example is teleseismic P waves that have signals in the
traditional short period and long period bands, but the signal level does
not exceed the microseism peak.  Nonetheless, the single passband assumption
is an obvious first order approach we have found useful for automated
data winnowing.

The function attempts to determine the data passband by a process illustrated in the
Figure below.  The algorithm is a bidirectional search.  The lower passband edge
initiates at the highest frequency distinguishable from zero as
defined by the time-bandwidth product (`tbp` input parameter).  The upper
passband edge search is initiated either from a user specified frequency
or the default of 80% of Nyquist.  Both searches increment/decrement through
frequency bins computing the ratio of the signal power spectral density to
the estimated noise power spectral density.  An input parameter with the
tag `band_cutoff_snr` defines the minimum snr the algorithm assumes is
indicating a signal is present.   A special feature of the search possible
because of the use of multitaper spectra uses the `tbp` parameter.
A property of multitaper spectra is the spectra are smoothed at a scale of the
number of frequency binds defined by the time-bandwidth product through
the formula:

.. math::

    W = T \Delta f

where :math:`T` is the window length in seconds and
:math:`\Delta f` is the desired frequency resolution of a
spectral estimate in Hz.   It is convenient to think of :math:`Delta f`
in terms of the number of Rayleigh bins, :math:`\frac{1}{T}`
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
properties.
