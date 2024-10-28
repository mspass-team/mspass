import pickle
import numpy as np
from bson import ObjectId
from scipy.signal import hilbert
from obspy.geodetics import locations2degrees
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, ErrorLogger
from mspasspy.ccore.seismic import TimeSeries, Seismogram, PowerSpectrum
from mspasspy.ccore.algorithms.deconvolution import MTPowerSpectrumEngine
from mspasspy.ccore.algorithms.amplitudes import (
    RMSAmplitude,
    PercAmplitude,
    MADAmplitude,
    PeakAmplitude,
    BandwidthStatistics,
    BandwidthData,
)
from mspasspy.ccore.algorithms.basic import TimeWindow, Butterworth, _ExtractComponent
from mspasspy.algorithms.window import WindowData

# DEBUG
import matplotlib.pyplot as plt


def EstimateBandwidth(
    S, N, snr_threshold=1.5, df_smoother=None, f0=1.0
) -> BandwidthData:
    """
    Estimates a set of signal bandwidth estimates returned in the the
    class `BandwidthData`.  The algorithm is most appropriate for body
    waves recorded at teleseimic distances from earthquake sources.
    The reason is that the algorithm is most appropriate for signal
    and noise spectra typical of that type of data.  In particular,
    broadband noise is very colored by the microseisms.   Large enough
    signals can exceed the microseism peak but smaller events will not.
    The smallest events typically are only visible in the traditional
    short-period band.   The most difficult are the ones that have
    signals in both the short and long period band but have high
    noise levels in the microseism band making a single bandwidth
    the wrong model.  This function handles that by only returning the
    band data for the section near the search start defined by the
    "f0" argument.

    The algorithm works by searching from a starting frequency defined
    by the "f0" argument.  The basic idea is it hunts up and down the
    frequency axis until it detects the band edge.   The "band edge"
    detection is defined as the point where the signal-to-noise ratio
    first falls below the value defined by the "snr_threshold" argument.
    The algorithm has two variants of note:
    1.  If no point in the snr curve exceeds the valued defined by
        "snr_threshold" the function returns immediately with all
        attributes of the `BandwidthData` object set to 0.
    2.  If the snr value at f0 does not exceed the threshold the essage = alg + ":  arg1 must be a PowerSpectrum object for noise estimate; actual type={}".format(type(S))
        raise TypeError(message)

        until it finds a value exceeding the threshold.  In that situation
        it marks the first point found as the high frequency band
        edge and continues hunt backward to attempt to define
        the low frequency band edge.

    The "df_smoother" argument can be used to smooth the internally
    generated signal-to-noise ratio vector.   It is particularly useful
    for data with noise containing spectral lines that can create
    incorrect bandwidth data.  It is rarely necessary if the
    power spectra are computed with the multitaper method as that
    method produces spectra that are inherently smooth at a specified
    scale.  In that case if smoothing is desired we recommend the
    smoothing width be of the form k*tbp*df where tbp is the time
    bandwidth product, df is the Rayleigh bin size, and k is some
    small multipler (note multitaper spectra a inherently smoothed
    by 2*tbp*df).

    :param S: power spectrum computed from signal time window.
    :type S:  :py:class:`mspasspy.ccore.seismic.PowerSpectrum`
    :param N: power spectrum computed from noise time window
       (Note S and N do not need to be on the same frequency
       grid but the signal grid is used to compute the signal to
       noise ratio curve)
    :type N:  :py:class:`mspasspy.ccore.seismic.PowerSpectrum`
    :param snr_threshold:  value of snr used to define the
       band edges.   As noted above the algorithm searches from
       the value f0 for the first points above and below that
       frequency where the snr curve has a value less than or
       equal to this value.
    :type snr_threshold:  float (default 1.5)
       grid but the signal grid is used to compute the signal to
       spectrum within the range defined by this parameter.
       i.e. the number of points in the smoother is
       round(df_smoother/S.df())
    :type df_smoother:  float (default is None which is taken
       as a signal turn off this option and not smooth the
       snr curve.)
    :param f0:  frequency to start searching up and down
       the frequency axis.
    :type f0:  float (default 1.0)

    :return:  Returns an instance of the `BandwidthData` class.
       `BandwidthData` has the following attributes that are
       set by this function:

       - "low_edge_f" low frequency corner of estimated bandwidth
       - "low_edge_snr" snr at low corner
       - "high_edge_f" high frequency corner of estimated bandwidth
       - "high_edge_snr" snr at high corner:type S:  :py:class:`mspasspy.ccore.seismic.PowerSpectrum`
       - "f_range" total frequency range of estimate (range of S)

       Note the low edge can be zero which must be handled
       carefully if the output is used for designing a filter.
       Further all values will be 0 if no points in the snr curve
       exceed the defined threshold.
    """
    alg = "EstimateBandwidth"
    if not isinstance(S, PowerSpectrum):
        message = (
            alg
            + ":  arg0 must be a PowerSpectrum object computed from signal; actual type={}".format(
                type(S)
            )
        )
        raise TypeError(message)
    if not isinstance(N, PowerSpectrum):
        message = (
            alg
            + ":  arg1 must be a PowerSpectrum object for noise estimate; actual type={}".format(
                type(N)
            )
        )
        raise TypeError(message)
    # use the S grid to define the snr curve - note N grid can be different
    snrdata = np.zeros(S.nf())
    for i in range(S.nf()):
        f = S.frequency(i)
        i_n = N.sample_number(f)
        # conditional needed in case S and N are computed with different sample intervals
        if i_n < N.nf():
            snrdata[i] = S.spectrum[i] / N.spectrum[i_n]
        else:
            snrdata[i] = 1.0
    # S and N are power, convert to amplitude
    snrdata = np.sqrt(snrdata)
    if df_smoother:
        smoother_npts = round(df_smoother / S.df())
        # silently do nothing if the smoother requested is smaller than df
        if smoother_npts > 1:
            smoother = np.ones(smoother_npts) / smoother_npts
            np.convolve(snrdata, smoother, mode="valid")
    result = BandwidthData()
    result.f_range = S.frequency(S.nf() - 1) - S.frequency(0)
    # test for no data exceeding tbhreshold - send null result if that is the case
    snrmax = np.max(snrdata)
    if snrmax < snr_threshold:
        result.high_edge_f = 0.0
        result.high_edge_snr = 0.0
        result.low_edge_f = 0.0
        result.low_edge_snr = 0.0
        return result
    # get index of search start
    i0 = S.sample_number(f0)
    # search upward in f
    i = i0
    while i < len(snrdata):
        if snrdata[i] <= snr_threshold:
            break
        else:
            i += 1
    # this should never happen with any data using antialias filters but
    # is possible if the inputs are bad
    if i >= len(snrdata):
        i = len(snrdata) + 1
    if i > i0:
        result.high_edge_f = S.frequency(i)
        result.high_edge_snr = snrdata[i]
    else:
        # if we land here snr at f0 is less than the threshold
        # in that case search backward to find the first
        # point above the threshold (there has to be one because of
        # test for max snrdata above)
        i = i0
        while (
            snrdata[i] < snr_threshold and i >= 0
        ):  # i>=0 test not essential but safer
            i -= 0
        result.high_edge_f = S.frequency(i)
        result.high_edge_snr = snrdata[i]
        i0 = i

    # now search backward to find low edge
    i = i0
    # gt 0 so if 0 is above threshold i will be 0 on exiting this loop
    while i > 0:
        if snrdata[i] <= snr_threshold:
            break
        else:
            i -= 1
    result.low_edge_f = S.frequency(i)
    result.low_edge_snr = snrdata[i]
    return result


def _window_invalid(d, win):
    """
    Small helper used internally in this module.  Tests if TimeWidow defined
    by win has a span inside the range of the data object d.  Return True
    if the range is invalid - somewhat reversed logic is used because of
    the name choice that make the code conditioals clearer.
    """
    if d.t0 < win.start and d.endtime() > win.end:
        return False
    else:
        return True


def _safe_snr_calculation(s, n):
    """
    Helper used in this module for all snr calculations.   snr is
    always defined as a ratio of signal amplitude divided by noise amplitude.
    An issue is that with simulation data it is very common to have a noise
    window that is pure zeros.   If computed naively snr would then be
    normally be returned as NaN.  NaNs can cause a lot of mysterious errors
    so we handle that differently here.  When noise amplitude is 0 we
    then test the signal amplitude.  If it is nonzero we return large
    number defined inside this function as 999999.9 (just under 1 million).
    If both amplitudes are zero we return -1.0 which can be properly treated
    as an error or data with low snr.
    """
    if n == 0.0:
        if s > 0.0:
            return 999999.9
        else:
            return -1.0
    else:
        return s / n


def snr(
    data_object,
    noise_window=TimeWindow(-130.0, -5.0),
    signal_window=TimeWindow(-5.0, 120.0),
    noise_metric="mad",
    signal_metric="mad",
    perc=95.0,
):
    """
    Compute time-domain based signal-to-noise ratio with a specified metric.

    Signal-to-noise ratio is a fundamental measurement in all forms of
    seismic data processing.   There is, however, not a single unified metric
    that ideal for all types of signals one may want to analyze.  One class
    of metrics used time-domain metrics to use some measure of amplitude in
    a signal and noise window cut from a single waveform segment.  A type
    example is snr of some particular "seismic phase" (P, S, PP, ScS, etc)
    relative to some measure of background noise.  e.g. for P phases it is
    nearly universal to try to estimate snr from some window defined by the
    arrival time of P and a noise window before the time P arrives (pre-event noise).

    This function provides a generic api to measure a large range of metrics
    using one of four choices for measuring the norm of the data in the
    signal and noise windows:
        1.  rms - L2 norm
        2.  mad - median absolute difference, which is essentially the median amplitude in this context
        3.  perc - percentage norm ala seismic unix.  perc is defined at as the
            amplitude level were perc percentage of the data have an amplitude
            smaller than this value.  It is computed by ranking (sorting) the
            data, computing the count of that perctage relative to the number of
            amplitude samples, and returning the amplitude of the nearest value
            to that position in the ranked data.
        4.  peak - is the peak value which in linear algebra is the L infinity norm

    Note the user can specify a different norm for the signal and noise windows.
    The perc metric requires specifying what percentage level to use.  It is
    important to recognize that ALL of these metrics are scaled to amplitude
    not power (amplitude squared).

    This function will throw a MsPASSError exception if the window parameters
    do not define a time period inside the range of the data_object. You will
    need a custom function if the model of windows insider a larger waveform
    segment does not match your data.

    There is one final detail about an snr calculation that we handle carefully.
    With simulation data it is very common to have error free simulations where
    the "noise" window one would use with real data is all zeros.  An snr calculated
    with this function in that situation would either return inf or NaN depending
    on some picky details.  Neither is good as either can cause downstream
    problems.  For that reason we trap any condition where the noise amplitude
    measure is computed as zero.  If the signal amplitude is also zero we return
    a -1.0.  Otherwise we return a large, constant, positive number.  Neither
    condition will cause an exception to be thrown as that condition is considered
    somewhat to be anticipated.

    :param data_object:  MsPASS atomic data object (TimeSeries or Seismogram)
      to use for computing the snr.  Note that for Seismogram objects the
      metrix always use L2 measures of amplitude of each sample (i.e. vector amplitudes)
      If snr for components of a Seismogram are desired use ExtractComponent and
      apply this function to each component separately.
    :param noise_window: TimeWindow objects defining the time range to extract
      from data_object to define the part of the signal considered noise.
      Times can be absolute or relative.  Default the range -5 to 120 which
      is makes sense only as time relative to some phase arrival time.
    :param signal_window:  TimeWindow object defining the time range to
      extract from data_object to define the part of the signal defines as
      signal to use for the required amplitude measure.  Default of -130 to
      -5 is consistent with the default noise window (in terms of length) and
      is assumes a time relative to a phase arrival time.  For absolute times
      each call to this function may need its own time window.
    :param noise_metric:  string defining one of the four metrics defined above
      ('mad','peak','perc' or 'rms') to use for noise window measurement.
    :param signal_metric:  string defining one of the four metrics defined above
      ('mad','peak','perc' or 'rms') to use for signal window measurement.
    :return: estimated signal-to-noise ratio as a single float.  Note the
      special returns noted above for any situation where the noise window
      amplitude is 0
    """
    if _window_invalid(data_object, noise_window):
        raise MsPASSError(
            "snr:  noise_window []{wstart} - {wend}] is outside input data range".format(
                wstart=noise_window.start, wend=noise_window.end
            ),
            ErrorSeverity.Invalid,
        )
    if _window_invalid(data_object, signal_window):
        raise MsPASSError(
            "snr:  noise_window []{wstart} - {wend}] is outside input data range".format(
                wstart=noise_window.start, wend=noise_window.end
            ),
            ErrorSeverity.Invalid,
        )
    n = WindowData(data_object, noise_window.start, noise_window.end)
    s = WindowData(data_object, signal_window.start, signal_window.end)
    if noise_metric == "rms":
        namp = RMSAmplitude(n)
    elif noise_metric == "mad":
        namp = MADAmplitude(n)
    elif noise_metric == "peak":
        namp = PeakAmplitude(n)
    elif noise_metric == "perc":
        namp = PercAmplitude(n, perc)
    else:
        raise MsPASSError(
            "snr:  Illegal noise_metric argument = " + noise_metric,
            ErrorSeverity.Invalid,
        )

    if signal_metric == "rms":
        samp = RMSAmplitude(s)
    elif signal_metric == "mad":
        samp = MADAmplitude(s)
    elif signal_metric == "peak":
        samp = PeakAmplitude(s)
    elif signal_metric == "perc":
        samp = PercAmplitude(s, perc)
    else:
        raise MsPASSError(
            "snr:  Illegal signal_metric argument = " + signal_metric,
            ErrorSeverity.Invalid,
        )

    return _safe_snr_calculation(samp, namp)


def _reformat_mspass_error(
    mserr, prefix_message, suffix_message="Some requested metrics may not be computed"
):
    """
    Helper for below used to reformat a message from ccore functions that
    throw a MsPASSError.   Needed to produce rational messages from
    different error metric calculations.


    :param mserr:  MsPASSError object caught with a try: except: block

    :param prefix_message:  string that becomes the first part of the revised
    message posted.

    :param suffix_message:  string that becomes a third line of the revised
    message posted.  Note this string is always preceded by a newline so do not
    put a newline in this arg unless you want a blank line.

    :return:  expand message string

    """
    log_message = "FD_snr_estimator:  error in computing an snr metric"
    log_message += prefix_message
    log_message += mserr.message
    log_message += "\n"
    log_message += suffix_message
    return log_message


def FD_snr_estimator(
    data_object,
    noise_window=TimeWindow(-130.0, -5.0),
    noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0, 120.0),
    signal_spectrum_engine=None,
    band_cutoff_snr=1.5,
    signal_detection_minimum_bandwidth=6.0,
    f_low_zero_test=None,
    tbp=4,
    ntapers=6,
    f0=1.0,
    poles=3,
    perc=95.0,
    optional_metrics=None,
    save_spectra=False,
) -> tuple:
    """
    Estimates one or more amplitude metrics of signal-to-noise from a TimeSeries object.
    Results are returned as a set of key-value pairs in a python dict.

    FD_snr_estimator first estimates bandwidth with the function in this
    module called `EstimateBandwidth`.  See the docstring of that function
    for how the bandwidth is estimated.  The metrics this function
    computes all depend upon that bandwidth estimate.  The default return
    of this function is the return of `EstimateBandwidth` translated to
    keyp-value pairs.   Those are:

      *low_f_band_edge* - lowest frequency exceeding threshold
      *high_f_band_edge* - highest frequency exeeding threshold
      *high_f_band_edge_snr* and *low_f_band_edge_snr* are the snr values
        at the band edges
      *spectrum_frequency_range* - total frequency band for estimate
        (really just 0 to Nyquist).
      *bandwidth* - bandwidth of estimate in dB.
        i.e. 20*log10(high_f_band_edge/low_f_band_edge)
      *bandwidth_fraction* - bandwidth/spectrum_frequency_range

    A set of optional metrics can be computed.  All optional metrics use
    the bandwidth estimates in one way or another.   Optional metrics are
    defined by the following keywords passed through a list (actually
    any iterable container will work) of strings defining one or more
    of the keywords. The metrics and a brief description of each follow:

    *snr_stats* computes what are commonly plotted in box plots for the
    snr estimates within the estimated bandwidth:  minimum, maximum,
    0.25 (1/4) point, 0.75 (3/4) point, and the median.   These are set
    with following dict keys:   'snr_band_maximum','snr_band_minimum',
    'snr_band_1/4', 'srn_band_3/4', and 'snr_band_median' respectively.

    *filtered_envelope*, *filtered_L2*, *filtered_Linf*, *filtered_perc*, and *filtered_MAD*:
    All of these optional metrics first copy the data_object and then
    filter the copy with a Butterworth bandpass filter with the number of
    poles specified by the npoles argument and corners at the estimated
    band edge by the EstimateBandwidth function.  The algorithm automatically
    handles the case of a zero low frequency edge.   That is, with large events
    the low band edge can be computed as 0 frequency.   More commonly the band edge
    is computed as one or two rayleigh bins above 0.  A bandpass filtered applied
    with a corner too close to 0 can produced distorted (or null) results.
    To prevent that the default behavior is to revert to a low pass filter
    versus a bandpass filter when the estimated value of low_f_band_edge is small.
    By default "small" is defined as <= 2.0*tbp*df, where tbp is the
    time bandwidth product for the multitaper spectral estimates (in optional
    argument) and df is the frequency sampling interval of the spectrum computed from
    the data in `signal_window`.  You can use a different recipe by passing
    a value for the optional parameter "f_low_zero_test" which will replace
    the computed value using the formula above for switching to a lowpass filter.
    The optional metrics are time domain estimates computed from
    the bandpass (lowpass) filtered data.  They are
    actually computed from functions in this same module that can be
    used independently and have their own docstring description. The
    functions called have the following names in order of the keyword
    list above:  *snr_envelope*, *snr_filtered_rms*, *snr_Linv*, and *snr_filtered_mad*.
    When the computed they are set in the output dictionary with the
    following (again in order) keys:  'snr_envelope','snr_filtered_rms', 'srn_Linf',
    and 'snr_filtered_mad'.

    It is important to note that all the metrics this function returns
    are measures of amplitude NOT power.   You need to be particularly
    aware of this if you unpickle the spectra created if you set
    save_spectra true as those are power spectra.

    :param data_object:  TimeSeries object to be processed. For Seismogram
      objects the assumption is algorithm would be used for a single
      component (e.g longitudinal or vertical for a P phase)
    :param noise_window: defines the time window to use for computing the
      spectrum considered noise. The time span can be either relative or
      UTC (absolute) time but we do not check for consistency.  This low
      level function assumes they are consistent.  If not, the calculations
      are nearly guaranteed to fail.  Type must be mspasspy.ccore.TimeWindow.
    :type noise_window:  :py:class:`mspasspy.ccore.algorithms.basic.TimeWindow`
      default -130 to -5 s.
    :param signal_window: defines the time window to use that defines what
      you consider "the signal".  The time span can be either relative or
      UTC (absolute) time but we do not check for consistency.  This low
      level function assumes they are consistent.  If not, the calculations
      are nearly guaranteed to fail.  Type must be mspasspy.ccore.TimeWindow.
    :type signal_window:  :py:class:`mspasspy.ccore.algorithms.basic.TimeWindow`
      default -5 to 120 s
    :param noise_spectrum_engine: is expected to either by a None type
      or an instance of a ccore object called an MTPowerSpectralEngine.
      When None an instance of MTPowerSpectralEngine is computed for
      each call to this function.   That is a convenience for small
      jobs or when called with data from mixed sample rates and/or variable
      length time windows.   It is very inefficient to use the default
      approach for processing large data sets and really for any use in a
      map operation with dask or spark.  Normal use should be for the user to
      predefine an MtPowerSpectralEngine from the expected window sizedef FD
    :type noise_spectrum_engine: None (default) or an instance of
      :py:class:`mspasspy.ccore.algorithms.deconvolution.MTPowerSpectrumEngine`
    :param signal_spectrum_engine:  is the comparable MTPowerSpectralEngine
      to use to compute the signal power spectrum.   Default is None with the
      same caveat as above for the noise_spectrum_engine.
    :type signal_spectrum_engine: None (default) or an instance of
      :py:class:`mspasspy.ccore.algorithms.deconvolution.MTPowerSpectrumEngine`
    :param band_cutoff_snr:   defines the signal-to-noise ratio floor
      used in the search for band edges.  See description of the algorithm
      above and in the user's manual.  Default is 1.5
    :type band_cutoff_snr: float
    :param signal_detection_minimum_bandwidth:  As noted above this
      algorithm first tries to estimate the bandwidth of data where the
      signal level exceeds the noise level defined by the parameter
      band_cutoff_snr.  It then computes the bandwidth of the data in
      dB computed as 20*log10(f_high/f_low).  For almost any application
      if the working bandwidth falls below some threshold the data is
      junk to all intends and purpose.  A factor more relevant to this
      algorithm is that the "optional parameters"  will all be meaningless
      and a waste of computational effort if the bandwidth is too small.
      A particular extreme example is zero bandwidth that happens all the
      time if no frequency band exceeds the band_cutoff_snr for a range
      over that minimum defined by the time-bandwidth product.  The
      default is 6.0. (One octave which is roughly the width of the traditional
      short-period band) which allows optional metrics to be computed
      but may be too small for some applications.  If your application
      requires higher snr and wider bandwidth adjust this parameter
      and/or band_cutoff_snr.
    :type signal_detection_minimum_bandwidth:  float (default 6.0 dB)
    :param f_low_zero_test: optional lower bound on frequency to use for
       test to disable the low frequency corner.   (see above)
    :type f_low_zero_test:  float (default is None which causes the test to
        revert to 2.0*tbp*df (see above).
    :param tbp:  time-bandwidth product to use for computing the set of
      Slepian functions used for the multitaper estimator.  This parameter is
      used only if the noise_spectrum_engine or signal_spectrum_engine
      arguments are set as None.
    :type tbp:  float (default 4.0)
    :param ntapers:  is the number of Slepian functions (tapers) to compute
      for the multitaper estimators. Like tbp it is referenced only if
      noise_spectrum_engine or signal_spectrum_engine are set to None.
      Note the function will throw an exception if the ntaper parameter is
      not consistent with the time-bandwidth product.
    :type ntapers:  integer (default 6)
    :param f0:   frequency to use to start search for bandwidth up and down
      the frequency axis (see above).
    :type f0:  float (default 1.0)
    :param npoles:   defines number of poles to us for the Butterworth
      bandpass or lowpass applied for the "filtered" metrics (see above).  Default is 3.
    :type npoles:  integer (default 3)
    :param perc:   used only if 'filtered_perc' is in the optional metrics list.
      Specifies the perc parameter as used in seismic unix.  Uses the percentage
      point specified of the sorted abs of all amplitudes.  (Not perc=50.0 is
      identical to MAD)  Default is 95.0 which is 2 sigma for Gaussian noise.
    :type perc:  float (default 95.0)
    :param optional_metrics: is an iterable container containing one or more
      of the optional snr metrics discussed above. Typos in names will create
      log messages but will not cause the function to abort.
    :type optional_metrics:  should be a list of strings matching the set of
       required keywords.  Default is None which means none of the optional
       metrics will be computed.
    :param save_spectra:   If set True (default is False) the function
      will pickle the computed noise and signal spectra and save the
      strings created along with a set of related metadata defining the
      time range to the output python dict (these will be saved in MongoDB
      when db is defined - see below).   This option should ONLY be used
      for spot checking, discovery of why an snr metric has unexpected
      results using graphics, or a research topic where the spectra would
      be of interest.  It is a very bad idea to turn this option on if
      you are processing a large quantity of data and saving the results
      to MongoDB as it may bloat the database.  Consider a
      different strategy if that essential for your work.
    :return:  python tuple with two components.  0 is a python dict with
      the computed metrics associated with keys defined above.  1 is a
      mspass.ccore.ErrorLogger object. Any errors in computng any of the
      metrics will be posted to this logger.  Users should then test this
      object using it's size() method and if it the log is not empty (size >0)
      the caller should handle that condition.   For normal use that means
      pushing any messages the log contains to the original data object's
      error log.  Component 0 will also be empty with no log entry if
      the estimated bandwidth falls below the threshold defined by the
      parameter signal_detection_minimum_bandwidth.
    """
    algname = "FN_snr_estimator"
    my_logger = ErrorLogger()
    # For this algorithm we dogmatically demand the input be a TimeSeries
    if not isinstance(data_object, TimeSeries):
        raise MsPASSError(
            "FD_snr_estimator:  Received invalid data object - arg0 data must be a TimeSeries",
            ErrorSeverity.Invalid,
        )
    # MTPowerSpectrum at the moment has an issue with how it handles
    # a user error in specifying time-band product and number of tapers.
    # We put in an explicit trap here and abort if the user makes a mistake
    # to avoid a huge spray of error message
    if ntapers > round(2 * tbp):
        message = (
            algname
            + "(Fatal Error):  ntapers={ntapers} inconsistent with tbp={tbp}\n".format(
                ntapers=ntapers, tbp=tbp
            )
        )
        message += "ntapers must be >= round(2*tbp)"
        raise MsPASSError(message, ErrorSeverity.Fatal)
    if data_object.dead():
        my_logger.log_error(
            algname,
            "Datum received was set dead - cannot compute anything",
            ErrorSeverity.Invalid,
        )
        return [dict(), my_logger]
    # We enclose all the main code here in a try block and cat any MsPASSErrors
    # they will be posted as log message. Others will not be handled
    # intentionally letting python's error mechanism handle them as
    # unexpected exceptions - MsPASSError can be anticipated for data problems
    snrdata = dict()
    try:
        # First extract the required windows and compute the power spectra
        n = WindowData(data_object, noise_window.start, noise_window.end)
        s = WindowData(data_object, signal_window.start, signal_window.end)
        # DEBUG
        fig1, ax1 = plt.subplots(2)
        ax1[0].plot(s.time_axis(), s.data)
        ax1[1].plot(n.time_axis(), n.data)
        plt.show()
        # WARNING:  this handler depends upon an implementation details
        # that could be a maintenance issue.  The python code has a catch
        # that kills a datum where windowing fails.   The C++ code throws
        # an exception when that happens.  The python code posts that error
        # message to the output which we extract here
        if n.dead() or s.dead():
            if n.dead():
                if n.elog.size() > 0:
                    my_logger += n.elog
            if s.dead():
                if s.elog.size() > 0:
                    my_logger += s.elog
        if noise_spectrum_engine:
            nengine = noise_spectrum_engine
        else:
            nengine = MTPowerSpectrumEngine(n.npts, tbp, ntapers, n.npts * 2, n.dt)
        if signal_spectrum_engine:
            sengine = signal_spectrum_engine
        else:
            sengine = MTPowerSpectrumEngine(s.npts, tbp, ntapers, s.npts * 2, s.dt)
        N = nengine.apply(n)
        S = sengine.apply(s)
        # bwd = EstimateBandwidth(
        #    S.df(),
        #    S,
        #    N,
        #    band_cutoff_snr,
        #    tbp,
        #    high_frequency_search_start,
        #    fix_high_edge,
        # )
        bwd = EstimateBandwidth(S, N, snr_threshold=band_cutoff_snr, f0=f0)
        # DEBUG
        ymax = np.max(S.spectrum)
        ymin = np.min(N.spectrum)
        x = [
            bwd.low_edge_f,
            bwd.high_edge_f,
            bwd.high_edge_f,
            bwd.low_edge_f,
            bwd.low_edge_f,
        ]
        y = [ymin, ymin, ymax, ymax, ymin]
        fig2, ax2 = plt.subplots(1)
        ax2.semilogy(S.frequencies(), S.spectrum, "-", N.frequencies(), N.spectrum, ":")
        ax2.semilogy(x, y, "-")
        plt.show()

        # TODO:   the C++ function implementing this method does not handle the case of low_f edge 0.
        # This is a workaround.
        if bwd.low_edge_f <= 0.0:
            bandwidth = 20.0 * np.log10(bwd.high_edge_f / S.df())
        else:
            bandwidth = bwd.bandwidth()
        # here we return empty result if the bandwidth is too low
        if bwd.bandwidth() < signal_detection_minimum_bandwidth:
            return [dict(), my_logger]
        # These estimates are always computed and posted once we pass the above test for validity
        snrdata["low_f_band_edge"] = bwd.low_edge_f
        snrdata["high_f_band_edge"] = bwd.high_edge_f
        snrdata["low_f_band_edge_snr"] = bwd.low_edge_snr
        snrdata["high_f_band_edge_snr"] = bwd.high_edge_snr
        snrdata["spectrum_frequency_range"] = bwd.f_range
        snrdata["bandwidth"] = bandwidth
        snrdata["bandwidth_fraction"] = bwd.bandwidth_fraction()
        if save_spectra:
            snrdata["signal_spectrum"] = pickle.dumps(S)
            snrdata["noise_spectrum"] = pickle.dumps(N)
            snrdata["signal_window_start_time"] = signal_window.start
            snrdata["signal_window_end_time"] = signal_window.end
            snrdata["noise_window_start_time"] = noise_window.start
            snrdata["noise_window_end_time"] = noise_window.end

    except MsPASSError as err:
        newmessage = _reformat_mspass_error(
            err,
            "Spectrum calculation and EstimateBandwidth function section failed with the following message\n",
            "No SNR metrics can be computed for this datum",
        )
        my_logger.log_error(algname, newmessage, ErrorSeverity.Invalid)
        return [dict(), my_logger]

    # For current implementation all the optional metrics require
    # computed a filtered version of the data.  If a new option is
    # desired that does not require filtering the data the logic
    # here will need to be changed to create a more exclusive test

    if optional_metrics:
        if f_low_zero_test:
            fcutoff = f_low_zero_test
        else:
            fcutoff = 2.0 * tbp * S.df()
        # use the mspass butterworth filter for speed - obspy
        # version requires a conversion to Trace objects
        # TODO:   setting low_poles to 0 seems necessary to
        # enable lowpass filter turning off low corner terms
        # I (GLP) am not sure why that is necessary looking at the
        # C++ code.
        if bwd.low_edge_f > fcutoff:
            use_lowcorner = True
            low_poles = poles
        else:
            use_lowcorner = False
            low_poles = 0
        BWfilt = Butterworth(
            False,
            use_lowcorner,
            True,
            low_poles,
            bwd.low_edge_f,
            poles,
            bwd.high_edge_f,
            data_object.dt,
        )
        filtered_data = TimeSeries(data_object)
        BWfilt.apply(filtered_data)
        nfilt = WindowData(filtered_data, noise_window.start, noise_window.end)
        sfilt = WindowData(filtered_data, signal_window.start, signal_window.end)
        # DEBUG
        fig, ax = plt.subplots(2)
        ax[0].plot(data_object.time_axis(), data_object.data)
        ax[1].plot(filtered_data.time_axis(), filtered_data.data)
        xmarker = []
        ymarker = []
        xmarker.append(noise_window.start)
        xmarker.append(noise_window.end)
        xmarker.append(signal_window.start)
        xmarker.append(signal_window.end)
        for i in range(4):
            ymarker.append(0.0)
        ax[1].plot(xmarker, ymarker, "+")
        plt.show()

        # In this implementation we don't need this any longer so we
        # delete it here.  If options are added beware
        del filtered_data
        # Some minor efficiency would be possible if we avoided
        # duplication of computations when multiple optional metrics
        # are requested, but the fragility that adds to maintenance
        # is not justified
        for metric in optional_metrics:
            if metric == "snr_stats":
                try:
                    stats = BandwidthStatistics(S, N, bwd)
                    # stats is a Metadata container - copy to snrdata
                    # but do so only if the results are marked valid
                    if stats["stats_are_valid"]:
                        for k in stats.keys():
                            snrdata[k] = stats[k]
                    else:
                        my_logger.log_error(
                            algname,
                            "BandwidthStatistics marked snr_stats data invalid",
                            ErrorSeverity.Complaint,
                        )
                except MsPASSError as err:
                    # This handler currently would never be entered but
                    # left in place to keep code more robust in the event
                    # of a change
                    newmessage = _reformat_mspass_error(
                        "BandwithStatistics throw the following error\n",
                        "Five snr_stats attributes were not computed",
                    )
                    my_logger.log_error(algname, newmessage, err.severity)
            elif metric == "filtered_envelope":
                try:
                    analytic_nfilt = hilbert(nfilt.data)
                    analytic_sfilt = hilbert(sfilt.data)
                    nampvector = np.abs(analytic_nfilt)
                    sampvector = np.abs(analytic_sfilt)
                    namp = np.median(nampvector)
                    samp = np.max(sampvector)
                    snrdata["snr_filtered_envelope_peak"] = _safe_snr_calculation(
                        samp, namp
                    )
                except:
                    my_logger.log_erro(
                        algname,
                        "Error computing filtered_envelope metrics:  snr_filtered_envelope_peak not computed",
                        ErrorSeverity.Complaint,
                    )
            elif metric == "filtered_L2":
                try:
                    namp = RMSAmplitude(nfilt)
                    samp = RMSAmplitude(sfilt)
                    snrvalue = _safe_snr_calculation(samp, namp)
                    snrdata["snr_filtered_rms"] = snrvalue
                except MsPASSError as err:
                    newmessage = _reformat_mspass_error(
                        err,
                        "Error computing filtered_L2 metric",
                        "snr_filtered_rms attribute was not compouted",
                    )
                    my_logger.log_error(algname, newmessage, err.severity)

            elif metric == "filtered_MAD":
                try:
                    namp = MADAmplitude(nfilt)
                    samp = MADAmplitude(sfilt)
                    snrvalue = _safe_snr_calculation(samp, namp)
                    snrdata["snr_filtered_mad"] = snrvalue
                except MsPASSError as err:
                    newmessage = _reformat_mspass_error(
                        err,
                        "Error computing filtered_MAD metric",
                        "snr_filtered_mad attribute was not computed",
                    )
                    my_logger.log_error(algname, newmessage, err.severity)

            elif metric == "filtered_Linf":
                try:
                    # the C function expects a fraction - for users a percentage
                    # is clearer
                    namp = PercAmplitude(nfilt, perc / 100.0)
                    samp = PeakAmplitude(sfilt)
                    snrvalue = _safe_snr_calculation(samp, namp)
                    snrdata["snr_filtered_peak"] = snrvalue
                except MsPASSError as err:
                    newmessage = _reformat_mspass_error(
                        err,
                        "Error computing filtered_Linf metric",
                        "snr_filtered_peak attribute was not computed",
                    )
                    my_logger.log_error(algname, newmessage, err.severity)

            elif metric == "filtered_perc":
                try:
                    namp = MADAmplitude(nfilt)
                    samp = PercAmplitude(sfilt, perc / 100.0)
                    snrvalue = _safe_snr_calculation(samp, namp)
                    snrdata["snr_filtered_perc"] = snrvalue
                    snrdata["snr_perc"] = perc
                except MsPASSError as err:
                    newmessage = _reformat_mspass_error(
                        err,
                        "Error computing filtered_perc metric",
                        "snr_perf metric was not computed",
                    )
                    my_logger.log_error(algname, newmessage, err.severity)
            else:
                message = "Illegal optional_metrics keyword=" + metric + "\n"
                message += (
                    "If that is a typo expect some metrics will be missing from output"
                )
                my_logger.log_error(algname, message, ErrorSeverity.Complaint)
    return [snrdata, my_logger]


def arrival_snr(
    data_object,
    noise_window=TimeWindow(-130.0, -5.0),
    noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0, 120.0),
    signal_spectrum_engine=None,
    band_cutoff_snr=2.0,
    signal_detection_minimum_bandwidth=6.0,
    tbp=4.0,
    ntapers=6,
    f_low_zero_test=None,
    f0=1.0,
    poles=3,
    perc=95.0,
    save_spectra=False,
    phase_name="P",
    arrival_time_key="Ptime",
    metadata_output_key="Parrival",
    kill_null_signals=True,
    optional_metrics=[
        "snr_stats",
        "filtered_envelope",
        "filtered_L2",
        "filtered_Linf",
        "filtered_MAD",
        "filtered_perc",
    ],
):
    """
    Specialization of FD_snr_estimator.   A common situation where snr
    data is a critical thing to estimate is data windowed around a given
    seismic phase.   FD_snr_estimator is a bit more generic.  This function
    removes some of the options from the more generic function and
    has a frozen structure appropriate for measuring snr of a particular phase.
    In particular it always stores the results as a subdocument (python dict)
    keyed by the name defined in the metadata_output_key argument.
    This function has a close sibling called "broadband_snr_QC" that
    has similar behavior but add some additional functionality.   The
    most significant limitation of this function relative to broadband_snr_QC
    is that this function ONLY accepts TimeSeries data as input.

    This function is most appropriate
    for QC done within a workflow where the model is to process a large
    data set and winnow it down to separate the wheat from the chaff, to
    use a cliche consistent with "winnow".   In that situation the normal
    use would be to run this function with a map operator on atomic data
    and follow it with a call to filter to remove dead data and/or filter
    with tests on the computed metrics.  See User's Manual for guidance on
    this topic.  Because that is the expected normal use of this function
    the kill_null_signals boolean defaults to True.

    To be more robust the function tries to handle a common error.  That is,
    if the input data has a UTC time standard then the noise and signal
    windows would need to be shifted to some reference time to make any sense.
    Consequently, this algorithm silently handles that situation automatically
    with a simple test.  If the data are relative time no test of the
    time window range is made.  If the data are UTC, however, it tests if
    the signal time window is inside the data range.  If not, it shifts the
    time windows by a time it tries to pull from the input with the key
    defined by "arrival_time_key".  If that attribute is not defined a
    message is posted to elog of the input datum and it is returned with
    no other change.   (i.e. the attribute normally output with the tag
    defined by metadata_output_key will not exist in the output).
    Large data workflows need to handle this condition,.

    Dead inputs are handled the standard way - returned immediately with no change.

    Most parameters for this function are described in detail in the
    docstring for FD_snr_estimator.  The user is referred there to
    see the usage.   The following are added for this specialization:

    :param phase_name:  Name tag for the seismic phase being analyzed.
      This string is saved to the output subdocument with the key "phase".
      The default is "P"
    :param arrival_time_key:  key (string) used to fetch an arrival time
      if the data are in UTC and the time window received does not overlap
      the data range (see above)
    :param kill_null_signals:  boolean controlling how null snr estimator
      returns are handled.  When True (default) if FD_snr_estimator returns a null
      result (no apparent signal) that input datum is killed before being
      returned.  In that situation no snr metrics will be in the output because
      null means FD_snr_estimator couldn't detect a signal and the algorithm
      failed.   When False the datum is returned silently but
      will have no snr data defined in a dict stored with the key
      metadata_output_key (i.e. that attribute will be undefined in output)
    :param metadata_output_key:  is a string used as a key under which the
      subdocument (python dict) created internally is stored.  Default is
      "Parrival".   The idea is if multiple phases are being analyzed
      each phase should have a different key set by this argument
      (e.g. if PP were also being analyzed in the same workflow you
       might use a key like "PParrival").
    :return:  a copy of data_object with the the results stored under
      the key defined by the metadata_output_key argument.
    """
    if not isinstance(data_object, TimeSeries):
        raise TypeError("arrival_snr:  input arg0 must be a TimeSeries")
    if data_object.dead():
        return data_object
    # here we try to recover incorrect window usage
    # Note we always make a deep copy for internal use
    signal_window = TimeWindow(signal_window)
    noise_window = TimeWindow(noise_window)
    if data_object.time_is_UTC():
        # should work for anything but an absurd test near epoch 0 which should happen
        if signal_window.end < data_object.t0:
            if arrival_time_key in data_object:
                atime = data_object[arrival_time_key]
                signal_window = signal_window.shift(atime)
                noise_window = noise_window.shift(atime)
                # could test again here but we let FD_snr_estimator handle that error
            else:
                message = (
                    "Input has UTC time standard but windows appear to be relative time\n"
                    + "Tried to recover with time set with key="
                    + arrival_time_key
                    + " but it was not defined in this datum\n"
                    + "Cannot compute snr metrics"
                )

                data_object.elog.log_error(
                    "arrival_snr", message, ErrorSeverity.Complaint
                )
                return data_object

    [snrdata, elog] = FD_snr_estimator(
        data_object,
        noise_window=noise_window,
        noise_spectrum_engine=noise_spectrum_engine,
        signal_window=signal_window,
        signal_spectrum_engine=signal_spectrum_engine,
        band_cutoff_snr=band_cutoff_snr,
        signal_detection_minimum_bandwidth=signal_detection_minimum_bandwidth,
        f_low_zero_test=f_low_zero_test,
        tbp=tbp,
        ntapers=ntapers,
        f0=f0,
        poles=poles,
        perc=perc,
        optional_metrics=optional_metrics,
        save_spectra=save_spectra,
    )
    if elog.size() > 0:
        data_object.elog += elog
    # FD_snr_estimator returns an empty dictionary if the snr
    # calculation fails or indicates no signal is present.  This
    # block combines that with the kill_null_signals in this logic
    if len(snrdata) > 0:
        snrdata["phase"] = phase_name
        data_object[metadata_output_key] = snrdata
    elif kill_null_signals:
        data_object.elog.log_error(
            "arrival_snr",
            "FD_snr_estimator flagged this datum as having no detectable signal",
            ErrorSeverity.Invalid,
        )
        data_object.kill()
    return data_object


def broadband_snr_QC(
    data_object,
    component=2,
    noise_window=TimeWindow(-130.0, -5.0),
    noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0, 120.0),
    signal_spectrum_engine=None,
    band_cutoff_snr=1.5,
    signal_detection_minimum_bandwidth=6.0,
    f_low_zero_test=None,
    tbp=4.0,
    ntapers=6,
    f0=1.0,
    kill_null_signals=True,
    poles=3,
    perc=95.0,
    phase_name="P",
    metadata_output_key="Parrival",
    optional_metrics=[
        "snr_stats",
        "filtered_envelope",
        "filtered_L2",
        "filtered_Linf",
        "filtered_MAD",
        "filtered_perc",
    ],
    save_spectra=False,
    use_measured_arrival_time=False,
    measured_arrival_time_key="Ptime",
    taup_model=None,
    source_collection="source",
    receiver_collection=None,
):
    """
    Compute a series of metrics that can be used for quality control
    filtering of seismic phase data.

    This function is intended as a workhorse to be used for low-level,
    automated QC of broadband data when the the data set is defined
    by signals linked to a timeable seismic phase.   It can be
    thought of as a version of a related function called
    "arrival_snr" with some additional features.  See the docstring
    for that function for what those base features are.   Features this
    function adds not found in arrival_snr are:
        1.   This function allows Seismogram inputs.  Only TimeSeries
             data are handled by arrival_snr.
        2.   This function provides an option to compute arrival times
             from source coordinates, receiver coordinates, and a handle
             to an obspy tau-p calculator.

    Otherwise it behaves the same.  Note both functions may or may not
    choose to interact with the function save_snr_arrival.   If you want to
    save the computed metrics into a form more easily fetched
    your workflow should extract the contents of the python dictionary
    stored under the metadata_output_key tag and save the result to
    MongoDB with the save_snr_arrival function.  That option is most
    useful for test runs on a more limited data set to sort out
    values of the computed metrics that are appropriate for a secondary
    winnowing of the your data.   See User's Manual for more on
    this concept.

    The input of arg0 (data_object) can be either a TimeSeries or
    a Seismogram object.  If a Seismogram object is passed the "component"
    argument is used to extract the specified single channel from the Seismogram
    object and that component is used for processing.  That is necessary
    because all the algorithms used are single channel algorithms.  To
    use this function on all components use a loop over components BUT
    make sure you use a unique value for the argument "metadata_output_key" for
    each component.  Note this will also produce multiple documents per
    input datum.

    The type of the data_object also has a more subtle implication the
    user must be aware of.  That is, in the MsPASS schema we store receiver coordinates
    in one of two different collections:  "channel" for TimeSeries data and
    "site" for Seismogram data.  When such data are loaded the generic keys
    like lat are always converted to names like channel_lat or site_lat
    for TimeSeries and Seismogram data respectively.   This function uses
    the data type to set that naming.  i.e. if the input is TimeSeries
    it tries to fetch the latitude data as channel_lat while if it the input
    is a Seismogram it tries to fetch site_lat.   That is true of all coordinate
    data loaded by normalization from a source and receiver collection.

    Most of the arguments to this function are passed directly to
    `FD_snr_estimator`.   See the docstring of that function for reference.
    The following are additional parameters specific to this function:

    :param data_object:  An atomic MsPASS data object to which the
      algorithms requested should be applied.   Currently that means a
      TimeSeries or Seismogram object.   Any other input will result
      in a TypeError exception.  As noted above for Seismogram input the
      component argument defines which data component is to be used for the
      snr computations.
    :param component: integer (0, 1, or 2) defining which component of a
      Seismogram object to use to compute the requested snr metrics.   This
      parameter is ignored if the input is a TimeSeries.
    :param metadata_output_key:  string defining the key where the results
      are to be posted to the returned data_object.   The results are always
      posted to a python dictionary and then posted to the returned
      data_object with this key.   Default is "Parrival"
    :param use_measured_arrival_time:  boolean defining the method used to
      define the time reference for windowing used for snr calculations.
      When True the function will attempt to fetch a phase arrival time with
      the key defined by the "measured_arrival_time_key" argument.  In that
      mode if the fetch fails the data_object will be killed and an error
      posted to elog.   That somewhat brutal choice was intentional as the
      expectation is if you want to use measured arrival times you don't
      want data where there are no picks.   The default is True to make
      the defaults consistent.  The reason is that the tau-p calculator
      handle is passed to the function when using model-based travel times.
      There is no way to default that so it defaults to None.
    :param measured_arrival_time_key: is the key used to fetch a
      measured arrival time.   This parameter is ignored if use_measured_arrival_time
      is False.
    :param taup_model: when use_measured_arrival_time is False this argument
      is required.  It defaults as None because there is no way the author
      knows to initialize it to anything valid.  If set it MUST be an instance
      of the obspy class TauPyModel
      (https://docs.obspy.org/packages/autogen/obspy.taup.tau.TauPyModel.html#obspy.taup.tau.TauPyModel)
      Mistakes in use of this argument can cause a MsPASSError exception to
      be thrown (not logged thrown as a fatal error) in one of two ways:
      (1)  If use_measured_arrival_time is False this argument must be defined,
      and (2) if it is defined it MUST be an instance of TauPyModel.
    :param source_collection:  normalization collection for source data.
      The default is the MsPASS name "source" which means the function will
      try to load the source hypocenter coordinates (when required) as
      source_lat, source_lon, source_depth, and source_time from the input
      data_object.  The id of that document is posted to the output dictionary
      stored under metadata_output_key.
    :param receiver_collection:  when set this name will override the
      automatic setting of the expected normalization collection naming
      for receiver functions (see above).  The default is None which causes
      the automatic switching to be involked.  If it is any other string
      the automatic naming will be overridden.

    :return:  the data_object modified by insertion of the snr QC data
      in the object's Metadata under the key defined by metadata_output_key.
    """
    if data_object.dead():
        return data_object
    if isinstance(data_object, TimeSeries):
        # We need to make a copy of a TimeSeries object to assure the only
        # thing we change is the Metadata we add to the return
        data_to_process = TimeSeries(data_object)
        if receiver_collection:
            rcol = receiver_collection
        else:
            rcol = "channel"
    elif isinstance(data_object, Seismogram):
        if component < 0 or component > 2:
            raise MsPASSError(
                "arrival_snr_QC:  usage error.  "
                + "component parameter passed with illegal value={n}\n".format(
                    n=component
                )
                + "Must be 0, 1, or 2",
                ErrorSeverity.Fatal,
            )
        data_to_process = _ExtractComponent(data_object, component)
        if receiver_collection:
            rcol = receiver_collection
        else:
            rcol = "site"
    else:
        raise MsPASSError(
            "arrival_snr_QC:   received invalid input data\n"
            + "Input must be either TimeSeries or a Seismogram object",
            ErrorSeverity.Fatal,
        )
    if use_measured_arrival_time:
        arrival_time = data_object[measured_arrival_time_key]
    else:
        # This test is essential or python will throw a more obscure,
        # generic exception
        if taup_model is None:
            raise MsPASSError(
                "arrival_snr_QC:  usage error.  "
                + "taup_model parameter is set None but use_measured_arrival_time is False\n"
                + "This gives no way to define processing windows.  See docstring",
                ErrorSeverity.Fatal,
            )
        source_lat = data_object[source_collection + "_lat"]
        source_lon = data_object[source_collection + "_lon"]
        source_depth = data_object[source_collection + "_depth"]
        source_time = data_object[source_collection + "_time"]
        receiver_lat = data_object[rcol + "_lat"]
        receiver_lon = data_object[rcol + "_lon"]
        delta = locations2degrees(source_lat, source_lon, receiver_lat, receiver_lon)
        arrival = taup_model.get_travel_times(
            source_depth_in_km=source_depth,
            distance_in_degree=delta,
            phase_list=[phase_name],
        )
        arrival_time = source_time + arrival[0].time
        taup_arrival_phase = arrival[0].phase.name
        # not sure if this will happen but worth trapping it as a warning if
        # it does
        if phase_name != taup_arrival_phase:
            data_object.elog.log_error(
                "arrival_snr_QC",
                "Requested phase name="
                + phase_name
                + " does not match phase name tag returned by obpsy taup calculator="
                + taup_arrival_phase,
                "Complaint",
            )
    if data_to_process.time_is_UTC():
        data_to_process.ator(arrival_time)
    [snrdata, elog] = FD_snr_estimator(
        data_to_process,
        noise_window=noise_window,
        noise_spectrum_engine=noise_spectrum_engine,
        signal_window=signal_window,
        signal_spectrum_engine=signal_spectrum_engine,
        band_cutoff_snr=band_cutoff_snr,
        signal_detection_minimum_bandwidth=signal_detection_minimum_bandwidth,
        f_low_zero_test=f_low_zero_test,
        tbp=tbp,
        ntapers=ntapers,
        f0=f0,
        poles=poles,
        perc=perc,
        optional_metrics=optional_metrics,
        save_spectra=save_spectra,
    )
    if elog.size() > 0:
        data_object.elog += elog
    # FD_snr_estimator returns an empty dictionary if the snr
    # calculation fails or indicates no signal is present.  This
    # block combines that with the kill_null_signals in this logic
    if len(snrdata) == 0:
        data_object.elog.log_error(
            "broadband_snr_QC",
            "FD_snr_estimator flagged this datum as having no detectable signal",
            ErrorSeverity.Invalid,
        )
        if kill_null_signals:
            data_object.kill()
        return data_object
    snrdata["phase"] = phase_name
    snrdata["snr_arrival_time"] = arrival_time
    snrdata["snr_signal_window_start"] = arrival_time + signal_window.start
    snrdata["snr_signal_window_end"] = arrival_time + signal_window.end
    snrdata["snr_noise_window_start"] = arrival_time + noise_window.start
    snrdata["snr_noise_window_end"] = arrival_time + noise_window.end

    # These cross-referencing keys may not always be defined when a phase
    # time is based on a pick so we add these cautiously
    scol_id_key = source_collection + "_id"
    rcol_id_key = rcol + "_id"
    if data_object.is_defined(scol_id_key):
        snrdata[scol_id_key] = data_object[scol_id_key]
    if data_object.is_defined(rcol_id_key):
        snrdata[rcol_id_key] = data_object[rcol_id_key]
    # Note we add this result to data_object NOT data_to_process because that
    # is not always the same thing - for a TimeSeries input it is a copy of
    # the original but it may have been altered while for a Seismogram it is
    # an extracted component
    data_object[metadata_output_key] = snrdata
    return data_object


def save_snr_arrival(
    db,
    doc_to_save,
    wfid,
    wf_collection="wf_Seismogram",
    save_collection="arrival",
    subdocument_key=None,
    use_update=False,
    update_id=None,
    validate_wfid=False,
) -> ObjectId:
    """
    This function is a companion to broadband_snr_QC.   It handles the
    situation where the workflow aims to post calculated snr metrics to
    an output database (normally in the "arrival" collection but optionally
    to a parent waveform collection.  ).   The alternative models as
    noted in the User's Manual is to use the kill option to broadband_snr_QC
    followed by a call to the filter method of bag/rdd to remove the
    deadwood and reduce the size of data passed downstream in large
    parallel workflow.   That case is better handled by using
    broadband_snr_QC directly.

    How the data are saved is controlled by four parameters:   save_collection,
    use_update, update_id and subdocument_key.   They interact in a way
    that is best summarized as a set of cases that procuce behavior
    you may want:
        1.  If save_collection is not the parent waveform collection,
            behavior is driven by use_update combined with subdocument_key.
            When use_update is False (default) the contents of doc_to_save
            will be used to define a new document in save_collection
            with the MongoDB insert_one method.
        2.  When use_update is True the update_id will be assumed to be
            defined and point be the ObjectId of an existing document
            in save_collection.   Specifically that id will be used as
            the query clause for a call the insert_one method.   This
            combination is useful if a workflow is being driven by
            arrival data stored in save_collection created, for example,
            for a css3.0 a event->origin->assoc->arrival catalog of
            arrival picks.   A variant of this mode will occur if the
            argument subdocument_key is defined (default is None).  If
            you define subgdocument_key the contents of doc_to_save will
            be stored as a subdocument in save_collection accessible
            with the key defined by subdocument_key.
        3.  If save_collection is the same as the parent waveform collection
            (defined via the input parameter wf_collection) the
            value of use_update will be ignored and only an update will
            be attempted.  The reason is that if one tried to save the
            contents of doc_to_save to a waveform collection would corrupt
            the database by have a bunch of documents that that could not
            be used to construct a valid data object (the normal use for
            one of the wf collections).

    :param db:  MongoDB database handle to use for transactions that
      are the focus of this algorithm.
    :param doc_to_save:  python dictionary containing data to be saved.
      Where and now this is saved is controlled by save_collection,
      use_update, and subdocument_key as described above.
    :param wfid:   waveform document id of the parent datum.   It is
      assumed to be an ObjectId of linking the data in doc_to_save to
      the parent.   It is ALWAYS saved in the output with the key "wfid".
    :param wf_collection:  string defining the collection from which the
      datum from which the data stored in doc_to_save are associated.   wfid
      is assumed define a valid document in wf_collection.   Default is
      "wf_Seismogram".
    :param save_collection:  string defining the collection name to which
      doc_to_save should be pushed.   See above for how this name interacts
      with other parameters.
    :param subdocument_key:   Optional key for saving doc_to_save as a
      a subdocument in the save_collection.   Default is None which means
      the contents of doc_to_save will be saved (or update) as is.
      For saves to (default) arrival collection this parameter should
      normally be left None, but is allowed.   If save_collection is the
      parent waveform collection setting this to some sensible key is
      recommended to avoid possible name collisions with waveform
      Metadata key-value pairs.    Default is None which means no
      subdocuments are created.
    :param use_update:  boolean controlling whether or not to use
      updates or inserts for the contents of doc_to_save.  See above for
      a description of how this interacts with other arguments to this
      function.  Default is False.
    :param update_id:   ObjectId of target document when running in update
      mode.  When save_collection is the same as wf_collection this parameter
      is ignored and the required id passed as wfid will be used for the
      update key matching.   Also ignored with the default behavior if
      inserting doc_to_save as a new document.  Required only if running
      with a different collection and updating is desired.  The type example
      noted above would be updates to existing arrival informations
      created from a css3.0 database.
    :param validate_wfid:   When set True the id defined by the
      required argument wfid will be validated by querying wf_collection.
      In this mode if wfid is not found the function will silently return None.
      Callers using this mode should handle that condition.
    :return:  ObjectId of saved record.  None if something went wrong
      and nothing was saved.
    """
    dbwfcol = db[wf_collection]
    if validate_wfid:
        query = {"_id": wfid}
        ndocs = dbwfcol.count_documents(query)
        if ndocs == 0:
            return None
    dbcol = db[save_collection]
    update_mode = use_update
    # silently enforce update mode if saving back to waveform collection
    if wf_collection == save_collection:
        update_mode = True
        upd_id = wfid
    else:
        upd_id = update_id
    # this block sets doc for save with or without subdoc option
    doc = dict(doc_to_save)  # this acts like a deep copy
    doc["wfid"] = wfid
    if subdocument_key:
        # The constructor for a dict is necesary to assure a deepcopy here
        doc[subdocument_key] = dict(doc)
    if update_mode:
        # note update is only allowed on the parent wf collection
        filt = {"_id": upd_id}
        update_clause = {"$set": doc}
        dbcol.update_one(filt, update_clause)
        save_id = upd_id
    else:
        save_id = dbcol.insert_one(doc).inserted_id

    return save_id
