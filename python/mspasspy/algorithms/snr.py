import pickle
import numpy as np
from scipy.signal import hilbert
from obspy.geodetics import locations2degrees
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, ErrorLogger
from mspasspy.ccore.seismic import TimeSeries, Seismogram
from mspasspy.ccore.algorithms.deconvolution import MTPowerSpectrumEngine
from mspasspy.ccore.algorithms.amplitudes import (
    RMSAmplitude,
    PercAmplitude,
    MADAmplitude,
    PeakAmplitude,
    EstimateBandwidth,
    BandwidthStatistics,
)
from mspasspy.ccore.algorithms.basic import TimeWindow, Butterworth, ExtractComponent
from mspasspy.algorithms.window import WindowData


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
    band_cutoff_snr=2.0,
    tbp=4,
    ntapers=8,
    high_frequency_search_start=2.0,
    fix_high_edge=True,
    poles=3,
    perc=95.0,
    optional_metrics=None,
    save_spectra=False,
):
    # optional_metrics=['snr_stats','filtered_envelope','filtered_L2','filtered_Linf','filtered_MAD','filtered_perc']):
    """
    Estimates one or more amplitude metrics of signal-to-noise from a TimeSeries object.
    An implicit assumption is that the analysis is centered on a timeable "phase"
    like P, PP, etc.

    This is a python function that can be used to compute one or several
    signal-to-noise ratio estimates based on an estimated bandwidth using
    the C++ function EstimateBandwidth.  The function has a fair number of
    options, but the core metrics computed are the bandwidth estimates
    computed by that function.  It uses a fairly simple search algorithm
    that functions well for most earthquake sources.  For the low end the
    algorithm searches from the first frequency indistinguishable from DC to
    find the lowest frequency for which the snr exceeds a threshold specified
    by the input parameter 'band_cutoff_snr'.   It does a similar search
    from the high end from a point 80% of Nyquist - a good choice for all
    modern digital data that use FIR antialias filters.   Both searches are
    not just defined with just the first frequency to satisfy the snr
    threshold criteria.  Only when a group of frequencies more than 2 times
    the time-bandwidth product exceed the threshold is the band edge
    defined.   The actual band edge is then defined as the first frequency
    exceeding the threshold.  That more elaborate algorithm was used to
    prevent pure lines in either the signal or noise spectrum from
    corrupting the estimates.

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
    band edge by the EstimateBandwidth function.   The metrics computed
    are time domain snr estimates computed with he filtered data.  They are
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

    :param signal_window: defines the time window to use that defines what
    you consider "the signal".  The time span can be either relative or
    UTC (absolute) time but we do not check for consistency.  This low
    level function assumes they are consistent.  If not, the calculations
    are nearly guaranteed to fail.  Type must be mspasspy.ccore.TimeWindow.

    :param noise_spectrum_engine: is expected to either by a None type
    or an instance of a ccore object called an MTPowerSpectralEngine.
    When None an instance of MTPowerSpectralEngine is computed for
    each call to this function.   That is a convenience for small
    jobs or when called with data from mixed sample rates and/or variable
    length time windows.   It is very inefficient to use the default
    approach for processing large data sets and really for any use in a
    map operation with dask or spark.  Normal use should be for the user to
    predefine an MtPowerSpectralEngine from the expected window size
    for a given data sample rate and include it in the function call.

    :param signal_spectrum_engine:  is the comparable MTPowerSpectralEngine
    to use to compute the signal power spectrum.   Default is None with the
    same caveat as above for the noise_spectrum_engine.

    :param tbp:  time-bandwidth product to use for computing the set of
    Slepian functions used for the multitaper estimator.  This parameter is
    used only if the noise_spectrum_engine or signal_spectrum_engine
    arguments are set as None.  The default is 4.0

    :param ntapers:  is the number of Slepian functions (tapers) to compute
    for the multitaper estimators. Like tbp it is referenced only if
    noise_spectrum_engine or signal_spectrum_engine are set to None.
    Note the function will throw an exception if the ntaper parameter is
    not consistent with the time-bandwidth product.  That is, the
    maximum number of tapers is round(2*tbp-1).   Default is 8 which is
    consistent with default tbp=4.0

    :param high_frequency_search_start: Used to specify the upper frequency
      used to start the search for the upper end of the bandwidth by
      the function EstimateBandwidth.  Default is 2.0 which reasonable for
      teleseismic P wave data.  Should be change for usage other than
      analysis of teleseimic P phases or you the bandwidth may be
      grossly underestimated.
    :param fix_high_edge:   boolean controlling upper search behavior.
      When set True the search from the upper frequency limit is disabled
      and the upper band limit edge is set as the value passed as
      high_frequency_search_start.  False enables the search.
      True is most useful for teleseismic body waves as many stations have
      a series of closely enough spaced lines (presumably from electronic
      sources) that set the high edge incorrectly.   False would be
      more appropriate for most local and regional earthquake data.
      The default is True.

    :param npoles:   defines number of poles to us for the Butterworth
    bandpass applied for the "filtered" metrics (see above).  Default is 3.

    :param perc:   used only if 'filtered_perc' is in the optional metrics list.
    Specifies the perc parameter as used in seismic unix.  Uses the percentage
    point specified of the sorted abs of all amplitudes.  (Not perc=50.0 is
    identical to MAD)  Default is 95.0 which is 2 sigma for Gaussian noise.

    :param optional_metrics: is an iterable container containing one or more
    of the optional snr metrics discussed above.

    :param store_as_subdocument:  This parameter is included for
    flexibility but should not normally be changed by the user.  As noted
    earlier the outputs of this function are best abstracted as Metadata.
    When this parameter is False the Metadata members are all posted with
    directly to data_object's Metadata container.  If set True the
    internally generated python dict is copied and stored with a key
    defined through the subdocument_key argument.  See use below in
    function arrival_snr.

    :param subdocument_key:  key for storing results as a subdocument.
    This parameter is ignored unless store_as_subdocument is True.
    Default is "snr_data"

    :param save_spectra:   If set True (default is False) the function
    will pickle the computed noise and signal spectra and save the
    strings created along with a set of related metadata defining the
    time range to the output python dict (these will be saved in MongoDB
    when db is defined - see below).   This option should ONLY be used
    for spot checking, discovery of why an snr metric has unexpected
    results using graphics, or a research topic where the spectra would
    be of interest.  It is a very bad idea to turn this option on if
    you are processing a large quantity of data and saving the results
    to MongoDB as it will bloat the arrival collection.  Consider a
    different strategy if that essential for your work.

    :return:  python tuple with two components.  0 is a python dict with
    the computed metrics associated with keys defined above.  1 is a
    mspass.ccore.ErrorLogger object. Any errors in computng any of the
    metrics will be posted to this logger.  Users should then test this
    object using it's size() method and if it the log is not empty (size >0)
    the caller should handle that condition.   For normal use that means
    pushing any messages the log contains to the original data object's
    error log.
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
        if noise_spectrum_engine:
            nengine = noise_spectrum_engine
        else:
            nengine = MTPowerSpectrumEngine(n.npts, tbp, ntapers)
        if signal_spectrum_engine:
            sengine = signal_spectrum_engine
        else:
            sengine = MTPowerSpectrumEngine(s.npts, tbp, ntapers)
        N = nengine.apply(n)
        S = sengine.apply(s)
        bwd = EstimateBandwidth(
            S.df, S, N, band_cutoff_snr, tbp, high_frequency_search_start, fix_high_edge
        )
        # These estimates are always computed and posted
        snrdata["low_f_band_edge"] = bwd.low_edge_f
        snrdata["high_f_band_edge"] = bwd.high_edge_f
        snrdata["low_f_band_edge_snr"] = bwd.low_edge_snr
        snrdata["high_f_band_edge_snr"] = bwd.high_edge_snr
        snrdata["spectrum_frequency_range"] = bwd.f_range
        snrdata["bandwidth_fraction"] = bwd.bandwidth_fraction()
        snrdata["bandwidth"] = bwd.bandwidth()
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
        return [snrdata, my_logger]

    # For current implementation all the optional metrics require
    # computed a filtered version of the data.  If a new option is
    # desired that does not require filtering the data the logic
    # here will need to be changed to create a more exclusive test

    if optional_metrics:
        # use the mspass butterworth filter for speed - obspy
        # version requires a conversion to Trace objects
        BWfilt = Butterworth(
            False,
            True,
            True,
            poles,
            bwd.low_edge_f,
            poles,
            bwd.high_edge_f,
            data_object.dt,
        )
        filtered_data = TimeSeries(data_object)
        BWfilt.apply(filtered_data)
        nfilt = WindowData(filtered_data, noise_window.start, noise_window.end)
        sfilt = WindowData(filtered_data, signal_window.start, signal_window.end)
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
                    for k in stats.keys():
                        snrdata[k] = stats[k]
                except MsPASSError as err:
                    newmessage = _reformat_mspass_error(
                        err,
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
    # check these are reasonable - don't remember the formula when writing this
    tbp=4.0,
    ntapers=8,
    high_frequency_search_start=2.0,
    fix_high_edge=True,
    poles=3,
    perc=95.0,
    save_spectra=False,
    phase_name="P",
    arrival_time_key="Ptime",
    metadata_output_key="Parrival",
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
    keyed by the name defined in the metadata_output_key argument.   The idea of
    that sturcture is the contents of the subdocument are readily extracted
    and saved to a MongoDB "arrival" collection with a normalization key.
    Arrival driven workflows can then use queries to reduce the number of
    data actually retrieved for final processing.  i.e. the arrival
    collection data should be viewed as a useful initial quality control
    feature.

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
        tbp=tbp,
        ntapers=ntapers,
        high_frequency_search_start=high_frequency_search_start,
        fix_high_edge=fix_high_edge,
        poles=poles,
        perc=perc,
        optional_metrics=optional_metrics,
        save_spectra=save_spectra,
    )
    if elog.size() > 0:
        data_object.elog += elog
    snrdata["phase"] = phase_name
    data_object[metadata_output_key] = snrdata
    return data_object


def arrival_snr_QC(
    data_object,
    noise_window=TimeWindow(-130.0, -5.0),
    noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0, 120.0),
    signal_spectrum_engine=None,
    band_cutoff_snr=2.0,
    tbp=4.0,
    ntapers=8,
    high_frequency_search_start=2.0,
    fix_high_edge=True,
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
    db=None,
    collection="arrival",
    use_measured_arrival_time=False,
    measured_arrival_time_key="Ptime",
    taup_model=None,
    update_mode=False,
    component=2,
    source_collection="source",
    receiver_collection=None,
):
    """
    Compute a series of metrics that can be used for quality control
    filtering of seismic phase data.

    This is the highest level function in this module for computing
    signal-to-noise ratio metrics for processing signals that can be
    defined by a computable or measurable "phase".  Features this
    function adds over lower level functions in this module are:
        1.  An option to save computed metrics to a MongoDB collection
            (defaults as "arrival").  If the update_mode argument is
            set True (default is False) the function expects the data_object
            to contain the attribute "arrival_id" that references the
            ObjectID of an existing entry in the the collection where the
            data this function computes is to be saved (default is"arrival").
        2.  Adds an option to use a computed or measured arrival as the
            time reference for all windowing.   The lower level snr
            functions in this module require the user do what this
            function does prior to calling the function.  Note one or the other is required
            (i.e. either computed or measured time will be define t0 of the
             processing)

    The input of arg 0 (data_object) can be either a TimeSeries or
    a Seismogram object.  If a Seismogram object is passed the "component"
    argument is used to extract the specified single channel from the Seismogram
    object and than component is used for processing.  That is necessary
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

    The following args are passed directly to the function arrival_snr:
    noise_window, signal_window, band_cutoff_snr, tbp, ntapers, poles,
    perc, phase_name, metadata_output_key, and optional_metrics.  See the docstring
    for arrival_snr and FD_snr_estimator for descriptions of how these
    arguments should be used.  This top level function adds arguments
    decribed below.

    :param db:  mspass Database object that is used as a handle for to MongoDB.
    Default is None, which the function takes to mean you don't want to
    save the computed values to MongoDB.   In this mode the computed
    metrics will all be posted to a python dict that can be found under the
    key defined by the "metadata_output_key" argument.   When db is defined the
    contents of that same python dict will save to MongoDB is the
    collection defined by the "collection" argument.  If db is run as
    the default None the user is responsible for saving and managing the
    computed snr data.   Be aware a simple later call to db.save_data
    will not produce the same normalized data with the (default) arrival
    collection.

    :param collection:  MongoDB collection name where the results of this
    function will be saved.  If the "update_mode" argument is also set
    True the update section will reference this collection. Default is "arrival".

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
    is required.  It defaults as None because there is now way the author
    knows to initialize it to anything valid.  If set it MUST be an instance
    of the obspy class TauPyModel (https://docs.obspy.org/packages/autogen/obspy.taup.tau.TauPyModel.html#obspy.taup.tau.TauPyModel)
    Mistakes in use of this argument can cause a MsPASSError exception to
    be thrown (not logged thrown as a fatal error) in one of two ways:
    (1)  If use_measured_arrival_time is False this argument must be defined,
    and (2) if it is defined it MUST be an instance of TauPyModel.

    :param update_mode:   When True the function will attempt to extract
    a MongoDB ObjectID from data_object's Metadata using the (currently fixed)
    key "arrival_id".   If found it will add the computed data to an existing
    document in the collection defined by the collection argument.  Otherwise
    it will simply add a new entry and post the ObjectID of the new document
    with the (same fixed) key arrival_id.  When False no attempt to fetch
    the arrival id is made and we simply add a record.  This parameter is
    completely ignored unless the db argument defines a valid Database class.

    :param component: integer (0, 1, or 2) defining which component of a
    Seismogram object to use to compute the requested snr metrics.   This
    parameter is ignored if the input is a TimeSeries.

    :param source_collection:  normalization collection for source data.
    The default is the MsPASS name "source" which means the function will
    try to load the source hypocenter coordinates (when required) as
    source_lat, source_lon, source_depth, and source_time.

    :param receiver_collection:  when set this name will override the
    automatic setting of the expected normalization collection naming
    for receiver functions (see above).  The default is None which causes
    the automatic switching to be involked.  If it is any other string
    the automatic naming will be overridden.

    :return:  the data_object modified by insertion of the snr QC data
    in the object's Metadata
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
        data_to_process = ExtractComponent(data_object, component)
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
        tbp=tbp,
        ntapers=ntapers,
        high_frequency_search_start=high_frequency_search_start,
        fix_high_edge=fix_high_edge,
        poles=poles,
        perc=perc,
        optional_metrics=optional_metrics,
        save_spectra=save_spectra,
    )
    if elog.size() > 0:
        data_object.elog += elog
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
    if db:
        arrival_id_key = collection + "_id"
        dbcol = db[collection]
        if update_mode:
            if data_object.is_defined(arrival_id_key):
                arrival_id = data_object[arrival_id_key]
                filt = {"_id": arrival_id}
                update_clause = {"$set": snrdata}
                dbcol.update_one(filt, update_clause)
            else:
                data_object.elog.log_error(
                    "arrival_snr_QC",
                    "Running in update mode but arrival id key="
                    + arrival_id_key
                    + " is not defined\n"
                    + "Inserting computed snr data as a new document in collection="
                    + collection,
                    "Complaint",
                )
                arrival_id = dbcol.insert_one(snrdata).inserted_id
                data_object[arrival_id_key] = arrival_id
        else:
            arrival_id = dbcol.insert_one(snrdata).inserted_id
            data_object[arrival_id_key] = arrival_id
    return data_object
