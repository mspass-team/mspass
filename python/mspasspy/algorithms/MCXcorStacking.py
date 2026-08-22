#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module implementing a similar algorithm to dbxcor in python.

Created on Tue Jul  9 05:37:40 2024

@author: pavlis
"""

from functools import wraps

import numpy as np
from scipy import signal
from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.algorithms.amplitudes import (
    MADAmplitude,
    RMSAmplitude,
    PeakAmplitude,
)
from mspasspy.algorithms.window import WindowData
from obspy.geodetics.base import gps2dist_azimuth, kilometers2degrees
from obspy.taup import TauPyModel


from mspasspy.ccore.utility import ErrorLogger, ErrorSeverity, Metadata, MsPASSError
from mspasspy.ccore.seismic import (
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    TimeReferenceType,
    DoubleVector,
)
from mspasspy.util.seismic import number_live, regularize_sampling, ensemble_time_range
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.signals import filter
from mspasspy.algorithms.window import WindowData, WindowData_autopad


def extract_initial_beam_estimate(
    ensemble,
    metric="bandwidth",
    subdoc_key="Parrival",
) -> TimeSeries:
    """
    The robust stacking method used in the `align_and_stack`
    function in this module requires an initial signal estimate
    for first-order alignment of signals in the input ensemble.
    In the original dbxcor implementation of that algorithm the
    user was required to select that initial signal interactively
    in a graphical user interface.   This function provides one
    possible algorithm to accomplish that task automatically.  It does
    so by scanning the ensemble to extract a member with the largest
    value of some quality control metric.   This implementation is
    limited to metrics computed by the MsPASS function called
    `broadband_snr_QC`.   The algorithm, however, would be easy
    to modify to produce a custom operator using some other
    metric posted to the Metadata container of each ensemble member.
    An oddity of the `broadband_snr_QC` is that it posts its
    output to a python dictionary (subdocument in pymongo jargon)
    fetched with a single key.  That key can be changed from
    the default "Parrival" (default of `broadband_snr_QC`) to
    something else via the "subdoc_key" argument.  This list of
    metric names accepted here map to the following keys produced by
    `broadband_snr_QC`:

    - ``bandwidth`` -> ``bandwidth``
    - ``filtered_envelope`` -> ``snr_filtered_envelope_peak``
    - ``filtered_L2`` -> ``snr_filtered_rms``
    - ``filtered_MAD`` -> ``snr_filtered_mad``
    - ``filtered_Linf`` -> ``snr_filtered_peak``
    - ``filtered_perc`` -> ``snr_filtered_perc``

    :param ensemble:   ensemble to be scanned
    :type ensemble:  `TimeSeriesEnsemble`
    :param metric:   quality metric to use for selecting member to use
      as return.  Always scans for the maximum of specified value as it
      assume the value is some norm measure.   Accepted values at present
      are ``bandwidth``, ``filtered_envelope``, ``filtered_L2``,
      ``filtered_MAD``, ``filtered_Linf``, and ``filtered_perc``.  Default is
      ``bandwidth``.  A `ValueError` is raised for any other value.
    :type metric:  string
    :param subdoc_key:   `broadband_snr_QC` normally posts output to
      a subdocument (dictionary).  This is the key that is used to
      extract that subdocument from each member.   Default is "Parrival"
      which is the default of `broadband_snr_QC`.
    :type subdoc_key:  string
    :return:  `TimeSeries` that is a copy of the ensemble member with
      the largest value of the requested member.   A default constructed, dead datum
      is returned if the algorithm failed.
    """
    if ensemble.dead():
        return TimeSeries()
    metric_keys = {
        "bandwidth": "bandwidth",
        "filtered_envelope": "snr_filtered_envelope_peak",
        "filtered_L2": "snr_filtered_rms",
        "filtered_MAD": "snr_filtered_mad",
        "filtered_Linf": "snr_filtered_peak",
        "filtered_perc": "snr_filtered_perc",
    }
    if metric not in metric_keys:
        message = "extract_initial_beam_estimate:  illegal value for argument metric={}".format(
            metric
        )
        message += "\nMust be one of: " + ", ".join(metric_keys)
        raise ValueError(message)
    key2use = metric_keys[metric]
    N = len(ensemble.member)
    # Use the index to associate each metric value with its ensemble member.
    # Missing metrics must never outrank a defined value, including zero.
    mvals = np.full(N, -np.inf)
    n_set = 0
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        if d.live and d.is_defined(subdoc_key):
            subdoc = d[subdoc_key]
            if key2use in subdoc:
                mvals[i] = subdoc[key2use]
                n_set += 1
    if n_set > 0:
        imax = np.argmax(mvals)
        return TimeSeries(ensemble.member[imax])
    else:
        return TimeSeries()


def estimate_ensemble_bandwidth(
    ensemble,
    snr_doc_key="Parrival",
) -> list:
    """
    Estimate average bandwidth of an ensemble of data using the output of
    broadband_snr_QC.

    The original dbxcor program used a GUI that allowed the user to select one of
    a set of predefined filters to be used to set the frequency band of the signal
    to be processed by the equivalent of the `align_and_stack` function of this module.
    The purpose of this function is to automate that process to defined an
    optimal bandwidth for processing of the input ensemble.

    This function only works on ensembles processed previously with the
    mspass function `broadband_snr_QC`.   That function computes a series of
    metrics for signal-to-noise ratio.  The only one this one uses is
    the two values defined by "low_f_band_edge" and "high_f_band_edge".
    The verbose names should make their definition obvious.   This
    function returns the median of the values of all values of those
    two attributes extracted from all live members of the input ensemble.
    The result is returned as a three-element list containing the low
    frequency edge, high frequency edge, and number of live members used.
    The expectation is that the data will be bandpass filtered between the
    low and high edges before running the `align_and_stack` function.

    :param ensemble:  ensemble of data to be scanned
    :type ensemble:  `TimeSeriesEnsemble`
    :param snr_doc_key:  subdocument key of attributes computed by
      `broadband_snr_QC` to fetch as estimates of low and high frequency
      edges.
    :type snr_doc_key:  string (default "Parrival" = default of
      `broadband_snr_QC`)
    """
    if ensemble.dead():
        return [0.0, 0.0, 0]
    f_l = []
    f_h = []
    for d in ensemble.member:
        if d.live:
            if d.is_defined(snr_doc_key):
                doc = d[snr_doc_key]
                f = doc["low_f_band_edge"]
                f_l.append(f)
                f = doc["high_f_band_edge"]
                f_h.append(f)
    if len(f_h) > 0:
        f_low = np.median(f_l)
        f_high = np.median(f_h)
        return [f_low, f_high, len(f_h)]
    else:
        return [0.0, 0.0, 0]


def _compute_default_robust_window(
    f_low,
    ncycles=3,
    starttime=2.0,
    min_endtime_multiplier=3,
) -> TimeWindow:
    """
    Small function to encapsulate the algorithm used to define the
    default robust window set by MCXcorPrepP.  Returns a time
    window with the starttime frozen as the function default
    argument.   endtime is the thing computed.  It is computed
    as ncycle times the period defined by the low frequency
    band edge f_low passed as arg0.   There is a safety
    called min_endtime_multiplier that would only be used if
    f_low is larger than 1 Hz - which for teleseismic P wave
    data means f_low is not reasonable.
    """
    T = 1.0 / f_low
    endtime = ncycles * T
    if endtime < min_endtime_multiplier:
        endtime = starttime * min_endtime_multiplier
    return TimeWindow(starttime, endtime)


@mspass_func_wrapper
def MCXcorPrepP(
    ensemble,
    noise_window,
    *args,
    noise_metric="mad",
    initial_beam_metric="bandwidth",
    snr_doc_key="Parrival",
    low_f_corner=None,
    high_f_corner=None,
    npoles=4,
    filter_parameter_keys=["MCXcor_f_low", "MCXcor_f_high", "MCXcor_npoles"],
    coda_level_factor=4.0,
    set_phases=True,
    model=None,
    Ptime_key="Ptime",
    pPtime_key="pPtime",
    PPtime_key="PPtime",
    station_collection="channel",
    search_window_fraction=0.9,
    minimum_coda_duration=5.0,
    correlation_window_start=-3.0,
    handles_ensembles=True,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
) -> list:
    """
    Function used to preprocess an ensemble to prepare input for
    running multichannel cross-correlation and stacking method
    (automated dbxcor algorithm) of P phase data.  This function should
    not be used for anything but teleseismic P data.

    The multichannel correlation and stacking function in this module called
    `align_and_stack` requires one to define a number of parameters that
    in the original dbxcor implementation were input through a graphical
    user interface.   This function can be thought of as a robot that will
    attempt to set all the required parameters automatically using signal processing.
    It will attempt to produce two inputs required by the `align_and_stack`:
    1.  What I call a "correlation window".
    2.  The ensemble member that is to be used as the initial estimate of the beam (stack of the data aligned by cross correlation).

    A CRITICAL assumption of this function is that the input ensemble's data
    have been processed with the snr module function `broadband_snr_QC`.
    That function computes and posts multiple snr metrics to a subdocument
    (dictionary) accessed with a single key.   This function requires that
    data exist and be accessible with the key defined by the argument
    "snr_doc_key".   The data in that dictionary are used in the following
    algorithms run within this function:

    1.  The working bandwidth of the data is established by computing the median
        of the attributes "low_f_band_edge" and "high_f_band_edge" extracted from
        each live member.  Those frequencies are computed using the function in this module
        called `estimate_ensemble_bandwidth`.  The working ensemble
        (which is what is returned on success) will be filtered in the band defined
        by the output of `estimate_ensemble_bandwidth` using a Butterworth,
        bandpass filter with the number of poles defined by the "npoles" argument.
    2.  What I call the correlation window is estimated by a complex recipe
        best understood from the user manual page and example jupyter notebooks
        related to this module.   Briefly, the correlation window is defined by
        applying an envelope function to each signal and defining the coda
        end using a common recipe for coda magnitudes where the end of the coda is
        defined by where the coda level (envelope) first falls below a
        level specified as a multiple of measured background noise.
        This function REQUIRES the input for each member to have a section
        that it can treat as background noise.  For most uses that means some
        section of data immediately before the P wave arrival time.  The
        noise level is estimated by the metric defined by the "noise_metric"
        argument and the coda level cutoff is computed as
        `noise_level*coda_level_factor` where `coda_level_factor` is the
        valued passed via the function argument with that key.
        Because this function is designed strictly for P phases it has to
        handle the complexity of interference by secondary P phases.   For
        that reason it computes arrival times for pP and PP and will always
        start the coda search before time of the smaller of pP or PP.
        Note, however, that pP is not used a constraint if the source depth is
        less than 100 km.   The justification for that number can be found in
        jupyter notebooks that should be part of the MsPASS documentation.
    3.  It then extracts one member from the ensemble the algorithm judges to
        be the best choice for an initial beam estimate.   That is, in
        align_and_stack the first step  is to align the data with a reference
        signal using cross-correlation of the "beam" with each member.  The
        aligned data are then summed with a robust stack to refine the beam.
        That is repeated until the stack does not change significantly.
        That algorithm requires a seed for the initial beam estimate.   That
        initial signal is extracted as the ensemble member with the largest
        value of the snr metric defined with the `initial_beam_metric` argument.
        The correlation window parameters computed earlier are then posted
        to the `Metadata` of that `TimeSeries` where `align_and_stack`
        can parse them to set the correlation time window.

    The function filters and prepares the input ensemble in place.  Component 0
    of the returned list is that same ensemble, and component 1 is the initial
    beam estimate that should be passed to `align_and_stack`.
    Callers should verify the beam signal is marked live.  A dead
    beam signal indicates the algorithm failed in one way or another.
    Errors at that level will result in messages being posted to the
    `ErrorLogger` container of the beam output (elog attribute).  Note also this
    algorithm can kill ensemble members that were marked live on input.

    :param ensemble:   input ensemble of data to use.  As noted above it
      must have been processed with `broadband_snr_QC` or this function will
      return a null result.  The function makes a tacit assumption that all
      the members of this ensemble are in relative time with 0 of each member
      being an estimate of the P wave arrival time.  It does no tests to
      validate this assumption, so if the assumption is wrong you will, at best,
      get junk as output.  This ensemble is filtered and prepared in place;
      members can be modified or killed.
    :type ensemble: `TimeSeriesEnsemble`  Note for most uses this is the
      output of ExtractComponent of a `SeismogramEnsemble` processed with
      `broadband_snr_QC` that is the longitudinal component for the P phase.
    :param noise_window:  time window defining the section of each
      ensemble member that is to be treated as "noise" for estimating
      a relative noise level.
    :type noise_window:  `TimeWindow` tacitly expected to be time relative
      to a measure of the P wave arrival time.
    :param noise_metric:  vector norm metric to use for measuring the
      amplitude of the noise extracted from the data in the range defined by
      noise_window.
    :type noise_metric: string.  ``"rms"`` and ``"peak"`` select those
      metrics; every other value, including the default ``"mad"``, uses median
      absolute deviation.
    :param initial_beam_metric:  key of attribute to extract from the
      output of `broadband_snr_QC` used to select initial beam signal.
      This argument is passed to `extract_initial_beam_estimate` as its
      ``metric`` argument.
    :type initial_beam_metric:  string (default "bandwidth").  For a list of
      allowed values see the docstring of `extract_initial_beam_estimate`.
    :param snr_doc_key:  key to use to fetch the subdocument containing the
      output of `broadband_snr_QC`.
    :type snr_doc_key:  string (default "Parrival" which is the default of
      `broadband_snr_QC`).
    :param low_f_corner: force the low frequency corner for the output data
      bandwidth to this value.   When defined, the internal call to
      `estimate_ensemble_bandwidth` is bypassed and the output data are filtered
      between the value defined by low_f_corner and high_f_corner.
    :type low_f_corner:  float (default None which is interpreted to mean
      scan ensemble to estimate the frequency range)
    :param high_f_corner: force the high frequency corner for the output data
      bandwidth to this value.   When defined, the internal call to
      `estimate_ensemble_bandwidth` is bypassed and the output data are filtered
      between the value defined by low_f_corner and high_f_corner.
    :type high_f_corner:  float (default None which is interpreted to mean
      scan ensemble to estimate the frequency range)
    :param npoles:  number of poles to use for the Butterworth bandpass filter.
    :type npoles:  integer (default 4)
    :param filter_parameter_keys: three Metadata keys used to record the low
      corner, high corner, and number of poles of the filter applied in place.
    :type filter_parameter_keys: list of three strings (default
      ``["MCXcor_f_low", "MCXcor_f_high", "MCXcor_npoles"]``)
    :param coda_level_factor: multiplier applied to each member's measured
      background-noise amplitude to define the coda-detection threshold.
    :type coda_level_factor: float (default 4.0)
    :param set_phases: boolean that if set True (default) the arrival times for
      P, pP (if defined), and PP (if defined) are computed and posted to the
      output of each ensemble member.  If False the algorithm assumes the
      same quantities were previously calculated and can be fetched from the
      member TimeSeries Metadata container using keys defined by Ptime_key,
      pPtime_key, and PPtime_key.
    :param Ptime_key:
    :param pPtime_key:
    :param PPtime_key:  These three arguments define alternative keys that
      will be used to fetch (if set_phases is false) or post (if set_phases is True)
      computed P, pP, and PP times to each member's Metadata container.
      Changing any of these values is not really advised when set_phases is True.
      These are most useful if the phase arrival times were previously
      computed or measured and posted with different keys.
    :param model: travel-time model used to compute P, pP, and PP arrivals when
      ``set_phases`` is True.  A default ``TauPyModel("iasp91")`` is created
      when this argument is None.
    :type model: ``obspy.taup.TauPyModel`` or None
    :param station_collection: MongoDB collection name used to fetch
      receiver coordinate data.   Normal practice in MsPASS is to save
      receiver Metadata in two channels called "channel" and "site" and
      to load the coordinate data through normalization when the data are
      loaded.  The MsPASS convention defines data loaded by normalization
      with a leading collection name.  e.g. the "lat" value extracted from a
      "channel" document would be posted to the data as "channel_lat".
      The same value, however, loaded from "site" would be tagged "site_lat".
      The default for this argument is "channel" which means the algorithm
      will require the attributes "channel_lat" and "channel_lon" as the
      receiver coordinates (in degrees).  The standard alternative is to
      define `station_collection="site"`, in which case the function will
      use "site_lat" and "site_lon".
    :type station_collection:  string (default "channel")
    :param search_window_fraction:   The window for defining the
      correlation window is determined by running the internal
      `_coda_duration` function on each ensemble member and computing the
      range from the median of the ranges computed from all the ensemble
      members.  Each coda search, however, is constrained by the times of
      pP and/or PP.   As noted above the function uses the pP time for
      events with depths greater than 100 km but PP for shallow sources.
      To allow for hypocenter errors  the duration defined by
      P to pP or P to PP is multiplied by this factor to define the
      search start for the coda estimation.
    :type search_window_fraction: float (default 0.9)
    :param minimum_coda_duration: minimum accepted coda-duration estimate.
      Shorter estimates are excluded.  If no estimate exceeds this floor, the
      ensemble is killed and returned with a dead beam.
    :type minimum_coda_duration:  float (default 5.0 seconds)
    :param correlation_window_start:  the time of the correlation window
      set in the output "beam" is fixed as this value.   It is normally
      a negative number defining a time before P that no signal is likely to
      have an arrival before this relative time.
    :type correlation_window_start:  float (default -3.0)
    :return: two-element list.  Component 0 is the input ensemble
      after in-place filtering, phase-time conversion, Metadata updates, and
      any member kills.  Component 1 is an initial beam estimate suitable for
      the dbxcor robust-stacking algorithm.
    """
    alg = "MCXcorPrepP"
    if not isinstance(ensemble, TimeSeriesEnsemble):
        message = alg + ":  Illegal type={} for arg0\n".format(type(ensemble))
        message += "Must be a TimeSeriesEnsemble object"
        raise TypeError(message)
    if ensemble.dead():
        return [ensemble, TimeSeries()]

    if model is None:
        model = TauPyModel(model="iasp91")

    if not isinstance(noise_window, TimeWindow):
        message = alg + ":  Illegal type={} for arg1\n".format(type(noise_window))
        message += "Must be a TimeWindow object"
        raise TypeError(message)

    # first sort out the issue of the passband to use for this
    # analysis.   Use kwargs if they are defined but otherwise assume
    # we extract what we need from the output of `broadband_snr_QC`.
    # effectively a declaration to keep these from going away when they
    # go out of scope
    f_low = 0.0
    f_high = 0.0
    if low_f_corner or high_f_corner:
        if low_f_corner and high_f_corner:
            f_low = low_f_corner
            f_high = high_f_corner
            if f_high <= f_low:
                message = (
                    alg
                    + ":  Inconsistent input.  low_f_corner={} and high_f_corner={}\n".format(
                        low_f_corner, high_f_corner
                    )
                )
                message += "low_f_corner value is greater than high_f_corner value"
                raise ValueError(message)
        else:
            message = (
                alg
                + ":  Inconsistent input.  low_f_corner={} and high_f_corner={}\n".format(
                    low_f_corner, high_f_corner
                )
            )
            message += "If you specify one you must specify the other"
            raise ValueError(message)

    else:
        [f_low, f_high, nused] = estimate_ensemble_bandwidth(
            ensemble,
            snr_doc_key=snr_doc_key,
        )
        if (f_low == 0) or (f_high == 0):
            message = "estimate_ensemble_bandwidth failed\n"
            message += "Either all members are dead or data were not previously processed with broadband_snr_QC"
            ensemble.elog.log_error(alg, message, ErrorSeverity.Invalid)
            ensemble.kill()
            return [ensemble, TimeSeries()]

    enswork = filter(
        ensemble,
        type="bandpass",
        freqmin=f_low,
        freqmax=f_high,
        corners=npoles,
    )
    # using a list like this creates this odd construct but docstring (should at least) advise againtst
    # changing these
    enswork[filter_parameter_keys[0]] = f_low
    enswork[filter_parameter_keys[1]] = f_high
    enswork[filter_parameter_keys[2]] = npoles
    if set_phases:
        for i in range(len(enswork.member)):
            # this handles dead data so don't test for life
            enswork.member[i] = _set_phases(
                enswork.member[i],
                model,
                Ptime_key=Ptime_key,
                pPtime_key=pPtime_key,
                PPtime_key=PPtime_key,
                station_collection=station_collection,
            )
        if number_live(enswork) == 0:
            message = "_set_phases killed all members of this ensemble\n"
            message += "station_collection is probably incorrect or the data have not been normalized for source and receiver coordinates"
            enswork.elog.log_error(alg, message, ErrorSeverity.Invalid)
            enswork.kill()
            return [enswork, TimeSeries()]
    # if set_phases is False we assume P, pP, and/or PP times were previously
    # set.   First make sure all data are in relative time with 0 as P time.
    # kill any datum for which P is not defined
    for i in range(len(enswork.member)):
        d = enswork.member[i]
        if d.live:
            if d.is_defined(Ptime_key):
                Ptime = d[Ptime_key]
                if d.time_is_relative():
                    d.rtoa()
                d.ator(Ptime)
            else:
                message = (
                    "Required key={} defining P arrival time is not defined\n".format(
                        Ptime_key
                    )
                )
                message += "Cannot process this datum without a P wave time value"
                d.elog.log_error(alg, message, ErrorSeverity.Invalid)
                d.kill()
            enswork.member[i] = d

    # now get coda durations and set the correlation window as median of
    # the coda durations
    coda_duration = []  # coda estimates are placed here
    search_range = []  # use this to set ranges - duration limited to min of these
    for d in enswork.member:
        if d.live:
            sr = _get_search_range(d)
            sr *= search_window_fraction
            search_range.append(sr)
            # compute a noise estimate without being too dogmatic about
            # window range
            if d.t0 < noise_window.start:
                nw = TimeWindow(d.t0, noise_window.end)
            else:
                nw = TimeWindow(noise_window)
            nd = WindowData(d, nw.start, nw.end, short_segment_handling="truncate")
            # silently skip any datum for which the WindowData algorithm fails
            # can happen if the noise window does not overlap with data
            if nd.dead():
                continue
            if noise_metric == "rms":
                namp = RMSAmplitude(nd)
            elif noise_metric == "peak":
                namp = PeakAmplitude(nd)
            else:
                # silently default to MAD - maybe should log an error to ensemble or throw an exception
                namp = MADAmplitude(nd)
            coda_window = _coda_duration(d, namp * coda_level_factor, search_start=sr)
            # silently drop any retun value less than the floor defined
            # by minimum_coda_duration
            duration = coda_window.end - coda_window.start
            if duration > minimum_coda_duration:
                coda_duration.append(duration)
    if len(coda_duration) == 0:
        message = "Calculation of correlation window from the envelop of filtered ensemble members failed\n"
        message += "No data detected with signal level in the passband above {} times the measured noise level\n".format(
            coda_level_factor
        )
        message += "noise_window range or coda_level_factor are likely inconsistent with the data\n"
        message += "Killing this ensemble"
        enswork.elog.log_error(alg, message, ErrorSeverity.Invalid)
        enswork.kill()
        return [enswork, TimeSeries()]
    correlation_window_endtime = np.median(coda_duration)  # relative time
    min_range = np.min(search_range)
    min_range *= search_window_fraction
    if correlation_window_endtime > min_range:
        correlation_window_endtime = min_range

    beam0 = extract_initial_beam_estimate(
        enswork,
        metric=initial_beam_metric,
        subdoc_key=snr_doc_key,
    )
    beam0["correlation_window_start"] = correlation_window_start
    beam0["correlation_window_end"] = correlation_window_endtime
    # finally use the low frequency band limit to set a default range
    # for the robust window
    rw = _compute_default_robust_window(f_low)
    # fudge this one sample to avoid a roundoff error on starttime computations
    beam0["robust_window_start"] = correlation_window_start + beam0.dt
    # make sure the end isn't larger than the correlation window end
    if rw.end < correlation_window_endtime:
        beam0["robust_window_end"] = rw.end
    else:
        # similar fudge by one sample for endtime
        beam0["robust_window_end"] = correlation_window_endtime - beam0.dt
    return [enswork, beam0]


def dbxcor_weights(ensemble, stack, residual_norm_floor=0.1):
    """
    Computes the robust weights used originally in dbxcor for each
    member of ensemble.   Result is returned in a parallel numpy
    array (i.e. return[i] is computed weight for ensemble.member[i])

    This function is made for speed and has no safeties.  It assumes
    all the members of ensemble are the same and it is the same length
    as stack.  It will throw an exception if that is violated so callers
    should guarantee that does not happen.

    This function adds a feature not found in the original Pavlis and Vernon(2011)
    paper via the `residual_norm_floor` argument.   Experience with this algorithm
    showed it tends to converge to a state with very high weights on one or two
    signals and low weights on the rest.   The high weights are given to the
    one or two signals most closely matching the stack at convergence.  This
    has the undesirable effect of causing a strong initial value dependence
    and suboptimal noise reduction.  This argument handles that by not allowing
    the L2 norm of the residual to fall below a floor computed from the
    ratio ||r||/||d||.   i.e. if ||r|| < residual_norm_floor*||d|| it is
    set to the value passed as `residual_norm_floor`.  Note that setting
    this to 1.0 effectively turns off the residual norm term in the
    weight equation which I now recognize is the cross-correlation of the
    beam and datum at zero lag.  That is true, however, only if the
    ensemble members are all time aligned perfectly.

    Returns a numpy vector of weights.  Any dead data will have a weight of
    -1.0 (test for negative is sufficient).  In addition the function has a
    safety to handle receiving a vector of all zeros.  If the function detects
    an all-zero stack, every live member receives a weight of 0.  If an
    individual live datum is all zeros, that datum likewise receives a weight
    of 0.

    :param ensemble:  `TimeSeriesEnsemble` of data from which weights are to
      be computed.
    :type ensemble:  Assumed to be a `TimeSeriesEnsemble`.  No type checking
      is done so if input is wrong an exception will occur but what is thown
      will depend on what ensemble actually is.
    :param stack:  TimeSeries containing stack to be used to compute weights.
      The method returns robust weights relative to the vector of data in
      this object.
    :type stack:  `TimeSeries`.
    :param residual_norm_floor:  nondimensional floor in the ratio norm2(r)/norm2(d)
      used as described above.  Default is 0.1 which is reasonable for high snr
      signals.   Data with irregular quality can profit from smaller values of this parameter.
    :type residual_norm_floor:  float
    :return: numpy vector of weights parallel with ensemble.member.  Dead
      members will have a negative weight in this vector.
    """
    norm_floor = np.finfo(np.float32).eps * stack.npts
    N = len(ensemble.member)
    wts = np.zeros(N)
    r = np.zeros(stack.npts)
    # Scale the scack vector to be a unit vector
    s_unit = np.array(stack.data)
    nrm_s = np.linalg.norm(stack.data)
    if nrm_s == 0.0:
        for i in range(N):
            if ensemble.member[i].dead():
                wts[i] = -1.0
        return wts
    s_unit /= nrm_s
    N_s = len(s_unit)
    for i in range(N):
        if ensemble.member[i].dead():
            wts[i] = -1.0
        else:
            N_d = ensemble.member[i].npts
            if N_d == N_s:
                d_dot_stack = np.dot(ensemble.member[i].data, s_unit)
                r = ensemble.member[i].data - d_dot_stack * s_unit
            else:
                # handle off by one errors silently
                # with usage here should that is possible but larger differences are not likely
                N2use = min(N_d, N_s)
                d_dot_stack = np.dot(ensemble.member[i].data[0:N2use], s_unit[0:N2use])
                r = ensemble.member[i].data[0:N2use] - d_dot_stack * s_unit[0:N2use]

            nrm_r = np.linalg.norm(r)
            nrm_d = np.linalg.norm(ensemble.member[i].data)
            if nrm_d < norm_floor:
                # this is a test for all zeros
                # Give zero weight in this situation
                wts[i] = 0.0
            elif nrm_r / nrm_d < residual_norm_floor:
                denom = residual_norm_floor * nrm_d * nrm_d
                wts[i] = abs(d_dot_stack) / denom
            # this attempts to duplicate dbxcor
            # if nrm_r < norm_floor or nrm_d < norm_floor or abs(d_dot_stack) < norm_floor:
            #    wts[i] = residual_norm_floor
            else:
                denom = nrm_r * nrm_d
                # dbxcor has logic to avoid an nan from a machine 0
                # denom.  Not needed her because of conditional chain here
                wts[i] = abs(d_dot_stack) / denom

    # Rescale live-member weights so the largest is 1.  Leave the negative
    # sentinel for dead members unchanged, and avoid dividing an all-zero
    # live subset by zero.
    live_weight_indices = wts >= 0.0
    if np.any(live_weight_indices):
        maxwt = np.max(wts[live_weight_indices])
        if maxwt > 0.0:
            wts[live_weight_indices] /= maxwt
    return wts


def regularize_ensemble(
    ensemble, starttime, endtime, pad_fraction_cutoff
) -> TimeSeriesEnsemble:
    """
    Secondary function to regularize an ensemble for input to robust
    stacking algorithm.  ASsumes all data have the same sample rate.
    Uses WindowData to assure all data are inside the common
    range starttime:endtime.  Dead data are dropped with one summary
    posted to the output ensemble.  Errors created while windowing a
    dropped live member are copied back to that input member, but this
    helper does not change the member's pre-call live/dead state.  Dropping
    some members is a nonfatal Complaint; the summary is Invalid only when
    no member survives and the returned ensemble is dead.
    """
    ensout = TimeSeriesEnsemble(Metadata(ensemble), len(ensemble.member))
    if ensemble.elog.size() > 0:
        ensout.elog = ensemble.elog
    dropped_indices = []
    for i in range(len(ensemble.member)):
        member = ensemble.member[i]
        if member.live:
            if (
                np.fabs((member.t0 - starttime)) > member.dt
                or np.fabs(member.endtime() - endtime) > member.dt
            ):
                d = WindowData_autopad(
                    member,
                    starttime,
                    endtime,
                    pad_fraction_cutoff=pad_fraction_cutoff,
                )
                if d.live:
                    ensout.member.append(d)
                else:
                    if d is not member:
                        member_error_count = member.elog.size()
                        for error in d.elog.get_error_log()[member_error_count:]:
                            member.elog.log_error(
                                error.algorithm, error.message, error.badness
                            )
                    dropped_indices.append(i)
            else:
                ensout.member.append(member)
        else:
            dropped_indices.append(i)
    if dropped_indices:
        message = "regularize_ensemble: dropped {} member(s) at indices {}".format(
            len(dropped_indices), ", ".join(str(i) for i in dropped_indices)
        )
        if ensout.member:
            ensout.elog.log_error(
                "regularize_ensemble", message, ErrorSeverity.Complaint
            )
        else:
            ensout.elog.log_error(MsPASSError(message, ErrorSeverity.Invalid))
    if len(ensout.member) > 0:
        ensout.set_live()
    else:
        ensout.kill()
    return ensout


def _logged_dead_timeseries(message) -> TimeSeries:
    """Return an empty dead TimeSeries with one Invalid diagnostic."""
    result = TimeSeries()
    result.elog.log_error(MsPASSError(message, ErrorSeverity.Invalid))
    result.kill()
    return result


def robust_stack(
    ensemble,
    method="dbxcor",
    stack0=None,
    stack_md=None,
    timespan_method="ensemble_inner",
    pad_fraction_cutoff=0.05,
    residual_norm_floor=0.01,
) -> list:
    """
    Generic function for robust stacking live members of a `TimeSeriesEnsemble`.
    An optional initial stack estimate can be used via the stack0 argument.
    The function currently supports two methods:  "median" for a median
    stack and "dbxcor" to implement the robust loss function
    used in the dbxcor program defined in Pavlis and Vernon (2010).
    Other algorithms could easily be implemented via this same api
    by adding an option for the "method" argument.

    All robust estimators I am aware of that use some form of penalty
    function (e.g. m-estimators or the dbxcor penalty function) require
    an initial estimator for the stack.  They do that because the
    penalty function is defined from a metric of residuals relative to
    the current estimate of center.   The median, however, does not
    require an initial estimator.  For the dbxcor method, ``stack0`` can
    supply that estimate; when it is omitted, this function computes a median
    stack for the initial estimate.  The median method always computes the
    sample median and ignores ``stack0`` values.

    The other complication of this function is handling of potential
    irregular time ranges of the ensemble input and how to set the
    time range for the output.   The problem is further complicated by
    use in an algorithm like `align_and_stack` in this module where
    the data can get shifted to have undefined data within the
    time range the data aims to utilize.  The ``timespan_method`` argument
    defines how the time span
    for the stack should be defined.   The following options
    are supported:

    "stack0" - sets the time span to that of the input
    `TimeSeries` passed as stack0.  i.e. the range is set to
    stack0.t0 to stack0.endtime().

    "ensemble_inner" - (default) use the range defined by the "inner" method
    for computing the range with the function `ensemble_time_range`.
    (see `ensemble_time_range` docstring for the definition).

    "ensemble_outer" - use the range defined by the "outer" method
    for computing the range with the function `ensemble_time_range`.
    (see `ensemble_time_range` docstring for the definition).

    "ensemble_median" -  use the range defined by the "median" method
    for computing the range with the function `ensemble_time_range`.
    (see `ensemble_time_range` docstring for the definition).

    When the selected range extends beyond a live ensemble member, that
    member is zero padded; the current implementation does not reject members
    based on the padded fraction.  ``pad_fraction_cutoff`` applies only when a
    dbxcor ``stack0`` must be padded to the selected range.

    :param ensemble:   input data to be stacked.   Should all be in
        relative time with all members having the same relative time span.
    :type ensemble:  TimeSeriesEnsemble
    :param method: Defines a name string of the method to be used to
        compute the stack.
    :type method:  string.  Currently must be one of two values or the
        function will abort:   "median" or "dbxcor".  As the names imply
        "median" will cause the function to return the median of the sample
        vectors while "dbxcor" applies the dbxcor method.
    :param stack0:  optional initial estimate for stack.  Estimators
        other than median I know of use a loss function for downweighting
        members of the stack that do not match the stack as defined by
        some misfit metric.   This argument can be used to input an optional
        starting estimate of the stack for the dbxcor method.  By default it
        uses the median as the starting point, but this can be used to
        input something else.   Note the function will silently ignore this
        argument if method == "median".
    :type stack0: TimeSeries or None.  For dbxcor, the seed is windowed or
        padded to the selected time span and is returned dead when the missing
        fraction exceeds ``pad_fraction_cutoff``.  A value is also required
        when ``timespan_method="stack0"`` even if ``method="median"``, because
        it defines the output range.  The default None uses a median initial
        estimate for dbxcor.
    :param stack_md:   optional Metadata container to define the
        content of the stack output.  By default the output will have only
        Metadata that duplicate required internal attributes (e.g. t0 and npts).
        An exception is when dbxcor uses ``stack0``: that seed's Metadata is
        copied and this argument is ignored.
    :type stack_md: Metadata container or None.  When dbxcor uses ``stack0``
        this argument is ignored.  Otherwise it should be used to add
        whatever Metadata is required to provide a tag that can be used to
        identify the output.  If not specified the stack Metadata
        will be only those produce from default construction of a
        TimeSeries.  That is almost never what you want.   Reiterate,
        however, that if dbxcor uses stack0 the output stack will be a
        clone of stack0 with possible modifications of time and data
        range attributes and anything the stack algorithm posts.
    :param timespan_method: method used to select the output time range.
        Accepted values are ``"ensemble_inner"`` (default),
        ``"ensemble_outer"``, ``"ensemble_median"``, and ``"stack0"`` as
        described above.
    :type timespan_method: string
    :param pad_fraction_cutoff: maximum fraction of missing samples that may
        be zero padded when a dbxcor ``stack0`` is adjusted to the selected
        time range.  This value does not control padding of ensemble members.
    :type pad_fraction_cutoff: float (default 0.05)
    :param residual_norm_floor: floor on residuals used to compute dbxcor weight
        function.  See docstring for `dbxcor_weights` for details.  Ignored
        unless method is "dbxcor"
    :type residual_norm_floor:   float (default 0.01)
    :return: two-element list containing the requested stack as component 0.  The
        stack is returned as a `TimeSeries`  with optional Metadata copied
        from the (optional) stack_md argument.   Component 1 is defined only
        for the dbxcor method in which case it is a numpy array containing the
        robust weights returned by the dbxcor algorithm.  If the method is
        set to "median" component 1 will be returned as a None type.  With no
        live ensemble members, component 0 is a logged dead `TimeSeries`,
        component 1 is `None`, and the input ensemble is marked dead and logged.
    """
    alg = "robust_stack"
    # if other values for method are added they need to be added here
    if method not in ["median", "dbxcor"]:
        message = alg + ":  Illegal value for argument method={}\n".format(method)
        message += "Currently must be either median or dbxcor"
        raise ValueError(message)
    # don't test type - if we get illegal type let it throw an exception
    M = number_live(ensemble)
    if M == 0:
        message = "robust_stack: input ensemble contains no live members"
        ensemble.kill()
        ensemble.elog.log_error(MsPASSError(message, ErrorSeverity.Invalid))
        return [_logged_dead_timeseries(message), None]
    live_member = next(d for d in ensemble.member if d.live)
    if timespan_method == "stack0":
        if stack0:
            # intentionally don't test type of stack0
            # if not a TimeSeries this will throw an exception
            timespan = TimeWindow(stack0.t0, stack0.endtime())
        else:
            message = alg + ":  usage error\n"
            message += (
                "timespan_method was set to stack0 but the stack0 argument is None\n"
            )
            message += "stack0 must be a TimeSeries to use this option"
            raise ValueError(message)
    elif timespan_method == "ensemble_inner":
        timespan = ensemble_time_range(ensemble, metric="inner")
    elif timespan_method == "ensemble_outer":
        timespan = ensemble_time_range(ensemble, metric="outer")
    elif timespan_method == "ensemble_median":
        timespan = ensemble_time_range(ensemble, metric="median")
    else:
        message = alg + ":  illegal value for argument timespan_method={}".format(
            timespan_method
        )
        raise ValueError(message)

    # A live member supplies the common sampling attributes.  It need not be
    # member 0 because dead members are allowed in the input.
    # TODO:   removing this for a test - I don't think this is needed with a
    # change in the algorithm.  If that proves true remove this function
    # from this module and remove this comment and the call to regularize_ensemble
    # ensemble = regularize_ensemble(
    #    ensemble, timespan.start, timespan.end, pad_fraction_cutoff
    # )
    # the above can remove some members
    M_e = len(ensemble.member)
    # can now assume they are all the same length and don't need to worry about empty ensembles
    # N = ensemble.member[0].npts
    dt = live_member.dt
    N = int((timespan.end - timespan.start) / dt) + 1

    if stack0 and method == "dbxcor":
        stack = WindowData_autopad(
            stack0,
            timespan.start,
            timespan.end,
            pad_fraction_cutoff=pad_fraction_cutoff,
        )
        if stack.dead():
            message = "Received an initial stack estimate with time range inconsistent with data\n"
            message += "Recovery not implemented - stack returned is invalid"

            stack.elog.log_error("robust_stack", message, ErrorSeverity.Invalid)
            return [stack, None]
    else:
        # bit of a weird logic here - needed because we need option for
        # dbxcor method to use median stack as starting point or use
        # the input via stack0.  This does that in what is admittedly a confusing way
        #
        # Also this is a bit of a weird trick using inheritance to construct a
        # TimeSeries object for stack using an uncommon constructor.
        # The actual constructor wants a BasicTimeSeries and Metadata as
        # arg0 and arg1.   A TimeSeries is a sublass of BasicTimeSeries so this
        # resolves.  Note the conditional is needed as None default for
        # stack_md would abort
        stack = TimeSeries(N)
        if stack_md:
            stack = TimeSeries(stack, stack_md)
        stack.t0 = timespan.start
        # this works because we can assume ensemble is not empty and clean
        stack.dt = live_member.dt
        # Make sure the stack has the same time base as the input
        stack.tref = live_member.tref
        # Always compute the median stack as a starting point
        # that was the algorithm of dbxcor and there are good reasons for it
        data_matrix = np.zeros(shape=[M, N])
        ii = 0
        for i in range(M_e):
            if ensemble.member[i].live:
                d = WindowData(
                    ensemble.member[i],
                    timespan.start,
                    timespan.end,
                    short_segment_handling="pad",
                )
                # this makes this bombproof.  Subject otherwise to
                # subsample t0 rounding ambiguity
                N2use = min(N, d.npts)
                data_matrix[ii, 0:N2use] = np.array(d.data[0:N2use])
                ii += 1
        stack_vector = np.median(data_matrix, axis=0)
        stack.data = DoubleVector(stack_vector)
        stack.set_live()
    if method == "median":
        return [stack, None]
    else:
        # since method can only be median or dbxcor at this point this
        # block is exectuted only when method=="dbxcor"
        # this works because _dbxcor_stacker returns a two-element list
        return _dbxcor_stacker(
            ensemble,
            stack,
            residual_norm_floor=residual_norm_floor,
        )


def _relative_stack_change(new_stack, previous_stack) -> float:
    """Return the relative L2 change between two stack estimates."""
    delta = new_stack - previous_stack
    delta_norm = np.linalg.norm(delta.data)
    previous_norm = np.linalg.norm(previous_stack.data)
    if previous_norm == 0.0:
        return 0.0 if delta_norm == 0.0 else np.inf
    return delta_norm / previous_norm


def _dbxcor_stacker(
    ensemble,
    stack0,
    eps=0.001,
    maxiterations=20,
    residual_norm_floor=0.1,
) -> list:
    """
    Runs the dbxcor robust stacking algorithm on `enemble` with initial
    stack estimate stack0.
    Returns a two-element list with the stack as component 0 and a numpy vector
    of the final robust weights as component 1.

    This function is intended to be used only internally in this module
    as it has no safeguards and assumes ensemble and stack0 are what it expects.

    :param ensemble:  TimeSeriesEnsemble assumed to have constant data
      range and sample interval and not contain any dead data.
    :param stack0:  TimeSeries of initial stack estimate - assumed to have
      same data vector length as all ensemble members.
    :param eps:  relative norm convergence criteria.  Stop iteration when
      norm(delta stack data)/norm(stack.data)<eps.
    :param maxiterations:  maximum number of iterations (default 20)
    :param residual_norm_floor: floor on residuals used to compute dbxcor weight
      function.  See docstring for `dbxcor_weights` for details.
    :type residual_norm_floor:   float (default 0.1)
    """
    stack = TimeSeries(stack0)
    previous_stack = TimeSeries(stack0)
    # useful shorthands
    N = stack0.npts
    M = len(ensemble.member)
    wts = None  # needed to keep this symbol from going out of scope before return
    for i in range(maxiterations):
        wts = dbxcor_weights(
            ensemble, previous_stack, residual_norm_floor=residual_norm_floor
        )
        # newstack = np.zeros(N)
        # this is just a fast way to initalize to 0s
        stack.set_npts(N)
        sumwts = 0.0
        for j in range(M):
            if ensemble.member[j].live and wts[j] > 0.0:
                d = TimeSeries(ensemble.member[j])
                d *= wts[j]
                stack += d
                sumwts += wts[j]
        if sumwts > 0.0:
            stack *= 1.0 / sumwts
        else:
            message = "all ensemble members are dead - cannot compute a stack"
            stack.elog.log_error("_dbxcor_stacker", message, ErrorSeverity.Invalid)
            stack.kill()
            return [stack, wts]
        # newstack /= sumwts
        # Compare against the same immutable estimate used for the weights.
        relative_delta = _relative_stack_change(stack, previous_stack)
        # Force one iteration to make this a do-while loop.
        if i > 0 and relative_delta < eps:
            break
        previous_stack = TimeSeries(stack)
    return [stack, wts]


def beam_align(ensemble, beam, window=None, time_shift_limit=10.0):
    """
    Aligns ensemble members using signal defined by beam (arg1) argument.

    Computes cross correlation between each ensemble member and the beam.
    An optional window can be specified that is applied to each ensemble
    member before computing the cross correlation function.
    All live ensemble members are shifted to align with time base of the
    beam.   Note that can be a huge shift if the beam is relative and
    the ensemble members are absolute time.  It should work in that context
    but the original context was aligning common-source gathers for
    teleseismic phase alignment where the expectation is all the ensemble
    members and the beam are in relative time with 0 defined by some
    estimate of the phase arrival time.   Will correctly handle irregular
    window sizes between ensemble members and beam signal.

    It is important to recognize that if the window option is used
    it is applied only internally.   In that situation the output
    will be time shifted but the number of samples of each member
    will be the same.

    :param ensemble:  ensemble of data to be correlated with
        beam data.
    :type ensemble:  assumed to be a TimeSeriesEnsemble
    :param beam:  common signal to correlate with ensemble members.
    :type beam:  assumed to be a TimeSeries object
    :param window:  optional window to apply to ensemble members
        before computing cross correlation.
    :type window:  :py:class:`mspasspy.ccore.algorithms.basic.TimeWindow`
    :param time_shift_limit:  ceiling on allowed time shift for
        ensemble members.   Any computed shift with absolute value
        larger than this value will be reset to this value with the
        sign of the shift preserved.   (i.e. a negative lag will
        be set to the negative of this number).  The default is 10.0
        which is large for most data more or less making this an optional
        parameter.
    :type time_shift_limit: float (may abort if you use an int
        because the value can to sent to a C++ method that it type
        sensitive)
    :return: the input ensemble with its members time shifted in place to align
        with the time base of beam.  Each shifted start time is placed on the
        nearest sample of the beam grid without changing any sample values.
        If a window is defined it is used only for correlation and is not
        applied to the ensemble members.

    """
    # this may not be necessary for internal use but if used
    # externally it is necessary to avoid mysterious results
    # we don't test ensemble or beam because exceptions are
    # guaranteed in that case that should allow problem solving
    if time_shift_limit < 0.0:
        message = "beam_align:  illegal value time_shift_limit={}\n".format(
            time_shift_limit
        )
        message += "value must be positive"
        raise ValueError(message)
    for i in range(len(ensemble.member)):
        d = TimeSeries(ensemble.member[i])
        # in this context not needed but tiny cost for robustness
        if d.live:
            if window:
                d = WindowData(
                    d,
                    window.start,
                    window.end,
                    short_segment_handling="truncate",
                )
                if d.dead():
                    # this should rarely if ever happen but safety prudent
                    # in this case the return will have  truncated length for this datum
                    ensemble.member[i] = d
                    continue
            timelag = _xcor_shift(d, beam)
            # apply a ceiling/floor to allowed time shift via
            # the time_shift_limit arg
            if timelag > time_shift_limit:
                timelag = time_shift_limit
            elif timelag < (-time_shift_limit):
                timelag = -time_shift_limit
            # We MUST use this method instead of dithering t0 to keep
            # absolute time right.  This will fail if the inputs were
            # not shifted from UTC times
            # also note a +lag requires a - shift
            ensemble.member[i].shift(timelag)
            _snap_start_time_to_grid(ensemble.member[i], beam)
    return ensemble


@mspass_func_wrapper
def align_and_stack(
    ensemble,
    beam,
    *args,
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
    demean_residuals=True,
    handles_ensembles=True,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
) -> list:
    """
    This function uses an initial estimate of the array stack passed as
    the `beam` argument as a seed to a robust algorithm that will
    align all the data in the input ensemble by cross-correlation with
    the beam, apply a robust stack to the aligned signals, update the
    beam with the robust stack, and repeat until the changes to the
    beam signal are small.  The input ensemble is modified in place to align
    its members with the beam time base, and the function returns that ensemble
    with an updated beam estimate created by the robust stack.  The shifts and
    weights of each input signal are stored in the Metadata of each live
    ensemble member returned with keys defined by `robust_weight_key` and
    `time_shift_key`.

    This function is a python implementation of the same basic
    algorithm used in the dbxcor program described by
    Pavlis and Vernon(2010) Array Processing of teleseismic body waves
    with the USArray, Computers and Geosciences,15, 910-920.
    It has additional options made possible by the python interface
    and integration into MsPASS.  In particular, the original algorithm
    was designed to work as part of a GUI where the user had to pick
    a set of required parameters for the algorithm.   In this function
    those are supplied through Metadata key-value pairs and/or arguments.
    This function uses a pythonic approach aimed to allow this function
    to be run in batch without user intervention.  The original dbxcor
    algorithm required four interactive picks to set the input.
    The way we set them for this automated algorithm is described in the
    following four numbered items:

        1.  The "correlation window", which is the waveform segment used to
            compute cross-correlations between the beam and all ensemble
            members, is set one of three ways.  An explicit `TimeWindow`
            passed as `correlation_window` takes precedence; a truthy value of
            any other type raises `TypeError`, while a falsy value is treated
            as unset.  In the unset case, the algorithm checks the pair of beam
            Metadata keys supplied through `correlation_window_keys`.  For example,

               correlation_window_keys = ['correlation_start','correlation_end']

            would cause the function to fetch the start time with
            "correlation_start" and end time with "correlation_end".  By
            default, `correlation_window_keys` is
            ["correlation_window_start", "correlation_window_end"].  The
            function first tries those beam Metadata keys and logs a complaint
            before falling back to the corresponding beam bound for each
            missing key.  Passing `correlation_window_keys=None` skips the
            Metadata lookup and silently uses [beam.t0, beam.endtime()] as the
            correlation window.  When `window_beam` is True, an explicit or
            Metadata-derived window is also applied to the beam.  With
            `correlation_window_keys=None`, the beam already defines the
            window and is left unchanged without an error.
        2.  The "robust window" is a concept used in dbxcor to
            implement a special robust stacking algorithm that is a novel
            feature of the dbxcor algorithm.   It uses a very aggressive
            weighting scheme to downweight signals that do not match the
            beam.  The Pavlis and Vernon paper shows examples of how this
            algorithm can cleanly handle ensembles with a mix of high
            signal-to-noise data with pure junk and produce a clean
            stack that is defined.   Note recent experience has shown
            that with large, consistent ensembles the dbxcor robust
            estimate tends to focus on the signal closest to the median
            stack.  By default the median stack is used as the initial
            estimator; ``use_median_initial_stack=False`` instead uses the
            supplied beam.
        3.  dbxcor required the user to pick a seed signal to use as
            the initial beam estimate.  This function receives that estimate
            through the beam (arg1) argument and uses it as the robust-stack
            seed when ``use_median_initial_stack=False``.  In MsPASS the
            working model is to apply the broadband_snr_QC function before
            running this function and select the initial seed (beam) from
            one or more of the computed snr metrics.   In addition,
            with this approach I envision a two-stage computation where
            an initial seed is used for a first pass.   The
            return is then used to revise the correlation window by
            examining stack coherence metrics and then rerunning the
            algorithm.   The point is it is a research problem for
            different types of data to know how to best handle the
            align and stack problem.
        4.  dbxcor had a final stage that required picking the arrival time
            to use as the reference from the computed beam trace.  That is
            actually necessary if absolute times are needed because the
            method used will be biased by the time shift of the beam
            relative to the reference time.   See the Pavlis and Vernon
            paper for more on this topic.   The idea here is that if
            absolute times are needed some secondary processing will be used
            to manually or automatically pick an arrival time from the
            beam output.

    This function checks the window arguments and input ensemble before the
    iterative stack.  The resulting state changes are reported through the
    ensemble or beam error log:

        1.  Members with irregular sample intervals are killed.  The ensemble
            is killed only when no live members remain.
        2.  All members are required to use relative time, but the function
            does not explicitly validate every member's time reference.  The
            caller must convert UTC data before calling this function.
        3.  If the correlation window lies outside the estimated median time
            span of the ensemble, the beam is marked dead and returned with the
            input ensemble.
        4.  If the robust window extends beyond the cross-correlation window,
            it is clipped to those bounds, a Complaint is logged on the beam,
            and processing continues.

    Cross-correlation can estimate a shift larger than the useful range of the
    input.  The `time_shift_limit` argument is an absolute ceiling applied to
    each estimate: a larger positive or negative lag is clipped to the limit
    with its sign preserved.  It does not validate the correlation-window
    margins against the ensemble time span.

    A related issue is that arrival times estimated by this algorithm
    will be biased by the model mismatch with whatever signal was used
    as the initial beam estimate.  In dbxcor that was handled by forcing
    the user to manually pick the first arrival of the computed stack.
    That could be done if desired but would require you to devise a scheme
    to do that picking.   The default here is handled by the boolean
    parameter demean_residuals.  When True (the default) the vector
    of computed time shifts is corrected by the mean value of the group.
    Note that is common practice in regional tomography inversion anyway.
    For reasonable sized ensembles it will tend to yield data that when
    aligned by arrival time are all close to 0 relative time.

    Note the output stack normally spans a different time range than
    either the correlation or robust windows.   That property is defined
    by the `output_stack_window` argument.  See below for details.

    :param ensemble:   ensemble of data to be aligned and stacked.
        This function requires all data to be on a relative time base, but it
        does not explicitly validate every member's time reference.  The caller
        must convert UTC data before use.  The correlation and robust windows
        are expected to lie within the ensemble's usable time range; validation
        outcomes are described above.
    :type ensemble:  `TimeSeriesEnsemble` with some fairly rigid requirements.
        (see above)
    :param beam:  Estimate of stack (may be just one representative member)
        used as the seed for initial alignment and stacking.
    :type beam:  `TimeSeries`.  Must have a length consistent with window
        parameters.
    :param correlation_window: Used to specify the time window for
        computing cross-correlations with the beam signal.   Closely linked to
        `correlation_window_keys` as described above.
    :type correlation_window:  `TimeWindow` to define explicitly.  If None
        (default) uses the recipe driven by `correlation_window_keys` (see above)
    :param correlation_window_keys:   optional pair of Metadata keys used to
        extract cross-correlation window attributes from beam Metadata container.
        If defined component 0 is taken as the key for the start time of the window and
        component 1 the key for the end time.
    :type correlation_window_keys: iterable list containing two strings.  The
        default is ["correlation_window_start", "correlation_window_end"].
        Passing None uses the span of the beam signal without a Metadata lookup.
    :param window_beam:  if True (default) the parsed cross-correlation window attributes
        are applied to the beam signal as well as the data before starting
        processing.   If False the beam signal is used directly in all cross-correlations.
        Set False only if you can be sure secondary phases are not present in the
        unwindowed input.
    :param robust_stack_window: Provide an explicit `TimeWindow` used for
        extracting the robust window for this algorithm.   Interacts with the
        robust_stack_window_keys argument as described above.
    :type robust_stack_window:  If defined must be a `TimeWindow` object.
        If a None type (default) use the logic defined above to set this time window.
    :param robust_stack_window_keys: specifies a pair of strings to be used
        as keys to extract the start time (component 0) and end time (component 1)
        of the robust time window to use from the beam `TimeSeries`.
    :type robust_stack_window_keys: iterable list of two strings
    :param output_stack_window:  optional `TimeWindow` to apply to the
        computed robust stack output.   Default returns a stack spanning the
        median start and end times of the live ensemble members.
    :type output_stack_window:  `TimeWindow` object.  If None (default) the range
        is derived from the median ensemble member time range.
    :param robust_weight_key:  The robust weight used for each member to
        compute the robust stack output is posted to the Metadata container of
        each live member with this key.
    :type robust_weight_key:  string
    :param robust_stack_method: keyword defining the method to use for
        computing the robust stack.  Currently accepted value are:
        "dbxcor" (default) and "median".
    :type robust_stack_method:  string  - must be one of options listed above.
    :param use_median_initial_stack: when True, initialize robust stacking with
        the member median.  When False, use the supplied beam as the initial
        stack estimate.
    :type use_median_initial_stack: boolean (default True)
    :param time_shift_key:  the time shift applied relative to the starting
        point is posted to each live member with this key.  It is
        IMPORTANT to realize this is the time for this pass.  If this function
        is applied more than once and you reuse this key the shift from the
        previous run will be overwritten.  If you need to accumulate shifts
        it needs to be handled outside this function.
    :type time_shift_key:  string  (default "arrival_time_correction")
    :param convergence:   fractional change in robust stack estimates in
        iterative loop to define convergence.  This should not be changed
        unless you deeply understand the algorithm.
    :type convergence:  real number (default 0.01)
    :param time_shift_limit: when time shifting data with the cross
        correlation algorithm any estimated time shift larger than
        this value will be truncated to this value with the sign
        of the shift preserved.
    :type time_shift_limit:  float
    :param abort_irregular_sampling: passed to ``regularize_sampling`` as
        ``abort_on_error``.  When True, any irregular live member causes that
        helper to abort; when False, offending members are killed and
        processing continues if live members remain.  Exceptions use this
        function's standard decorator error handling.
    :type abort_irregular_sampling: boolean (default False)
    :param residual_norm_floor: floor on residuals used to compute dbxcor weight
        function.  See docstring for `dbxcor_weights` for details.
    :type residual_norm_floor:   float (default 0.1)
    :param demean_residuals: boolean controlling if the computed shifts
        are corrected with a demean operation.   Default is True which
        means the set of all time shifts computed by this function will
        have zero mean.

    :return: two-element list with component 0 containing the input ensemble,
        modified in place by cross-correlation time shifts and member kills.
        On a successful return, every member still live has the key defined by
        ``time_shift_key``.  With the ``dbxcor`` stack method, live members
        with positive final weight also have ``robust_weight_key``; members
        killed by sampling or windowing failures are marked dead.  Component 1
        is the computed stack windowed to the range defined by
        ``output_stack_window``.  Empty and all-dead inputs return the same
        ensemble identity marked dead and a separately logged dead `TimeSeries`.
    """
    alg = "align_and_stack"
    # xcor ensemble has the initial start time posted to each
    # member using this key - that content goes away because
    # xcorens has function scope
    it0_key = "_initial_t0_value_"
    ensemble_index_key = "_ensemble_i0_"
    # maximum iterations before the stack is returned dead
    MAXITERATION = 20
    # Enformce types of ensemble and beam
    if not isinstance(ensemble, TimeSeriesEnsemble):
        message = alg + ":  illegal type for arg0 (ensemble) = {}\n".format(
            str(type(ensemble))
        )
        message += "Must be a TimeSeriesEnsemble"
        raise TypeError(message)
    if not isinstance(beam, TimeSeries):
        message = alg + ":  illegal type for arg1 (beam) = {}\n".format(str(type(beam)))
        message += "Must be a TimeSeries"
        raise TypeError(message)
    if number_live(ensemble) == 0:
        message = "align_and_stack: input ensemble contains no live members"
        ensemble.kill()
        ensemble.elog.log_error(MsPASSError(message, ErrorSeverity.Invalid))
        return [ensemble, _logged_dead_timeseries(message)]
    if beam.dead():
        message = "ensemble was marked live but beam input was marked dead - cannot process this ensemble"
        ensemble.elog.log_error(alg, message, ErrorSeverity.Invalid)
        ensemble.kill()
        return [ensemble, beam]
    ensemble = regularize_sampling(
        ensemble,
        beam.dt,
        Nsamp=beam.npts,
        abort_on_error=abort_irregular_sampling,
    )
    if ensemble.dead():
        message = "align_and_stack: sampling regularization removed all live members"
        return [ensemble, _logged_dead_timeseries(message)]
    # we need to make sure this is part of a valid set of algorithms
    if robust_stack_method not in ["dbxcor", "median"]:
        message = "Invalid value for robust_stack_method={}.  See docstring".format(
            robust_stack_method
        )
        raise ValueError(message)
    # This section implements the somewhat complex chain of options for
    # setting the correlation window
    xcor_window_is_defined = False  # needed for parsing logic below
    # when this value is True window constraint errors cause the beam returned to be killed
    # with error messages.   If set in parsers to False an exception is thrown
    # as in that situation both windows would be set as arguments and the function would
    # always fail
    windows_extracted_from_metadata = True
    if correlation_window:
        if isinstance(correlation_window, TimeWindow):
            xcorwin = correlation_window
            xcor_window_is_defined = True
            windows_extracted_from_metadata = False
        else:
            message = "Illegal type for correlation_window={}\n".format(
                str(type(correlation_window))
            )
            message += "For this option must be a TimeWindow object"
            raise TypeError(message)
    elif correlation_window_keys:
        # this is a bit dogmatic - I know there is a less restrictive
        # test than this
        if isinstance(correlation_window_keys, list):
            skey = correlation_window_keys[0]
            ekey = correlation_window_keys[1]
            if beam.is_defined(skey) and beam.is_defined(ekey):
                stime = beam[skey]
                etime = beam[ekey]
            else:
                message0 = "missing one or both of correlation_window_keys\n"
                if beam.is_defined(skey):
                    stime = beam[skey]
                else:
                    message = (
                        message0
                        + "start time key={} is not set in beam signal\n".format(skey)
                    )
                    message += "reverting to beam signal start time"
                    ensemble.elog.log_error(alg, message, ErrorSeverity.Complaint)
                    stime = beam.t0
                if beam.is_defined(ekey):
                    etime = beam[ekey]
                else:
                    message = (
                        message0
                        + "end time key={} is not set in beam signal\n".format(ekey)
                    )
                    message += "reverting to beam signal endtime() method output"
                    ensemble.elog.log_error(alg, message, ErrorSeverity.Complaint)
                    etime = beam.endtime()
            xcorwin = TimeWindow(stime, etime)
            xcor_window_is_defined = True
        else:
            message = "Illegal type={} for correlation_window_keys argument\n".format(
                str(type(correlation_window_keys))
            )
            message += "If defined must be a list with 2 component string used as keys"
            raise TypeError(message)
    else:
        # it isn't considered an error to land here as this is actually the default
        # note it is important in the logic that xcor_window_is_defined be
        # left false
        xcorwin = TimeWindow(beam.t0, beam.endtime())
        windows_extracted_from_metadata = False
    if xcor_window_is_defined and window_beam:
        beam = WindowData(
            beam, xcorwin.start, xcorwin.end, short_segment_handling="truncate"
        )
        # this shouldn't happen but requires an exit if it did
        if beam.dead():
            return [ensemble, beam]
    # now a simpler logic to handle robust window
    if robust_stack_window:
        if isinstance(robust_stack_window, TimeWindow):
            rwin = robust_stack_window
            windows_extracted_from_metadata = False

        else:
            message = "Illegal type for robust_stack_window={}\n".format(
                str(type(robust_stack_window))
            )
            message += "when using robust_stack_window option value passed must be a TimeWindow object"
            raise ValueError(message)

    elif robust_stack_window_keys:
        # this is a bit dogmatic - I know there is a less restrictive
        # test than this
        if isinstance(robust_stack_window_keys, list):
            skey = robust_stack_window_keys[0]
            ekey = robust_stack_window_keys[1]
            if beam.is_defined(skey) and beam.is_defined(ekey):
                stime = beam[skey]
                etime = beam[ekey]
            else:
                message = "missing one or both of robust_stack_window_keys\n"
                if beam.is_defined(skey):
                    stime = beam[skey]
                else:
                    message += "start time key={} is not set in beam signal\n".format(
                        skey
                    )
                    message += "reverting to beam signal start time"
                    ensemble.elog.log_error(alg, message, ErrorSeverity.Complaint)
                    stime = beam.t0
                if beam.is_defined(ekey):
                    etime = beam[ekey]
                else:
                    message += "endtime key={} is not set in beam signal\n".format(ekey)
                    message += "reverting to beam signal endtime() method output"
                    ensemble.elog.log_error(alg, message, ErrorSeverity.Complaint)
                    etime = beam.endtime()
            rwin = TimeWindow(stime, etime)
        else:
            message = "Illegal type={} for robust_stack_window_keys argument\n".format(
                str(type(robust_stack_window_keys))
            )
            message += "If defined must be a list with 2 component string used as keys"
            raise ValueError(message)
    else:
        message = "Must specify either a value for robust_stack_window or robust_stack_window_keys - both were None"
        raise ValueError(message)

    # Validate the ensemble
    #  First verify the robust window is inside the correlation window (inclusive of edges)
    # reset to xcor range limits if wrong
    if not (rwin.start >= xcorwin.start and rwin.end <= xcorwin.end):
        message = (
            "Cross correlation window and robust window intervals are not consistent\n"
        )
        message += (
            "Cross-correlation window:  {}->{}.   Robust window:  {}->{}\n".format(
                xcorwin.start, xcorwin.end, rwin.start, rwin.end
            )
        )
        message += (
            "Robust window interval should be within bounds of correlation window\n"
        )
        if rwin.start < xcorwin.start:
            rwin.start = xcorwin.start
        if rwin.end > xcorwin.end:
            rwin.end = xcorwin.end
        message += "Robust window set to range {}->{}".format(rwin.start, rwin.end)

        beam.elog.log_error(alg, message, ErrorSeverity.Complaint)
    ensemble_timespan = ensemble_time_range(ensemble, metric="median")
    if ensemble_timespan.start > xcorwin.start or ensemble_timespan.end < xcorwin.end:
        message = "Correlation window defined is not consistent with input ensemble\n"
        message += "Estimated ensemble time span is {} to {}\n".format(
            ensemble_timespan.start, ensemble_timespan.end
        )
        message += "Correlation window time span is {} to {}\n".format(
            xcorwin.start, xcorwin.end
        )
        message += "Correlation window range must be inside the data range"
        # we don't use the windows_extracted_from_metadata boolean and never throw
        # an exception in this case because data range depends upon each ensemble
        # so there is not always fail case
        beam.elog.log_error(alg, message, ErrorSeverity.Invalid)
        beam.kill()
        return [ensemble, beam]
    # need this repeatedly so set it
    N_members = len(ensemble.member)
    # We need this Metadata posted to sort out total time
    # shifts needed for arrival time estimates
    for i in range(N_members):
        if ensemble.member[i].live:
            ensemble.member[i].put_double(it0_key, ensemble.member[i].t0)

    # above guarantees this cannot return a dead datum
    rbeam0 = WindowData(beam, rwin.start, rwin.end)
    converged = False
    for i in range(MAXITERATION):
        ensemble = beam_align(
            ensemble, beam, xcorwin, time_shift_limit=time_shift_limit
        )
        rens = WindowData(ensemble, rwin.start, rwin.end, short_segment_handling="pad")
        # this clones the Metadata of beam for the output using the stack_md
        # parameter - this won't work without that because of how robust stack
        # is implemented.  Note also that not passing an initial stack value
        # with the dbxcor method forces a median stack as the starting point
        rbeam, wts = robust_stack(
            rens,
            method=robust_stack_method,
            stack0=None if use_median_initial_stack else rbeam0,
            residual_norm_floor=residual_norm_floor,
            timespan_method="ensemble_median",
            stack_md=Metadata(rbeam0),
        )
        relative_change = _relative_stack_change(rbeam, rbeam0)
        if relative_change < convergence:
            converged = True
            break
        # this updates the always longer beam signal for correlation
        # use rbeam is used for convergence testing
        beam = _update_xcor_beam(ensemble, beam, robust_stack_method, wts)
        if beam.dead():
            message = "all members were killed in robust stack estimation loop\n"
            message += "Stack estimation failed"
            beam.elog.log_error(alg, message, ErrorSeverity.Invalid)
            return [ensemble, beam]
        rbeam0 = rbeam
    if not converged:
        beam.kill()
        message = (
            "align_and_stack: robust_stack iterative loop did not converge "
            "after 20 iterations"
        )
        beam.elog.log_error(MsPASSError(message, ErrorSeverity.Invalid))
        return [ensemble, beam]

    # apply time shifts to original ensemble that we will return
    # and set the value for the attribute defined by "time_shift_key"
    # argument.   This has to be done here so we can properly cut the
    # window to be stacked
    #
    # first remove the average time shift if requested to get more
    # rational arrival times - otherwise will be biased by initial beam time
    if demean_residuals:
        allshifts = []
        for i in range(len(ensemble.member)):
            if ensemble.member[i].live:
                initial_starttime = ensemble.member[i][it0_key]
                tshift = ensemble.member[i].t0 - initial_starttime
                allshifts.append(tshift)
        tshift_mean = np.average(allshifts)
        for i in range(len(ensemble.member)):
            if ensemble.member[i].live:
                # this method alters the t0 values of the ensemble members
                # when used plots will tend to be aligned with 0 relative time
                ensemble.member[i].shift(tshift_mean)
                _snap_start_time_to_grid(ensemble.member[i], beam)
    for i in range(len(ensemble.member)):
        if ensemble.member[i].live:
            # in this context it0_key should always be defined
            # intentionally let it throw and exception if that assumption
            # is wrong as it implies a bug
            initial_starttime = ensemble.member[i][it0_key]
            tshift = ensemble.member[i].t0 - initial_starttime
            ensemble.member[i].put_double(time_shift_key, tshift)
    if output_stack_window:
        # this will clone the beam trace metadata automatically
        # using pad option assures t0 will be output_stack_window.start
        # and npts is consistent with window requested
        output_stack = WindowData(
            beam,
            output_stack_window.start,
            output_stack_window.end,
            short_segment_handling="pad",
        )
        # this is an obscure but fast way to initialize the data vector to all 0s
        output_stack.set_npts(output_stack.npts)
    else:
        # also clones beam metadata but in this case we get the size from the ensemble time span
        output_stack = TimeSeries(beam)
        output_stack_window = TimeWindow(ensemble_timespan)
        output_stack.set_t0(output_stack_window.start)
        npts = int((output_stack_window.end - output_stack_window.start) / beam.dt) + 1
        output_stack.set_npts(npts)
    if robust_stack_method == "dbxcor":
        # We need to post the final weights to all live members
        wts = dbxcor_weights(rens, rbeam, residual_norm_floor=residual_norm_floor)
        for i in range(len(ensemble.member)):
            if ensemble.member[i].live and wts[i] > 0.0:
                ensemble.member[i].put(robust_weight_key, wts[i])

    else:
        wts = None
    # this private function is used for forming the longer xcor beam but it works in this
    # context the same way.  The only difference is the output_stack will normally be longer
    # than the signal used for cross-correlation
    output_stack = _update_xcor_beam(ensemble, output_stack, robust_stack_method, wts)
    return [ensemble, output_stack]


# these are intended to use only on the output from align_and stack
_SAMPLE_GRID_TOLERANCE = 1.0e-6


def _validate_sample_grid_compatibility(d, beam):
    dt_scale = max(abs(d.dt), abs(beam.dt))
    if d.dt <= 0.0 or beam.dt <= 0.0:
        raise ValueError(
            "d and beam sample intervals are incompatible across their sample "
            "span; resample before comparison"
        )
    paired_intervals = max(0, min(d.npts, beam.npts) - 1)
    accumulated_drift = abs(d.dt - beam.dt) * paired_intervals / dt_scale
    if accumulated_drift > _SAMPLE_GRID_TOLERANCE:
        raise ValueError(
            "d and beam sample intervals are incompatible across their sample "
            "span; resample before comparison"
        )
    offset_in_samples = (d.t0 - beam.t0) / dt_scale
    if abs(offset_in_samples - round(offset_in_samples)) > _SAMPLE_GRID_TOLERANCE:
        raise ValueError("d and beam start times are on incompatible sample grids")


def _inclusive_sample_bounds(d, start, end):
    first = int(np.ceil((start - d.t0) / d.dt - _SAMPLE_GRID_TOLERANCE))
    last = int(np.floor((end - d.t0) / d.dt + _SAMPLE_GRID_TOLERANCE))
    return max(0, first), min(d.npts - 1, last)


def _common_sample_interval(d, beam, window=None):
    _validate_sample_grid_compatibility(d, beam)
    overlap_start = max(d.t0, beam.t0)
    overlap_end = min(d.endtime(), beam.endtime())
    if window:
        overlap_start = max(overlap_start, window.start)
        overlap_end = min(overlap_end, window.end)
    if overlap_start > overlap_end:
        return None

    d_start, d_end = _inclusive_sample_bounds(d, overlap_start, overlap_end)
    beam_start, beam_end = _inclusive_sample_bounds(beam, overlap_start, overlap_end)
    sample_count = min(d_end - d_start + 1, beam_end - beam_start + 1)
    if sample_count <= 0:
        return None
    return d_start, beam_start, sample_count


def _overlap_vectors(d, beam, window=None):
    indices = _common_sample_interval(d, beam, window)
    if indices is None:
        return None
    d_start, beam_start, sample_count = indices
    d_vector = np.asarray(d.data[d_start : d_start + sample_count])
    beam_vector = np.asarray(beam.data[beam_start : beam_start + sample_count])
    return d_vector, beam_vector


def beam_correlation(d, beam, window=None, aligned=True) -> float:
    """
    Computes normalized peak cross-correlation value a datum with an array stack.

    Cross-correlation is a heavily used concept in seismology.   This function
    is a specialized version designed to compute a peak cross correlation
    value between a datum and an array stack.  The normal use is to call this
    function in a loop and post the results to each live member of the
    ensemble used to compute the beam.   Note it is assumed beam and d
    are filtered in the same passband.   Windowing is bombproof
    """
    alg = beam_correlation
    if not isinstance(d, TimeSeries):
        message = alg
        message += ":  arg0 must be a TimeSeries. Actual type={}".format(type(d))
        raise TypeError(message)
    if not isinstance(beam, TimeSeries):
        message = alg
        message += ":  arg0 must be a TimeSeries. Actual type={}".format(type(beam))
        raise TypeError(message)
    if d.dead() or beam.dead():
        return 0.0
    if not aligned:
        _validate_sample_grid_compatibility(d, beam)
        d_for_shift = TimeSeries(d)
        beam_for_shift = TimeSeries(beam)
        if window:
            d_for_shift = WindowData(
                d, window.start, window.end, short_segment_handling="pad"
            )
            beam_for_shift = WindowData(
                beam, window.start, window.end, short_segment_handling="pad"
            )
        shifted_d = TimeSeries(d)
        timelag = _xcor_shift(d_for_shift, beam_for_shift)
        shifted_d.set_t0(shifted_d.t0 - timelag)
        vectors = _overlap_vectors(shifted_d, beam, window)
    else:
        vectors = _overlap_vectors(d, beam, window)
    if vectors is None:
        return 0.0
    d_vector, beam_vector = vectors
    nrm1 = np.linalg.norm(d_vector)
    nrm2 = np.linalg.norm(beam_vector)
    if nrm1 <= 0.0 or nrm2 <= 0.0:
        return 0.0
    return abs(np.dot(d_vector, beam_vector) / (nrm1 * nrm2))


def beam_coherence(d, beam, window=None) -> float:
    """
    Compute time-domain coherence of a datum relative to the stack.

    Time domain coherence is a measure of misfit between a signal and a
    reference signal (normally a stack).  On the physical overlap, let
    ``bhat = beam / norm(beam)``, ``a = d dot bhat``, and
    ``residual = d - a*bhat``.  This implementation returns
    ``max(0, 1 - norm(residual)/norm(d))``.  Thus the measure compares the
    residual after the best scalar projection on the beam with the energy in
    the datum; it is not the unscaled ``d - beam`` residual.

    This function has an optional window parameter that computes the
    coherence with a specified time window.  By default the entire
    d and beam signals are used.   The default is done cautiously by
    using windowing to the mininum overlap of the two signals (if they differ)
    """
    alg = beam_correlation
    if not isinstance(d, TimeSeries):
        message = alg
        message += ":  arg0 must be a TimeSeries. Actual type={}".format(type(d))
        raise TypeError(message)
    if not isinstance(beam, TimeSeries):
        message = alg
        message += ":  arg0 must be a TimeSeries. Actual type={}".format(type(beam))
        raise TypeError(message)
    # assume beam is live but don't assume d is
    if d.dead() or beam.dead():
        return 0.0
    vectors = _overlap_vectors(d, beam, window)
    if vectors is None:
        return 0.0
    d_vector, beam_vector = vectors
    nrmd1 = np.linalg.norm(d_vector)
    if nrmd1 <= 0.0:
        return 0.0
    nrmd2 = np.linalg.norm(beam_vector)
    if nrmd2 <= 0.0:
        return 0.0
    normalized_beam = beam_vector / nrmd2
    amp = np.dot(d_vector, normalized_beam)
    residual = d_vector - amp * normalized_beam
    coh = 1.0 - np.linalg.norm(residual) / nrmd1
    if coh < 0.0:
        coh = 0.0
    return coh


def amplitude_relative_to_beam(d, beam, normalize_beam=True, window=None):
    """
    Compute and return amplitude relative to the stack (beam).

    dbxcor computed a useful metric of amplitude relative to the beam
    (stack) as ``(d dot beam)/N``, where ``dot`` is the vector dot product
    over the physical sample overlap and ``N`` is the number of samples in
    that overlap.  Relative-amplitude use normally requires a unit-L2 beam,
    so by default this function normalizes the overlapping beam vector first.
    That normalization can be disabled with ``normalize_beam=False`` when the
    caller has already prepared the desired beam scaling.

    :param d:  datum for which the relative amplitude is to be computed.
    :type d:  `TimeSeries` assumed - will throw an exception if it isn't
    :param beam:  stack with which it is to be compared.
    :type beam:  `TimeSeries` assumed  - will throw an exception if it isn't
    :param normalize_beam:   If True (default) the windowed beam vector
       will be normalized to be a unit vector (i.e. L2 norm of 1.0) for
       the calculation.  If False that will not be done and the vector
       in beam will be used directly.   Use False ONLY if windowing is off
       and beam is already normalized.   A minor efficiency gain is possible
       if beam is already normalized.
    :type normalize_beam:  boolean

    :return: float amplitude.  ``-1.0`` indicates dead input, no physical
      overlap, or a zero-norm beam when normalization was requested.  A
      zero-norm datum has the valid dot-product amplitude ``0.0``.  With
      ``normalize_beam=False``, a zero-norm beam also yields ``0.0`` because
      no division by its norm is required.
    """
    if d.dead() or beam.dead():
        return -1.0
    vectors = _overlap_vectors(d, beam, window)
    if vectors is None:
        return -1.0
    d_vector, beam_vector = vectors
    if normalize_beam:
        nrm_beam = np.linalg.norm(beam_vector)
        if nrm_beam <= 0.0:
            return -1.0
        beam_vector = beam_vector / nrm_beam
    return np.dot(d_vector, beam_vector) / float(len(d_vector))


def phase_time(
    d, phase_time_key="Ptime", time_shift_key="arrival_time_correction"
) -> float:
    """
    Small generic function to return a UTC arrival time combining an initial arrival
    time estimate (normally a model based time) defined by the "phase_time_key"
    metadata value extracted from d and the time shift computed by align_and_stack
    (or any other algorithm that computes relative time shifts) and set with the
    Metadata key defined by the "time_shift_key" argment.  The defaults work for
    P phase times computed by `MCXcorPrepP` and shifts computed by
    `align_and_stack`.   The computation here is trivial (just a difference) but
    the fluff is all the safeties in handling missing values.  Returns -1.0
    if any of the requried keys are missing.  Returns -2.0 if d is not defined
    as UTC.  That is basically a reminder this function only makes sense for
    data with a UTC time standard.
    """
    phase_time = d.t0
    if d.is_defined(phase_time_key) and d.is_defined(time_shift_key):
        phase_time = d[phase_time_key] + d[time_shift_key]
        return phase_time
    else:
        return -1.0


@mspass_func_wrapper
def post_MCXcor_metrics(
    d,
    beam,
    *args,
    metrics={
        "arrival_time": "Ptime_xcor",
        "cross_correlation": "beam_correlation",
        "coherence": "beam_coherence",
        "amplitude": "beam_relative_amplitude",
    },
    window=None,
    phase_time_key="Ptime",
    time_shift_key="arrival_time_correction",
    handles_ensembles=False,
    checks_arg0_type=False,
    handles_dead_data=False,
    **kwargs,
) -> TimeSeries:
    """
    Computes and posts a set of standard QC metrics for result of multichannel cross-correlation
    algorithm.

    This function should be thought of as a post-processing function to standardize a set of
    useful calculations from the output of the multichannel cross-correlation algorithm.
    The default is set up for post processing teleseismic P wave data but with changes to the
    arguments it should be possible to use it for any teleseismic body wave phase.

    What is computed is controlled by the input parameter "metrics".   "metrics" is
    expected to be a dictionary with the keys defining the concept of what is to be
    computed and posted and a "value" being the actual key to use for posting the
    computed value.  Defaults for "metrics" are as follows the dictionary key
    in quotes at the start of each paragraph:

    -  "arrival_time"  - compute arrival time from posted initial time estimate and
       correlation time shift computed by `align_and_stack`.  Uses the "phase_time_key"
       and "time_shift_key" to fetch required values.  Default works for Pwave
       data processed with `align_and_stack`.  Changes needed for other phases.
    -  "cross_correlation" - compute cross correlation with respect to beam
    -  "coherence" - compute time domain coherence with respect to beam
    -  "amplitude" - compute amplitude of d relative to the beam.

    :param d:  datum to be processed
    :type d:  `TimeSeries`
    :param beam: array stack (beam) output of `align_and_stack`
    :type beam:  `TimeSeries`
    :param metrics:  defines what metrics should be computed ans posted
       (see above for options)
    :type metrics:  python dictionary with one or more of the keys defined above.
    :param window:  optional time window to use for computing specified metric(s).
       Windowing is normally applied for cross-correlation, coherence, and amplitude
       calculations (it is ignored for the time computation).  If it is not
       defined behavior depends on the algorithm as the object is passed directly
       to functions used to compute correlation, coherence, and amplitude metrics.
    :type window:  `TimeWindow` or None (default)
    :param phase_time_key: key used to fetch the initial phase time used for
       initial alignment for `align_and_stack` input.   The assumption is the
       content is defined in d and when fetch is an epoch time defining the
       initial time shift for the given phase.  Note it could be either a measured
       or model-based arrival time.
    :type phase_time_key: string (default "Ptime" which is default expectation used
       in `align_and_stack`)
    :param time_shift_key:  key used to store the relative time shifts computed
       by `align_and_stack`.
    :type time_shift_key: string (default "arrival_time_correction" is the key
       used to post the cross-correlation shifts in `align_and_stack`)
    :return:  copy of d `TimeSeries` with the requested metrics posted to the
       Metadata container of the output.
    """
    # no safeties for d and beam type in this function because of stock use.
    # that maybe should be changed
    if "arrival_time" in metrics:
        atime = phase_time(
            d, time_shift_key=time_shift_key, phase_time_key=phase_time_key
        )
        if atime > 0.0:
            key = metrics["arrival_time"]
            d[key] = atime
    if "cross_correlation" in metrics:
        xcor = beam_correlation(d, beam, window=window, aligned=True)
        key = metrics["cross_correlation"]
        d[key] = xcor
    if "coherence" in metrics:
        coh = beam_coherence(d, beam, window=window)
        key = metrics["coherence"]
        d[key] = coh
    if "amplitude" in metrics:
        amp = amplitude_relative_to_beam(d, beam, normalize_beam=True, window=window)
        key = metrics["amplitude"]
        d[key] = amp
    return d


def demean_residuals(
    ensemble,
    measured_time_key="Ptime_xcor",
    model_time_key="Ptime",
    residual_time_key="Presidual",
    corrected_measured_time_key="Pmeasured",
    center_method="median",
    center_estimate_key="Presidual_bias",
    kill_on_failure=False,
):
    """
    Residuals measured by any method are always subject to a bias
    from Earth structure and earthquake location errors.   With the
    MsPASS multichannel xcor algorithm there is an additional bias
    inherited from the choice of the initial beam estimate that can
    produce an even larger bias.   This algorithm id designed to
    process an ensemble to remove and estimate of center from the
    residuals stored in the Metadata containers of the ensemble members.

    The arguments can be used to change the keys used to access
    attributes needed to compute the residual and store the result.
    The formula is trivial an with kwarg symbols is:

        residual_time_key = measured_time_key - model_time_key - bias

    where bias is the estimate of center computed from the vector
    of (uncorrected) residual extracted from the ensemble members.
    Each member of the ensemble where the measured and model times
    are defined will have a value set for the residual_time_key in
    the output.  The actual estimate of bias will be posted to the
    output in the ensemble's Mewtadata container with the key
    defined by the argument "center_estimate_key".

    Note the corrected (by estimate of center that is) measured
    phase arrival time will be stored in each member with the key
    defined by the "corrected_measured_time_key" argument.  By default
    that is a different key than measured_time_key but some many want
    to make measured_time_key==corrected_measured_time_key to reduce
    complexity.  That is allowed but is a choice not a requirement.
    Again, default will make a new entry for the corrected value.

    :param ensemble:  ensemble object to be processed.   Assumes
       the members have defined values for the keys defined by
       the "measured_time_key" and "model_time_key".
    :type ensemble:  :py:class:`mspasspy.ccore.seismic.TimeSeriesEnsemble`
       or :py:class:`mspasspy.ccore.seismic.SeismogramEnsemble`.  The function
       will throw a ValueError exception if this require arg is any other
       type.
    :param measured_time_key:  key to use to fetch the measured arrival
       time of interest from member Metadata containers.
    :type measured_time_key: string (default "Ptime_xcor")
    :param model_time_key:  key to use to fetch the arrival time computed
       from an earth model and location estimate.  Each member is assumed
       to have this value defined.
    :type model_time_key:  string (default "Ptime")
    :param residual_time_key:   key use to store the output demeaned
       residual.   Be warned that this will overwrite a previously stored value
       if the key was previously defined for something else.  That can,
       however, be a useful feature if the same data are reprocessed.
    :type residual_time_key:  string (default "Presidual")
    :param corrected_mesured_time_key:   key to use to save the bias
       corrected measured time estimate.   This value is just the
       input measured time value minus the computed bias.
    :type corrected_measured_time_key:  string (default "Pmeasured")
    :param center_method:  method of center method to use to compute
       bias correction from vector of residuals.  Valid values are: "median"
       to use the median and "mean" ("average" is also accepted an is treated as "mean")
       to use the average/mean value operator.
    :type center_method: string (default "median")
    :param center_estimate_key:  key to use to post the estimated center of
       the vector of residuals.  Note that value is posted to the ensemble's
       Metadata container, not the members.  Be warned, however, that
       currentlyh when ensemble data are saved this value will be posted to
       all members before saving. Be sure this name does not overwrite
       some other desired member Metdata when that happens.
    :type center_estimate_key:  string (default "Presidual_bias")
    :param kill_on_failure:  boolean controlling what the function does
       to the output ensemble if the algorithm totally fails.  "totally fails"
       in this case means it the number of residuals it could compute was
       less than or equal 1.  If set True the output will be killed.
       When False it will be returned with no values set for the
       keys defined by "residual_time_key" and "center_estimate_key".
       Default is False.

    :return:  edited copy of input ensemble altering only Metadata containers

    """
    alg = "demean_residuals"
    if not isinstance(ensemble, (TimeSeriesEnsemble, SeismogramEnsemble)):
        message = alg + ":  arg0 has invalid type={}\n".format(str(type(ensemble)))
        message += "Must be TimeSeriesEnsemble or SeismogramEnsemble"
        raise TypeError(message)
    if ensemble.dead():
        return ensemble
    valid_center_methods = ["mean", "average", "median"]
    if center_method not in valid_center_methods:
        message = "center_method={} is not a valid - defaulting to median"
        ensemble.elog.log_error(alg, message, ErrorSeverity.Complaint)
        center_method = "median"
    residuals = list()
    r_index = list()
    number_problem_members = 0
    message = ""
    for i in range(len(ensemble.member)):
        if ensemble.member[i].live:
            # use d as shorthand to reduce complexity of expressions
            d = ensemble.member[i]
            if d.is_defined(measured_time_key) and d.is_defined(model_time_key):
                r = d[measured_time_key] - d[model_time_key]
                residuals.append(r)
                r_index.append(i)
            else:
                message += (
                    "member {} is missing one or both of keys {} and {}\n".format(
                        i, measured_time_key, model_time_key
                    )
                )
                number_problem_members += 1
    if number_problem_members > 0:
        ensemble_message = (
            "Could not compute residuals for all live members - list problems:\n"
        )
        ensemble_message += message
        ensemble.elog.log_error(alg, ensemble_message, ErrorSeverity.Complaint)
    if len(residuals) <= 1:
        message = "Number of residuals computed = {} - demean is not feasible".format(
            len(residuals)
        )
        ensemble.elog.log_error(alg, message, ErrorSeverity.Complaint)
        if kill_on_failure:
            ensemble.kill()
        return ensemble
    if center_method in ["average", "mean"]:
        r_mean = np.average(residuals)
    else:
        # for now this means median - if more methods are added they
        # should appear in elif blocks above this
        r_mean = np.median(residuals)

    # drive the setting of computed output with the residuals dictionary
    # that is a clean mechanism to guarantee
    for i in r_index:
        # we can assume these will resolve or i would not have been set
        # in the r_index list
        r = ensemble.member[i][measured_time_key] - ensemble.member[i][model_time_key]
        r -= r_mean
        ensemble.member[i][residual_time_key] = r
        ensemble.member[i][corrected_measured_time_key] = (
            ensemble.member[i][measured_time_key] - r_mean
        )
    ensemble[center_estimate_key] = r_mean
    return ensemble


def _require_incident_grid_compatibility(func):
    @wraps(func)
    def checked(d, beam, *args, **kwargs):
        if (
            isinstance(d, TimeSeries)
            and isinstance(beam, TimeSeries)
            and d.live
            and beam.live
        ):
            _validate_sample_grid_compatibility(d, beam)
        return func(d, beam, *args, **kwargs)

    return checked


@_require_incident_grid_compatibility
@mspass_func_wrapper
def remove_incident_wavefield(d, beam, *args, handles_ensembles=True, **kwargs):
    """
    Remove incident wavefield for teleseismic P wave data using a beam estimate.

    In imaging of S to P conversion with teleseimic P wave data a critical step
    is removing the incident wavefield.   The theoretical reason is described in
    multiple publications on S to P imaging theory.  This function implements
    a novel method using the output of `align_and_stack` to remove the
    incident wavefield.   The approach make sense ONLY IF d is a member of the
    ensemble used to compute beam and is the longitudinal component estimate
    for a P phase.  The best choice for that is the free surface transformation
    operator but in could be used for LQT or even the very crude RTZ
    transformation.

    The actual operation is very simple:  the overlaping section of beam
    and d is determined.  The algorthm then computes a scaling factor for
    beam as the dot product of beam and d in the common time range.
    That number is used to scale the beam which is then subtracted
    (in the overlapping time range) from d.  There are several key
    assumptions this algorithm makes about the input that should be
    kept in mind before using it:

    1.  d is implicitly expected to span a longer or a least
        equal span to the beam time range.
    2.  beam should always be tapered to prevent an offset at the
        front and end when it is subtracted from d.
    3.  beam should be normalized so it L2 norm is 1.0.  Note that
        should be the taper beam not the beam cut with WindowData.
        That is not computed in this function for efficiency as normal
        use would apply the same beam estimate to all live ensemble
        members.
    4.  Because this is assumed to be used at deep in a processing
        chain it has no safties.   d and beam and assumped to
        be TimeSeries objects.  The only safety is that if d or
        beam are marked dead it does nothing but return d.

    :param d:  datum from which beam is to be subtracted
        (Assumed to have a time span containing the time span of beam)
    :type d:  `TimeSeries`
    :param beam:   windowed and tapered output stack from
        `align_and_stack`.  Assumed to have L2 norm of 1.0.
    """
    if beam.dead() or d.dead():
        return d
    indices = _common_sample_interval(d, beam)
    if indices is None:
        return d
    d_start, beam_start, sample_count = indices
    d_vector = np.asarray(d.data[d_start : d_start + sample_count])
    beam_vector = np.asarray(beam.data[beam_start : beam_start + sample_count])
    if np.linalg.norm(d_vector) <= 0.0 or np.linalg.norm(beam_vector) <= 0.0:
        return d
    amp = np.dot(d_vector, beam_vector)
    x = DoubleVector(beam.data[beam_start : beam_start + sample_count])
    x *= amp
    d.data[d_start : d_start + sample_count] -= x
    return d


# private functions - should be used only internally.  Use with care if imported
def _coda_duration(ts, level, t0=0.0, search_start=None) -> TimeWindow:
    """
    Low-level function to estimate the range of the "coda" of a particular
    seismic phase. This function first computes the envelope of the
    input signal passed via arg0 as a `TimeSeries` object.   The function
    then searches backward in time from `search_start` to the first
    sample of the envelop function that exceeds the value defined by
    `level`.  It returns a `TimeWindow` object with a `start` set as the
    value passed as t0 and the `end` attribute set to the time stamp
    of the estimated coda end time.   Normal use by processing functions
    inside this module assume the input has a relative time standard
    but the function could work with UTC data you treat `t0` and
    `search_start` as required, not optional, arguments.

    The function return a zero length `TimeWindow` (start == end) if the
    level of the envelope never exceeds the value defined by the level
    argument.

    :param ts:  Datum to be processed.   The function will return a null
      result (zero length window) if ts.t0> t0 (argument value).  The sample
      data is assumed filtered to an appropriate band where the envelope will
      properly define the coda.
    :type ts: `TimeSeries` is assumed.  There is not explicit type checking
      but a type mismatch will always cause an error.
    :param level:  amplitude of where the backward time search will be
      terminated.   This value should normally be computed from a background
      noise estimate.
    :type level:   float
    :param t0:  beginning of coda search interval.  This time is mainly used
      as the failed search.  That is, if the level never rises above the
      value defined by the `level` argument a zero length window will be
      returned with start and end both set to this value.   If the value
      is inconsistent with the start time of the data it is reset to the
      time of the first data sample.
    :type t0:  float (default 0.0 - appropriate for normal use with data
      time shifted so 0 is a P or S phase arrival time)
    :param search_start:  optional time to start backward search.
      The default, which is defined with a None type, is to start the search
      at ts.endtime().  If a value is given and it exceeds ts.endtime()
      the search will silently be truncated to start at ts.endtime().
      Similarly if `search_start` is less than ts.t0 the search will also
      be silently reset to the ts.endtime().
    :type search_start:  float (time - either relative time or an epoch time)
    :return:  `TimeWindow` object with window `end` value defining the
      end of the coda.  window.end-window.start is a measure of coda duration.
      Returns a zero length window if the amplitude never exceeds the
      valued defined by the `level` parameter.   Not the start value may
      be different from the input t0 value if the data start time (ts.t0) is
      greater than the value defined by the `t0` argument.
    """
    # silently handle inconsistencies in t0 and search_start
    # perhaps should have these issue an elog complaint
    it0 = ts.sample_number(t0)
    # silently reset to 0 if t0 is not valid for
    if it0 < 0:
        it0 = 0
        t0used = ts.time(0)
    else:
        t0used = t0
    if search_start:
        itss = ts.sample_number(search_start)
        if itss >= ts.npts or itss < 0:
            itss = ts.npts - 1
    else:
        itss = ts.npts - 1
    if itss <= it0:
        message = "_coda_durations:   values for inferred search range {} to {} do not overlap data range"
        raise ValueError(message)
    httsd = signal.hilbert(ts.data)
    envelope = np.abs(httsd)
    it0 = ts.sample_number(t0)
    i = itss
    while i > it0:
        if envelope[i] > level:
            break
        i -= 1
    # A failed search will have start and end the same
    return TimeWindow(t0used, ts.time(i))


def _set_phases(
    d,
    model,
    Ptime_key="Ptime",
    pPtime_key="pPtime",
    PPtime_key="PPtime",
    station_collection="channel",
    default_depth=10.0,
) -> TimeSeries:
    """
    Cautiously sets model-based arrival times in Metadata of input
    d for first arrival of the phases P pP, and PP.   pP and PP
    do not exist at all distances and depths so the times may be
    left undefined.   A value for P should always be set unless that
    datum was marked dead on input.

    The input datum must contain Metadata entries required to compute the
    travel times.  That can be in one of two forms:
        (1)  The normal expectation is to source coordinates are defined with
             keys "source_iat" , "source_lon", and "source_time", and
             "source_depth" while receiver coordinates are defined with
             "channel_lat" and "channel_lon".   A common variant is possible
             if the `station_collection` argument is set to "site".  Then
             the function will try to fetch "site_lat" and "site_lon".
             A special case is source depth.  If "source_depth" is not
             defined the value defined with the `default_depth` argument is
             used.
        (2)  If the key "dist" is defined in d it is assumed to contain
             a previously computed distance in degrees from source to this
             receiver.  In that case the only other required source property
             is "source_time" - event origin time.
    :param d:  seismic datum to process.   This datum requires source and
      receiver metadata attributes as noted above
    :type d:   Assumed to be a `TimeSeries`.   That is not tested as this is
      an internal function.  Use outside the module should assure input is
      a `TimeSeries` or create an error handler for exceptions.
    :param model:   an instance of an obspy TauPyModel object used to
      compute times.
      :type model:   obspy TauPyModel object
    :param Ptime_key:  key used to set the pP arrival time.
    :type pPtime_key:  string (default "pPtime")
    :param pPtime_key:  key used to set the P arrival time.
    :type PPtime_key:  string (default "PPtime")
    :param PPtime_key:  key used to set the PP arrival time.
    :type PPtime_key:  string (default "PPtime")
    :param station_collection:   normalizing collection used to define
      receiver coordinates.   Used only to create names for coordinates.
      e.g. if set to "site" expects to find "site_lat" while if set to
      default "channel" would expect to find "channel_lat".
    :type station_collection:  string (default "channel")
    :param default_depth:   source depth to use if the attribute "source_depth"
      is not defined.   This is a rare recovery to handle the case where
      the epicenter and origin time are defined but the depth is not.
    :type default_depth:  float
    :return:  `TimeSeries` copy of input with model-based arrival times posted
      to Metadata container with the specified keys.
    """
    if d.dead():
        return d
    alg = "_set_phases"
    # these symbols need to be effectively declared here or the go out of scope
    # before being used as arguments for the get_travel_time method of the taup calculator
    depth = 0.0
    dist = 0.0
    if d.is_defined("dist"):
        dist = d["dist"]
        origin_time = d["source_time"]
    else:
        if (
            d.is_defined("source_lat")
            and d.is_defined("source_lon")
            and d.is_defined("source_time")
        ):
            srclat = d["source_lat"]
            srclon = d["source_lon"]
            origin_time = d["source_time"]
            # compute dist
        else:
            message = "Missing required source coordinates:  source_lat,source_lon, and/or source_time values\n"
            message += "Cannot handle this datum"
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
            return d
        lat_key = station_collection + "_lat"
        lon_key = station_collection + "_lon"
        if d.is_defined(lat_key) and d.is_defined(lon_key):
            stalat = d[lat_key]
            stalon = d[lon_key]
            [dist, seaz, esaz] = gps2dist_azimuth(stalat, stalon, srclat, srclon)
            dist = kilometers2degrees(dist / 1000.0)
            # always post these - warning produces a state dependency as there is no guarantee these
            # get posted if "dist" was defined on input
            # keys are frozen and based on standard mspass schema
            d["dist"] = dist
            d["seaz"] = seaz
            d["esaz"] = esaz
        else:
            message = "Missing required receiver coordinates defined with keys: {} and {}\n".format(
                lat_key, lon_key
            )
            message += "Cannot handle this datum"
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
            return d

    if d.is_defined("source_depth"):
        depth = d["source_depth"]
    else:
        depth = default_depth
        message = "source_depth value was not defined - using default value={}".format(
            depth
        )
        d.elog.log_error(alg, message, ErrorSeverity.Complaint)
    arrivals = model.get_travel_times(
        source_depth_in_km=depth, distance_in_degree=dist, phase_list=["P", "pP", "PP"]
    )
    # from all I can tell with the list above arrivals always has entries for at least one of
    # the phases in phase_list.   If depth is invalid it throws an exception and it seems to
    # handle dist for anything.   Hence, this assumes arrivals is not empty.  An earlier
    # had an unnecessary error handler for that case.
    # only set first arrival for multivalued arrivals
    P = []
    pP = []
    PP = []
    for arr in arrivals:
        if arr.name == "P":
            P.append(arr.time)
        elif arr.name == "pP":
            pP.append(arr.time)
        elif arr.name == "PP":
            PP.append(arr.time)
    if len(P) > 0:
        d[Ptime_key] = min(P) + origin_time
    if len(pP) > 0:
        d[pPtime_key] = min(pP) + origin_time
    if len(PP) > 0:
        d[PPtime_key] = min(PP) + origin_time
    return d


def _get_search_range(
    d, Pkey="Ptime", pPkey="pPtime", PPkey="PPtime", duration_undefined=20.0
):
    """
    Small internal function used to standardize the handling of the search
    range for P coda.   It returns a time duration to use as the search
    range relative to 0 (P time) based on a simple recipe to avoid interference
    from pP and PP phases.   Specifically, if the source depth is greater than
    100 km the pP phase is used as the maximum duration of the coda.
    For shallower sources PP is used.

    This function should not normally be used except as a component of the
    MCXcorPrepP function.   It has no safeties and is pretty simple.
    That simple recipe, however, took some work to establish that is documented
    a notebook in the distribution.
    """
    if d.live:
        depth = d["source_depth"]
        if depth > 100.0:
            if d.is_defined(pPkey):
                tend = d[pPkey]
            elif d.is_defined(PPkey):
                tend = d[PPkey]
            else:
                d[Pkey] + duration_undefined
        else:
            tend = d[PPkey]
        duration = tend - d[Pkey]
    else:
        duration = 0.0
    return duration


def _xcor_shift(ts, beam):
    """
    Internal function with no safeties to compute a time shift in
    seconds for beam correlation.   The shift is computed by
    using the scipy correlate function.  The computed shift is the
    time shift that would need to be applied to ts to align with
    that of beam.   Note that number can be enormous if using UTC
    times and it will still work.   The expected use, however,
    in this function is with data prealigned by an phase arrival
    time (predicted from a mdoel of an estimate).
    :param ts:  TimeSeries datum to correlate with beam
    :param beam:  TimeSeries defining the common signal (beam) for
      correlation - the b argument to correlation.
    """
    # note this assumed default mode of "full"
    xcor = signal.correlate(ts.data, beam.data)
    lags = signal.correlation_lags(ts.npts, beam.npts)
    # numpy/scipy treat sample 0 as time 0
    # with TimeSeries we have to correct with the t0 values to get timing right
    lag_of_max_in_samples = lags[np.argmax(xcor)]
    lagtime = ts.dt * lag_of_max_in_samples + ts.t0 - beam.t0
    return lagtime


def _snap_start_time_to_grid(datum, reference):
    """Move ``datum.t0`` to the nearest sample on ``reference``'s grid.

    This is an explicit MCXcor alignment operation:  it changes only the time
    assigned to sample zero and never changes the sample values, ``dt``, or
    ``npts``.  General waveform arithmetic remains strict and rejects operands
    that have not first been placed on a common grid.
    """
    datum.t0 = reference.time(reference.sample_number(datum.t0))


def _update_xcor_beam(xcorens, beam0, robust_stack_method, wts) -> TimeSeries:
    """
    Internal method used to update the beam signal used for cross correlation
    to the current best stack estimate.   Note the algorithm assumes
    xcorens members have a larger time range than beam0.   To compute the
    beam stack each member is windowed in the range beam0.t0 to beam0.endtime()
    before use.

    :param xcorens:   ensemble data being used for cross-correlation
    :type xcorens:  TimeSeriesEnsemble
    :param rens:  ensemble returned by robust_stack (Ignored for median stack)
    :type rens:  TimeSeriesEnsemble
    :param beam0: current beam estimate.   The output will have size
        determined by beam.t0 and beam.endtime()
    :type beam0:  TimeSeries
    :param robust_stack_method:  name of robust stacking method to use.
        Currently must either "dbxcor" or "median".
    :type robust_stack_method:  string
    :param robust_weight_key:  key needed to extract robust stack weight
        from rens when running an estimator like dbxcor that uses a weighted
        stack.   ignored for median stack.
    """
    beam = TimeSeries(beam0)
    # A tricky but fast way to initialize the data vector to all zeros
    # warning this depends upon a special property of the C++ implementation
    beam.set_npts(beam0.npts)
    if number_live(xcorens) == 0:
        message = "_update_xcor_beam: input ensemble contains no live members"
        beam.elog.log_error(MsPASSError(message, ErrorSeverity.Invalid))
        beam.kill()
        return beam
    # Cautioniously copy these to beam.   Maybe should log an error if they
    # aren't defined but for now do this silently.   Possible
    # maintenace issue if these keys ever change in MCXcorPPrep.
    # at present it sets these in ensemble's Metadata container
    for key in ["MCXcor_f_low", "MCXcor_f_high", "MCXcor_npoles"]:
        if xcorens.is_defined(key):
            beam[key] = xcorens[key]
    stime = beam.t0
    etime = beam.endtime()
    N = len(xcorens.member)
    if robust_stack_method == "dbxcor":
        # we can assume for internal use that xcorens and rens are the
        # same size. We don't assume all are live though to allow
        # flexibility in the algorithm

        sumwt = 0.0
        for i in range(N):
            # the dbxcor stacking function weight is set negative if
            # a datum was marked dead
            if xcorens.member[i].live and wts[i] > 0:
                wt = wts[i]
                d = WindowData(
                    xcorens.member[i], stime, etime, short_segment_handling="pad"
                )
                d *= wt
                _snap_start_time_to_grid(d, beam)
                beam += d
                sumwt += wt
        if sumwt > 0.0:
            # TimeSeries does not have operator /= defined but it does have *= defined
            scale = 1.0 / sumwt
            beam *= scale
        else:
            beam.elog.log_error(
                "_update_xcor_beam",
                "dbxcor weights are all 0.0 - all ensemble members were probably killed",
                ErrorSeverity.Invalid,
            )
            beam.kill()
    else:
        # for now landing here means median stack - change if new algorithms addede
        nlive = 0
        # note sizes passed to shape are set above when validating inputs and are assumed to not have changed
        Npts = beam.npts
        gather_matrix = np.zeros(shape=[N, Npts])
        i = 0
        for d in xcorens.member:
            # always use the zero padding method of WindowData
            # run silently for median stack.  Maybe should allow
            # options for this case
            dcut = WindowData(
                d,
                stime,
                etime,
                short_segment_handling="pad",
                log_recoverable_errors=False,
            )
            i += 1
            if dcut.live:
                # rounding effects with a window time range
                # iteractions with t0 can cause the size of
                # dcut to differ from beam.npts
                # logic handles that
                if dcut.npts == Npts:
                    gather_matrix[nlive, :] = np.array(dcut.data)
                elif dcut.npts < Npts:
                    gather_matrix[nlive, 0 : dcut.npts] = np.array(dcut.data)
                else:
                    gather_matrix[nlive, :] = np.array(dcut.data[0:Npts])
                nlive += 1
        stack_vector = np.median(gather_matrix[0:nlive, :], axis=0)
        # this matrix could be huge so we release it as quickly as possible
        del gather_matrix
        # logic above guarantees stack_vector is beam.npts in size
        beam.data = DoubleVector(stack_vector)
    return beam
