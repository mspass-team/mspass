import obspy.signal.filter
import obspy.signal.detrend
import obspy.signal.cross_correlation
import obspy.signal.interpolation
import obspy.core.utcdatetime

from mspasspy.util.decorators import (
    mspass_func_wrapper,
    mspass_func_wrapper_multi,
    timeseries_as_trace,
    seismogram_as_stream,
    timeseries_ensemble_as_stream,
    seismogram_ensemble_as_stream,
)

from mspasspy.util.converter import Stream2Seismogram, Stream2TimeSeriesEnsemble


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def filter(
    data,
    type,
    *args,
    object_history=False,
    alg_name="filter",
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    handles_dead_data=False,
    **options,
):
    """
    Applies a time invariant filter to a MsPASS data object.

    This entry is a wrapper around the obspy filter function.  It accepts the
    same arguments as the obspy function and runs the same implementation
    with the same idiosyncracies of their implementation.   Refer to their
    documentation for details, but because this function is so fundamental
    for data processing we note a few key points.

    The "type" argument is required by their implementation and must be one
    of the following:  *bandpass*, *lowpass*, or *highpass*.  A confusing
    feature is that different kwarg values are required for different values
    of "type" that are not consistent.   Requirements for the different
    acceptable values of "type" are:

    - If `type=="bandpass"` you must specify `freqmin` and `freqmax` values for
      the passband with units of Hz.  You can optionally change the (integer)
      value `corners` to specify the number of poles of the Butterworth
      filter this option implies.   Note there is also a `zerophase` boolean
      that if set True (default is false for minimum phase) causes the function
      to apply a zerophase Butterworth instead of the default minimum phase
      version.
    - If `type=="lowpass"` you must specify only one corner but with a different
      keyword of `freq`.   `corners` and `zerophase` are the same same as for
      the "bandpass" option.  Again this option implies a Butterworth filter.
    - If `type=="highpass"` the function behaves like "lowpass" except the
      value of `freq` is the low frequency corner versus the high frequency
      corner for lowpass.   `corners` and `zerophase` are the same same as for
      the "bandpass" option.  Again this option implies a Butterworth filter.

    There are also `bandstop`, `lowpass_fir`, `lowpass_cheby_2`, and
    `remez_fir` options for the "type" argument.   See the obspy documentation
    if you want to use these more obscure options.

    :param data: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param type: type of filter, 'bandpass', 'bandstop', 'lowpass', 'highpass', 'lowpass_cheby_2', 'lowpass_fir',
      'remez_fir'. as noted above You can refer to
      `Obspy <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.filter.html>` for details.
    :type type: str
    :param args: extra arguments - see above as type value determines what is required here.
    :param object_history: True to preserve the processing history. For details, refer to :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper.
    :param options: extra kv options
    :return: None
    """
    data.filter(type, **options)  # inplace filtering


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def detrend(
    data,
    *args,
    object_history=False,
    alg_name="dtrend",
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    handles_dead_data=False,
    type="simple",
    **options,
):
    """
    This function removes a trend from the data, which is a mspasspy object. Note it is wrapped by mspass_func_wrapper,
    so the processing history and error logs can be preserved.

    :param data: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param type: type of filter, 'simple', 'linear', 'constant', 'polynomial', 'spline'. You can refer to
     `Obspy <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.detrend.html>` for details.
    :type type: str
    :param args: extra arguments
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper.
    :param options: extra kv options
    :return: None
    """
    data.detrend(type, **options)


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def interpolate(
    data,
    sampling_rate,
    *args,
    object_history=False,
    alg_name="interpolate",
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    handles_dead_data=False,
    method="weighted_average_slopes",
    starttime=None,
    npts=None,
    time_shift=0.0,
    **kwargs,
):
    """
    This function interpolates data, which is a mspasspy object. Note it is wrapped by mspass_func_wrapper,
    so the processing history and error logs can be preserved.

    :param data: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param sampling_rate: The new sampling rate in Hz.
    :param args: extra arguments.
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper.
    :param method: One of "linear", "nearest", "zero", "slinear", "quadratic", "cubic", "lanczos",
     or "weighted_average_slopes". You can refer to
     `Obspy <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.interpolate.html>` for details.
    :type method: str
    :param starttime: The start time (or timestamp) for the new interpolated stream.
     Will be set to current start time of the data if not given.
    :type starttime: :class:`~obspy.core.utcdatetime.UTCDateTime` or int
    :param npts: The new number of samples. Will be set to the best fitting number to retain the current end time
     of the trace if not given.
    :type npts: int
    :param time_shift: Shift the trace by adding time_shift to the starttime. The time shift is always given in seconds.
     A positive shift means the data is shifted towards the future, e.g. a positive time delta.
     Note that this parameter solely affects the metadata. The actual interpolation of the underlaying data is governed
     by the parameters sampling_rate, starttime and npts.
    :param kwargs: extra kv arguments
    :return: None.
    """
    data.interpolate(
        sampling_rate, method, starttime, npts, time_shift, *args, **kwargs
    )


@mspass_func_wrapper_multi
@timeseries_as_trace
def correlate(
    a,
    b,
    shift,
    object_history=False,
    alg_name="correlate",
    alg_id=None,
    dryrun=False,
    demean=True,
    normalize="naive",
    method="auto",
):
    """
    Cross-correlation of two signals up to a specified maximal shift.

    :param a: first signal
    :param b: second signal
    :param shift: Number of samples to shift for cross correlation. The cross-correlation will consist of 2*shift+1 or
        2*shift samples. The sample with zero shift will be in the middle.
    :param object_history: True to preserve the processing history. For details, refer to :class:`~mspasspy.util.decorators.mspass_func_wrapper_multi`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param demean: Demean data beforehand.
    :param normalize: Method for normalization of cross-correlation. One of 'naive' or None (True and False are
        supported for backwards compatibility). 'naive' normalizes by the overall standard deviation. None does not normalize.
    :param method: Method to use to calculate the correlation. 'direct': The correlation is determined directly from
        sums, the definition of correlation. 'fft' The Fast Fourier Transform is used to perform the correlation more
        quickly. 'auto' Automatically chooses direct or Fourier method based on an estimate of which is faster.
        (Only available for SciPy versions >= 0.19. For older Scipy version method defaults to 'fft'.)
    :type a: :class:`~mspasspy.ccore.seismic.TimeSeries`
    :type b: :class:`~mspasspy.ccore.seismic.TimeSeries`
    :type shift: int
    :type demean: bool
    :type method: str
    :return: cross-correlation function.
    """
    return obspy.signal.cross_correlation.correlate(
        a, b, shift, demean, normalize, method
    )


@mspass_func_wrapper
@timeseries_as_trace
def correlate_template(
    data,
    template,
    object_history=False,
    alg_name="correlate_template",
    alg_id=None,
    dryrun=False,
    mode="valid",
    normalize="full",
    demean=True,
    method="auto",
):
    """
    Correlate a time series against a template signal.

    This is an MsPASS wrapper around
    :func:`obspy.signal.cross_correlation.correlate_template`.  MsPASS
    :class:`TimeSeries` inputs are converted to ObsPy ``Trace`` objects for
    the call.

    :param data: input data to search.
    :type data: :class:`mspasspy.ccore.seismic.TimeSeries`
    :param template: template signal to correlate with ``data``.
    :type template: :class:`mspasspy.ccore.seismic.TimeSeries`
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param mode: correlation mode passed to ObsPy.
    :param normalize: normalization mode passed to ObsPy.
    :param demean: when True remove means before correlation.
    :param method: correlation method passed to ObsPy.
    :return: correlation sequence returned by ObsPy.
    """
    return obspy.signal.cross_correlation.correlate_template(
        data, template, mode, normalize, demean, method
    )


@mspass_func_wrapper
@timeseries_ensemble_as_stream
@seismogram_as_stream
def correlate_stream_template(
    stream,
    template,
    object_history=False,
    alg_name="correlate_stream_template",
    alg_id=None,
    dryrun=False,
    template_time=None,
    return_type="seismogram",
    handles_dead_data=True,
    **kwargs,
):
    """
    Correlate a multichannel data object against a stream template.

    This wrapper converts MsPASS ensemble or seismogram inputs to ObsPy
    ``Stream`` objects, calls
    :func:`obspy.signal.cross_correlation.correlate_stream_template`, and
    converts the result back to an MsPASS container selected by
    ``return_type``.

    :param stream: input :class:`Seismogram` or :class:`TimeSeriesEnsemble`.
    :param template: template stream-compatible object.
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param template_time: optional template reference time passed to ObsPy.
    :param return_type: ``"seismogram"`` or ``"timeseries_ensemble"``.
    :param handles_dead_data: wrapper flag declaring dead-data handling.
    :param kwargs: additional wrapper options.
    :return: converted correlation result.
    :rtype: :class:`mspasspy.ccore.seismic.Seismogram` or
        :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble`
    :raises TypeError: if ``return_type`` is not supported.
    """
    res = obspy.signal.cross_correlation.correlate_stream_template(
        stream, template, template_time
    )
    if return_type == "seismogram":
        return Stream2Seismogram(res, cardinal=True)
    elif return_type == "timeseries_ensemble":
        return Stream2TimeSeriesEnsemble(res)
    else:
        raise TypeError("Only seismogram and timeseries_ensemble types are supported")


@mspass_func_wrapper
@timeseries_ensemble_as_stream
@seismogram_as_stream
def correlation_detector(
    stream,
    templates,
    heights,
    distance,
    object_history=False,
    alg_name="correlation_detector",
    alg_id=None,
    dryrun=False,
    template_times=None,
    template_magnitudes=None,
    template_names=None,
    similarity_func=None,
    details=None,
    plot=None,
    return_type="seismogram",
    handles_dead_data=False,
    **kwargs,
):
    """
    Run ObsPy's correlation detector on MsPASS data.

    Input data and templates are converted to ObsPy streams before calling
    :func:`obspy.signal.cross_correlation.correlation_detector`.  The
    returned detections and similarity traces keep ObsPy's native return
    shape.

    :param stream: input :class:`Seismogram` or :class:`TimeSeriesEnsemble`.
    :param templates: iterable of template objects convertible to ObsPy
        streams.
    :param heights: detection threshold or thresholds passed to ObsPy.
    :param distance: minimum separation between detections passed to ObsPy.
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param template_times: optional template times passed to ObsPy.
    :param template_magnitudes: optional template magnitudes passed to ObsPy.
    :param template_names: optional template names passed to ObsPy.
    :param similarity_func: optional ObsPy similarity function.
    :param details: optional ObsPy details flag.
    :param plot: optional ObsPy plotting flag or target.
    :param return_type: retained for backward compatibility; ignored because
        ObsPy returns detection dictionaries rather than waveform streams.
    :param handles_dead_data: wrapper flag declaring dead-data handling.
    :param kwargs: additional options passed to ObsPy.
    :return: tuple ``(detections, similarities)`` returned by ObsPy.  The
        detections are dictionaries describing each match and the similarities
        are ObsPy similarity traces.
    :rtype: tuple
    """
    tem_list = []
    for template in templates:
        tem_list.append(template.toStream())
    detections, sims = obspy.signal.cross_correlation.correlation_detector(
        stream,
        tem_list,
        heights,
        distance,
        template_times,
        template_magnitudes,
        template_names,
        similarity_func,
        details,
        plot,
        **kwargs,
    )
    return detections, sims


@mspass_func_wrapper
@timeseries_ensemble_as_stream
@seismogram_as_stream
def templates_max_similarity(
    st,
    time,
    streams_templates,
    object_history=False,
    alg_name="templates_max_similarity",
    alg_id=None,
    dryrun=False,
    handles_dead_data=False,
):
    """
    Compute the maximum similarity across multiple templates.

    :param st: input :class:`Seismogram` or :class:`TimeSeriesEnsemble`.
    :param time: time at which similarity is evaluated.
    :param streams_templates: template objects convertible to ObsPy streams.
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param handles_dead_data: wrapper flag declaring dead-data handling.
    :return: maximum similarity value returned by ObsPy.
    """
    tem_list = []
    for template in streams_templates:
        tem_list.append(template.toStream())
    return obspy.signal.cross_correlation.templates_max_similarity(st, time, tem_list)


@mspass_func_wrapper_multi
@seismogram_as_stream
def xcorr_3c(
    st1,
    st2,
    shift_len,
    object_history=False,
    alg_name="xcor_3c",
    alg_id=None,
    dryrun=False,
    components=None,
    full_xcorr=False,
    abs_max=True,
):
    """
    Cross-correlate two three-component seismograms.

    This is an MsPASS wrapper for
    :func:`obspy.signal.cross_correlation.xcorr_3c`.  Inputs are converted
    to ObsPy ``Stream`` objects before correlation.

    :param st1: first three-component seismogram.
    :param st2: second three-component seismogram.
    :param shift_len: maximum sample shift.
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param components: component codes passed to ObsPy; defaults to
        ``["Z", "N", "E"]``.
    :param full_xcorr: when True return the full cross-correlation function.
    :param abs_max: when True choose the absolute maximum correlation.
    :return: ObsPy cross-correlation result.
    """
    if components is None:
        components = ["Z", "N", "E"]
    return obspy.signal.cross_correlation.xcorr_3c(
        st1, st2, shift_len, components, full_xcorr, abs_max
    )


@mspass_func_wrapper
@timeseries_as_trace
def xcorr_max(
    data,
    object_history=False,
    alg_name="xcor_max",
    alg_id=None,
    dryrun=False,
    abs_max=True,
    handles_dead_data=False,
):
    """
    Return the maximum of a cross-correlation sequence.

    :param data: cross-correlation sequence as a :class:`TimeSeries` or
        compatible object converted to an ObsPy ``Trace``.
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param abs_max: when True choose the absolute maximum correlation.
    :param handles_dead_data: wrapper flag declaring dead-data handling.
    :return: sample shift and correlation value returned by ObsPy.
    """
    return obspy.signal.cross_correlation.xcorr_max(data, abs_max)


@mspass_func_wrapper_multi
@timeseries_as_trace
def xcorr_pick_correction(
    trace1,
    trace2,
    pick1,
    pick2,
    t_before,
    t_after,
    cc_maxlag,
    object_history=False,
    alg_name="xcorr_pick_correction",
    alg_id=None,
    dryrun=False,
    filter=None,
    filter_options={},
    plot=False,
    filename=None,
):
    """
    Estimate differential pick correction by cross-correlation.

    This is an MsPASS wrapper for
    :func:`obspy.signal.cross_correlation.xcorr_pick_correction`;
    :class:`TimeSeries` inputs are converted to ObsPy ``Trace`` objects.

    :param trace1: first waveform.
    :param trace2: second waveform.
    :param pick1: pick time on ``trace1``.
    :param pick2: pick time on ``trace2``.
    :param t_before: window start time before each pick.
    :param t_after: window end time after each pick.
    :param cc_maxlag: maximum lag allowed for the correction.
    :param object_history: enable MsPASS object-history logging.
    :param alg_name: algorithm name stored in object history.
    :param alg_id: optional algorithm id stored in object history.
    :param dryrun: validate wrapper behavior without changing data.
    :param filter: optional ObsPy filter name.
    :param filter_options: filter options passed to ObsPy.
    :param plot: optional ObsPy plotting flag.
    :param filename: optional output filename for plots.
    :return: pick correction and correlation coefficient returned by ObsPy.
    """
    return obspy.signal.cross_correlation.xcorr_pick_correction(
        pick1,
        trace1,
        pick2,
        trace2,
        t_before,
        t_after,
        cc_maxlag,
        filter,
        filter_options,
        plot,
        filename,
    )
