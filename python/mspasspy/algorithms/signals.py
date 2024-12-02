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
    **options,
):
    """
    This function filters the data of mspasspy objects. Note it is wrapped by mspass_func_wrapper, so the processing
    history and error logs can be preserved.

    :param data: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param type: type of filter, 'bandpass', 'bandstop', 'lowpass', 'highpass', 'lowpass_cheby_2', 'lowpass_fir',
     'remez_fir'. You can refer to
     `Obspy <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.filter.html>` for details.
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
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper_multi`.
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
    **kwargs,
):
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
    **kwargs,
):
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
    converted_detections = []
    for detection in detections:
        if return_type == "seismogram":
            converted_detections.append(Stream2Seismogram(detection, cardinal=True))
        elif return_type == "timeseries_ensemble":
            converted_detections.append(Stream2TimeSeriesEnsemble(detection))
        else:
            raise TypeError(
                "Only seismogram and timeseries_ensemble types are supported"
            )
    return converted_detections, sims


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
):
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
):
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
