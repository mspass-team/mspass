import numpy as np

import obspy.signal.filter
import obspy.signal.detrend
import obspy.signal.cross_correlation
import obspy.signal.interpolation
from obspy.core.stream import Stream
from obspy.core.trace import Trace

import mspasspy.ccore as mspass
from mspasspy.util import logging_helper
from mspasspy.util.decorators import (mspass_func_wrapper,
                                      mspass_func_wrapper_multi,
                                      timeseries_as_trace,
                                      seismogram_as_stream,
                                      timeseries_ensemble_as_stream,
                                      seismogram_ensemble_as_stream)

from mspasspy.io.converter import (TimeSeries2Trace,
                                   Seismogram2Stream,
                                   TimeSeriesEnsemble2Stream,
                                   SeismogramEnsemble2Stream,
                                   Stream2Seismogram,
                                   Trace2TimeSeries,
                                   Stream2TimeSeriesEnsemble,
                                   Stream2SeismogramEnsemble)

@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def filter(data, type, *args, preserve_history=False, instance=None, dryrun=False, inplace_return=True, **options):
    data.filter(type, **options)  # inplace filtering


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def detrend(data, *args, preserve_history=False, instance=None, dryrun=False, inplace_return=True, type='simple', **options):
    data.detrend(type, **options)


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def interpolate(data, sampling_rate, *args,preserve_history=False, instance=None, dryrun=False, inplace_return=True,
                method='weighted_average_slopes', starttime=None, npts=None, time_shift=0.0, **kwargs):
    data.interpolate(sampling_rate, method, starttime, npts, time_shift, *args, **kwargs)


@mspass_func_wrapper_multi
@timeseries_as_trace
def correlate(a, b, shift, preserve_history=False, instance=None, dryrun=False,
              demean=True, normalize='naive', method='auto', domain=None):
    # only accepts two timeseries inputs as trace
    return obspy.signal.cross_correlation.correlate(a, b, shift, demean, normalize, method, domain)


@mspass_func_wrapper
@timeseries_as_trace
def correlate_template(data, template, preserve_history=False, instance=None, dryrun=False,
                       mode='valid', normalize='full', demean=True, method='auto'):
    return obspy.signal.cross_correlation.correlate_template(data, template, mode, normalize, demean, method)


@mspass_func_wrapper
@timeseries_ensemble_as_stream
@seismogram_as_stream
def correlate_stream_template(stream, template, preserve_history=False, instance=None, dryrun=False,
                              template_time=None, return_type="seismogram", **kwargs):
    res = obspy.signal.cross_correlation.correlate_stream_template(stream, template, template_time)
    if return_type == "seismogram":
        return Stream2Seismogram(res, cardinal=True)
    elif return_type == "timeseries_ensemble":
        return Stream2TimeSeriesEnsemble(res)
    else:
        raise TypeError("Only seismogram and timeseries_ensemble types are supported")


@mspass_func_wrapper
@timeseries_ensemble_as_stream
@seismogram_as_stream
def correlation_detector(stream, templates, heights, distance, preserve_history=False, instance=None, dryrun=False,
                         template_times=None, template_magnitudes=None, template_names=None, similarity_func=None,
                         details=None, plot=None, return_type="seismogram", **kwargs):
    tem_list = []
    for template in templates:
        tem_list.append(template.toStream())
    detections, sims = obspy.signal.cross_correlation.correlation_detector(stream, tem_list, heights, distance,
        template_times, template_magnitudes, template_names, similarity_func, details, plot, **kwargs)
    converted_detections = []
    for detection in detections:
        if return_type == "seismogram":
            converted_detections.append(Stream2Seismogram(detection, cardinal=True))
        elif return_type == "timeseries_ensemble":
            converted_detections.append(Stream2TimeSeriesEnsemble(detection))
        else:
            raise TypeError("Only seismogram and timeseries_ensemble types are supported")
    return converted_detections, sims


@mspass_func_wrapper
@timeseries_ensemble_as_stream
@seismogram_as_stream
def templates_max_similarity(st, time, streams_templates, preserve_history=False, instance=None, dryrun=False):
    tem_list = []
    for template in streams_templates:
        tem_list.append(template.toStream())
    return obspy.signal.cross_correlation.templates_max_similarity(st, time, tem_list)

@mspass_func_wrapper_multi
@seismogram_as_stream
def xcorr_3c(st1, st2, shift_len, preserve_history=False, instance=None, dryrun=False,
             components=None, full_xcorr=False, abs_max=True):
    if components is None:
        components = ['Z', 'N', 'E']
    return obspy.signal.cross_correlation.xcorr_3c(st1, st2, shift_len, components, full_xcorr, abs_max)

@mspass_func_wrapper
@timeseries_as_trace
def xcorr_max(data, preserve_history=False, instance=None, dryrun=False, abs_max=True,):
    return obspy.signal.cross_correlation.xcorr_max(data, abs_max)


@mspass_func_wrapper_multi
@timeseries_as_trace
def xcorr_pick_correction(trace1, trace2, pick1, pick2, t_before, t_after, cc_maxlag,
                          preserve_history=False, instance=None, dryrun=False, filter=None, filter_options={},
                          plot=False, filename=None):
    return obspy.signal.cross_correlation.xcorr_pick_correction(pick1, trace1, pick2, trace2, t_before, t_after,
                                                                cc_maxlag, filter, filter_options, plot, filename)

if __name__ == "__main__":
    pass
    # ts_size = 255
    # sampling_rate = 20.0
    #
    # # init a seismogram
    # seismogram = Seismogram()
    # seismogram.u = dmatrix(3, ts_size)
    # for i in range(3):
    #     for j in range(ts_size):
    #         seismogram.u[i,j] = np.random.rand()
    #
    # seismogram.live = True
    # seismogram.dt = 1/sampling_rate
    # seismogram.t0 = 0
    # seismogram.npts = ts_size
    # seismogram.put('net', 'IU')
    # seismogram.put('npts', ts_size)
    # seismogram.put('sampling_rate', sampling_rate)
    #
    # bandpass(seismogram, 1, 20, 1/seismogram.dt)
