import sys
import pytest
import obspy
import obspy.signal.cross_correlation
import numpy as np

import mspasspy.ccore as mspass
from mspasspy.ccore import (Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble)

# module to test
sys.path.append("python/tests")
sys.path.append("python/mspasspy/algorithms")

from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble,
                    get_stream,
                    get_trace)

from signals import (filter,
                     detrend,
                     interpolate,
                     correlate,
                     correlate_template,
                     correlate_stream_template,
                     correlation_detector,
                     templates_max_similarity,
                     xcorr_3c,
                     xcorr_max,
                     xcorr_pick_correction)

def test_filter():
    ts = get_live_timeseries()
    seis = get_live_seismogram()
    tse = get_live_timeseries_ensemble(3)
    seis_e = get_live_seismogram_ensemble(3)
    filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    filter(seis, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    filter(tse, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    filter(seis_e, "bandpass",  freqmin=1, freqmax=5, preserve_history=True, instance='0')
    filter(ts, "bandstop", freqmin=1, freqmax=5)
    filter(ts, "lowpass", freq=1)
    filter(ts, "highpass", freq=1)
    filter(ts, "lowpass_cheby_2", freq=1)

    # fixme fix testing warning
    # filter(ts, "lowpass_fir", freq=10) these two types are not supported
    # filter(ts, "remez_fir", freqmin=10, freqmax=20)

    # functionality verification testing
    ts = get_live_timeseries()
    tr = obspy.Trace()
    tr.data = np.array(ts.s)
    copy = np.array(ts.s)
    tr.stats.sampling_rate = 20
    tr.filter("bandpass", freqmin=1, freqmax=5)
    filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    assert all(a == b for a,b in zip(ts.s, tr.data))
    assert not all(a == b for a, b in zip(ts.s, copy))

def test_detrend():
    ts = get_live_timeseries()
    seis = get_live_seismogram()
    tse = get_live_timeseries_ensemble(3)
    seis_e = get_live_seismogram_ensemble(3)
    detrend(ts, preserve_history=True, instance='0')
    detrend(seis, preserve_history=True, instance='0')
    detrend(tse, preserve_history=True, instance='0')
    detrend(seis_e, preserve_history=True, instance='0')
    detrend(ts, type="linear", preserve_history=True, instance='0')
    detrend(ts, type="constant", preserve_history=True, instance='0')
    detrend(ts, type="polynomial", order=2, preserve_history=True, instance='0')
    detrend(ts, type="spline", order=2, dspline=1000, preserve_history=True, instance='0')

    # functionality verification testing
    ts = get_live_timeseries()
    tr = obspy.Trace()
    tr.data = np.array(ts.s)
    copy = np.array(ts.s)
    tr.stats.sampling_rate = 20
    tr.detrend(type="simple")
    detrend(ts, "simple", preserve_history=True, instance='0')
    assert all(a == b for a, b in zip(ts.s, tr.data))
    assert not all(a == b for a, b in zip(ts.s, copy))

def test_interpolate():
    ts = get_live_timeseries()
    seis = get_live_seismogram()
    tse = get_live_timeseries_ensemble(3)
    seis_e = get_live_seismogram_ensemble(3)
    interpolate(ts, 255, preserve_history=True, instance='0')
    interpolate(seis, 255, preserve_history=True, instance='0')
    interpolate(tse, 255, preserve_history=True, instance='0')
    interpolate(seis_e, 255, preserve_history=True, instance='0')
    interpolate(ts, 255, method='lanczos', a=20, preserve_history=True, instance='0')
    ts = get_live_timeseries()
    interpolate(ts, 25, method='slinear', preserve_history=True, instance='0')
    ts = get_live_timeseries()
    interpolate(ts, 255, method='linear', preserve_history=True, instance='0')
    ts = get_live_timeseries()
    interpolate(ts, 255, method='nearest', preserve_history=True, instance='0')
    ts = get_live_timeseries()
    interpolate(ts, 255, method='zero', preserve_history=True, instance='0')

    # functionality verification testing
    ts = get_live_timeseries()
    tr = obspy.Trace()
    tr.data = np.array(ts.s)
    copy = np.array(ts.s)
    tr.stats.sampling_rate = 20
    tr.interpolate(255, method="zero")
    interpolate(ts, 255, method='zero', preserve_history=True, instance='0')
    assert all(a == b for a, b in zip(ts.s, tr.data))
    assert not all(a == b for a, b in zip(ts.s, copy))

def test_correlate():
    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    tr1 = ts1.toTrace()
    tr2 = ts2.toTrace()
    res1 = correlate(ts1, ts2, 2, preserve_history=True, instance='0')
    res2 = obspy.signal.cross_correlation.correlate(tr1, tr2, 2)
    assert all(a==b for a,b in zip(res1, res2))

def test_correlate_template():
    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    tr1 = ts1.toTrace()
    tr2 = ts2.toTrace()
    res1 = correlate_template(ts1, ts2, preserve_history=True, instance='0')
    res2 = obspy.signal.cross_correlation.correlate_template(tr1, tr2)
    assert all(a == b for a, b in zip(res1, res2))

def test_correlate_stream_template():
    tse1 = get_live_seismogram()
    tse2 = get_live_seismogram()
    st1 = tse1.toStream()
    st2 = tse2.toStream()
    res1 = correlate_stream_template(tse1, tse2, preserve_history=True, instance='0')
    res2 = obspy.signal.cross_correlation.correlate_stream_template(st1, st2)
    for i in range(3):
        assert all(a == b for a, b in zip(res1.u[i,:], res2[i].data))

def test_correlation_detector():
    seis = get_live_seismogram()
    seis_list = []
    for i in range(3):
        seis_list.append(get_live_timeseries_ensemble(2))
    st1 = seis.toStream()
    tem_lists = []
    for i in range(3):
        tem_lists.append(seis_list[i].toStream())
    res, sims = correlation_detector(seis, seis_list, 0.5, 10, return_type="timeseries_ensemble")
    res2, sims2 = obspy.signal.cross_correlation.correlation_detector(st1, tem_lists, 0.5, 10)
    assert all(a==b for a, b in zip(sims, sims2))


def test_templates_max_similarity():
    tse1 = get_live_timeseries_ensemble(3)
    tse2 = get_live_timeseries_ensemble(3)
    st1 = tse1.toStream()
    st2 = tse2.toStream()
    res = templates_max_similarity(tse1, 0, [tse2])
    res2 = obspy.signal.cross_correlation.templates_max_similarity(st1, 0, [st2])
    assert res == res2

def test_xcorr_3c():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    st1 = seis1.toStream()
    st2 = seis2.toStream()
    res1 = xcorr_3c(seis1, seis2, 1)
    res2 = obspy.signal.cross_correlation.xcorr_3c(st1, st2, 1)
    assert res1 == res2

def test_xcorr_max():
    ts1 = get_live_timeseries()
    tr1 = ts1.toTrace()
    res1 = xcorr_max(ts1)
    res2 = obspy.signal.cross_correlation.xcorr_max(tr1)
    assert res1 == res2

def test_xcorr_pick_correction():
    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    tr1 = ts1.toTrace()
    tr2 = ts2.toTrace()
    # r1, r2 = xcorr_pick_correction(ts1, ts2, 10, 10, 0.1, 0.1, 0.10)
    # print(r1, r2)
    # rr1, rr2 = obspy.signal.cross_correlation.xcorr_pick_correction(10, tr1, 10, tr2, 0.05, 0.2, 0.10)
    # assert r2 == rr2

if __name__ == "__main__":
    test_xcorr_pick_correction()