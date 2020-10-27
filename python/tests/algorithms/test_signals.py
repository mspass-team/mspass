import sys
import pytest
import obspy
import obspy.signal.cross_correlation
import numpy as np
from obspy import UTCDateTime, read, Trace

from mspasspy.ccore.seismic import (Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble)

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

from mspasspy.io.converter import (TimeSeries2Trace,
                                   Seismogram2Stream,
                                   TimeSeriesEnsemble2Stream,
                                   SeismogramEnsemble2Stream,
                                   Stream2Seismogram,
                                   Trace2TimeSeries,
                                   Stream2TimeSeriesEnsemble,
                                   Stream2SeismogramEnsemble)

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
    tr.data = np.array(ts.data)
    copy = np.array(ts.data)
    tr.stats.sampling_rate = 20
    tr.filter("bandpass", freqmin=1, freqmax=5)
    filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    assert all(a == b for a,b in zip(ts.data, tr.data))
    assert not all(a == b for a, b in zip(ts.data, copy))

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
    tr.data = np.array(ts.data)
    copy = np.array(ts.data)
    tr.stats.sampling_rate = 20
    tr.detrend(type="simple")
    detrend(ts, "simple", preserve_history=True, instance='0')
    assert all(a == b for a, b in zip(ts.data, tr.data))
    assert not all(a == b for a, b in zip(ts.data, copy))

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
    tr.data = np.array(ts.data)
    copy = np.array(ts.data)
    tr.stats.sampling_rate = 20
    tr.interpolate(255, method="zero")
    interpolate(ts, 255, method='zero', preserve_history=True, instance='0')
    assert all(a == b for a, b in zip(ts.data, tr.data))
    assert not all(a == b for a, b in zip(ts.data, copy))

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
        assert all(a == b for a, b in zip(res1.data[i,:], res2[i].data))

def test_correlation_detector():
    template = read().filter('highpass', freq=5).normalize()
    pick = UTCDateTime('2009-08-24T00:20:07.73')
    template.trim(pick, pick + 10)
    n1 = len(template[0])
    n2 = 100 * 3600  # 1 hour
    dt = template[0].stats.delta
    # shift one template Trace
    template[1].stats.starttime += 5
    stream = template.copy()
    np.random.seed(42)
    for tr, trt in zip(stream, template):
        tr.stats.starttime += 24 * 3600
        tr.data = np.random.random(n2) - 0.5  # noise
        if tr.stats.channel[-1] == 'Z':
            tr.data[n1:2 * n1] += 10 * trt.data
            tr.data = tr.data[:-n1]
        tr.data[5 * n1:6 * n1] += 100 * trt.data
        tr.data[20 * n1:21 * n1] += 2 * trt.data
    # make one template trace a bit shorter
    template[2].data = template[2].data[:-n1 // 5]
    # make two stream traces a bit shorter
    stream[0].trim(5, None)
    stream[1].trim(1, 20)
    detections, sims = obspy.signal.cross_correlation.correlation_detector(stream, template, 0.2, 30)

    # fixme seed id problem
    # tse = Stream2Seismogram(stream, cardinal=True)
    # tem = Stream2Seismogram(template, cardinal=True)
    # detections2, sims2 = correlation_detector(tse, [tem], 0.2, 30)

    # seis = get_live_seismogram()
    # seis_list = []
    # for i in range(3):
    #     seis_list.append(get_live_timeseries_ensemble(2))
    # st1 = seis.toStream()
    # tem_lists = []
    # for i in range(3):
    #     tem_lists.append(seis_list[i].toStream())
    # res, sims = correlation_detector(seis, seis_list, 0.5, 10, return_type="timeseries_ensemble")
    # res2, sims2 = obspy.signal.cross_correlation.correlation_detector(st1, tem_lists, 0.5, 10)
    # assert all(a==b for a, b in zip(sims, sims2))


def test_templates_max_similarity():
    # fixme seed id problem
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
    st1 = read('./python/tests/data/BW.UH1._.EHZ.D.2010.147.a.slist.gz')
    st2 = read('./python/tests/data/BW.UH1._.EHZ.D.2010.147.b.slist.gz')
    tr1 = st1.select(component="Z")[0]
    tr2 = st2.select(component="Z")[0]
    t1 = UTCDateTime("2010-05-27T16:24:33.315000Z")
    t2 = UTCDateTime("2010-05-27T16:27:30.585000Z")

    ts1 = Trace2TimeSeries(tr1)
    ts2 = Trace2TimeSeries(tr2)

    dt, coeff = obspy.signal.cross_correlation.xcorr_pick_correction(t1, tr1, t2, tr2, 0.05, 0.2, 0.1)
    dt2, coeff2 = xcorr_pick_correction(ts1, ts2, t1, t2, 0.05, 0.2, 0.1)
    assert dt == dt2
    assert coeff == coeff2

if __name__ == "__main__":
    test_correlation_detector()