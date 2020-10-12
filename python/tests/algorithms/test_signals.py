import sys
import pytest
import obspy
import numpy as np

import mspasspy.ccore as mspass
from mspasspy.ccore import (Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble)

# module to test
sys.path.append("python/tests")

from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble,
                    get_stream,
                    get_trace)

from mspasspy.algorithms.signals import (filter,
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
    filter(ts, "bandpass", freqmin=10, freqmax=20, preserve_history=True, instance='0')
    filter(seis, "bandpass", freqmin=10, freqmax=20, preserve_history=True, instance='0')
    filter(tse, "bandpass", freqmin=10, freqmax=20, preserve_history=True, instance='0')
    filter(seis_e, "bandpass",  freqmin=10, freqmax=20, preserve_history=True, instance='0')
    filter(ts, "bandstop", freqmin=10, freqmax=20)
    filter(ts, "lowpass", freq=10)
    filter(ts, "highpass", freq=1)
    filter(ts, "lowpass_cheby_2", freq=10)
    # filter(ts, "lowpass_fir", freq=10)
    # filter(ts, "remez_fir", freqmin=10, freqmax=20)

    # todo validate the alhorithm

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
    interpolate(ts, 255, method='slinear', preserve_history=True, instance='0')
    interpolate(ts, 255, method='linear', preserve_history=True, instance='0')
    interpolate(ts, 255, method='nearest', preserve_history=True, instance='0')
    interpolate(ts, 255, method='zero', preserve_history=True, instance='0')

def test_correlate():
    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    res = correlate(ts1, ts2, 2, preserve_history=True, instance='0')

def test_correlate_template():
    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    correlate_template(ts1, ts2, preserve_history=True, instance='0')

def test_correlate_stream_template():
    tse1 = get_live_timeseries_ensemble(3)
    tse2 = get_live_timeseries_ensemble(3)
    correlate_stream_template(tse1, tse2, preserve_history=True, instance='0')

def test_correlation_detector():
    tse1 = get_live_timeseries_ensemble(3)
    seis_e = get_live_seismogram_ensemble(3)
    correlation_detector(tse1, seis_e, 0.5, 10)

def test_templates_max_similarity():
    tse1 = get_live_timeseries_ensemble(3)
    tse2 = get_live_timeseries_ensemble(3)
    templates_max_similarity(tse1, 0, [tse2])

def test_xcorr_3c():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    xcorr_3c(seis1, seis2, 1)

def test_xcorr_max():
    ts1 = get_live_timeseries()
    xcorr_max(ts1)

def test_xcorr_pick_correction():
    pass

if __name__ == "__main__":
    test_xcorr_max()