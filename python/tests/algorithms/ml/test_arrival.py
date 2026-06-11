import numpy as np
import sys

import os
import pytest

pytest.importorskip("seisbench.models")
from mspasspy.algorithms.ml.arrival import annotate_arrival_time
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.util.converter import Trace2TimeSeries
from obspy import Stream, Trace, UTCDateTime, read

sys.path.append("python/tests")

from helper import get_live_timeseries


class _LocalPhaseNetModel:
    channel = "PhaseNet_P"
    probabilities = np.array(
        [
            0.0,
            0.03,
            0.12,
            0.31,
            0.62,
            0.92,
            0.48,
            0.77,
            0.08,
            0.23,
            0.55,
            0.99,
        ]
    )

    def annotate(self, stream):
        source = stream[0]
        tr = Trace(self.probabilities.copy())
        tr.stats.starttime = source.stats.starttime
        tr.stats.channel = self.channel
        if tr.stats.npts > 1:
            source_duration = float(source.stats.endtime - source.stats.starttime)
            fallback_duration = float(source.stats.delta) * (tr.stats.npts - 1)
            prediction_duration = max(source_duration, fallback_duration)
            tr.stats.delta = prediction_duration / (tr.stats.npts - 1)
        else:
            tr.stats.delta = source.stats.delta
        return Stream([tr])


pn_model = _LocalPhaseNetModel()


def test_annotate_arrival_time():
    """
    Test the annotate_arrival_time function.
    1. Test that the output is a dictionary where keys are the arrival times and values are the probabilities
    2. Test the result is the same as the one from directly usingseisbench
    """
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries, 0, model=pn_model)
    mspass_picks = timeseries["p_wave_picks"]  # should be a dictionary
    # assert that for each pick, the value is a float number that is between 0 and 1
    assert all(0 <= value <= 1 for value in mspass_picks.values())

    # picks from seisbench
    pn_preds = pn_model.annotate(Stream(stream[0]))
    trace = pn_preds[0]
    assert trace.stats.channel == "PhaseNet_P"
    seis_picks = trace.times("timestamp")  # should be an array

    # Convert both to sets of rounded values
    mspass_set = set(round(v, 6) for v in mspass_picks.keys())
    seis_set = set(round(v, 6) for v in seis_picks)

    # Compare the sets
    assert mspass_set == seis_set


def test_annotate_arrival_time_threshold():
    """
    Test the annotate_arrival_time function with a threshold.
    1. Test that all picks with probability less than the threshold are removed
    """
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries, 0.5, model=pn_model)
    mspass_picks = timeseries["p_wave_picks"]
    assert all(value >= 0.5 for value in mspass_picks.values())
    # picks from seisbench
    pn_preds = pn_model.annotate(Stream(stream[0]))
    trace = pn_preds[0]
    assert trace.stats.channel == "PhaseNet_P"
    seis_picks = trace.times("timestamp")

    # assert that the number of picks from mspass is less than the number of picks from seisbench
    assert len(mspass_picks) < len(seis_picks)


def test_annotate_arrival_time_window():
    """
    Test the annotate_arrival_time function with a time window.
    1. Test that all picks should be within the time window
    2. Test the result is the same as the one from directly using seisbench
    """
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    window_start = UTCDateTime(2009, 4, 6, 1, 30).timestamp
    window_end = window_start + 1000
    annotate_arrival_time(
        timeseries,
        threshold=0,
        time_window=TimeWindow(window_start, window_end),
        model=pn_model,
    )
    mspass_picks = timeseries["p_wave_picks"]

    assert len(mspass_picks.keys()) > 0
    mspass_keys = np.array(list(mspass_picks.keys()))
    assert np.all(mspass_keys >= window_start)
    assert np.all(mspass_keys <= window_end)

    # picks from seisbench
    new_stream = Stream(stream[0])
    new_stream.trim(UTCDateTime(window_start), UTCDateTime(window_end))
    pn_preds = pn_model.annotate(new_stream)
    seis_picks = pn_preds[0].times("timestamp")

    # Convert both to sets of rounded values
    mspass_set = set(round(v, 6) for v in mspass_picks.keys())
    seis_set = set(round(v, 6) for v in seis_picks)

    # Compare the sets
    assert mspass_set == seis_set


class _BoundaryPredictionModel:
    channel = "PhaseNet_P"

    def annotate(self, stream):
        tr = Trace(np.ones(stream[0].stats.npts))
        tr.stats.starttime = stream[0].stats.starttime
        tr.stats.delta = stream[0].stats.delta
        tr.stats.channel = self.channel
        return Stream([tr])


def test_annotate_arrival_time_filters_trimmed_boundary_samples():
    trace = Trace(np.zeros(10))
    trace.stats.starttime = UTCDateTime(0)
    trace.stats.delta = 1.0
    timeseries = Trace2TimeSeries(trace)

    window_start = 0.4
    window_end = 3.6
    annotate_arrival_time(
        timeseries,
        threshold=0,
        time_window=TimeWindow(window_start, window_end),
        model=_BoundaryPredictionModel(),
    )

    picks = timeseries["p_wave_picks"]
    assert set(picks.keys()) == {1.0, 2.0, 3.0}
    assert all(window_start <= pick_time <= window_end for pick_time in picks)


class _NoPPhasePredictionModel(_BoundaryPredictionModel):
    channel = "PhaseNet_S"


def test_annotate_arrival_time_handles_missing_p_trace():
    trace = Trace(np.zeros(10))
    trace.stats.starttime = UTCDateTime(0)
    trace.stats.delta = 1.0
    timeseries = Trace2TimeSeries(trace)

    annotate_arrival_time(timeseries, threshold=0, model=_NoPPhasePredictionModel())

    assert timeseries["p_wave_picks"] == {}


def test_annotate_arrival_time_for_mseed():
    """
    Test the annotate_arrival_time function for a mseed file.
    1. Test that all picks should be within the time window
    2. Test all picks should have probability greater than or equal to 0.1
    3. Test the result is the subset of the picks from seisbench (no threshold applied)
    """
    trace = get_mseed_trace_for_test()
    timeseries = Trace2TimeSeries(trace)

    window_start = UTCDateTime(2011, 3, 11, 6, 35).timestamp
    window_end = window_start + 1200  # 20 minutes
    annotate_arrival_time(
        timeseries,
        0.1,
        time_window=TimeWindow(window_start, window_end),
        model=pn_model,
    )
    mspass_picks = timeseries["p_wave_picks"]

    # assert the picks are not empty
    assert len(mspass_picks.keys()) > 0

    # assert that all picks should have probability greater than or equal to 0.5
    assert all(value >= 0.1 for value in mspass_picks.values())

    # assert that all picks (keys) should be within the time window
    assert all(key >= window_start for key in mspass_picks.keys())
    assert all(key <= window_end for key in mspass_picks.keys())

    # picks from seisbench
    new_stream = Stream(trace)
    new_stream.trim(UTCDateTime(window_start), UTCDateTime(window_end))
    pn_preds = pn_model.annotate(new_stream)
    seis_picks = pn_preds[0].times("timestamp")

    # Convert both to sets of rounded values
    mspass_set = set(round(v, 6) for v in mspass_picks.keys())
    seis_set = set(round(v, 6) for v in seis_picks)

    # every pick in mspass should be in seisbench
    assert mspass_set.issubset(seis_set)

    # assert that the number of picks from mspass is less than the number of picks from seisbench
    assert len(mspass_picks) < len(seis_picks)


def get_mseed_trace_for_test():
    file_path = os.path.join(os.getcwd(), "python/tests/data/db_mseeds/test_277.mseed")
    st = read(file_path)
    # start time of the trace: Fri Mar 11 2011 06:34:33 GMT+0000
    # end time of the trace: Fri Mar 11 2011 06:59:09 GMT+0000
    return st[0]


def get_trace_for_test():
    trace = get_mseed_trace_for_test().copy()
    trace.stats.starttime = UTCDateTime(2009, 4, 6, 1, 30)
    return Stream([trace])


if __name__ == "__main__":
    test_annotate_arrival_time_window()