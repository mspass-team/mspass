import numpy as np
import sys

import os
import seisbench.models as sbm
from mspasspy.algorithms.ml.arrival import annotate_arrival_time
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.util.converter import Trace2TimeSeries
from obspy import Stream, UTCDateTime, read
from obspy.clients.fdsn import Client

sys.path.append("python/tests")

from helper import get_live_timeseries

pn_model = sbm.PhaseNet.from_pretrained("stead")


def test_annotate_arrival_time():
    """
    Test the annotate_arrival_time function.
    1. Test that the output is a dictionary where keys are the arrival times and values are the probabilities
    2. Test the result is the same as the one from directly usingseisbench
    """
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries, 0)
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
    annotate_arrival_time(timeseries, 0.5)
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
        timeseries, threshold=0, time_window=TimeWindow(window_start, window_end)
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
        timeseries, 0.1, time_window=TimeWindow(window_start, window_end)
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
    client = Client("INGV")
    t = UTCDateTime(2009, 4, 6, 1, 30)
    return client.get_waveforms(
        network="MN",
        station="AQU",
        location="*",
        channel="HH?",
        starttime=t,
        endtime=t + 3600,
    )


if __name__ == "__main__":
    test_annotate_arrival_time_window()
