import numpy as np
import sys

import seisbench.models as sbm
from mspasspy.algorithms.ml.arrival import annotate_arrival_time
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.util.converter import Trace2TimeSeries
from obspy import Stream, UTCDateTime
from obspy.clients.fdsn import Client

sys.path.append("python/tests")

from helper import get_live_timeseries

pn_model = sbm.PhaseNet.from_pretrained("stead")

def test_annotate_arrival_time():
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries, 0)
    mspass_picks = timeseries["p_wave_picks"]

    # picks from seisbench
    pn_preds = pn_model.annotate(Stream(stream[0]))
    trace = pn_preds[0]
    assert trace.stats.channel == "PhaseNet_P"
    seis_picks = trace.times()

    assert np.array_equal(mspass_picks, seis_picks)

def test_annotate_arrival_time_threshold():
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries, 0.5)
    mspass_picks = timeseries["p_wave_picks"]

    # picks from seisbench
    pn_preds = pn_model.annotate(Stream(stream[0]))
    trace = pn_preds[0]
    assert trace.stats.channel == "PhaseNet_P"
    seis_picks = trace.times()

    assert len(mspass_picks) < len(seis_picks)

def test_annotate_arrival_time_window():
    stream = get_trace_for_test()

    # picks from mspass
    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries, threshold = 0, time_window=TimeWindow(100, 1000))
    mspass_picks = timeseries["p_wave_picks"]

    assert len(mspass_picks) > 0
    assert np.all(mspass_picks >= 100)
    assert np.all(mspass_picks <= 1000)

    # picks from seisbench
    new_stream = Stream(stream[0])
    new_stream.trim(starttime=new_stream[0].stats.starttime + 100, endtime=new_stream[0].stats.starttime + 1000)
    pn_preds = pn_model.annotate(new_stream)
    seis_picks = pn_preds[0].times() + 100

    assert np.array_equal(mspass_picks, seis_picks)

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
