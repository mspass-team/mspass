import sys
from obspy.clients.fdsn import Client
from obspy import UTCDateTime

from mspasspy.util.converter import Trace2TimeSeries

sys.path.append("python/tests")

from helper import get_live_timeseries
from mspasspy.algorithms.ml.arrival import annotate_arrival_time

def test_annotate_arrival_time():
    client = Client("INGV")
    t = UTCDateTime(2009, 4, 6, 1, 30)
    stream = client.get_waveforms(network="MN", station="AQU", location="*", channel="HH?", starttime=t, endtime=t+3600)

    timeseries = Trace2TimeSeries(stream[0])
    annotate_arrival_time(timeseries)