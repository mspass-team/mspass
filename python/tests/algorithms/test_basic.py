import sys
import obspy
import numpy as np
from obspy import UTCDateTime

from mspasspy.algorithms.basic import ator, rtoa, rotate, rotate_to_standard, free_surface_transformation, transform

# module to test
sys.path.append("python/tests")

from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_sin_timeseries)

def test_ator():
    ts = get_live_timeseries()
    ts_new = ator(ts, 1)
    assert ts_new is not None