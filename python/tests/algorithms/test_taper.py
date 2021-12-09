import sys
from mspasspy.algorithms.basic import linear_taper, cosine_taper, vector_taper
import numpy as np
import pickle
import random
import math
import copy

from mspasspy.ccore.seismic import Seismogram, SlownessVector
from mspasspy.ccore.utility import SphericalCoordinate
from mspasspy.ccore.algorithms.basic import LinearTaper, CosineTaper, VectorTaper, _TopMute

from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_sin_timeseries)

# module to test
sys.path.append("python/tests")

def test_vectorTaper():
    vTaperData = []
    for i in range(200):
        vTaperData.append(random.random())
    vtaper = VectorTaper(vTaperData)
    v_data = pickle.dumps(vtaper)
    v_copy = pickle.loads(v_data)   #   It seems not easy to compare the copy and the original one.

    ts = get_live_timeseries()
    ts.t0 = 0
    ts.dt = 1
    ts.npts = 200
    ts.data += 1
    ts_copy = copy.deepcopy(ts)

    vtaper.apply(ts)
    v_copy.apply(ts_copy)

    assert len(ts.data) == len(ts_copy.data)
    for i in range(len(ts.data)):
        assert math.isclose(ts.data[i], ts_copy.data[i])

def test_cosineTaper():
    t0h = 4
    t1h = 14
    t0t = 170
    t1t = 180

    costaper = CosineTaper(t0h, t1h, t1t, t0t)
    assert math.isclose(t0h, costaper.get_t0head())
    assert math.isclose(t1h, costaper.get_t1head())
    assert math.isclose(t0t, costaper.get_t0tail())
    assert math.isclose(t1t, costaper.get_t1tail())
    
    cos_data = pickle.dumps(costaper)
    cos_copy = pickle.loads(cos_data)
    assert math.isclose(cos_copy.get_t0head(), costaper.get_t0head())
    assert math.isclose(cos_copy.get_t1head(), costaper.get_t1head())
    assert math.isclose(cos_copy.get_t0tail(), costaper.get_t0tail())
    assert math.isclose(cos_copy.get_t1tail(), costaper.get_t1tail())

def test_linearTaper():
    t0h = 4
    t1h = 14
    t0t = 170
    t1t = 180

    ltaper = LinearTaper(t0h, t1h, t1t, t0t)
    assert math.isclose(t0h, ltaper.get_t0head())
    assert math.isclose(t1h, ltaper.get_t1head())
    assert math.isclose(t0t, ltaper.get_t0tail())
    assert math.isclose(t1t, ltaper.get_t1tail())

    l_data = pickle.dumps(ltaper)
    l_copy = pickle.loads(l_data)
    assert math.isclose(l_copy.get_t1head(), ltaper.get_t1head())
    assert math.isclose(l_copy.get_t0tail(), ltaper.get_t0tail())
    assert math.isclose(l_copy.get_t1tail(), ltaper.get_t1tail())

def test_topMute():
    t0 = 4
    t1 = 14
    lmute = _TopMute(t0, t1, "linear")
    cmute = _TopMute(t0, t1, "cosine")

    assert math.isclose(t0, lmute.get_t0())
    assert math.isclose(t1, lmute.get_t1())
    assert "linear" == lmute.taper_type()

    assert math.isclose(t0, cmute.get_t0())
    assert math.isclose(t1, cmute.get_t1())
    assert "cosine" == cmute.taper_type()

    ts_l = get_live_timeseries()
    ts_l.t0 = 0
    ts_l.dt = 1
    ts_l.npts = 200
    ts_l.data += 1

    lmute.apply(ts_l)
    assert ts_l.data[4] == 0
    assert ts_l.data[9] == 0.5
    assert ts_l.data[14] == 1

    ts_c = get_live_timeseries()
    ts_c.t0 = 0
    ts_c.dt = 1
    ts_c.npts = 200
    ts_c.data += 1

    cmute.apply(ts_c)
    assert ts_c.data[4] == 0
    assert ts_c.data[9] == 0.5
    assert ts_c.data[14] == 1

def test_taper_wrapper():
    ts = get_live_timeseries()
    ts.t0 = 0
    ts.dt = 1
    ts.npts = 200
    ts.data += 1

    ts_l = linear_taper(ts, 4, 14, 170, 180)
    assert ts_l.data[4] == 0
    assert ts_l.data[9] == 0.5
    assert ts_l.data[14] == 1
    assert ts_l.data[170] == 1
    assert ts_l.data[175] == 0.5
    assert ts_l.data[180] == 0

    
    ts.npts = 200
    ts.data += 1
    
    ts_c = cosine_taper(ts, 4, 14, 170, 180)
    assert ts_c.data[4] == 0
    assert ts_c.data[9] == 0.5
    assert ts_c.data[14] == 1
    assert ts_c.data[170] == 1
    assert ts_c.data[175] == 0.5
    assert ts_c.data[180] == 0

    ts.npts = 200
    ts.data += 1
    
    vtaper = np.zeros(200)
    vtaper += 0.5
    ts_v = vector_taper(ts, vtaper)
    assert ts_v.data[4] == 0.5
    assert ts_v.data[9] == 0.5
    assert ts_v.data[14] == 0.5
    assert ts_v.data[170] == 0.5
    assert ts_v.data[175] == 0.5
    assert ts_v.data[180] == 0.5