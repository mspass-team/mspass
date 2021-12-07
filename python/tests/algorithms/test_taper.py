import sys
import numpy as np
import pickle
import random
import math

from mspasspy.ccore.seismic import Seismogram, SlownessVector
from mspasspy.ccore.utility import SphericalCoordinate
from mspasspy.ccore.algorithms.basic import LinearTaper, CosineTaper, VectorTaper

# module to test
sys.path.append("python/tests")

def test_taper_pickle():
    t0h = random.random()
    t1h = random.random()
    t0t = random.random()
    t1t = random.random()

    ltaper = LinearTaper(t0h, t1h, t1t, t0t)
    assert math.isclose(t0h, ltaper.get_t0head())
    assert math.isclose(t1h, ltaper.get_t1head())
    assert math.isclose(t0t, ltaper.get_t0tail())
    assert math.isclose(t1t, ltaper.get_t1tail())
    l_data = pickle.dumps(ltaper)
    l_copy = pickle.loads(l_data)
    assert math.isclose(l_copy.get_t0head(), ltaper.get_t0head())
    assert math.isclose(l_copy.get_t1head(), ltaper.get_t1head())
    assert math.isclose(l_copy.get_t0tail(), ltaper.get_t0tail())
    assert math.isclose(l_copy.get_t1tail(), ltaper.get_t1tail())

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

    vTaperData = []
    for i in range(10):
        vTaperData.append(random.random())
    vtaper = VectorTaper(vTaperData)
    v_data = pickle.dumps(vtaper)
    v_copy = pickle.loads(v_data)   #   It seems not easy to compare the copy and the original one.
