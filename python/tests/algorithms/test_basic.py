import sys
import numpy as np

from mspasspy.ccore.seismic import Seismogram, SlownessVector
from mspasspy.ccore.utility import SphericalCoordinate
from mspasspy.algorithms.basic import (
    ator,
    rtoa,
    rotate,
    rotate_to_standard,
    free_surface_transformation,
    transform,
    linear_taper,
    cosine_taper,
    vector_taper,
)

# module to test
sys.path.append("python/tests")

from helper import get_live_seismogram, get_live_timeseries, get_sin_timeseries


def test_ator_rtoa():
    ts = get_live_timeseries()
    original_t0 = ts.t0
    ts_new = ator(ts, 1)
    assert ts_new.time_is_relative()
    assert ts_new.t0 == original_t0 - 1

    ts_new2 = rtoa(ts_new)
    assert ts_new2.time_is_UTC()
    assert ts_new2.t0 == original_t0


def test_rotate():
    seis = Seismogram()
    seis.npts = 100
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.live = 1
    for i in range(3):
        for j in range(100):
            if i == 0:
                seis.data[i, j] = 1.0
            else:
                seis.data[i, j] = 0.0
    seis.data[0, 1] = 1.0
    seis.data[0, 2] = 1.0
    seis.data[0, 3] = 0.0
    seis.data[1, 1] = 1.0
    seis.data[1, 2] = 1.0
    seis.data[1, 3] = 0.0
    seis.data[2, 1] = 1.0
    seis.data[2, 2] = 0.0
    seis.data[2, 3] = 1.0

    sc = SphericalCoordinate()
    sc.phi = 0.0
    sc.theta = np.pi / 4
    seis2 = rotate(seis, sc)
    assert all(np.isclose(seis2.data[:, 3], [0, -0.707107, 0.707107]))
    seis3 = rotate_to_standard(seis2)
    assert all(np.isclose(seis3.data[:, 3], [0, 0, 1]))


def test_transform():
    seis = Seismogram()
    seis.npts = 100
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.live = 1
    for i in range(3):
        for j in range(100):
            if i == 0:
                seis.data[i, j] = 1.0
            else:
                seis.data[i, j] = 0.0
    seis.data[0, 1] = 1.0
    seis.data[0, 2] = 1.0
    seis.data[0, 3] = 0.0
    seis.data[1, 1] = 1.0
    seis.data[1, 2] = 1.0
    seis.data[1, 3] = 0.0
    seis.data[2, 1] = 1.0
    seis.data[2, 2] = 0.0
    seis.data[2, 3] = 1.0

    a = np.zeros((3, 3))
    a[0][0] = 1.0
    a[0][1] = 1.0
    a[0][2] = 1.0
    a[1][0] = -1.0
    a[1][1] = 1.0
    a[1][2] = 1.0
    a[2][0] = 0.0
    a[2][1] = -1.0
    a[2][2] = 0.0
    seis1 = transform(seis, a)
    assert all(np.isclose(seis1.data[:, 0], [1, -1, 0]))
    assert all(np.isclose(seis1.data[:, 1], [3, 1, -1]))
    assert all(np.isclose(seis1.data[:, 2], [2, 0, -1]))
    assert all(np.isclose(seis1.data[:, 3], [1, 1, 0]))
    seis2 = rotate_to_standard(seis1)
    assert all(np.isclose(seis2.data[:, 0], [1, 0, 0]))
    assert all(np.isclose(seis2.data[:, 1], [1, 1, 1]))
    assert all(np.isclose(seis2.data[:, 2], [1, 1, 0]))
    assert all(np.isclose(seis2.data[:, 3], [0, 0, 1]))

    uvec = SlownessVector()
    uvec.ux = 0.17085  # cos(-20deg)/5.5
    uvec.uy = -0.062185  # sin(-20deg)/5.5
    seis3 = free_surface_transformation(seis2, uvec, 5.0, 3.5)
    assert (
        np.isclose(
            seis3.tmatrix,
            np.array(
                [
                    [-0.171012, -0.469846, 0],
                    [0.115793, -0.0421458, 0.445447],
                    [-0.597975, 0.217647, 0.228152],
                ]
            ),
        )
    ).all()

    # test with invalid uvec, but inplace return
    seis4 = free_surface_transformation(seis2, SlownessVector(1.0, 1.0, 0.0), 5.0, 3.5)
    assert seis4


def test_taper():
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
