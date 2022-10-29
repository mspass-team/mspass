import sys
import numpy as np

from mspasspy.ccore.seismic import (
    Seismogram,
    SlownessVector,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.ccore.utility import SphericalCoordinate
from mspasspy.algorithms.basic import (
    ExtractComponent,
    ator,
    rtoa,
    rotate,
    rotate_to_standard,
    free_surface_transformation,
    transform,
)

# module to test
sys.path.append("python/tests")

from helper import get_live_seismogram, get_live_timeseries, get_sin_timeseries


def test_ExtractComponent():
    seis = Seismogram(10)
    seis.set_live()
    for i in range(3):
        for j in range(10):
            seis.data[i, j] = i
    t0 = ExtractComponent(seis, 0)
    t1 = ExtractComponent(seis, 1)
    t2 = ExtractComponent(seis, 2)
    assert t0.data == DoubleVector([0] * 10)
    assert t1.data == DoubleVector([1] * 10)
    assert t2.data == DoubleVector([2] * 10)
    assert ExtractComponent(seis, 3).data == DoubleVector([])
    assert ExtractComponent(seis, 3).dead()
    dead_seis = Seismogram(seis)
    dead_seis.kill()  # if input data is not alive
    assert dead_seis.dead()
    assert ExtractComponent(dead_seis, 0).dead()
    assert ExtractComponent(dead_seis, 1).dead()
    assert ExtractComponent(dead_seis, 3).dead()

    ensemble = SeismogramEnsemble()
    ensemble.member.append(Seismogram(seis))
    ensemble.member.append(Seismogram(seis))
    ensemble.set_live()
    seisEnsemble0 = ExtractComponent(ensemble, 0)
    seisEnsemble1 = ExtractComponent(ensemble, 1)
    seisEnsemble2 = ExtractComponent(ensemble, 2)
    assert seisEnsemble0.member[0].data == DoubleVector([0] * 10)
    assert seisEnsemble0.member[1].data == DoubleVector([0] * 10)
    assert seisEnsemble1.member[0].data == DoubleVector([1] * 10)
    assert seisEnsemble1.member[1].data == DoubleVector([1] * 10)
    assert seisEnsemble2.member[0].data == DoubleVector([2] * 10)
    assert seisEnsemble2.member[1].data == DoubleVector([2] * 10)
    assert ExtractComponent(ensemble, 3).dead()
    ensemble.kill()  # if input data is not alive
    assert ExtractComponent(ensemble, 0).dead()
    assert ExtractComponent(ensemble, 1).dead()
    assert ExtractComponent(ensemble, 3).dead()
    try:
        ExtractComponent(1, 1)
    except Exception as e:
        assert type(e) == TypeError


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
