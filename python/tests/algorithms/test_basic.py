import sys
import pytest
import numpy as np

from mspasspy.ccore.seismic import (
    TimeSeries,
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
    transform_to_RTZ,
    transform_to_LQT,
)

# module to test
sys.path.append("python/tests")

from helper import get_live_seismogram, get_live_timeseries, get_sin_timeseries


def is_identity(tm):
    """
    File-scope function used to standardize test that output of
    a 3x3 matrix is an identity.
    """
    assert all(np.isclose(tm[:, 0], [1.0, 0.0, 0.0]))
    assert all(np.isclose(tm[:, 1], [0.0, 1.0, 0.0]))
    assert all(np.isclose(tm[:, 2], [0.0, 0.0, 1.0]))
    return True


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
    """
    This tests the function forms of the rtoa and ator methods common
    to Seismogram and TimeSeries objects.   This test only verifies
    TimeSeries objects work correctly.
    """
    ts = get_live_timeseries()
    original_t0 = ts.t0
    ts_new = ator(ts, 1)
    assert ts_new.time_is_relative()
    assert ts_new.t0 == original_t0 - 1

    ts_new2 = rtoa(ts_new)
    assert ts_new2.time_is_UTC()
    assert ts_new2.t0 == original_t0


def test_rotate():
    """
    Tests the 3D rotate method of Seismogram.    rotate is overloaded
    and has a different algorithm with the same name that only rotates
    the coordinates around the z axis.  Not tested independently here
    as it is tested indirectly in the higher level function
    test_transform_to_RTZ below.

    """
    seis = Seismogram()
    seis.npts = 100
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.set_live()
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
    """
    Tests the general matrix transformation operator of Seismogram.
    """
    seis = Seismogram()
    seis.npts = 100
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.set_live()
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


def test_free_surface_transform():
    """
    Tests the free_surface_transform method and wrapper in basic.py.
    The wrapper adds flexibility in the api not in the C++ method.
    Those are tested here along with error handlers in the python
    wrappers.
    """

    # This is the same data matrix used in transform_to_LQT
    # probably should be made a fixture since I'm reusing it but
    # if it still is here it means I didn't do that.
    seis = Seismogram(3)
    seis.set_npts(3)
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.set_live()
    phi = 60.0
    phi_rad = np.radians(phi)
    theta = 10.0
    theta_rad = np.radians(theta)
    # define a set of vectors that resolve a set of consistent LQT directions. \
    # anchor is L that is azimuth 30 degrees and with ema of 10 degrees
    sc_L = SphericalCoordinate()
    sc_L.phi = phi_rad
    sc_L.theta = theta_rad
    sc_L.radius = 1.0

    sc_Q = SphericalCoordinate()
    sc_Q.phi = phi_rad
    sc_Q.theta = theta_rad + np.pi / 2.0
    sc_Q.radius = 1.0

    sc_T = SphericalCoordinate()
    sc_T.phi = phi_rad + np.pi / 2.0
    sc_T.theta = np.pi / 2.0
    sc_T.radius = 1.0

    # now load those three vectors into seis
    for i in range(3):
        x = sc_L.unit_vector
        seis.data[i, 0] = x[i]
    for i in range(3):
        x = sc_Q.unit_vector
        seis.data[i, 1] = x[i]
    for i in range(3):
        x = sc_T.unit_vector
        seis.data[i, 2] = x[i]

    # copy so we can reuse original
    seis0 = Seismogram(seis)

    # we use this to test output - column 0 is L, column 1 is Q, and 2 is T
    # hence the tests used in this function
    def fstout_ok(s):
        assert np.isclose(s.data[0, 0], 0.0)  # L should have zero T
        assert np.isclose(s.data[0, 1], 0.0)  # Q should have zero T
        assert all(
            np.isclose(s.data[:, 2], [-0.5, 0.0, 0.0])
        )  # fst has factor of 2 corection on T
        return True

    umag = 1.0 / 10.0  # slowness of 10 km/s apparent velocity
    u_phi = phi_rad  # set so radial of data matrix will be in u direction
    ux = umag * np.cos(u_phi)
    uy = umag * np.sin(u_phi)
    uvec = SlownessVector(ux, uy)
    sout = free_surface_transformation(seis, uvec, 5.0, 3.5)
    ok = fstout_ok(sout)
    assert ok
    # verify fst properly sets the transformation matrix so
    # rotate_to_standard can restore
    sout.rotate_to_standard()
    ok = is_identity(sout.tmatrix)
    assert ok
    # verify the data are restored properly as well - only doing this test once
    for i in range(3):
        assert all(np.isclose(sout.data[:, i], seis0.data[:, i]))

    # now test loading slowness vector through metadata
    seis = Seismogram(seis0)
    seis["ux"] = uvec.ux
    seis["uy"] = uvec.uy
    seis["vp0"] = 5.0
    seis["vs0"] = 3.5
    sout = free_surface_transformation(seis)
    ok = fstout_ok(sout)
    assert ok

    # test variants with ux,uy in Metadata but vp0 and vs0 in args
    seis = Seismogram(seis0)
    seis["ux"] = uvec.ux
    seis["uy"] = uvec.uy
    seis["vs0"] = 3.5
    sout = free_surface_transformation(seis, vp0=5.0)
    ok = fstout_ok(sout)
    assert ok

    seis = Seismogram(seis0)
    seis["ux"] = uvec.ux
    seis["uy"] = uvec.uy
    seis["vp0"] = 5.0
    sout = free_surface_transformation(seis, vs0=3.5)
    ok = fstout_ok(sout)
    assert ok

    # the uvec but metadata vp0 and vs0
    seis = Seismogram(seis0)
    seis["vp0"] = 5.0
    seis["vs0"] = 3.5
    sout = free_surface_transformation(seis, uvec)
    ok = fstout_ok(sout)
    assert ok

    # verify alternate key usage - testing all at once sufficient for current implementation
    seis = Seismogram(seis0)
    seis["slow_x"] = uvec.ux
    seis["slow_y"] = uvec.uy
    seis["P0"] = 5.0
    seis["S0"] = 3.5
    sout = free_surface_transformation(
        seis,
        ux_key="slow_x",
        uy_key="slow_y",
        vp0_key="P0",
        vs0_key="S0",
    )
    ok = fstout_ok(sout)
    assert ok

    # now test ensemble - test just with using Metadata fetch
    ens = SeismogramEnsemble(3)
    seis = Seismogram(seis0)
    seis["ux"] = uvec.ux
    seis["uy"] = uvec.uy
    seis["vp0"] = 5.0
    seis["vs0"] = 3.5
    for i in range(3):
        ens.member.append(Seismogram(seis))
    ens.set_live()
    ensout = free_surface_transformation(ens)
    assert ensout.live
    assert len(ensout.member) == 3
    for i in range(len(ensout.member)):
        ok = fstout_ok(ensout.member[i])
        assert ok

    # finally test error handlers
    # first test slowness data missing from Metadata when required
    seis = Seismogram(seis0)
    seis["vp0"] = 5.0
    seis["vs0"] = 3.5
    sout = free_surface_transformation(seis)
    assert sout.dead()
    assert sout.elog.size() == 1
    # repeat for vp0 an vs0
    seis = Seismogram(seis0)
    seis["ux"] = uvec.ux
    seis["uy"] = uvec.uy
    seis["vs0"] = 3.5
    sout = free_surface_transformation(seis)
    assert sout.dead()
    assert sout.elog.size() == 1
    seis = Seismogram(seis0)
    seis["ux"] = uvec.ux
    seis["uy"] = uvec.uy
    seis["vp0"] = 5.0
    sout = free_surface_transformation(seis)
    assert sout.dead()
    assert sout.elog.size() == 1
    # invalid arg0 should throw an exception
    x = TimeSeries()
    x.set_live()
    with pytest.raises(TypeError, match="received invalid type"):
        sout = free_surface_transformation(x)


def test_transform_to_RTZ():
    """
    This function tests the higher level function called transform_to_RTZ.
    That function supports both atomic an ensemble Seismogram input so
    we test both.  Error handlers are tested at the end.
    """
    # create a Seismogram that should resolve to [1,0,0] after RTZ
    # transform.  Multiple duplicate samples are probably not essential
    # but more like real data where npts of 1 would be never occur.
    seis = Seismogram(10)
    seis.npts = 10
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.set_live()
    phi = 30.0
    phi_rad = np.radians(phi)
    seis.data[0, :] = np.cos(phi_rad)
    seis.data[1, :] = np.sin(phi_rad)
    seis.data[2, :] = 10.0

    # need this more than once so make a deep copy
    seis0 = Seismogram(seis)

    # test atomic version for input via args
    sout = transform_to_RTZ(seis, phi=phi, angle_units="degrees")
    assert all(np.isclose(sout.data[:, 0], [1.0, 0.0, 10.0]))
    sout.rotate_to_standard()
    assert all(np.isclose(sout.data[:, 0], seis0.data[:, 0]))

    # test with radians
    seis = Seismogram(seis0)
    sout = transform_to_RTZ(seis, phi=phi_rad, angle_units="radians")
    assert all(np.isclose(sout.data[:, 0], [1.0, 0.0, 10.0]))
    sout.rotate_to_standard()
    assert all(np.isclose(sout.data[:, 0], seis0.data[:, 0]))

    # test with metadata input
    seis = Seismogram(seis0)
    az = 90.0 - phi
    seis["seaz"] = az + 180.0
    sout = transform_to_RTZ(seis)
    assert all(np.isclose(sout.data[:, 0], [1.0, 0.0, 10.0]))
    sout.rotate_to_standard()
    assert all(np.isclose(sout.data[:, 0], seis0.data[:, 0]))

    # repeat with alternate key
    seis = Seismogram(seis0)
    az = 90.0 - phi
    seis["backaz"] = az + 180.0
    sout = transform_to_RTZ(seis, key="backaz")
    assert all(np.isclose(sout.data[:, 0], [1.0, 0.0, 10.0]))
    sout.rotate_to_standard()
    assert all(np.isclose(sout.data[:, 0], seis0.data[:, 0]))

    # test of ensembles will only use Metadata method.
    # atomic version tests what is needed for arg method and
    # is not recommended for ensembles anyway
    nmembers = 3
    e = SeismogramEnsemble(nmembers)
    az = 90.0 - phi
    seaz = az + 180.0
    seis = Seismogram(seis0)
    seis["seaz"] = seaz
    for i in range(nmembers):
        e.member.append(Seismogram(seis))
    # ensemble is 3 copies of the same data this test used above
    # has seaz set
    eout = transform_to_RTZ(e)
    for d in eout.member:
        assert d.live
        assert all(np.isclose(d.data[:, 0], [1.0, 0.0, 10.0]))

    # test error handlers
    # illegal units argument error - logs but doesn't kill
    seis = Seismogram(seis0)
    sout = transform_to_RTZ(seis, phi=phi_rad, angle_units="invalid")
    assert sout.elog.size() == 1
    assert sout.live
    # should default back to radians so the tests as above should pass
    assert all(np.isclose(sout.data[:, 0], [1.0, 0.0, 10.0]))
    sout.rotate_to_standard()
    assert all(np.isclose(sout.data[:, 0], seis0.data[:, 0]))

    # required metadata not defined will cause a kill
    seis = Seismogram(seis0)
    sout = transform_to_RTZ(seis)
    assert sout.elog.size() == 1
    assert sout.dead()

    # invalid data throws an exception
    x = TimeSeries()
    x.set_live()
    with pytest.raises(TypeError, match="received invalid type"):
        sout = transform_to_RTZ(x)


def test_transform_to_LQT():
    """
    This function tests the higher level function called transform_to_LQT.
    That function supports both atomic an ensemble Seismogram input so
    we test both.  Error handlers are tested at the end.
    """
    # create a Seismogram that should resolve to an identity in the first
    # three columns of the data matrix.  We do that by making
    # x1-L, x2-Q, and x3-T as defined by LQT using L as the anchor
    seis = Seismogram(3)
    seis.set_npts(3)
    seis.t0 = 0.0
    seis.dt = 0.001
    seis.set_live()
    phi = 60.0
    phi_rad = np.radians(phi)
    theta = 10.0
    theta_rad = np.radians(theta)
    # define a set of vectors that resolve a set of consistent LQT directions. \
    # anchor is L that is azimuth 30 degrees and with ema of 10 degrees
    sc_L = SphericalCoordinate()
    sc_L.phi = phi_rad
    sc_L.theta = theta_rad
    sc_L.radius = 1.0

    sc_Q = SphericalCoordinate()
    sc_Q.phi = phi_rad
    sc_Q.theta = theta_rad + np.pi / 2.0
    sc_Q.radius = 1.0

    sc_T = SphericalCoordinate()
    sc_T.phi = phi_rad + np.pi / 2.0
    sc_T.theta = np.pi / 2.0
    sc_T.radius = 1.0

    # now load those three vectors into seis
    for i in range(3):
        x = sc_L.unit_vector
        seis.data[i, 0] = x[i]
    for i in range(3):
        x = sc_Q.unit_vector
        seis.data[i, 1] = x[i]
    for i in range(3):
        x = sc_T.unit_vector
        seis.data[i, 2] = x[i]

    def is_identity(s):
        """
        Inline function used to standardize test that output of
        transform_to_LQT make data vector in this test an identity
        matrix.
        """
        assert all(np.isclose(s.data[:, 0], [1.0, 0.0, 0.0]))
        assert all(np.isclose(s.data[:, 1], [0.0, 1.0, 0.0]))
        assert all(np.isclose(s.data[:, 2], [0.0, 0.0, 1.0]))
        return True

    # need this more than once so make a deep copy
    seis0 = Seismogram(seis)

    # test atomic version for input via args
    sout = transform_to_LQT(seis, phi=phi, theta=theta, angle_units="degrees")
    ok = is_identity(sout)
    assert ok
    sout.rotate_to_standard()
    for i in range(3):
        assert all(np.isclose(sout.data[:, i], seis0.data[:, i]))

    # test with radians
    sout = transform_to_LQT(seis, phi=phi_rad, theta=theta_rad, angle_units="radians")
    ok = is_identity(sout)
    assert ok
    sout.rotate_to_standard()
    for i in range(3):
        assert all(np.isclose(sout.data[:, i], seis0.data[:, i]))

    # test with metadata input
    seis = Seismogram(seis0)
    az = 90.0 - phi
    seis["seaz"] = az + 180.0
    seis["ema"] = theta
    sout = transform_to_LQT(seis)
    ok = is_identity(sout)
    assert ok
    sout.rotate_to_standard()
    for i in range(3):
        assert all(np.isclose(sout.data[:, i], seis0.data[:, i]))

    # repeat with alternate keys
    seis = Seismogram(seis0)
    az = 90.0 - phi
    seis["backaz"] = az + 180.0
    seis["theta"] = theta
    sout = transform_to_LQT(seis, seaz_key="backaz", ema_key="theta")
    ok = is_identity(sout)
    assert ok
    sout.rotate_to_standard()
    for i in range(3):
        assert all(np.isclose(sout.data[:, i], seis0.data[:, i]))

    # test of ensembles will only use Metadata method.
    # atomic version tests what is needed for arg method and
    # is not recommended for ensembles anyway
    nmembers = 3
    e = SeismogramEnsemble(3)
    az = 90.0 - phi
    seaz = az + 180.0
    seis = Seismogram(seis0)
    seis["seaz"] = seaz
    seis["ema"] = theta
    for i in range(3):
        e.member.append(Seismogram(seis))
    # ensemble is 3 copies of the same data this test used above
    # has seaz set
    eout = transform_to_LQT(e)
    for d in eout.member:
        assert d.live
        ok = is_identity(d)
        assert ok

    # test error handlers
    # illegal units argument error - logs but doesn't kill
    seis = Seismogram(seis0)
    sout = transform_to_LQT(seis, phi=phi_rad, theta=theta_rad, angle_units="invalid")
    assert sout.elog.size() == 1
    assert sout.live
    # should default back to radians so the tests as above should pass
    ok = is_identity(sout)
    assert ok
    sout.rotate_to_standard()
    for i in range(3):
        assert all(np.isclose(sout.data[:, i], seis0.data[:, i]))

    # required metadata not defined will cause a kill
    seis = Seismogram(seis0)
    sout = transform_to_LQT(seis)
    assert sout.elog.size() == 1
    assert sout.dead()

    # invalid data throws an exception
    x = TimeSeries()
    x.set_live()
    with pytest.raises(TypeError, match="received invalid type"):
        sout = transform_to_LQT(x)
