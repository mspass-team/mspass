import pytest
from mspasspy.util.seismic import (
    print_metadata,
    number_live,
    ensemble_time_range,
    regularize_sampling,
)
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble, Seismogram
import numpy as np


def test_print_metadata():
    """
    This test function just runs the function with different objects
    to verify it works
    """
    # only test with this TimeSeries.  If inheritance works correctly
    # there it will with other seismic data types
    d = TimeSeries(10)
    d["foo"] = "bar"
    d["sta"] = "AAK"
    d.set_live()
    print_metadata(d)
    d = Seismogram(10)
    d["sta"] = "AAK"
    print_metadata(d)
    x = dict(d)
    print_metadata(x)
    with pytest.raises(
        TypeError, match="arg0 must be a dictionary or a subclass of Metadata"
    ):
        print_metadata("foobar")


def test_number_live():
    nme = 3
    e0 = TimeSeriesEnsemble(nme)
    d = TimeSeries()
    d.set_npts(20)
    d.set_live()
    for i in range(nme):
        e0.member.append(d)

    e = TimeSeriesEnsemble(e0)
    assert number_live(e) == nme

    e = TimeSeriesEnsemble(e0)
    e.member[1].kill()
    assert number_live(e) == nme - 1

    with pytest.raises(TypeError, match="illegal type for arg0="):
        number_live(d)


def test_regularize_sampling():
    # test only on TimeSeriesEnsemble
    # with our implementation using C++ templates there should be no
    # difference for SeismogramEnsemble objects
    d0 = TimeSeries()
    d0.set_npts(10)
    d0.set_live()
    d0.t0 = 1000.0
    dt = 0.1
    d0.dt = dt
    e = TimeSeriesEnsemble(4)
    for i in range(4):
        # this copy is probably not necessary but safer
        d = TimeSeries(d0)
        e.member.append(d)
    e.set_live()
    e0 = TimeSeriesEnsemble(e)
    # do nothing test
    e = regularize_sampling(e, dt)
    assert number_live(e) == 4

    # dither sample interval on one member enough to have it killed
    e = TimeSeriesEnsemble(e0)
    e.member[1].dt = dt + dt / 10.0
    e = regularize_sampling(e, dt)
    assert number_live(e) == 3
    assert e.member[1].elog.size() >= 1

    # similar but set to abort
    e = TimeSeriesEnsemble(e0)
    e.member[1].dt = dt + dt / 10.0
    with pytest.raises(ValueError, match="input ensemble has different sample rate"):
        e = regularize_sampling(e, dt, abort_on_error=True)

    # tiny change should not kill
    e = TimeSeriesEnsemble(e0)
    e.member[1].dt = dt + dt / 1000.0
    e = regularize_sampling(e, dt, Nsamp=10)
    assert number_live(e) == 4

    # test completely mismatched sample interval error handling
    e = TimeSeriesEnsemble(e0)
    e = regularize_sampling(e, 10.0)
    assert number_live(e) == 0
    assert e.dead()
    assert e.elog.size() >= 0


def test_ensemble_time_range():
    # test only on TimeSeriesEnsemble
    # with our implementation using C++ templates there should be no
    # difference for SeismogramEnsemble objects
    d0 = TimeSeries()
    d0.set_npts(10)
    d0.set_live()
    d0.t0 = 1000.0
    e = TimeSeriesEnsemble(4)
    stvals = []
    etvals = []
    for i in range(4):
        d = TimeSeries(d0)
        d.t0 += float(i)
        stvals.append(d.t0)
        etvals.append(d.endtime())
        e.member.append(d)
    e0 = TimeSeriesEnsemble(e)

    tw = ensemble_time_range(e, metric="inner")
    assert tw.start == np.max(stvals)
    assert tw.end == np.min(etvals)

    tw = ensemble_time_range(e, metric="outer")
    assert tw.start == np.min(stvals)
    assert tw.end == np.max(etvals)

    tw = ensemble_time_range(e, metric="median")
    assert tw.start == np.median(stvals)
    assert tw.end == np.median(etvals)

    tw = ensemble_time_range(e, metric="mean")
    assert tw.start == np.mean(stvals)
    assert tw.end == np.mean(etvals)


# test_print_metadata()
# test_number_live()
# test_ensemble_time_range()
test_regularize_sampling()
