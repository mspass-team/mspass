import sys
import pytest
import obspy
import numpy as np

import mspasspy.ccore as mspass
from mspasspy.ccore import (Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble)

# module to test
sys.path.append("../../mspasspy/util/")
sys.path.append("..")

from decorators import (mspass_func_wrapper,
                        is_input_dead,
                        timeseries_as_trace,
                        seismogram_as_stream,
                        timeseries_ensemble_as_stream,
                        seismogram_ensemble_as_stream)
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble,
                    get_stream,
                    get_trace)


def test_is_input_dead():
    seis = get_live_seismogram()
    assert False == is_input_dead(seis)
    assert False == is_input_dead(any=seis)
    seis.kill()
    assert True == is_input_dead(seis)
    assert True == is_input_dead(any=seis)

    ts = get_live_timeseries()
    assert False == is_input_dead(ts)
    assert False == is_input_dead(any=ts)
    ts.kill()
    assert True == is_input_dead(ts)
    assert True == is_input_dead(any=ts)

    seis_e = get_live_seismogram_ensemble(3)
    assert False == is_input_dead(seis_e)
    assert False == is_input_dead(any=seis_e)
    seis_e.member[0].kill()
    assert False == is_input_dead(seis_e)
    assert False == is_input_dead(any=seis_e)
    seis_e.member[1].kill()
    seis_e.member[2].kill()
    assert True == is_input_dead(seis_e)
    assert True == is_input_dead(any=seis_e)

    tse = get_live_timeseries_ensemble(3)
    assert False == is_input_dead(tse)
    assert False == is_input_dead(any=tse)
    tse.member[0].kill()
    assert False == is_input_dead(tse)
    assert False == is_input_dead(any=tse)
    tse.member[1].kill()
    tse.member[2].kill()
    assert True == is_input_dead(tse)
    assert True == is_input_dead(any=tse)


@mspass_func_wrapper
def dummy_func(data, *args, preserve_history=False, instance=None, dryrun=False, **kwargs):
    return "dummy"


def test_mspass_func_wrapper():
    with pytest.raises(RuntimeError) as err:
        dummy_func(1)
    assert str(err.value) == "mspass_func_wrapper only accepts mspass object as data input"

    seis = get_live_seismogram()
    assert seis.elog.size() == 0
    dummy_func(seis, preserve_history=True)
    assert seis.elog.size() == 1
    log = seis.elog.get_error_log()
    assert log[0].message.find("preserve_history was true but instance not defined") != 0

    assert "OK" == dummy_func(seis, dryrun=True)

    assert "dummy" == dummy_func(seis)

    assert seis.number_of_stages() == 0
    dummy_func(seis, preserve_history=True, instance='0')
    assert seis.number_of_stages() == 1


@timeseries_as_trace
def dummy_func_timeseries_as_trace(d, any=None):
    d.data = np.array([0, 1, 2])
    any.data = np.array([2,3,4])


def test_timeseries_as_trace():
    ts = get_live_timeseries()
    ts2 = get_live_timeseries()
    cp = np.array(ts.s)
    cp2 = np.array(ts2.s)
    dummy_func_timeseries_as_trace(ts, ts2)
    assert len(cp) != len(ts.s)
    assert len(cp2) != len(ts2.s)
    assert all(a == b for a,b in zip([0,1,2], ts.s))
    assert all(a == b for a, b in zip([2,3,4], ts2.s))


@seismogram_as_stream
def dummy_func_seismogram_as_stream(d1, d2=None):
    d1[0].data[0] = -1
    d2[0].data[0] = -1

def test_seismogram_as_trace():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    cp1 = np.array(seis1.u[0])
    cp2 = np.array(seis2.u[0])
    dummy_func_seismogram_as_stream(seis1, seis2)
    assert cp1[0] != seis1.u[0,0]
    assert cp2[0] != seis2.u[0,0]
    assert seis1.u[0,0] == -1
    assert seis2.u[0, 0] == -1

@timeseries_ensemble_as_stream
def dummy_func_timeseries_ensemble_as_stream(data):
    new = get_stream()
    for t in new:
        data.append(t)

@timeseries_ensemble_as_stream
def dummy_func_timeseries_ensemble_as_stream_2(data=None):
    new = get_stream()
    for t in new:
        data.append(t)

def test_timeseries_ensemble_as_stream():
    tse = get_live_timeseries_ensemble(2)
    assert len(tse.member) == 2
    cp = TimeSeriesEnsemble(tse)
    dummy_func_timeseries_ensemble_as_stream(tse)
    assert len(tse.member) == 5
    assert all(a == b for a,b in zip(cp.member[0].s, tse.member[0].s))
    assert all(a == b for a, b in zip(cp.member[1].s, tse.member[1].s))

    tse = get_live_timeseries_ensemble(2)
    assert len(tse.member) == 2
    cp = TimeSeriesEnsemble(tse)
    dummy_func_timeseries_ensemble_as_stream_2(data=tse)
    assert len(tse.member) == 5
    assert all(a == b for a, b in zip(cp.member[0].s, tse.member[0].s))
    assert all(a == b for a, b in zip(cp.member[1].s, tse.member[1].s))

@seismogram_ensemble_as_stream
def dummy_func_seismogram_ensemble_as_stream(data):
    res = get_stream()
    for t in res:
        data.append(t)
    return data

@seismogram_ensemble_as_stream
def dummy_func_seismogram_ensemble_as_stream_2(data=None):
    res = get_stream()
    for t in res:
        data.append(t)
    return data

def test_seismogram_ensemble_as_stream():
    seis_e = get_live_seismogram_ensemble(2)
    assert len(seis_e.member) == 2
    cp = SeismogramEnsemble(seis_e)
    dummy_func_seismogram_ensemble_as_stream(seis_e)
    assert len(seis_e.member) == 3
    assert all(a.any() == b.any() for a,b in zip(cp.member[0].u, seis_e.member[0].u))
    assert all(a.any() == b.any() for a, b in zip(cp.member[1].u, seis_e.member[1].u))

    seis_e = get_live_seismogram_ensemble(2)
    assert len(seis_e.member) == 2
    cp = SeismogramEnsemble(seis_e)
    dummy_func_seismogram_ensemble_as_stream_2(data=seis_e)
    assert len(seis_e.member) == 3
    assert all(a.any() == b.any() for a, b in zip(cp.member[0].u, seis_e.member[0].u))
    assert all(a.any() == b.any() for a, b in zip(cp.member[1].u, seis_e.member[1].u))


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def dummy_func_2(data, *args, preserve_history=False, instance=None, dryrun=False, **kwargs):
    if isinstance(data, obspy.Trace):
        data.data = np.array([0, 1, 2])
    elif isinstance(data, obspy.Stream):
        data[0].data[0] = -1
    else:
        return None

def test_all_decorators():
    # test mspass_func_wrapper
    with pytest.raises(RuntimeError) as err:
        dummy_func_2(1)
    assert str(err.value) == "mspass_func_wrapper only accepts mspass object as data input"

    seis = get_live_seismogram()
    assert seis.elog.size() == 0
    dummy_func_2(seis, preserve_history=True)
    assert seis.elog.size() == 1
    log = seis.elog.get_error_log()
    assert log[0].message.find("preserve_history was true but instance not defined") != 0

    assert "OK" == dummy_func_2(seis, dryrun=True)

    assert seis.number_of_stages() == 0
    dummy_func_2(seis, preserve_history=True, instance='0')
    assert seis.number_of_stages() == 1

    # test timeseries_as_trace
    ts = get_live_timeseries()
    cp = np.array(ts.s)
    dummy_func_2(ts, preserve_history=True, instance='0')
    assert len(cp) != len(ts.s)
    assert all(a == b for a, b in zip([0, 1, 2], ts.s))
    assert ts.number_of_stages() == 1

    # test seismogram_as_stream
    seis1 = get_live_seismogram()
    cp1 = np.array(seis1.u[0])
    dummy_func_2(seis1, preserve_history=True, instance='0')
    assert cp1[0] != seis1.u[0, 0]
    assert seis1.u[0, 0] == -1
    assert seis1.number_of_stages() == 1

    # test timeseries_ensemble_as_stream
    tse = get_live_timeseries_ensemble(2)
    cp = TimeSeriesEnsemble(tse)
    dummy_func_2(tse, preserve_history=True, instance='0')
    assert tse.member[0].s[0] == -1
    assert tse.member[0].s[0] != cp.member[0].s[0]
    assert tse.member[0].number_of_stages() == 1

    # test seismogram_ensemble_as_stream
    seis_e = get_live_seismogram_ensemble(2)
    cp = SeismogramEnsemble(seis_e)
    dummy_func_2(seis_e, preserve_history=True, instance='0')
    assert seis_e.member[0].u[0,0] == -1
    assert seis_e.member[0].u[0,0] != cp.member[0].u[0,0]
    assert seis_e.member[0].number_of_stages() == 1


if __name__ == "__main__":
    test_all_decorators()
