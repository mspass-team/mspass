import sys
import numpy as np
import pytest

from mspasspy.ccore.seismic import Seismogram

sys.path.append("python/tests")
sys.path.append("python/mspasspy/util/")
import logging_helper
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)


def test_info_new_map():
    # Seismogram and TimeSeries
    seis = get_live_seismogram()
    assert seis.number_of_stages() == 0
    logging_helper.info(seis, "1", "dummy_func")
    assert seis.number_of_stages() == 1

    ts = get_live_timeseries()
    assert ts.number_of_stages() == 0
    logging_helper.info(ts, "1", "dummy_func")
    assert ts.number_of_stages() == 1

    # ensemble
    seis_e = get_live_seismogram_ensemble(3)
    logging_helper.info(seis_e, "0", "dummy_func")
    for i in range(3):
        assert seis_e.member[i].number_of_stages() == 1

    seis_e = get_live_seismogram_ensemble(3)
    logging_helper.info(seis_e, "0", "dummy_func", 0)
    assert seis_e.member[0].number_of_stages() == 1

    tse = get_live_timeseries_ensemble(3)
    logging_helper.info(tse, "0", "dummy_func", 0)
    assert tse.member[0].number_of_stages() == 1


def test_info_not_live():
    # Seismogram and TimeSeries
    seis = get_live_seismogram()
    seis.kill()
    assert seis.number_of_stages() == 0
    logging_helper.info(seis, "1", "dummy_func")
    assert seis.number_of_stages() == 0

    # ensemble
    seis_e = get_live_seismogram_ensemble(3)
    assert seis_e.member[0].number_of_stages() == 0
    seis_e.member[0].kill()
    logging_helper.info(seis_e, "0", "dummy_func", 0)
    assert seis_e.member[0].number_of_stages() == 0


def test_info_out_of_bound():
    seis_e = get_live_seismogram_ensemble(3)
    with pytest.raises(IndexError) as err:
        logging_helper.info(seis_e, "0", "dummy_func", 3)
    assert seis_e.member[0].number_of_stages() == 0


def test_info_empty():
    # Seismogram and TimeSeries
    seis = Seismogram()
    seis.set_live()
    assert len(seis.elog.get_error_log()) == 0
    logging_helper.info(seis, "1", "dummy_func")
    assert len(seis.elog.get_error_log()) == 1


def test_reduce_functionality():
    # Seismogram and TimeSeries
    seis = get_live_seismogram()
    assert seis.number_of_stages() == 0
    logging_helper.info(seis, "1", "dummy_func")
    logging_helper.info(seis, "2", "dummy_func_2")
    assert seis.number_of_stages() == 2
    seis2 = get_live_seismogram()
    assert seis2.number_of_stages() == 0
    logging_helper.reduce(seis2, seis, "3", "reduce")
    assert len(seis2.get_nodes()) == 3

    ts = get_live_timeseries()
    ts2 = get_live_timeseries()
    assert ts.number_of_stages() == 0
    logging_helper.info(ts, "1", "dummy_func")
    logging_helper.info(ts, "2", "dummy_func")
    assert ts.number_of_stages() == 2
    logging_helper.reduce(ts2, ts, "3", "reduce")
    assert len(ts2.get_nodes()) == 3

    # ensemble
    seis_e = get_live_seismogram_ensemble(3)
    seis_e2 = get_live_seismogram_ensemble(3)
    logging_helper.info(seis_e, "0", "dummy_func")
    logging_helper.info(seis_e, "1", "dummy_func")
    logging_helper.info(seis_e, "2", "dummy_func")
    logging_helper.reduce(seis_e2, seis_e, "3", "reduce")
    for i in range(3):
        assert len(seis_e2.member[i].get_nodes()) == 4

    tse = get_live_timeseries_ensemble(3)
    tse2 = get_live_timeseries_ensemble(3)
    logging_helper.info(tse, "0", "dummy_func")
    logging_helper.info(tse, "1", "dummy_func")
    logging_helper.info(tse, "2", "dummy_func")
    logging_helper.reduce(tse2, tse, "3", "reduce")
    for i in range(3):
        assert len(tse2.member[i].get_nodes()) == 4


def test_reduce_error():
    tse = get_live_timeseries_ensemble(3)
    tse2 = get_live_timeseries_ensemble(2)
    with pytest.raises(IndexError) as err:
        logging_helper.reduce(tse, tse2, "0", "dummy_func")
    assert (
        str(err.value)
        == "logging_helper.reduce: data1 and data2 have different sizes of member"
    )

    tse3 = get_live_timeseries_ensemble(3)
    ts = get_live_timeseries()
    with pytest.raises(TypeError) as ex:
        logging_helper.reduce(ts, tse3, "0", "dummy_func")
    assert str(ex.value) == "logging_helper.reduce: data2 has a different type as data1"


def test_reduce_dead_silent():
    seis = get_live_seismogram()
    assert seis.number_of_stages() == 0
    logging_helper.info(seis, "1", "dummy_func")
    logging_helper.info(seis, "2", "dummy_func_2")
    assert seis.number_of_stages() == 2
    seis.kill()
    seis2 = get_live_seismogram()
    assert seis2.number_of_stages() == 0
    logging_helper.reduce(seis2, seis, "3", "reduce")
    assert len(seis2.get_nodes()) == 3

    seis = get_live_seismogram()
    seis2 = get_live_seismogram()
    logging_helper.info(seis, "1", "dummy_func")
    logging_helper.info(seis, "2", "dummy_func_2")
    seis2.kill()
    logging_helper.reduce(seis2, seis, "3", "reduce")
    assert len(seis2.get_nodes()) == 0


if __name__ == "__main__":
    test_reduce_functionality()
