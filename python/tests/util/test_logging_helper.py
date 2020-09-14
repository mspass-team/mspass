import sys
import numpy as np
import pytest

# stable modules in mspasspy
import mspasspy.ccore as mspass
from mspasspy.ccore import (Seismogram)

# module to test
sys.path.append("../../mspasspy/util/")
sys.path.append("..")
import logging_helper
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)


def test_info_new_map():
    # Seismogram and TimeSeries
    seis = get_live_seismogram()
    assert seis.number_of_stages() == 0
    logging_helper.info(seis, 'dummy_func', '1')
    assert seis.number_of_stages() == 1

    ts = get_live_timeseries()
    assert ts.number_of_stages() == 0
    logging_helper.info(ts, 'dummy_func', '1')
    assert ts.number_of_stages() == 1

    # ensemble
    seis_e = get_live_seismogram_ensemble(3)
    logging_helper.info(seis_e, 'dummy_func', '0')
    for i in range(3):
        assert seis_e.member[i].number_of_stages() == 1

    seis_e = get_live_seismogram_ensemble(3)
    logging_helper.info(seis_e, 'dummy_func', '0', 0)
    assert seis_e.member[0].number_of_stages() == 1

    tse = get_live_timeseries_ensemble(3)
    logging_helper.info(tse, 'dummy_func', '0', 0)
    assert tse.member[0].number_of_stages() == 1


def test_info_not_live():
    # Seismogram and TimeSeries
    seis = get_live_seismogram()
    seis.kill()
    assert seis.number_of_stages() == 0
    logging_helper.info(seis, 'dummy_func', '1')
    assert seis.number_of_stages() == 0

    # ensemble
    seis_e = get_live_seismogram_ensemble(3)
    assert seis_e.member[0].number_of_stages() == 0
    seis_e.member[0].kill()
    logging_helper.info(seis_e, 'dummy_func', '0', 0)
    assert seis_e.member[0].number_of_stages() == 0

def test_info_out_of_bound():
    seis_e = get_live_seismogram_ensemble(3)
    with pytest.raises(IndexError) as err:
        logging_helper.info(seis_e, 'dummy_func', '0', 3)
    assert seis_e.member[0].number_of_stages() == 0

def test_info_empty():
    # Seismogram and TimeSeries
    seis = Seismogram()
    seis.set_live()
    assert len(seis.elog.get_error_log()) == 0
    logging_helper.info(seis, 'dummy_func', '1')
    assert len(seis.elog.get_error_log()) == 1

