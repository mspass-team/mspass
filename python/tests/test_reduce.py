import sys
import pytest
import obspy
import numpy as np
import dask
import dask.bag as db
import findspark
from pyspark import SparkConf, SparkContext
import numpy as np
import mspasspy.algorithms.signals as signals
from mspasspy.ccore.utility import dmatrix
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, DoubleVector
from mspasspy.reduce import stack

sys.path.append("python/tests")

from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble,
                    get_stream,
                    get_trace)


def dask_map(input):
    ddb = db.from_sequence(input)
    res = ddb.map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    return res.compute()


def spark_map(input):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    data = sc.parallelize(input)
    res = data.map(lambda ts: signals.filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0'))
    return res.collect()


def test_map_spark_and_dask():
    l = [get_live_timeseries() for i in range(5)]
    spark_res = spark_map(l)
    dask_res = dask_map(l)

    ts_cp = TimeSeries(l[0])
    res = signals.filter(ts_cp, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    assert np.isclose(spark_res[0].data, ts_cp.data).all()
    assert np.isclose(dask_res[0].data, ts_cp.data).all()


def test_reduce_stack():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    seis_cp = np.array(seis1.data)
    stack(seis1, seis2)
    res = np.array(seis_cp) + np.array(seis2.data)
    for i in range(3):
        assert np.isclose(seis1.data[i], res[i]).all() # fixme

    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    ts1_cp = np.array(ts1.data)
    stack(ts1, ts2)
    assert np.isclose(ts1.data, (np.array(ts1_cp) + np.array(ts2.data))).all()

    tse1 = get_live_timeseries_ensemble(2)
    tse2 = get_live_timeseries_ensemble(2)
    tse1_cp = TimeSeriesEnsemble(tse1)
    stack(tse1, tse2)
    for i in range(2):
        assert np.isclose(tse1.member[i].data, (np.array(tse1_cp.member[i].data) + np.array(tse2.member[i].data))).all()

    seis_e1 = get_live_seismogram_ensemble(2)
    seis_e2 = get_live_seismogram_ensemble(2)
    seis_e1_cp = SeismogramEnsemble(seis_e1)
    stack(seis_e1, seis_e2)
    for i in range(2):
        res = np.array(seis_e1_cp.member[i].data) + np.array(seis_e2.member[i].data)
        for j in range(3):
            assert np.isclose(seis_e1.member[i].data[j], res[j]).all() # fixme


def test_reduce_stack_exception():
    tse1 = get_live_timeseries_ensemble(2)
    tse2 = get_live_timeseries_ensemble(3)
    with pytest.raises(IndexError) as err:
        stack(tse1, tse2)
    assert str(err.value) == "data1 and data2 have different sizes of member"

    # fixme cxx is not throwing error
    # ts1 = get_live_timeseries()
    # ts1.data = DoubleVector([0, 1, 2])
    # ts2 = get_live_timeseries()
    # with pytest.raises(IndexError) as err:
    #     stack(ts1, ts2)
    # assert str(err.value) == "two inputs have different data dimensions for reduce operations"


def dask_reduce(input):
    ddb = db.from_sequence(input)
    res = ddb.fold(lambda a, b: stack(a, b, preserve_history=True, instance='3'))
    return res.compute()


def spark_reduce(input, sc):
    data = sc.parallelize(input)
    # zero = get_live_timeseries()
    # zero.data = DoubleVector(np.zeros(255))
    res = data.reduce(lambda a, b: stack(a, b, preserve_history=True, instance='3'))
    return res


def test_reduce_dask_spark(spark_context):
    findspark.init()
    l = [get_live_timeseries() for i in range(5)]
    res = np.zeros(255)
    for i in range(5):
        for j in range(255):
            res[j] = (res[j] + l[i].data[j])
    spark_res = spark_reduce(l, spark_context)
    dask_res = dask_reduce(l)
    assert np.isclose(res, dask_res.data).all()
    assert np.isclose(res, spark_res.data).all()
    assert len(res) == len(spark_res.data)


if __name__ == "__main__":
    test_reduce_dask_spark()
    # a1 = get_live_seismogram()
    # a2 = get_live_seismogram()
    # print(a1.data[0, 0])
    # print(a2.data[0, 0])
    # a1 += a2
    # print(a1.data[0, 0])
