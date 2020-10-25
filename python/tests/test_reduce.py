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
from mspasspy.ccore import Seismogram, TimeSeries, DoubleVector, TimeSeriesEnsemble, SeismogramEnsemble, dmatrix
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
    assert all(a == b for a, b in zip(spark_res[0].s, ts_cp.s))
    assert all(a == b for a, b in zip(dask_res[0].s, ts_cp.s))


def test_reduce_stack():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    seis_cp = np.array(seis1.u)
    stack(seis1, seis2)
    assert all(a.any() == b.any() for a, b in zip(seis1.u, (np.array(seis_cp) + np.array(seis2.u))))

    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    ts1_cp = np.array(ts1.s)
    stack(ts1, ts2)
    assert all(a == b for a, b in zip(ts1.s, (np.array(ts1_cp) + np.array(ts2.s))))

    tse1 = get_live_timeseries_ensemble(2)
    tse2 = get_live_timeseries_ensemble(2)
    tse1_cp = TimeSeriesEnsemble(tse1)
    stack(tse1, tse2)
    for i in range(2):
        assert all(a == b for a, b in zip(tse1.member[i].s,
                                          (np.array(tse1_cp.member[i].s) + np.array(tse2.member[i].s))))

    seis_e1 = get_live_seismogram_ensemble(2)
    seis_e2 = get_live_seismogram_ensemble(2)
    seis_e1_cp = SeismogramEnsemble(seis_e1)
    stack(seis_e1, seis_e2)
    for i in range(2):
        assert all(a.any() == b.any() for a, b in zip(seis_e1.member[i].u,
                                                      (np.array(seis_e1_cp.member[i].u) +
                                                       np.array(seis_e2.member[i].u))))


def test_reduce_stack_exception():
    tse1 = get_live_timeseries_ensemble(2)
    tse2 = get_live_timeseries_ensemble(3)
    with pytest.raises(IndexError) as err:
        stack(tse1, tse2)
    assert str(err.value) == "data1 and data2 have different sizes of member"

    # fixme cxx is not throwing error
    # ts1 = get_live_timeseries()
    # ts1.s = DoubleVector([0, 1, 2])
    # ts2 = get_live_timeseries()
    # with pytest.raises(IndexError) as err:
    #     stack(ts1, ts2)
    # assert str(err.value) == "two inputs have different data dimensions for reduce operations"


def dask_reduce(input):
    ddb = db.from_sequence(input)
    res = ddb.fold(lambda a, b: stack(a, b, preserve_history=True, instance='3'))
    return res.compute()


def spark_reduce(input):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    data = sc.parallelize(input)
    zero = get_live_timeseries()
    zero.s = DoubleVector(np.zeros(255))
    res = data.fold(zero, lambda a, b: stack(a, b, preserve_history=True, instance='3'))
    return res


def test_reduce_dask_spark():
    findspark.init()
    l = [get_live_timeseries() for i in range(5)]
    res = np.zeros(255)
    for i in range(5):
        for j in range(255):
            res[j] = (res[j] + l[i].s[j])
    spark_res = spark_reduce(l)
    dask_res = dask_reduce(l)
    assert all(a == b for a, b in zip(res, dask_res.s))
    assert all(a == b for a, b in zip(res, spark_res.s))

if __name__ == "__main__":
    test_reduce_dask_spark()
