import dask
import dask.bag as db
from pyspark import SparkConf, SparkContext
import pandas as pd
import numpy as np

import mspasspy.algorithms.signals as signals
import mspasspy.ccore as mspass
from mspasspy.ccore import Seismogram, TimeSeries, DoubleVector


def get_live_timeseries():
    ts_size = 255
    sampling_rate = 20.0
    ts = TimeSeries()
    ts.set_live()
    ts.dt = 1 / sampling_rate
    ts.t0 = 0
    ts.npts = ts_size
    ts.put('net', 'IU')
    ts.put('npts', ts_size)
    ts.put('sampling_rate', sampling_rate)
    ts.set_as_origin('test', '0', '0',
                     mspass.AtomicType.TIMESERIES)
    ts.s = DoubleVector(np.random.rand(ts_size))
    return ts

def dummy(l):
    l.append(1)
    return l

def test_dask_map(input):
    ddb = db.from_sequence(input)
    res = ddb.map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    return res.compute()


def test_spark_map(input):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    data = sc.parallelize(input)
    res = data.map(lambda ts: signals.filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0'))
    return res.collect()

def mspass_reduce(object, **options):
    # call function
    # matin
    pass


if __name__ == "__main__":
    l = [get_live_timeseries() for i in range(5)]
    spark_res = test_spark_map(l)
    dask_res = test_dask_map(l)

    ts_cp = TimeSeries(l[0])
    res = signals.filter(ts_cp, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    assert all(a == b for a, b in zip(spark_res[0].s, ts_cp.s))
    assert all(a == b for a, b in zip(dask_res[0].s, ts_cp.s))