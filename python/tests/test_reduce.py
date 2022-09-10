import sys
import pytest
import obspy
import numpy as np
import dask
import dask.bag as db
import numpy as np
import mspasspy.algorithms.signals as signals
from mspasspy.ccore.utility import dmatrix
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.reduce import stack

sys.path.append("python/tests")

from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
    get_live_seismogram_list,
    get_stream,
    get_trace,
)


def dask_map(input):
    ddb = db.from_sequence(input)
    res = ddb.map(
        signals.filter,
        "bandpass",
        freqmin=1,
        freqmax=5,
        object_history=True,
        alg_id="0",
    )
    return res.compute()


def spark_map(input, sc):
    data = sc.parallelize(input)
    res = data.map(
        lambda ts: signals.filter(
            ts, "bandpass", freqmin=1, freqmax=5, object_history=True, alg_id="0"
        )
    )
    return res.collect()


def test_map_spark_and_dask(spark_context):
    l = [get_live_timeseries() for i in range(5)]
    # add net, sta, chan, loc to avoid metadata serialization problem
    for i in range(5):
        l[i]["chan"] = "HHZ"
        l[i]["loc"] = "test_loc"
        l[i]["net"] = "test_net"
        l[i]["sta"] = "test_sta"
    spark_res = spark_map(l, spark_context)
    dask_res = dask_map(l)

    ts_cp = TimeSeries(l[0])
    res = signals.filter(
        ts_cp, "bandpass", freqmin=1, freqmax=5, object_history=True, alg_id="0"
    )
    assert np.isclose(spark_res[0].data, ts_cp.data).all()
    assert np.isclose(dask_res[0].data, ts_cp.data).all()


def test_reduce_stack():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    seis_cp = np.array(seis1.data)
    stack(seis1, seis2)
    res = np.add(np.array(seis_cp), np.array(seis2.data))
    for i in range(3):
        assert np.isclose(seis1.data[i], res[i]).all()  # fixme

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
        assert np.isclose(
            tse1.member[i].data,
            np.add(np.array(tse1_cp.member[i].data), np.array(tse2.member[i].data)),
        ).all()

    seis_e1 = get_live_seismogram_ensemble(2)
    seis_e2 = get_live_seismogram_ensemble(2)
    seis_e1_cp = SeismogramEnsemble(seis_e1)
    stack(seis_e1, seis_e2)
    for i in range(2):
        res = np.add(
            np.array(seis_e1_cp.member[i].data), np.array(seis_e2.member[i].data)
        )
        for j in range(3):
            assert np.isclose(seis_e1.member[i].data[j], res[j]).all()  # fixme


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
    res = ddb.fold(lambda a, b: stack(a, b, object_history=True, alg_id="3"))
    return res.compute()


def spark_reduce(input, sc):
    data = sc.parallelize(input)
    # zero = get_live_timeseries()
    # zero.data = DoubleVector(np.zeros(255))
    res = data.reduce(lambda a, b: stack(a, b, object_history=True, alg_id="3"))
    return res


def test_reduce_dask_spark(spark_context):
    l = [get_live_timeseries() for i in range(5)]
    res = np.zeros(255)
    for i in range(5):
        for j in range(255):
            res[j] = res[j] + l[i].data[j]
    spark_res = spark_reduce(l, spark_context)
    dask_res = dask_reduce(l)
    assert np.isclose(res, dask_res.data).all()
    assert np.isclose(res, spark_res.data).all()
    assert len(res) == len(spark_res.data)


def test_foldby(spark_context):
    seis1 = get_live_seismogram_list(2, 200)
    seis2 = get_live_seismogram_list(4, 100)
    bag1 = db.from_sequence(seis1 + seis2)
    res = bag1.mspass_foldby(key="npts")
    res_dask = res.compute()
    for d in res_dask[0].member:
        assert res_dask[0]["npts"] == d["npts"]
    for d in res_dask[1].member:
        assert res_dask[1]["npts"] == d["npts"]

    rdd1 = spark_context.parallelize(seis1 + seis2)
    res = rdd1.mspass_foldby(key="npts")
    res_spark = res.collect()

    assert len(res_dask[0].member) == len(res_spark[0].member)
    assert len(res_dask[1].member) == len(res_spark[1].member)


if __name__ == "__main__":
    # test_reduce_stack()
    a1 = get_live_seismogram()
    a2 = get_live_seismogram()
    print(a1.data[0, 0])
    print(a2.data[0, 0])
    print(a1.t0, a1.endtime())
    print(a2.t0, a2.endtime())
    a1 += a2
    print(a1.data[0, 0])
