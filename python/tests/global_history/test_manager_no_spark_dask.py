from unittest import mock
import sys

sys.path.append("python/tests")
from helper import get_live_timeseries

with mock.patch.dict(sys.modules, {"pyspark": None, "dask": None}):
    from mspasspy.reduce import stack
    from mspasspy.global_history.manager import mspass_map, mspass_reduce

    def add(
        data,
        object_history=False,
        alg_name="filter",
        alg_id=None,
        dryrun=False,
        inplace_return=True,
    ):
        return data + data

    def test_normal_map():
        t = [get_live_timeseries() for i in range(5)]
        test_map_res = list(mspass_map(t, add))
        for i in range(5):
            assert test_map_res[i].data == t[i].data + t[i].data
        # test user provided alg_name and parameter(exist)
        test_map_res = list(
            mspass_map(
                t,
                add,
                object_history=True,
                parameters="length=5",
            )
        )
        for i in range(5):
            assert test_map_res[i].data == t[i].data + t[i].data
        test_map_res = list(mspass_map(t, add, alg_id="7", alg_name="add"))
        for i in range(5):
            assert test_map_res[i].data == t[i].data + t[i].data

    def test_normal_reduce():
        t = [get_live_timeseries() for i in range(5)]
        s = t[0]
        for i in range(1, 5):
            s += t[i]
        test_reduce_res = mspass_reduce(t, stack, object_history=True)
        assert test_reduce_res.data == s.data
        test_reduce_res = mspass_reduce(
            t,
            stack,
            object_history=False,
            alg_id="5",
            alg_name="stack",
        )
        assert test_reduce_res.data == s.data
        test_reduce_res = mspass_reduce(
            t,
            stack,
            object_history=False,
            parameters="length=5",
        )
        assert test_reduce_res.data == s.data
