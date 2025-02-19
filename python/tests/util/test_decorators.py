import sys
import pytest
import obspy
import numpy as np

from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.global_history.manager import GlobalHistoryManager


# pytest in mspass is always run from the top level director of the
# source tree so we need to add this to path to see the helper module
# used only for testing
sys.path.append("python/tests")


from mspasspy.util.decorators import (
    mspass_func_wrapper,
    mspass_func_wrapper_multi,
    is_input_dead,
    timeseries_as_trace,
    seismogram_as_stream,
    timeseries_ensemble_as_stream,
    seismogram_ensemble_as_stream,
    mspass_reduce_func_wrapper,
    seismogram_copy_helper,
    timeseries_copy_helper,
    mspass_method_wrapper,
)
from mspasspy.util import logging_helper
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
    get_stream,
    get_trace,
)


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
def dummy_func(
    data,
    *args,
    object_history=False,
    alg_id=None,
    dryrun=False,
    inplace_return=False,
    function_return_key=None,
    **kwargs,
):
    return "dummy"


@mspass_func_wrapper
def dummy_numeric_function(
    data,
    *args,
    object_history=False,
    alg_id=None,
    dryrun=False,
    inplace_return=False,
    function_return_key=None,
    handles_ensembles=False,
    **kwargs,
):
    """
    Use this dummy function if the test has to actually do something
    rational to the data - dummy_func will return invalid types that
    would confuse tests on ensembles.
    """
    data["foobar"] = 1.0
    return data


@mspass_func_wrapper
def check_arg0_tester(
    data,
    *args,
    checks_arg0_type=True,
    handle_ensemble=True,
    **kwargs,
):
    """
    Test function used to validate behavior of the check_arg0_type
    feature.  Raises a ValueError typical of processing functions
    when data is not a valid type.
    """
    if isinstance(data, TimeSeries):
        data["foo"] = "bar"
        return data
    else:
        raise TypeError("test function arg0 is an invalid type")


def test_mspass_func_wrapper():
    with pytest.raises(TypeError) as err:
        dummy_func(1)
    assert "mspass_func_wrapper only accepts mspass object" in str(err.value)

    with pytest.raises(ValueError) as err:
        seis = get_live_seismogram()
        dummy_func(seis, object_history=True)
    assert (
        str(err.value) == "dummy_func: object_history was true but alg_id not defined"
    )
    # added Feb 2025 to test new error handler for ValueError exception
    # do not allow function_return_key option with ensembles
    with pytest.raises(ValueError, match="Usage error:"):
        e = get_live_timeseries_ensemble(3)
        dummy_numeric_function(e, function_return_key="foobar")

    assert "OK" == dummy_func(seis, dryrun=True)

    # default behavior
    assert "dummy" == dummy_func(seis)
    assert seis.number_of_stages() == 0

    # object_history is true
    dummy_func(seis, object_history=True, alg_id="0")
    assert seis.number_of_stages() == 1
    assert len(seis.get_nodes()) == 1
    assert seis.current_nodedata().algorithm == "dummy_func"
    assert seis.current_nodedata().algid == "0"

    # inplace return
    data = dummy_func(seis, inplace_return=True)
    assert isinstance(data, Seismogram)

    # valid function_return_key
    data = dummy_func(seis, inplace_return=True, function_return_key="test_key")
    assert isinstance(data, Seismogram)
    assert "test_key" in data and data["test_key"] == "dummy"

    # invalid function_return_key and not inplace_return
    data = dummy_func(seis, inplace_return=False, function_return_key=dict())
    assert isinstance(data, Seismogram)
    errs = seis.elog.get_error_log()
    assert len(errs) == 2
    assert errs[-1].algorithm == "dummy_func"
    assert (
        errs[-1].message
        == "Inconsistent arguments; inplace_return was set False and function_return_key was not None.\nAssuming inplace_return == True is correct"
    )
    assert errs[-2].algorithm == "dummy_func"
    assert (
        errs[-2].message
        == "Illegal type received for function_return_key argument=<class 'dict'>\nReturn value not saved in Metadata"
    )
    # tests for new ensemble handling features Feb 2025
    # note we use a TimeSeriesEnsemble but a SeismogramEnsemble would behave
    # the same for the current api - careful if there is divergence
    e = get_live_timeseries_ensemble(3)
    e = dummy_numeric_function(e, handles_ensembles=False)
    # in this case all the members need to be tested
    for d in e.member:
        assert d["foobar"] == 1.0
    assert "foobar" not in e
    # with handles_ensembles_false reverse the tests above
    e = get_live_timeseries_ensemble(3)
    e = dummy_numeric_function(e, handles_ensembles=True)
    # in this case all the members need to be tested
    for d in e.member:
        assert "foobar" not in d
    assert e["foobar"] == 1.0

    # dead object will return immediately
    seis.kill()
    data = dummy_func(seis)
    assert not data.live
    data = dummy_func(seis, inplace_return=True)
    assert not data.live

    # check behavir of checks_arg0_type with handles_ensembles
    d = get_live_timeseries()
    # should work in this case
    d = check_arg0_tester(d)
    assert "foo" in d
    # this should have a message from the actuall function
    with pytest.raises(TypeError, match="test function arg0 is an invalid type"):
        d = check_arg0_tester("notvaliddata", checks_arg0_type=True)
    # this should return the decorator error
    with pytest.raises(
        TypeError, match="mspass_func_wrapper only accepts mspass object"
    ):
        d = check_arg0_tester("notvaliddata", checks_arg0_type=False)

    # test selective typte behavior with different settings of
    # handles ensemble.  Notice check_arg0_tester internally only accepts
    # TimeSeries.   When check_arg0_type s False and handles_ensemble is
    # True it should process an ensemble.  Otherwise it should throw a
    # ValueError exception
    e = get_live_timeseries_ensemble(3)
    assert e.live
    e = check_arg0_tester(e, checks_arg0_type=True, handles_ensembles=False)
    for d in e.member:
        assert d["foo"] == "bar"
    e = get_live_timeseries_ensemble(3)
    with pytest.raises(TypeError, match="test function arg0 is an invalid type"):
        e = check_arg0_tester(e, checks_arg0_type=True, handles_ensembles=True)
    # this would be a mistake in usage for this test function but is the
    # behavio the decoraor should have - returns the functions message not
    # the decorator message.  Note using seismogram because the
    # function only takes TimeSeries - this will work for a TimeSeriesEnsemble
    # and not throw an exceptoin
    e = get_live_seismogram_ensemble(3)
    with pytest.raises(TypeError, match="test function arg0 is an invalid type"):
        e = check_arg0_tester(e, checks_arg0_type=False, handles_ensembles=False)
    # True-True not testable directly - would fail referencing "member"
    # attibute in decorator - an incorrect usage not worth testing

    # check handling of an algorithm killing all data
    e = get_live_timeseries_ensemble(3)
    for i in range(3):
        e.member[i].kill()
    assert e.live
    e = check_arg0_tester(e, checks_arg0_type=True, handles_ensembles=False)
    assert e.dead()
    assert e.elog.size() == 1


@timeseries_as_trace
def dummy_func_timeseries_as_trace(d, any=None):
    d.data = np.array([0, 1, 2])
    d.stats["channel"] = "Z"
    any.data = np.array([2, 3, 4])


def test_timeseries_as_trace():
    ts = get_live_timeseries()
    ts2 = get_live_timeseries()
    cp = np.array(ts.data)
    cp2 = np.array(ts2.data)
    dummy_func_timeseries_as_trace(ts, ts2)
    assert len(cp) != len(ts.data)
    assert len(cp2) != len(ts2.data)
    np.isclose([0, 1, 2], ts.data).all()
    np.isclose([2, 3, 4], ts2.data).all()
    assert ts["chan"] == "Z"


@seismogram_as_stream
def dummy_func_seismogram_as_stream(d1, d2=None):
    d1[0].data[0] = -1
    d1[0].stats["test"] = "test"
    d2[0].data[0] = -1


def test_seismogram_as_trace():
    seis1 = get_live_seismogram()
    seis2 = get_live_seismogram()
    cp1 = np.array(seis1.data[0])
    cp2 = np.array(seis2.data[0])
    dummy_func_seismogram_as_stream(seis1, seis2)
    assert cp1[0] != seis1.data[0, 0]
    assert cp2[0] != seis2.data[0, 0]
    assert seis1.data[0, 0] == -1
    assert seis2.data[0, 0] == -1
    assert seis1["test"] == "test"


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
    np.isclose(cp.member[0].data, tse.member[0].data).all()
    np.isclose(cp.member[0].data, tse.member[1].data).all()

    tse = get_live_timeseries_ensemble(2)
    assert len(tse.member) == 2
    cp = TimeSeriesEnsemble(tse)
    dummy_func_timeseries_ensemble_as_stream_2(data=tse)
    assert len(tse.member) == 5
    np.isclose(cp.member[0].data, tse.member[0].data).all()
    np.isclose(cp.member[0].data, tse.member[1].data).all()


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
    assert all(
        np.isclose(a, b).all() for a, b in zip(cp.member[0].data, seis_e.member[0].data)
    )
    assert all(
        np.isclose(a, b).all() for a, b in zip(cp.member[1].data, seis_e.member[1].data)
    )

    seis_e = get_live_seismogram_ensemble(2)
    assert len(seis_e.member) == 2
    cp = SeismogramEnsemble(seis_e)
    dummy_func_seismogram_ensemble_as_stream_2(data=seis_e)
    assert len(seis_e.member) == 3
    assert all(
        np.isclose(a, b).all() for a, b in zip(cp.member[0].data, seis_e.member[0].data)
    )
    assert all(
        np.isclose(a, b).all() for a, b in zip(cp.member[1].data, seis_e.member[1].data)
    )


class dummy_class_method_wrapper:
    def __init__(self):
        pass

    @mspass_method_wrapper
    def dummy_func_method_wrapper(
        self,
        data,
        *args,
        object_history=False,
        alg_id=None,
        alg_name=None,
        dryrun=False,
        inplace_return=False,
        function_return_key=None,
        handles_ensembles=False,
        checks_arg0_type=False,
        handles_dead_data=False,
        **kwargs,
    ):
        return "Finish"

    @mspass_method_wrapper
    def dummy_numeric_method(
        self,
        data,
        *args,
        object_history=False,
        alg_id=None,
        alg_name=None,
        dryrun=False,
        inplace_return=False,
        function_return_key=None,
        handles_ensembles=False,
        checks_arg0_type=False,
        handles_dead_data=False,
        **kwargs,
    ):
        """
        Used for testing handling of ensembles with handles_ensemble option.
        """
        data["foobar"] = 1.0
        return data

    @mspass_method_wrapper
    def check_arg0_tester(
        self,
        data,
        *args,
        checks_arg0_type=True,
        handle_ensemble=True,
        **kwargs,
    ):
        """
        Test function used to validate behavior of the check_arg0_type
        feature.  Raises a ValueError typical of processing functions
        when data is not a valid type.
        """
        if isinstance(data, TimeSeries):
            data["foo"] = "bar"
            return data
        else:
            raise TypeError("test function arg0 is an invalid type")


def test_mspass_method_wrapper():
    dummy_instance = dummy_class_method_wrapper()
    with pytest.raises(TypeError, match="mspass_method_wrapper only accepts") as err:
        dummy_instance.dummy_func_method_wrapper(1)

    with pytest.raises(
        ValueError, match="object_history was true but alg_id not defined"
    ) as err:
        seis = get_live_seismogram()
        dummy_instance.dummy_func_method_wrapper(seis, object_history=True)

    # added Feb 2025 to test new error handler for ValueError exception
    # do not allow function_return_key option with ensemble
    with pytest.raises(ValueError, match="Usage error:"):
        e = get_live_timeseries_ensemble(3)
        dummy_instance.dummy_numeric_method(e, function_return_key="foobar")

    # Default behavior
    assert "Finish" == dummy_instance.dummy_func_method_wrapper(seis)
    assert seis.number_of_stages() == 0

    # object_history is true
    dummy_instance.dummy_func_method_wrapper(seis, object_history=True, alg_id="0")
    assert seis.number_of_stages() == 1
    assert len(seis.get_nodes()) == 1
    assert "dummy_class_method_wrapper" in seis.current_nodedata().algorithm
    assert seis.current_nodedata().algid == "0"

    # inplace return
    data = dummy_instance.dummy_func_method_wrapper(seis, inplace_return=True)
    assert isinstance(data, Seismogram)

    # valid function_return_key
    data = dummy_instance.dummy_func_method_wrapper(
        seis, inplace_return=True, function_return_key="test_key"
    )
    assert isinstance(data, Seismogram)
    assert "test_key" in data and data["test_key"] == "Finish"

    # invalid function_return_key and not inplace_return
    data = dummy_instance.dummy_func_method_wrapper(
        seis, inplace_return=False, function_return_key=dict()
    )
    assert isinstance(data, Seismogram)
    errs = seis.elog.get_error_log()
    assert len(errs) == 2
    assert "dummy_class_method_wrapper" in errs[1].algorithm
    assert (
        errs[1].message
        == "Inconsistent arguments; inplace_return was set False and function_return_key was not None.\nAssuming inplace_return == True is correct"
    )
    assert "dummy_class_method_wrapper" in errs[0].algorithm
    assert (
        errs[0].message
        == "Illegal type received for function_return_key argument=<class 'dict'>\nReturn value not saved in Metadata"
    )

    # Test immediate return
    seis.kill()
    data = dummy_instance.dummy_func_method_wrapper(seis)
    assert data.dead()

    # test dryrun feature
    seis.set_live()
    assert "OK" == dummy_instance.dummy_func_method_wrapper(seis, dryrun=True)

    # tests for new ensemble handling features Feb 2025
    # note we use a TimeSeriesEnsemble but a SeismogramEnsemble would behave
    # the same for the current api - careful if there is divergence
    e = get_live_timeseries_ensemble(3)
    e = dummy_instance.dummy_numeric_method(e, handles_ensembles=False)
    # in this case all the members need to be tested
    for d in e.member:
        assert d["foobar"] == 1.0
    assert "foobar" not in e
    # with handles_ensembles_false reverse the tests above
    e = get_live_timeseries_ensemble(3)
    e = dummy_instance.dummy_numeric_method(e, handles_ensembles=True)
    # in this case all the members need to be tested
    for d in e.member:
        assert "foobar" not in d
    assert e["foobar"] == 1.0

    # check behavir of checks_arg0_type with handles_ensembles
    d = get_live_timeseries()
    # should work in this case
    d = dummy_instance.check_arg0_tester(d)
    assert "foo" in d
    # this should have a message from the actuall function
    with pytest.raises(TypeError, match="test function arg0 is an invalid type"):
        d = dummy_instance.check_arg0_tester("notvaliddata", checks_arg0_type=True)
    # this should return the decorator error
    with pytest.raises(
        TypeError, match="mspass_method_wrapper only accepts mspass object"
    ):
        d = dummy_instance.check_arg0_tester("notvaliddata", checks_arg0_type=False)

    # test selective typte behavior with different settings of
    # handles ensemble.  Notice check_arg0_tester internally only accepts
    # TimeSeries.   When check_arg0_type s False and handles_ensemble is
    # True it should process an ensemble.  Otherwise it should throw a
    # ValueError exception
    e = get_live_timeseries_ensemble(3)
    assert e.live
    e = dummy_instance.check_arg0_tester(
        e, checks_arg0_type=True, handles_ensembles=False
    )
    for d in e.member:
        assert d["foo"] == "bar"
    e = get_live_timeseries_ensemble(3)
    with pytest.raises(TypeError, match="test function arg0 is an invalid type"):
        e = dummy_instance.check_arg0_tester(
            e, checks_arg0_type=True, handles_ensembles=True
        )
    # simulate case of mismatched member type but valid seismic data type
    # here we get the method's exception message
    e = get_live_seismogram_ensemble(3)
    with pytest.raises(TypeError, match="test function arg0 is an invalid type"):
        e = dummy_instance.check_arg0_tester(
            e, checks_arg0_type=False, handles_ensembles=False
        )

    # check handling of an algorithm killing all data
    e = get_live_timeseries_ensemble(3)
    for i in range(3):
        e.member[i].kill()
    assert e.live
    e = dummy_instance.check_arg0_tester(
        e, checks_arg0_type=True, handles_ensembles=False
    )
    assert e.dead()
    assert e.elog.size() == 1


@mspass_func_wrapper
@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def dummy_func_2(
    data,
    *args,
    object_history=False,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    handles_ensembles=True,  # needed or mspass_func_wrapper will throw an exception
    **kwargs,
):
    if isinstance(data, obspy.Trace):
        data.data = np.array([0, 1, 2])
    elif isinstance(data, obspy.Stream):
        data[0].data[0] = -1
    else:
        return None


def test_all_decorators():
    # test mspass_func_wrapper
    with pytest.raises(TypeError) as err:
        dummy_func_2(1)
    assert "mspass_func_wrapper only accepts mspass object" in str(err.value)

    with pytest.raises(ValueError) as err:
        seis = get_live_seismogram()
        dummy_func_2(seis, object_history=True)
    assert (
        str(err.value) == "dummy_func_2: object_history was true but alg_id not defined"
    )

    assert "OK" == dummy_func_2(seis, dryrun=True)

    assert seis.number_of_stages() == 0
    dummy_func_2(seis, object_history=True, alg_id="0")
    assert seis.number_of_stages() == 1

    # test timeseries_as_trace
    ts = get_live_timeseries()
    cp = np.array(ts.data)
    dummy_func_2(ts, object_history=True, alg_id="0")
    assert len(cp) != len(ts.data)
    np.isclose([0, 1, 2], ts.data).all()
    assert ts.number_of_stages() == 1

    # test seismogram_as_stream
    seis1 = get_live_seismogram()
    cp1 = np.array(seis1.data[0])
    dummy_func_2(seis1, object_history=True, alg_id="0")
    assert cp1[0] != seis1.data[0, 0]
    assert seis1.data[0, 0] == -1
    assert seis1.number_of_stages() == 1

    # test timeseries_ensemble_as_stream
    tse = get_live_timeseries_ensemble(2)
    cp = TimeSeriesEnsemble(tse)
    dummy_func_2(tse, object_history=True, alg_id="0")
    assert tse.member[0].data[0] == -1
    assert tse.member[0].data[0] != cp.member[0].data[0]
    assert tse.member[0].number_of_stages() == 1

    # test seismogram_ensemble_as_stream
    seis_e = get_live_seismogram_ensemble(2)
    cp = SeismogramEnsemble(seis_e)
    dummy_func_2(seis_e, object_history=True, alg_id="0")
    assert seis_e.member[0].data[0, 0] == -1
    assert seis_e.member[0].data[0, 0] != cp.member[0].data[0, 0]
    assert seis_e.member[0].number_of_stages() == 1

    # test inplace return
    seis1 = get_live_seismogram()
    # upgrade of decorator -> should explicitly pass the positional arguments
    ret = dummy_func_2(seis1, object_history=True, alg_id="0")
    assert seis1 == ret


@mspass_func_wrapper_multi
def dummy_func_multi(
    data1, data2, *args, object_history=False, alg_id=None, dryrun=False, **kwargs
):
    return None


def test_mspass_func_wrapper_multi():
    with pytest.raises(TypeError) as err:
        dummy_func_multi(1, 2)
    assert "mspass_func_wrapper_multi only accepts mspass object" in str(err.value)

    with pytest.raises(ValueError) as err:
        seis1 = get_live_seismogram()
        seis2 = get_live_seismogram()
        dummy_func_multi(seis1, seis2, object_history=True)
    assert (
        str(err.value)
        == "dummy_func_multi: object_history was true but alg_id not defined"
    )

    assert "OK" == dummy_func_multi(seis1, seis2, dryrun=True)

    assert seis1.number_of_stages() == 0
    assert seis2.number_of_stages() == 0
    dummy_func_multi(seis1, seis2, object_history=True, alg_id="0")
    assert seis1.number_of_stages() == 1
    assert seis2.number_of_stages() == 1

    seis_e = get_live_seismogram_ensemble(3)
    for i in range(3):
        assert seis_e.member[i].number_of_stages() == 0
    dummy_func_multi(seis1, seis_e, object_history=True, alg_id="0")
    assert seis1.number_of_stages() == 2
    for i in range(3):
        assert seis_e.member[i].number_of_stages() == 1

    # dead object will return immediately
    seis1.kill()
    seis2.kill()
    data = dummy_func_multi(seis1, seis2)
    assert data is None


@mspass_reduce_func_wrapper
def dummy_reduce_func(
    data1, data2, *args, object_history=False, alg_id=None, dryrun=False, **kwargs
):
    data1.data[0] = -1


@mspass_reduce_func_wrapper
def dummy_reduce_func_runtime(
    data1, data2, *args, object_history=False, alg_id=None, dryrun=False, **kwargs
):
    raise RuntimeError("test")


@mspass_reduce_func_wrapper
def dummy_reduce_func_mspasserror(
    data1, data2, *args, object_history=False, alg_id=None, dryrun=False, **kwargs
):
    raise MsPASSError("test", ErrorSeverity.Fatal)


def test_mspass_reduce_func_wrapper():
    ts1 = get_live_timeseries()
    ts1.data[0] = 1
    ts2 = get_live_timeseries()
    logging_helper.info(ts2, "dummy_func", "1")
    logging_helper.info(ts2, "dummy_func_2", "2")
    assert len(ts1.get_nodes()) == 0
    dummy_reduce_func(ts1, ts2, object_history=True, alg_id="3")
    assert ts1.data[0] == -1
    assert len(ts1.get_nodes()) == 3

    with pytest.raises(TypeError) as err:
        dummy_reduce_func([0], [1], object_history=True, alg_id="3")
    assert (
        str(err.value) == "only mspass objects are supported in reduce wrapped methods"
    )

    with pytest.raises(TypeError) as err:
        dummy_reduce_func(ts1, get_live_seismogram(), object_history=True, alg_id="3")
    assert str(err.value) == "data2 has a different type as data1"

    with pytest.raises(ValueError) as err:
        seis1 = get_live_seismogram()
        seis2 = get_live_seismogram()
        dummy_reduce_func(seis1, seis2, object_history=True)
    assert (
        str(err.value)
        == "dummy_reduce_func: object_history was true but alg_id not defined"
    )

    assert "OK" == dummy_reduce_func(seis1, seis2, dryrun=True)

    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    assert len(ts1.elog.get_error_log()) == 0
    dummy_reduce_func_runtime(ts1, ts2, object_history=True, alg_id="3")
    assert len(ts1.elog.get_error_log()) == 1
    assert len(ts2.elog.get_error_log()) == 1

    ts1 = get_live_timeseries()
    ts2 = get_live_timeseries()
    assert len(ts1.elog.get_error_log()) == 0
    with pytest.raises(MsPASSError) as err:
        dummy_reduce_func_mspasserror(ts1, ts2, object_history=True, alg_id="3")
    assert str(err.value) == "test"


def test_copy_helpers():
    ts1 = get_live_timeseries()
    assert ts1.dt != 1 / 255
    ts2 = get_live_timeseries()
    ts2.dt = 1 / 255
    timeseries_copy_helper(ts1, ts2)
    assert ts1.dt == 1 / 255

    seis1 = get_live_seismogram()
    assert seis1.dt != 1 / 255
    seis2 = get_live_seismogram()
    seis2.dt = 1 / 255
    seismogram_copy_helper(seis1, seis2)
    assert seis1.dt == 1 / 255


# @mspass_func_wrapper_global_history
# def dummy_global_history_func(array, *args, mode='promiscuous', global_history=None, **kwargs):
#     array.append('test')

# def test_mspass_func_wrapper_global_history():
#     array = []
#     client = DBClient('localhost')
#     db = Database(client, 'test_decorator')
#     db['history'].delete_many({})

#     manager = GlobalHistoryManager(db, 'test_decorator_job', collection='history')
#     dummy_global_history_func(array, mode='promiscuous', global_history=manager)

#     assert array[0] == 'test'
#     # check record in the manager
#     assert db['history'].count_documents({'job_name': 'test_decorator_job'}) == 1
#     res = db['history'].find_one({'job_name': 'test_decorator_job'})
#     assert res['job_id'] == manager.job_id
#     assert res['job_name'] == manager.job_name
#     assert res['alg_name'] == 'dummy_global_history_func'
#     assert res['parameters'] == '[],mode=promiscuous'

if __name__ == "__main__":
    test_copy_helpers()
