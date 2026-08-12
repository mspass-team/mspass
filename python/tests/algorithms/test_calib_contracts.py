import math

import numpy as np
import pytest

from mspasspy.algorithms.calib import ApplyCalibEngine
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorSeverity


def _engine():
    result = object.__new__(ApplyCalibEngine)
    result.calib = {"matched": 3.0}
    return result


def _datum(old_calib=None, include_calib=True, id_value="matched"):
    result = TimeSeries(4)
    result.set_live()
    result["custom_id"] = id_value
    for sample in range(result.npts):
        result.data[sample] = sample + 1.0
    if include_calib:
        result["calib"] = old_calib
    return result


@pytest.mark.parametrize(
    "old_calib,include_calib,expected_calib",
    [
        (None, False, 3.0),
        (1.0, True, 3.0),
        (2.5, True, 7.5),
    ],
)
def test_apply_calib_records_the_cumulative_applied_factor(
    old_calib, include_calib, expected_calib
):
    datum = _datum(old_calib, include_calib)
    original_samples = np.array(datum.data, copy=True)

    result = _engine().apply_calib(datum, id_key="custom_id")

    assert result is datum
    assert np.array_equal(np.asarray(datum.data), original_samples * 3.0)
    assert datum["calib"] == expected_calib
    assert datum.live


@pytest.mark.parametrize("old_calib", ["bad", math.nan, math.inf, -math.inf])
def test_apply_calib_rejects_invalid_existing_calibration_without_mutation(old_calib):
    datum = _datum(old_calib)
    original_samples = np.array(datum.data, copy=True)
    original_calib = datum["calib"]
    original_elog_size = datum.elog.size()

    result = _engine().apply_calib(datum, id_key="custom_id")

    assert result is datum
    assert datum.dead()
    assert np.array_equal(np.asarray(datum.data), original_samples)
    if isinstance(original_calib, float) and math.isnan(original_calib):
        assert math.isnan(datum["calib"])
    else:
        assert datum["calib"] == original_calib
    assert datum.elog.size() == original_elog_size + 1
    error = datum.elog.get_error_log()[-1]
    assert error.algorithm == "ApplyCalibEngine.apply_calib"
    assert error.badness == ErrorSeverity.Invalid
    assert "Existing calib metadata must be a finite numeric value" in error.message
    assert repr(old_calib) in error.message


@pytest.mark.parametrize("kill_if_undefined", [False, True])
@pytest.mark.parametrize("failure", ["missing_id", "cache_miss"])
def test_ensemble_forwards_matching_and_error_options(kill_if_undefined, failure):
    if failure == "missing_id":
        atomic = _datum(1.0)
        atomic.erase("custom_id")
    else:
        atomic = _datum(1.0, id_value="not-cached")
    matched = _datum(1.0)
    ensemble = TimeSeriesEnsemble()
    ensemble.member.append(TimeSeries(atomic))
    ensemble.member.append(TimeSeries(matched))

    engine = _engine()
    direct = [
        engine.apply_calib(
            datum, id_key="custom_id", kill_if_undefined=kill_if_undefined
        )
        for datum in (atomic, matched)
    ]
    result = engine.apply_calib(
        ensemble, id_key="custom_id", kill_if_undefined=kill_if_undefined
    )

    for member, expected in zip(result.member, direct):
        assert member.live == expected.live
        assert np.array_equal(np.asarray(member.data), np.asarray(expected.data))
        assert dict(member) == dict(expected)
        assert member.elog.size() == expected.elog.size()
        if member.elog.size():
            member_error = member.elog.get_error_log()[-1]
            expected_error = expected.elog.get_error_log()[-1]
            assert member_error.algorithm == expected_error.algorithm
            assert member_error.message == expected_error.message
            assert member_error.badness == expected_error.badness


def test_ensemble_custom_id_success_matches_direct_atomic_call():
    atomic = _datum(2.0)
    ensemble = TimeSeriesEnsemble()
    ensemble.member.append(TimeSeries(atomic))

    engine = _engine()
    direct = engine.apply_calib(atomic, id_key="custom_id", kill_if_undefined=False)
    result = engine.apply_calib(ensemble, id_key="custom_id", kill_if_undefined=False)

    assert result is ensemble
    member = result.member[0]
    assert member.live == direct.live
    assert np.array_equal(np.asarray(member.data), np.asarray(direct.data))
    assert dict(member) == dict(direct)
    assert member["calib"] == direct["calib"] == 6.0
    assert member.elog.size() == direct.elog.size() == 1
    member_error = member.elog.get_error_log()[-1]
    direct_error = direct.elog.get_error_log()[-1]
    assert member_error.algorithm == direct_error.algorithm
    assert member_error.message == direct_error.message
    assert member_error.badness == direct_error.badness
