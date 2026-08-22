import ast
import math
from pathlib import Path

import numpy as np
import pytest

from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.util.seismic import regularize_sampling, sort_ensemble


def _ensemble(intervals, live=True):
    ensemble = TimeSeriesEnsemble()
    for index, interval in enumerate(intervals):
        member = TimeSeries(3)
        member.dt = interval
        member["marker"] = index
        member.set_live()
        ensemble.member.append(member)
    if live:
        ensemble.set_live()
    else:
        ensemble.kill()
    return ensemble


def _snapshot(ensemble):
    return (
        ensemble.live,
        ensemble.elog.size(),
        tuple(
            (
                member.live,
                member.dt,
                dict(member),
                member.elog.size(),
                np.asarray(member.data).copy(),
            )
            for member in ensemble.member
        ),
    )


@pytest.mark.parametrize("nsamp", [0, 1, 1.5, True, "2"])
@pytest.mark.parametrize("abort", [False, True])
def test_invalid_nsamp_is_rejected_before_scanning_or_mutation(nsamp, abort):
    ensemble = _ensemble([0.1, math.nan])
    before = _snapshot(ensemble)

    with pytest.raises(ValueError, match="Nsamp must be an integer >= 2"):
        regularize_sampling(ensemble, 0.1, Nsamp=nsamp, abort_on_error=abort)

    _assert_snapshot_equal(_snapshot(ensemble), before)


@pytest.mark.parametrize(
    "expected", [0.0, -0.1, math.nan, math.inf, -math.inf, True, "0.1"]
)
@pytest.mark.parametrize("abort", [False, True])
def test_invalid_expected_interval_is_rejected_before_scanning_or_mutation(
    expected, abort
):
    ensemble = _ensemble([0.1, 0.2])
    before = _snapshot(ensemble)

    with pytest.raises(ValueError, match="finite positive real"):
        regularize_sampling(ensemble, expected, Nsamp=2, abort_on_error=abort)

    _assert_snapshot_equal(_snapshot(ensemble), before)


def _assert_snapshot_equal(actual, expected):
    assert actual[0:2] == expected[0:2]
    assert len(actual[2]) == len(expected[2])
    for actual_member, expected_member in zip(actual[2], expected[2]):
        assert actual_member[0] == expected_member[0]
        if math.isnan(expected_member[1]):
            assert math.isnan(actual_member[1])
        else:
            assert actual_member[1] == expected_member[1]
        assert actual_member[2].keys() == expected_member[2].keys()
        for key, expected_value in expected_member[2].items():
            actual_value = actual_member[2][key]
            if isinstance(expected_value, float) and math.isnan(expected_value):
                assert math.isnan(actual_value)
            else:
                assert actual_value == expected_value
        assert actual_member[3] == expected_member[3]
        assert np.array_equal(actual_member[4], expected_member[4])


@pytest.mark.parametrize("abort", [False, True])
def test_cutoff_is_inclusive_and_next_representable_value_is_invalid(abort):
    expected = 0.125
    nsamp = 2
    cutoff = expected / (2 * (nsamp - 1))
    at_cutoff = expected + cutoff
    beyond = np.nextafter(at_cutoff, math.inf)
    ensemble = _ensemble([at_cutoff, beyond])

    if abort:
        before = _snapshot(ensemble)
        with pytest.raises(ValueError, match="Member 1"):
            regularize_sampling(ensemble, expected, Nsamp=nsamp, abort_on_error=True)
        _assert_snapshot_equal(_snapshot(ensemble), before)
        return

    result = regularize_sampling(ensemble, expected, Nsamp=nsamp, abort_on_error=False)

    assert result is ensemble
    assert result.member[0].live
    assert result.member[0].elog.size() == 0
    assert result.member[1].dead()
    assert result.member[1].elog.size() == 1
    assert result.member[1].elog.get_error_log()[0].badness == ErrorSeverity.Invalid


@pytest.mark.parametrize("bad_interval", [0.0, -0.1, math.nan, math.inf, -math.inf])
def test_invalid_member_intervals_are_atomic_or_killed_once(bad_interval):
    abort_ensemble = _ensemble([0.1, bad_interval])
    before = _snapshot(abort_ensemble)

    with pytest.raises(ValueError, match="Member 1"):
        regularize_sampling(abort_ensemble, 0.1, Nsamp=10, abort_on_error=True)

    _assert_snapshot_equal(_snapshot(abort_ensemble), before)

    nonabort_ensemble = _ensemble([0.1, bad_interval])
    result = regularize_sampling(nonabort_ensemble, 0.1, Nsamp=10, abort_on_error=False)

    assert result is nonabort_ensemble
    assert [member.live for member in result.member] == [True, False]
    assert [member.elog.size() for member in result.member] == [0, 1]
    assert result.member[1].elog.get_error_log()[0].badness == ErrorSeverity.Invalid


def test_nonabort_kills_each_invalid_live_member_once_and_preserves_others():
    ensemble = _ensemble([0.2, 0.1, 0.3, 0.1])
    ensemble.member[3].kill()
    valid_before = (
        ensemble.member[1].live,
        ensemble.member[1].dt,
        dict(ensemble.member[1]),
        ensemble.member[1].elog.size(),
        np.asarray(ensemble.member[1].data).copy(),
    )
    dead_before = (
        ensemble.member[3].live,
        ensemble.member[3].dt,
        dict(ensemble.member[3]),
        ensemble.member[3].elog.size(),
        np.asarray(ensemble.member[3].data).copy(),
    )

    result = regularize_sampling(ensemble, 0.1, Nsamp=10, abort_on_error=False)

    assert result is ensemble
    assert result.live
    assert [member.live for member in result.member] == [False, True, False, False]
    assert [member.elog.size() for member in result.member] == [1, 0, 1, 0]
    valid_after = (
        result.member[1].live,
        result.member[1].dt,
        dict(result.member[1]),
        result.member[1].elog.size(),
        np.asarray(result.member[1].data).copy(),
    )
    dead_after = (
        result.member[3].live,
        result.member[3].dt,
        dict(result.member[3]),
        result.member[3].elog.size(),
        np.asarray(result.member[3].data).copy(),
    )
    assert valid_after[0:4] == valid_before[0:4]
    assert np.array_equal(valid_after[4], valid_before[4])
    assert dead_after[0:4] == dead_before[0:4]
    assert np.array_equal(dead_after[4], dead_before[4])


def test_nonabort_kills_ensemble_and_logs_once_when_no_live_member_remains():
    ensemble = _ensemble([0.2, math.inf])

    result = regularize_sampling(ensemble, 0.1, Nsamp=10, abort_on_error=False)

    assert result is ensemble
    assert result.dead()
    assert [member.elog.size() for member in result.member] == [1, 1]
    assert result.elog.size() == 1
    error = result.elog.get_error_log()[0]
    assert error.badness == ErrorSeverity.Invalid
    assert error.algorithm == "regularize_sampling"


def test_dead_ensemble_is_returned_unchanged_after_validating_callers():
    ensemble = _ensemble([math.nan], live=False)
    before = _snapshot(ensemble)

    result = regularize_sampling(ensemble, 0.1, Nsamp=2)

    assert result is ensemble
    _assert_snapshot_equal(_snapshot(ensemble), before)


def test_sort_ensemble_type_error_contains_algorithm_type_and_severity():
    with pytest.raises(MsPASSError) as captured:
        sort_ensemble(TimeSeries(), "sta")

    assert "sort_ensemble:" in captured.value.message
    assert "TimeSeries" in captured.value.message
    assert captured.value.severity == ErrorSeverity.Fatal


def test_every_mspass_error_in_seismic_uses_two_arguments():
    source_path = Path(__file__).resolve().parents[2] / "mspasspy/util/seismic.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "MsPASSError"
    ]

    assert calls
    assert all(len(call.args) == 2 and not call.keywords for call in calls)
