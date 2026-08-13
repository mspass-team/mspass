import os
from contextlib import ExitStack
from numbers import Real
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

import mspasspy.algorithms.MCXcorStacking as mcx
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import (
    DoubleVector,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorSeverity


@pytest.fixture(scope="module", autouse=True)
def _verify_worktree_module():
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    assert source_root, "MSPASS_TEST_SOURCE_ROOT must identify the tree under test"
    module_path = Path(mcx.__file__).resolve()
    expected_root = (Path(source_root).resolve() / "mspasspy").resolve()
    assert module_path.is_relative_to(
        expected_root
    ), f"loaded {module_path}, expected a module below {expected_root}"


def _live_timeseries(data, *, t0=0.0, dt=1.0):
    datum = TimeSeries()
    datum.t0 = t0
    datum.dt = dt
    datum.set_npts(len(data))
    datum.data = DoubleVector(data)
    datum.set_live()
    return datum


def _zero_live_ensemble(case):
    ensemble = TimeSeriesEnsemble()
    if case == "empty":
        return ensemble
    for values in ([1.0, 2.0], [3.0, 4.0]):
        member = _live_timeseries(values)
        member.kill()
        ensemble.member.append(member)
    # Exercise an inconsistent but representable container state: the
    # container is live although every member is dead.
    ensemble.set_live()
    return ensemble


def _error_snapshot(datum):
    return [
        (entry.algorithm, entry.message, entry.badness)
        for entry in datum.elog.get_error_log()
    ]


def _member_snapshot(ensemble):
    return [
        {
            "object": member,
            "live": member.live,
            "metadata": dict(member),
            "data": np.array(member.data, dtype=float),
            "errors": _error_snapshot(member),
        }
        for member in ensemble.member
    ]


def _assert_member_snapshot(ensemble, snapshot):
    assert len(ensemble.member) == len(snapshot)
    for member, expected in zip(ensemble.member, snapshot):
        assert member is expected["object"]
        assert member.live == expected["live"]
        assert dict(member) == expected["metadata"]
        assert np.array_equal(np.array(member.data), expected["data"])
        assert _error_snapshot(member) == expected["errors"]


def _assert_finite_timeseries(datum):
    assert np.isfinite(datum.t0)
    assert np.isfinite(datum.dt)
    assert np.all(np.isfinite(np.array(datum.data, dtype=float)))
    for value in dict(datum).values():
        if isinstance(value, Real):
            assert np.isfinite(value)


def _assert_single_invalid(datum, expected_message):
    errors = datum.elog.get_error_log()
    assert len(errors) == 1
    assert errors[0].algorithm == "MsPASSError"
    assert errors[0].message == expected_message
    assert errors[0].badness == ErrorSeverity.Invalid


def _fail_if_called(name):
    return AssertionError(f"{name} must not run for a zero-live ensemble")


@pytest.mark.parametrize("case", ["empty", "all_dead"])
def test_align_and_stack_zero_live_contract(case, capsys):
    ensemble = _zero_live_ensemble(case)
    members_before = _member_snapshot(ensemble)
    input_beam = _live_timeseries([1.0, 2.0, 1.0])

    patched_calls = [
        (mcx, "regularize_sampling"),
        (mcx, "ensemble_time_range"),
        (mcx, "beam_align"),
        (mcx, "robust_stack"),
        (mcx, "_update_xcor_beam"),
        (mcx, "dbxcor_weights"),
        (mcx.np, "median"),
        (mcx.np, "average"),
        (mcx.np.linalg, "norm"),
        (mcx.signal, "correlate"),
    ]
    with ExitStack() as patches:
        for owner, name in patched_calls:
            patches.enter_context(
                patch.object(owner, name, side_effect=_fail_if_called(name))
            )
        result = mcx.align_and_stack(ensemble, input_beam)

    assert isinstance(result, list)
    assert len(result) == 2
    returned_ensemble, returned_beam = result
    assert returned_ensemble is ensemble
    assert returned_ensemble.dead()
    assert returned_beam is not input_beam
    assert input_beam.live
    assert input_beam.elog.size() == 0
    assert np.array_equal(np.array(input_beam.data), np.array([1.0, 2.0, 1.0]))
    assert isinstance(returned_beam, TimeSeries)
    assert returned_beam.dead()
    _assert_member_snapshot(returned_ensemble, members_before)
    message = "align_and_stack: input ensemble contains no live members"
    _assert_single_invalid(returned_ensemble, message)
    _assert_single_invalid(returned_beam, message)
    _assert_finite_timeseries(returned_beam)
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("case", ["empty", "all_dead"])
def test_robust_stack_zero_live_short_circuit(case, capsys):
    ensemble = _zero_live_ensemble(case)
    members_before = _member_snapshot(ensemble)

    patched_calls = [
        (mcx, "ensemble_time_range"),
        (mcx, "WindowData"),
        (mcx, "_dbxcor_stacker"),
        (mcx.np, "zeros"),
        (mcx.np, "median"),
    ]
    with ExitStack() as patches:
        for owner, name in patched_calls:
            patches.enter_context(
                patch.object(owner, name, side_effect=_fail_if_called(name))
            )
        stack, weights = mcx.robust_stack(ensemble)

    assert stack.dead()
    assert weights is None
    assert ensemble.dead()
    _assert_member_snapshot(ensemble, members_before)
    message = "robust_stack: input ensemble contains no live members"
    _assert_single_invalid(ensemble, message)
    _assert_single_invalid(stack, message)
    _assert_finite_timeseries(stack)
    assert capsys.readouterr().out == ""


def test_robust_stack_uses_live_member_sampling_when_member_zero_is_dead(capsys):
    ensemble = TimeSeriesEnsemble()
    dead_first = _live_timeseries([9.0, 9.0, 9.0])
    dead_first.dt = float("nan")
    dead_first.t0 = float("nan")
    dead_first.kill()
    ensemble.member.append(dead_first)
    live_member = _live_timeseries([1.0, 2.0, 3.0], t0=10.0, dt=0.5)
    ensemble.member.append(live_member)
    ensemble.set_live()

    stack, weights = mcx.robust_stack(ensemble, method="median")

    assert stack.live
    assert weights is None
    assert stack.t0 == 10.0
    assert stack.dt == 0.5
    assert np.array_equal(np.array(stack.data), np.array([1.0, 2.0, 3.0]))
    _assert_finite_timeseries(stack)
    assert ensemble.member[0].dead()
    assert ensemble.member[1].live
    assert capsys.readouterr().out == ""


def test_align_and_stack_one_live_control(capsys):
    samples = [0.0, 1.0, 2.0, 1.0, 0.0]
    ensemble = TimeSeriesEnsemble()
    member = _live_timeseries(samples, t0=1000.0)
    member.tref = TimeReferenceType.UTC
    member.ator(1000.0)
    ensemble.member.append(member)
    ensemble.set_live()
    beam = _live_timeseries(samples, t0=1000.0)
    beam.tref = TimeReferenceType.UTC
    beam.ator(1000.0)
    window = TimeWindow(0.0, 4.0)

    returned_ensemble, returned_beam = mcx.align_and_stack(
        ensemble,
        beam,
        correlation_window=window,
        robust_stack_window=window,
        robust_stack_method="median",
    )

    assert returned_ensemble is ensemble
    assert returned_ensemble.live
    assert returned_ensemble.member[0].live
    assert returned_beam.live
    assert np.allclose(np.array(returned_beam.data), np.array(samples))
    _assert_finite_timeseries(returned_beam)
    assert capsys.readouterr().out == ""


def test_align_and_stack_regularization_drop_is_logged_once(monkeypatch, capsys):
    ensemble = TimeSeriesEnsemble()
    member = _live_timeseries([1.0, 2.0, 3.0])
    ensemble.member.append(member)
    ensemble.set_live()
    beam = _live_timeseries([1.0, 2.0, 3.0])

    def forced_regularization_drop(input_ensemble, dt, Nsamp):
        assert input_ensemble is ensemble
        assert dt == beam.dt
        assert Nsamp == beam.npts
        dropped_member = input_ensemble.member[0]
        dropped_member.elog.log_error(
            "forced_regularization",
            "forced lower-level member failure",
            ErrorSeverity.Invalid,
        )
        dropped_member.kill()
        input_ensemble.elog.log_error(
            "forced_regularization",
            "forced regularization summary",
            ErrorSeverity.Invalid,
        )
        input_ensemble.kill()
        return input_ensemble

    monkeypatch.setattr(mcx, "regularize_sampling", forced_regularization_drop)
    returned_ensemble, returned_beam = mcx.align_and_stack(ensemble, beam)

    assert returned_ensemble is ensemble
    assert returned_ensemble.dead()
    assert _error_snapshot(returned_ensemble.member[0]) == [
        (
            "forced_regularization",
            "forced lower-level member failure",
            ErrorSeverity.Invalid,
        )
    ]
    assert _error_snapshot(returned_ensemble) == [
        (
            "forced_regularization",
            "forced regularization summary",
            ErrorSeverity.Invalid,
        )
    ]
    _assert_single_invalid(
        returned_beam,
        "align_and_stack: sampling regularization removed all live members",
    )
    assert returned_beam.dead()
    _assert_finite_timeseries(returned_beam)
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    ("method", "weights"),
    [("median", None), ("dbxcor", np.array([-1.0, -1.0]))],
)
def test_update_xcor_beam_zero_live_short_circuit(method, weights, capsys):
    ensemble = _zero_live_ensemble("all_dead")
    input_beam = _live_timeseries([1.0, 2.0, 3.0])

    with (
        patch.object(mcx, "WindowData", side_effect=_fail_if_called("WindowData")),
        patch.object(mcx.np, "median", side_effect=_fail_if_called("median")),
    ):
        beam = mcx._update_xcor_beam(ensemble, input_beam, method, weights)

    assert beam is not input_beam
    assert input_beam.live
    assert np.array_equal(np.array(input_beam.data), np.array([1.0, 2.0, 3.0]))
    assert beam.dead()
    assert np.array_equal(np.array(beam.data), np.zeros(3))
    message = "_update_xcor_beam: input ensemble contains no live members"
    _assert_single_invalid(beam, message)
    _assert_finite_timeseries(beam)
    assert capsys.readouterr().out == ""


def test_regularize_ensemble_transfers_drop_diagnostics_once(capsys):
    ensemble = TimeSeriesEnsemble()
    failed_member = _live_timeseries([1.0, 2.0])
    failed_member.elog.log_error(
        "prior", "prior member message", ErrorSeverity.Complaint
    )
    ensemble.member.append(failed_member)
    already_dead = _live_timeseries([3.0] * 11)
    already_dead.elog.log_error(
        "prior", "preexisting dead message", ErrorSeverity.Invalid
    )
    already_dead.kill()
    ensemble.member.append(already_dead)
    good_member = _live_timeseries([4.0] * 11)
    ensemble.member.append(good_member)
    ensemble.set_live()
    output = mcx.regularize_ensemble(ensemble, 0.0, 10.0, pad_fraction_cutoff=0.05)

    assert output.live
    assert len(output.member) == 1
    assert np.array_equal(np.array(output.member[0].data), np.array([4.0] * 11))
    assert ensemble.member[0].dead()
    failed_errors = ensemble.member[0].elog.get_error_log()
    assert [entry.algorithm for entry in failed_errors] == [
        "prior",
        "WindowDataAtomic",
        "WindowData_autopad",
    ]
    assert [entry.badness for entry in failed_errors] == [
        ErrorSeverity.Complaint,
        ErrorSeverity.Complaint,
        ErrorSeverity.Invalid,
    ]
    assert failed_errors[0].message == "prior member message"
    assert "Window end time is after data end time" in failed_errors[1].message
    assert "time span of data is too short" in failed_errors[2].message
    assert _error_snapshot(ensemble.member[1]) == [
        ("prior", "preexisting dead message", ErrorSeverity.Invalid)
    ]
    summary = "regularize_ensemble: dropped 2 member(s) at indices 0, 1"
    _assert_single_invalid(output, summary)
    assert capsys.readouterr().out == ""


def test_align_and_stack_all_dropped_by_sampling_regularization(capsys):
    ensemble = TimeSeriesEnsemble()
    for values in ([1.0] * 10, [2.0] * 10):
        ensemble.member.append(_live_timeseries(values, dt=2.0))
    ensemble.set_live()
    member_objects = list(ensemble.member)
    beam = _live_timeseries([1.0] * 10, dt=1.0)

    returned_ensemble, returned_beam = mcx.align_and_stack(ensemble, beam)

    assert returned_ensemble is ensemble
    assert returned_ensemble.dead()
    assert len(returned_ensemble.member) == 2
    for returned_member, original_member in zip(
        returned_ensemble.member, member_objects
    ):
        assert returned_member is original_member
        assert returned_member.dead()
        errors = returned_member.elog.get_error_log()
        assert len(errors) == 1
        assert errors[0].algorithm == "regularize_sampling"
        assert errors[0].badness == ErrorSeverity.Invalid
    ensemble_errors = returned_ensemble.elog.get_error_log()
    assert len(ensemble_errors) == 1
    assert ensemble_errors[0].algorithm == "regularize_sampling"
    assert ensemble_errors[0].badness == ErrorSeverity.Invalid
    assert returned_beam is not beam
    assert returned_beam.dead()
    _assert_single_invalid(
        returned_beam,
        "align_and_stack: sampling regularization removed all live members",
    )
    _assert_finite_timeseries(returned_beam)
    assert capsys.readouterr().out == ""
