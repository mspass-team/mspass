import os
from pathlib import Path

import numpy as np
import pytest

import mspasspy.algorithms.MCXcorStacking as mcxcor
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import DoubleVector, TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorSeverity

SOURCE_PYTHON_ROOT = Path(
    os.environ.get("MSPASS_TEST_SOURCE_ROOT", Path(__file__).resolve().parents[2])
)


def make_timeseries(values, t0=0.0, dt=1.0):
    datum = TimeSeries(len(values))
    datum.t0 = t0
    datum.dt = dt
    datum.data = DoubleVector(values)
    datum.force_t0_shift(0.0)
    datum.set_live()
    return datum


def make_ensemble(*members):
    ensemble = TimeSeriesEnsemble()
    for member in members:
        ensemble.member.append(member)
    ensemble.set_live()
    return ensemble


@pytest.fixture(scope="session", autouse=True)
def assert_mcxcor_module_loaded_from_selected_worktree():
    expected = SOURCE_PYTHON_ROOT / "mspasspy/algorithms/MCXcorStacking.py"
    assert Path(mcxcor.__file__).resolve() == expected.resolve()


def test_relative_stack_change_is_padding_invariant_and_handles_zero_denominator():
    changes = []
    for padding in (0, 50):
        previous = make_timeseries([2.0, 0.0] + [0.0] * padding)
        new = make_timeseries([3.0, 0.0] + [0.0] * padding)
        changes.append(mcxcor._relative_stack_change(new, previous))
    assert changes == pytest.approx([0.5, 0.5])

    zeros = make_timeseries([0.0, 0.0])
    assert mcxcor._relative_stack_change(zeros, zeros) == 0.0
    assert np.isinf(mcxcor._relative_stack_change(make_timeseries([1.0, 0.0]), zeros))


def test_dbxcor_convergence_uses_previous_stack_and_not_padding_length(monkeypatch):
    def run_with_padding(padding):
        first = make_timeseries([1.0, 0.0] + [0.0] * padding)
        second = make_timeseries([3.0, 0.0] + [0.0] * padding)
        ensemble = make_ensemble(first, second)
        initial = make_timeseries([2.0, 0.0] + [0.0] * padding)
        weight_sequence = (
            np.array([1.0, 0.0]),
            np.array([0.0, 1.0]),
            np.array([0.0, 1.0]),
        )
        calls = []

        def fake_weights(*args, **kwargs):
            calls.append(np.array(args[1].data))
            return weight_sequence[min(len(calls) - 1, len(weight_sequence) - 1)]

        monkeypatch.setattr(mcxcor, "dbxcor_weights", fake_weights)
        stack, _ = mcxcor._dbxcor_stacker(ensemble, initial, eps=0.75, maxiterations=10)
        return calls, np.array(stack.data), np.array(initial.data)

    short_calls, short_stack, short_initial = run_with_padding(0)
    padded_calls, padded_stack, padded_initial = run_with_padding(50)

    assert [call[0] for call in short_calls] == [2.0, 1.0, 3.0]
    assert [call[0] for call in padded_calls] == [2.0, 1.0, 3.0]
    assert short_stack[0] == padded_stack[0] == 3.0
    assert np.array_equal(short_initial, np.array([2.0, 0.0]))
    assert np.array_equal(padded_initial, np.array([2.0, 0.0] + [0.0] * 50))


@pytest.mark.parametrize("abort_irregular_sampling", [False, True])
def test_align_and_stack_forwards_regularization_option(
    monkeypatch, abort_irregular_sampling, capsys
):
    ensemble = make_ensemble(make_timeseries([1.0, 1.0]))
    beam = make_timeseries([1.0, 1.0])
    calls = []

    def fake_regularize(data, dt_expected, Nsamp, abort_on_error=None):
        calls.append((data, dt_expected, Nsamp, abort_on_error))
        data.kill()
        return data

    monkeypatch.setattr(mcxcor, "regularize_sampling", fake_regularize)
    result = mcxcor.align_and_stack(
        ensemble,
        beam,
        abort_irregular_sampling=abort_irregular_sampling,
    )

    assert len(result) == 2
    assert calls == [(ensemble, beam.dt, beam.npts, abort_irregular_sampling)]
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    ("use_median_initial_stack", "expected_seed"),
    [(True, [2.0, 2.0]), (False, [9.0, 9.0])],
)
def test_align_and_stack_honors_initial_stack_option_and_success_shape(
    monkeypatch,
    use_median_initial_stack,
    expected_seed,
    capsys,
):
    ensemble = make_ensemble(make_timeseries([1.0, 1.0]), make_timeseries([3.0, 3.0]))
    beam = make_timeseries([9.0, 9.0])
    window = TimeWindow(0.0, 1.0)
    seeds = []

    monkeypatch.setattr(mcxcor, "regularize_sampling", lambda data, *a, **k: data)
    monkeypatch.setattr(mcxcor, "beam_align", lambda data, *a, **k: data)

    def fake_stacker(data, stack0, **kwargs):
        seeds.append(np.array(stack0.data))
        return [TimeSeries(stack0), np.ones(len(data.member))]

    monkeypatch.setattr(mcxcor, "_dbxcor_stacker", fake_stacker)
    monkeypatch.setattr(
        mcxcor,
        "dbxcor_weights",
        lambda data, stack, **kwargs: np.ones(len(data.member)),
    )
    monkeypatch.setattr(
        mcxcor, "_update_xcor_beam", lambda data, output, method, weights: output
    )

    result = mcxcor.align_and_stack(
        ensemble,
        beam,
        correlation_window=window,
        robust_stack_window=window,
        use_median_initial_stack=use_median_initial_stack,
        convergence=np.inf,
    )

    assert len(result) == 2
    assert result[0] is ensemble
    assert result[1].live
    assert len(seeds) == 1
    assert np.array_equal(seeds[0], np.array(expected_seed))
    assert all(
        member.is_defined("arrival_time_correction") for member in ensemble.member
    )
    assert capsys.readouterr().out == ""


def test_align_and_stack_iteration_exhaustion_is_dead_atomic_and_silent(
    monkeypatch, capsys
):
    first = make_timeseries([1.0, 1.0])
    second = make_timeseries([1.0, 1.0])
    second["arrival_time_correction"] = 4.5
    ensemble = make_ensemble(first, second)
    beam = make_timeseries([1.0, 1.0])
    window = TimeWindow(0.0, 1.0)
    robust_stack_calls = []

    monkeypatch.setattr(mcxcor, "regularize_sampling", lambda data, *a, **k: data)
    monkeypatch.setattr(mcxcor, "beam_align", lambda data, *a, **k: data)

    def never_converges(data, stack0=None, **kwargs):
        robust_stack_calls.append(stack0)
        next_stack = TimeSeries(beam)
        next_stack *= 2.0 ** len(robust_stack_calls)
        return [next_stack, np.ones(len(data.member))]

    monkeypatch.setattr(mcxcor, "robust_stack", never_converges)
    monkeypatch.setattr(
        mcxcor, "_update_xcor_beam", lambda data, output, method, weights: output
    )

    result = mcxcor.align_and_stack(
        ensemble,
        beam,
        correlation_window=window,
        robust_stack_window=window,
        use_median_initial_stack=False,
        convergence=0.5,
    )

    assert len(result) == 2
    assert result[0] is ensemble
    dead_beam = result[1]
    assert dead_beam.dead()
    assert len(robust_stack_calls) == 20
    assert not first.is_defined("arrival_time_correction")
    assert second["arrival_time_correction"] == 4.5
    errors = dead_beam.elog.get_error_log()
    assert len(errors) == 1
    assert errors[0].algorithm == "MsPASSError"
    assert errors[0].message == (
        "align_and_stack: robust_stack iterative loop did not converge "
        "after 20 iterations"
    )
    assert errors[0].badness == ErrorSeverity.Invalid
    assert capsys.readouterr().out == ""
