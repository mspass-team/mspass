import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

import mspasspy.algorithms.window as window_module
from mspasspy.ccore.seismic import (
    DoubleVector,
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError

AMPLITUDE_KEYS = {
    "amplitude",
    "rms_amplitude",
    "perc_amplitude",
    "mad_amplitude",
}


def _atomic(multiplier=1.0, waveform_type="timeseries"):
    samples = np.array([1.0, -2.0, 4.0, -3.0]) * multiplier
    if waveform_type == "timeseries":
        datum = TimeSeries(4)
        datum.data = DoubleVector(samples)
    else:
        datum = Seismogram(4)
        for sample_index, sample in enumerate(samples):
            datum.data[0, sample_index] = sample
            datum.data[1, sample_index] = 0.5 * sample
            datum.data[2, sample_index] = -sample
    datum.dt = 1.0
    datum.t0 = 0.0
    datum.set_live()
    return datum


def _ensemble(include_dead=True, waveform_type="timeseries"):
    if waveform_type == "timeseries":
        ensemble = TimeSeriesEnsemble()
    else:
        ensemble = SeismogramEnsemble()
    ensemble.member.append(_atomic(1.0, waveform_type))
    ensemble.member.append(_atomic(2.0, waveform_type))
    if include_dead:
        dead_member = _atomic(3.0, waveform_type)
        dead_member.kill()
        ensemble.member.append(dead_member)
    ensemble.set_live()
    return ensemble


def _defined_amplitude_keys(datum):
    return {key for key in AMPLITUDE_KEYS if datum.is_defined(key)}


def _error_log(datum):
    return datum.elog.get_error_log()


def _input_and_target(input_kind, include_dead=True, waveform_type="timeseries"):
    if input_kind == "atomic":
        datum = _atomic(waveform_type=waveform_type)
        return datum, "_scale", {}, [datum], []

    ensemble = _ensemble(include_dead=include_dead, waveform_type=waveform_type)
    live_members = ensemble.member[:2]
    dead_members = ensemble.member[2:] if include_dead else []
    if input_kind == "ensemble_members":
        return (
            ensemble,
            "_scale_ensemble_members",
            {"scale_by_section": False},
            live_members,
            dead_members,
        )
    return (
        ensemble,
        "_scale_ensemble",
        {"scale_by_section": True},
        live_members,
        dead_members,
    )


def _current_members(data, input_kind, include_dead=True):
    if input_kind == "atomic":
        return [data], []
    live_members = [data.member[0], data.member[1]]
    dead_members = [data.member[2]] if include_dead else []
    return live_members, dead_members


def test_contract_module_is_loaded_from_selected_build():
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    relative_path = Path("mspasspy/algorithms/window.py")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(window_module.__file__).resolve() == Path(expected_module).resolve()


@pytest.mark.parametrize(
    "method,expected_key",
    [
        ("peak", "amplitude"),
        ("RMS", "rms_amplitude"),
        ("rms", "rms_amplitude"),
        ("perc", "perc_amplitude"),
        ("MAD", "mad_amplitude"),
        ("mad", "mad_amplitude"),
    ],
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_atomic_methods_post_exact_amplitude_key(method, expected_key, waveform_type):
    datum = _atomic(waveform_type=waveform_type)
    level = 0.5 if method == "perc" else 1.0

    result = window_module.scale(datum, method=method, level=level)

    assert result is datum
    assert datum.live
    assert _defined_amplitude_keys(datum) == {expected_key}
    assert datum[expected_key] > 0.0


@pytest.mark.parametrize(
    "method,expected_key",
    [
        ("peak", "amplitude"),
        ("RMS", "rms_amplitude"),
        ("rms", "rms_amplitude"),
        ("perc", "perc_amplitude"),
        ("MAD", "mad_amplitude"),
        ("mad", "mad_amplitude"),
    ],
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_section_methods_post_exact_amplitude_key(method, expected_key, waveform_type):
    ensemble = _ensemble(waveform_type=waveform_type)
    level = 0.5 if method == "perc" else 1.0

    result = window_module.scale(
        ensemble, method=method, level=level, scale_by_section=True
    )

    assert result is ensemble
    assert _defined_amplitude_keys(ensemble) == {expected_key}
    assert ensemble[expected_key] > 0.0
    for member in ensemble.member:
        assert _defined_amplitude_keys(member) == set()


@pytest.mark.parametrize(
    "method,expected_key",
    [
        ("peak", "amplitude"),
        ("RMS", "rms_amplitude"),
        ("rms", "rms_amplitude"),
        ("perc", "perc_amplitude"),
        ("MAD", "mad_amplitude"),
        ("mad", "mad_amplitude"),
    ],
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_ensemble_member_methods_post_exact_amplitude_key(
    method, expected_key, waveform_type
):
    ensemble = _ensemble(include_dead=False, waveform_type=waveform_type)
    level = 0.5 if method == "perc" else 1.0

    result = window_module.scale(ensemble, method=method, level=level)

    assert result is ensemble
    assert _defined_amplitude_keys(ensemble) == set()
    for member in ensemble.member:
        assert member.live
        assert _defined_amplitude_keys(member) == {expected_key}
        assert member[expected_key] > 0.0


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
@pytest.mark.parametrize("scale_by_section", [False, True])
def test_percentile_validation_posts_only_to_live_ensemble_members(
    waveform_type, scale_by_section
):
    ensemble = _ensemble(waveform_type=waveform_type)
    target_name = "_scale_ensemble" if scale_by_section else "_scale_ensemble_members"
    return_value = 1.0 if scale_by_section else [1.0, 2.0, 0.0]
    passed_levels = []

    def record_level(*args, **kwargs):
        passed_levels.append(args[2])
        return return_value

    with patch.object(window_module, target_name, side_effect=record_level):
        result = window_module.scale(
            ensemble,
            method="perc",
            level=0.0,
            scale_by_section=scale_by_section,
        )

    assert result is ensemble
    assert passed_levels == [1.0]
    for member in ensemble.member[:2]:
        errors = _error_log(member)
        assert len(errors) == 1
        assert errors[0].algorithm == "scale"
        assert errors[0].badness == ErrorSeverity.Complaint
        assert "Defaulted to 1.0" in errors[0].message
    dead_member = ensemble.member[2]
    assert dead_member.dead()
    assert _error_log(dead_member) == []


@pytest.mark.parametrize(
    "input_kind", ["atomic", "ensemble_members", "ensemble_section"]
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
@pytest.mark.parametrize(
    "level,expected_level",
    [(0.5, 0.5), (1.0, 1.0), (0.0, 1.0), (-0.25, 1.0), (1.25, 1.0)],
)
def test_percentile_validation_recovers_and_continues(
    input_kind, waveform_type, level, expected_level
):
    data, target_name, scale_kwargs, live_members, dead_members = _input_and_target(
        input_kind, include_dead=False, waveform_type=waveform_type
    )
    original_target = getattr(window_module, target_name)
    passed_levels = []

    def record_level(*args, **kwargs):
        passed_levels.append(args[2])
        if level == 0.5:
            return original_target(*args, **kwargs)
        if input_kind == "atomic":
            return 4.0
        if input_kind == "ensemble_section":
            return 6.0
        return [4.0, 8.0]

    with patch.object(window_module, target_name, side_effect=record_level):
        result = window_module.scale(data, method="perc", level=level, **scale_kwargs)

    assert result is data
    assert passed_levels == [expected_level]
    live_members, dead_members = _current_members(data, input_kind, include_dead=False)
    for member in live_members:
        assert member.live
    for member in dead_members:
        assert member.dead()
        assert _error_log(member) == []

    if input_kind == "ensemble_section":
        assert _defined_amplitude_keys(data) == {"perc_amplitude"}
    else:
        for member in live_members:
            assert _defined_amplitude_keys(member) == {"perc_amplitude"}

    expected_error_count = 0 if 0.0 < level <= 1.0 else 1
    for member in live_members:
        errors = _error_log(member)
        assert len(errors) == expected_error_count
        if errors:
            assert errors[0].algorithm == "scale"
            assert errors[0].badness == ErrorSeverity.Complaint
            assert "Defaulted to 1.0" in errors[0].message


@pytest.mark.parametrize(
    "input_kind", ["atomic", "ensemble_members", "ensemble_section"]
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_mspass_errors_kill_and_return_original_object(input_kind, waveform_type):
    data, target_name, scale_kwargs, live_members, dead_members = _input_and_target(
        input_kind, waveform_type=waveform_type
    )
    dead_snapshots = [
        (dict(member), np.asarray(member.data).copy()) for member in dead_members
    ]
    error = MsPASSError("injected scale failure", ErrorSeverity.Complaint)

    with patch.object(window_module, target_name, side_effect=error):
        result = window_module.scale(data, **scale_kwargs)

    assert result is data
    live_members, dead_members = _current_members(data, input_kind)
    for member in live_members:
        assert member.dead()
        errors = _error_log(member)
        assert len(errors) == 1
        assert errors[0].algorithm == "scale"
        assert errors[0].badness == ErrorSeverity.Invalid
        assert "injected scale failure" in errors[0].message
        assert _defined_amplitude_keys(member) == set()
    for member, (metadata, samples) in zip(dead_members, dead_snapshots):
        assert member.dead()
        assert _error_log(member) == []
        assert dict(member) == metadata
        assert np.array_equal(np.asarray(member.data), samples)


@pytest.mark.parametrize(
    "input_kind", ["atomic", "ensemble_members", "ensemble_section"]
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
@pytest.mark.parametrize("exception_type", [RuntimeError, TypeError])
def test_unexpected_exceptions_propagate_without_mutation(
    input_kind, waveform_type, exception_type
):
    data, target_name, scale_kwargs, live_members, dead_members = _input_and_target(
        input_kind, waveform_type=waveform_type
    )
    before = [
        (dict(member), np.asarray(member.data).copy(), member.live)
        for member in live_members + dead_members
    ]

    with patch.object(
        window_module, target_name, side_effect=exception_type("injected failure")
    ):
        with pytest.raises(exception_type, match="injected failure"):
            window_module.scale(data, **scale_kwargs)

    current_live, current_dead = _current_members(data, input_kind)
    for member, (metadata, samples, live) in zip(current_live + current_dead, before):
        assert dict(member) == metadata
        assert np.array_equal(np.asarray(member.data), samples)
        assert member.live is live
        assert _error_log(member) == []


def test_timeseries_percentile_preserves_rank_and_caps_terminal_index():
    median_rank = _atomic()
    window_module.scale(median_rank, method="perc", level=0.5)
    assert median_rank["perc_amplitude"] == pytest.approx(3.0)

    terminal_rank = _atomic()
    window_module.scale(terminal_rank, method="perc", level=1.0)
    assert terminal_rank["perc_amplitude"] == pytest.approx(4.0)


@pytest.mark.parametrize(
    "input_kind", ["atomic", "ensemble_members", "ensemble_section"]
)
@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
@pytest.mark.parametrize("interrupt_type", [KeyboardInterrupt, SystemExit])
def test_process_control_exceptions_propagate(
    input_kind, waveform_type, interrupt_type
):
    data, target_name, scale_kwargs, live_members, dead_members = _input_and_target(
        input_kind, waveform_type=waveform_type
    )
    before = [np.asarray(member.data).copy() for member in live_members]

    with patch.object(window_module, target_name, side_effect=interrupt_type()):
        with pytest.raises(interrupt_type):
            window_module.scale(data, **scale_kwargs)

    live_members, dead_members = _current_members(data, input_kind)
    for member, original_samples in zip(live_members, before):
        assert member.live
        assert _error_log(member) == []
        assert np.array_equal(np.asarray(member.data), original_samples)
    for member in dead_members:
        assert member.dead()
        assert _error_log(member) == []


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_dead_atomic_input_is_returned_unchanged(waveform_type):
    datum = _atomic(waveform_type=waveform_type)
    datum["sentinel"] = "unchanged"
    datum.kill()
    metadata = dict(datum)
    samples = np.asarray(datum.data).copy()

    result = window_module.scale(datum, method="perc", level=0.0)

    assert result is datum
    assert datum.dead()
    assert dict(datum) == metadata
    assert np.array_equal(np.asarray(datum.data), samples)
    assert _error_log(datum) == []


@pytest.mark.parametrize("ensemble_type", [TimeSeriesEnsemble, SeismogramEnsemble])
def test_empty_ensemble_is_returned_unchanged(ensemble_type):
    ensemble = ensemble_type()

    result = window_module.scale(ensemble, method="perc", level=-1.0)

    assert result is ensemble
    assert ensemble.dead()
    assert len(ensemble.member) == 0
    assert dict(ensemble) == {}
