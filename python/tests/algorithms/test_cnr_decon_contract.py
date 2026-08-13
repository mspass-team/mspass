import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import numpy as np
import pytest

import mspasspy.algorithms.CNRDecon as cnr_module
import mspasspy.ccore.algorithms.deconvolution as deconvolution_binding
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.algorithms.CNRDecon import CNRArrayDecon, CNRRFDecon
from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.ccore.seismic import (
    DoubleVector,
    PowerSpectrum,
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
)
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError, pfread

DT = 0.05
NOISE_WINDOW = TimeWindow(-4.0, -2.0)
SIGNAL_WINDOW = TimeWindow(0.0, 2.0)


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
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
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


def test_contract_suite_uses_selected_build_and_real_binding():
    _assert_module_from_selected_build(cnr_module, "mspasspy/algorithms/CNRDecon.py")
    assert Path(deconvolution_binding.__file__).suffix == ".so"


def _timeseries(npts=161, t0=-4.0):
    datum = TimeSeries(npts)
    datum.dt = DT
    datum.t0 = t0
    datum.tref = TimeReferenceType.Relative
    datum.set_live()
    datum.data[:] = DoubleVector(2.0 + np.sin(np.arange(npts) * 0.07))
    datum["low_f_band_edge"] = 0.2
    datum["high_f_band_edge"] = 4.0
    return datum


def _seismogram(npts=161, t0=-4.0):
    datum = Seismogram(npts)
    datum.dt = DT
    datum.t0 = t0
    datum.tref = TimeReferenceType.Relative
    datum.set_live()
    samples = np.arange(npts, dtype=float)
    for component in range(3):
        datum.data[component, :] = DoubleVector(10.0 * (component + 1) + samples)
    datum["low_f_band_edge"] = 0.2
    datum["high_f_band_edge"] = 4.0
    return datum


def _ensemble():
    result = SeismogramEnsemble()
    result.member.append(_seismogram())
    result.set_live()
    return result


def _engine_and_spectrum():
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    spectrum = engine.compute_noise_spectrum(_timeseries(npts=128, t0=-3.0))
    assert spectrum.live
    return engine, spectrum


def _waveform_from_result(result, return_wavelet, expected_type):
    if return_wavelet:
        assert isinstance(result, list)
        assert len(result) == 3
        assert result[1:] == [None, None]
        waveform = result[0]
    else:
        assert isinstance(result, expected_type)
        waveform = result
    assert isinstance(waveform, expected_type)
    return waveform


def _record_array_calls(monkeypatch, spectrum):
    calls = {"scalar": [], "three_component": [], "initialize": [], "process": []}

    def scalar(self, noise):
        calls["scalar"].append(noise)
        return spectrum

    def three_component(self, noise):
        calls["three_component"].append(noise)
        return spectrum

    def initialize(self, wavelet, noise_spectrum):
        calls["initialize"].append((TimeSeries(wavelet), noise_spectrum))

    def process(self, datum, noise_spectrum, flow, fhigh):
        calls["process"].append((noise_spectrum, flow, fhigh))
        return Seismogram(datum)

    monkeypatch.setattr(CNRDeconEngine, "compute_noise_spectrum", scalar)
    monkeypatch.setattr(CNRDeconEngine, "compute_noise_spectrum_3C", three_component)
    monkeypatch.setattr(CNRDeconEngine, "initialize_inverse_operator", initialize)
    monkeypatch.setattr(CNRDeconEngine, "process", process)
    return calls


@pytest.mark.parametrize(
    "beam_kind,use_3c_noise,beam_component,expected_binding",
    [
        ("timeseries", False, 2, "scalar"),
        ("timeseries", True, 2, "scalar"),
        ("seismogram", False, 0, "scalar"),
        ("seismogram", False, 1, "scalar"),
        ("seismogram", False, 2, "scalar"),
        ("seismogram", True, 0, "three_component"),
        ("seismogram", True, 1, "three_component"),
        ("seismogram", True, 2, "three_component"),
    ],
)
def test_array_computed_noise_uses_matching_binding_and_beam_component(
    monkeypatch,
    capsys,
    beam_kind,
    use_3c_noise,
    beam_component,
    expected_binding,
):
    engine, spectrum = _engine_and_spectrum()
    beam = _timeseries() if beam_kind == "timeseries" else _seismogram()
    expected_noise = WindowData(beam, NOISE_WINDOW.start, NOISE_WINDOW.end)
    if isinstance(expected_noise, Seismogram) and not use_3c_noise:
        expected_noise = ExtractComponent(expected_noise, beam_component)
    expected_wavelet = beam
    if isinstance(expected_wavelet, Seismogram):
        expected_wavelet = ExtractComponent(expected_wavelet, beam_component)
    expected_wavelet = WindowData(
        expected_wavelet, SIGNAL_WINDOW.start, SIGNAL_WINDOW.end
    )
    calls = _record_array_calls(monkeypatch, spectrum)

    result = CNRArrayDecon(
        _ensemble(),
        beam,
        engine,
        noise_window=NOISE_WINDOW,
        signal_window=SIGNAL_WINDOW,
        use_3C_noise=use_3c_noise,
        beam_component=beam_component,
        return_wavelet=False,
    )

    assert result.live
    assert len(calls[expected_binding]) == 1
    other_binding = "three_component" if expected_binding == "scalar" else "scalar"
    assert calls[other_binding] == []
    noise_argument = calls[expected_binding][0]
    assert type(noise_argument) is type(expected_noise)
    assert np.array_equal(noise_argument.data, expected_noise.data)
    assert len(calls["initialize"]) == 1
    initialized_wavelet = calls["initialize"][0][0]
    assert isinstance(initialized_wavelet, TimeSeries)
    assert np.array_equal(initialized_wavelet.data, expected_wavelet.data)
    assert len(calls["process"]) == 1
    if beam_kind == "timeseries" and use_3c_noise:
        logs = result.elog.get_error_log()
        assert len(logs) == 1
        assert logs[0].badness == ErrorSeverity.Complaint
        assert "scalar beam" in logs[0].message
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "beam_kind,beam_component",
    [
        ("timeseries", 2),
        ("seismogram", 0),
        ("seismogram", 1),
        ("seismogram", 2),
    ],
)
def test_array_precomputed_spectrum_uses_the_same_scalar_beam(
    monkeypatch, capsys, beam_kind, beam_component
):
    engine, spectrum = _engine_and_spectrum()
    beam = _timeseries(t0=0.0) if beam_kind == "timeseries" else _seismogram(t0=0.0)
    expected = beam
    if isinstance(expected, Seismogram):
        expected = ExtractComponent(expected, beam_component)
    calls = _record_array_calls(monkeypatch, spectrum)

    result = CNRArrayDecon(
        _ensemble(),
        beam,
        engine,
        noise_spectrum=spectrum,
        beam_component=beam_component,
        return_wavelet=False,
    )

    assert result.live
    assert calls["scalar"] == []
    assert calls["three_component"] == []
    assert len(calls["initialize"]) == 1
    initialized_wavelet = calls["initialize"][0][0]
    assert isinstance(initialized_wavelet, TimeSeries)
    assert np.array_equal(initialized_wavelet.data, expected.data)
    assert capsys.readouterr().out == ""


def _dead_spectrum():
    spectrum = PowerSpectrum()
    spectrum.kill()
    spectrum.elog.log_error("lower", "lower complaint", ErrorSeverity.Complaint)
    spectrum.elog.log_error("lower", "lower suspect", ErrorSeverity.Suspect)
    assert spectrum.dead()
    return spectrum


@pytest.mark.parametrize("wrapper", ("rf", "array"))
@pytest.mark.parametrize("source", ("precomputed", "computed"))
@pytest.mark.parametrize("use_3c_noise", (False, True))
@pytest.mark.parametrize("return_wavelet", (False, True))
def test_dead_spectrum_logs_once_and_returns_immediately(
    monkeypatch, capsys, wrapper, source, use_3c_noise, return_wavelet
):
    engine, _ = _engine_and_spectrum()
    dead_spectrum = _dead_spectrum()

    def return_dead(self, noise):
        return dead_spectrum

    def forbidden_initialize(self, wavelet, noise_spectrum):
        raise AssertionError("inverse operator must not be initialized")

    monkeypatch.setattr(CNRDeconEngine, "compute_noise_spectrum", return_dead)
    monkeypatch.setattr(CNRDeconEngine, "compute_noise_spectrum_3C", return_dead)
    monkeypatch.setattr(
        CNRDeconEngine, "initialize_inverse_operator", forbidden_initialize
    )
    supplied_spectrum = dead_spectrum if source == "precomputed" else None
    if wrapper == "rf":
        result = CNRRFDecon(
            _seismogram(),
            engine,
            signal_window=SIGNAL_WINDOW if source == "computed" else None,
            noise_window=NOISE_WINDOW if source == "computed" else None,
            noise_spectrum=supplied_spectrum,
            use_3C_noise=use_3c_noise,
            return_wavelet=return_wavelet,
            window_output=False,
        )
        output = _waveform_from_result(result, return_wavelet, Seismogram)
    else:
        result = CNRArrayDecon(
            _ensemble(),
            _seismogram() if use_3c_noise else _timeseries(),
            engine,
            signal_window=SIGNAL_WINDOW if source == "computed" else None,
            noise_window=NOISE_WINDOW if source == "computed" else None,
            noise_spectrum=supplied_spectrum,
            use_3C_noise=use_3c_noise,
            return_wavelet=return_wavelet,
        )
        output = _waveform_from_result(result, return_wavelet, SeismogramEnsemble)
    assert output.dead()
    logs = output.elog.get_error_log()
    messages = [entry.message for entry in logs]
    assert [(entry.message, entry.badness) for entry in logs[:2]] == [
        ("lower complaint", ErrorSeverity.Complaint),
        ("lower suspect", ErrorSeverity.Suspect),
    ]
    assert messages.count("lower complaint") == 1
    assert messages.count("lower suspect") == 1
    assert sum("noise_spectrum" in message for message in messages) == 1
    assert len(logs) == 3
    assert logs[2].badness == ErrorSeverity.Invalid
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("wrapper", ("rf", "array"))
@pytest.mark.parametrize("return_wavelet", (False, True))
def test_missing_bandwidth_subdocument_is_one_invalid_and_no_engine_mutation(
    monkeypatch, capsys, wrapper, return_wavelet
):
    engine, spectrum = _engine_and_spectrum()

    def forbidden_initialize(self, wavelet, noise_spectrum):
        raise AssertionError("inverse operator must not be initialized")

    monkeypatch.setattr(
        CNRDeconEngine, "initialize_inverse_operator", forbidden_initialize
    )
    if wrapper == "rf":
        result = CNRRFDecon(
            _seismogram(t0=0.0),
            engine,
            noise_spectrum=spectrum,
            bandwidth_subdocument_key="missing",
            return_wavelet=return_wavelet,
            window_output=False,
        )
        output = _waveform_from_result(result, return_wavelet, Seismogram)
    else:
        result = CNRArrayDecon(
            _ensemble(),
            _seismogram(t0=0.0),
            engine,
            noise_spectrum=spectrum,
            bandwidth_subdocument_key="missing",
            beam_component=1,
            return_wavelet=return_wavelet,
        )
        output = _waveform_from_result(result, return_wavelet, SeismogramEnsemble)
    assert output.dead()
    logs = output.elog.get_error_log()
    assert len(logs) == 1
    assert logs[0].badness == ErrorSeverity.Invalid
    assert "missing" in logs[0].message
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("numeric_state", ("finite", "nan", "inf"))
@pytest.mark.parametrize("return_wavelet", (False, True))
def test_rf_numeric_state_preserves_return_contract(
    monkeypatch, capsys, numeric_state, return_wavelet
):
    engine, spectrum = _engine_and_spectrum()
    installed_wavelet = {}

    def initialize(self, wavelet, noise_spectrum):
        installed_wavelet["value"] = TimeSeries(wavelet)

    def process(self, datum, noise_spectrum, flow, fhigh):
        result = Seismogram(datum)
        if numeric_state == "nan":
            result.data[0, 0] = np.nan
        elif numeric_state == "inf":
            result.data[0, 0] = np.inf
        return result

    def qc_metrics(self):
        return {"contract_test": True}

    def actual_output(self, wavelet):
        return TimeSeries(installed_wavelet["value"])

    def shaping_wavelet(self):
        return TimeSeries(installed_wavelet["value"])

    monkeypatch.setattr(CNRDeconEngine, "initialize_inverse_operator", initialize)
    monkeypatch.setattr(CNRDeconEngine, "process", process)
    monkeypatch.setattr(CNRDeconEngine, "QCMetrics", qc_metrics)
    monkeypatch.setattr(CNRDeconEngine, "actual_output", actual_output)
    monkeypatch.setattr(CNRDeconEngine, "output_shaping_wavelet", shaping_wavelet)

    result = CNRRFDecon(
        _seismogram(t0=0.0),
        engine,
        noise_spectrum=spectrum,
        return_wavelet=return_wavelet,
        window_output=False,
    )

    if numeric_state == "finite":
        if return_wavelet:
            assert isinstance(result, list)
            assert len(result) == 3
            assert isinstance(result[0], Seismogram)
            assert isinstance(result[1], TimeSeries)
            assert isinstance(result[2], TimeSeries)
            output = result[0]
        else:
            assert isinstance(result, Seismogram)
            output = result
        assert output.live
        assert output["CNRFDecon_properties"]["contract_test"]
    else:
        if return_wavelet:
            assert isinstance(result, list)
            assert len(result) == 3
            assert result[1:] == [None, None]
            output = result[0]
        else:
            assert isinstance(result, Seismogram)
            output = result
        assert output.dead()
        logs = output.elog.get_error_log()
        assert len(logs) == 1
        assert logs[0].badness == ErrorSeverity.Invalid
        assert "NaN or Inf" in logs[0].message
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("wrapper", ("rf", "array"))
@pytest.mark.parametrize("stage", ("initialize", "process"))
def test_caught_mspass_error_preserves_message_and_severity(
    monkeypatch, capsys, wrapper, stage
):
    engine, spectrum = _engine_and_spectrum()

    def fail_initialize(self, wavelet, noise_spectrum):
        raise MsPASSError("sentinel complaint", ErrorSeverity.Complaint)

    def fail_process(self, datum, noise_spectrum, flow, fhigh):
        raise MsPASSError("sentinel complaint", ErrorSeverity.Complaint)

    if stage == "initialize":
        monkeypatch.setattr(
            CNRDeconEngine, "initialize_inverse_operator", fail_initialize
        )
    else:
        monkeypatch.setattr(
            CNRDeconEngine,
            "initialize_inverse_operator",
            lambda self, wavelet, noise: None,
        )
        monkeypatch.setattr(CNRDeconEngine, "process", fail_process)
    if wrapper == "rf":
        result = CNRRFDecon(
            _seismogram(t0=0.0),
            engine,
            noise_spectrum=spectrum,
            window_output=False,
        )
    else:
        result = CNRArrayDecon(
            _ensemble(),
            _timeseries(t0=0.0),
            engine,
            noise_spectrum=spectrum,
        )
    if wrapper == "array" and stage == "process":
        assert result.live
        target = result.member[0]
    else:
        target = result
    assert target.dead()
    logs = target.elog.get_error_log()
    assert len(logs) == 1
    assert logs[0].message == "sentinel complaint"
    assert logs[0].badness == ErrorSeverity.Complaint
    assert capsys.readouterr().out == ""
