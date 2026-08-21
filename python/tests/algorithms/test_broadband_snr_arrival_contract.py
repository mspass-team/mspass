import inspect
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

import mspasspy.algorithms.snr as snr_module
from mspasspy.ccore.seismic import TimeReferenceType, TimeSeries
from mspasspy.ccore.utility import ErrorLogger, ErrorSeverity


def _datum(pick=...):
    datum = TimeSeries(8)
    datum.dt = 1.0
    datum.t0 = 0.0
    for index in range(datum.npts):
        datum.data[index] = float(index + 1)
    datum.set_live()
    if pick is not ...:
        datum["Ptime"] = pick
    return datum


def _successful_estimator(*args, **kwargs):
    return [{"bandwidth": 1.0}, ErrorLogger()]


@pytest.mark.parametrize("pick", [123.5, np.float32(123.5), np.int64(123)])
def test_measured_arrival_mode_is_the_public_default(monkeypatch, pick):
    assert (
        inspect.signature(snr_module.broadband_snr_QC)
        .parameters["use_measured_arrival_time"]
        .default
        is True
    )
    estimator = Mock(side_effect=_successful_estimator)
    taup = Mock()
    monkeypatch.setattr(snr_module, "FD_snr_estimator", estimator)
    datum = _datum(pick)
    datum.t0 = 120.0
    datum.tref = TimeReferenceType.UTC

    result = snr_module.broadband_snr_QC(datum, taup_model=taup)

    assert result is datum
    assert result.live
    assert result["Parrival"]["snr_arrival_time"] == pick
    estimator.assert_called_once()
    processed = estimator.call_args.args[0]
    assert processed.time_is_relative()
    assert processed.t0 == pytest.approx(120.0 - float(pick))
    assert result.time_is_UTC()
    assert result.t0 == 120.0
    taup.get_travel_times.assert_not_called()


@pytest.mark.parametrize(
    "pick",
    [
        pytest.param(..., id="missing"),
        pytest.param("123.5", id="string"),
        pytest.param(True, id="bool"),
        pytest.param(np.bool_(True), id="numpy-bool"),
        pytest.param(np.nan, id="nan"),
        pytest.param(np.float32(np.nan), id="numpy-nan"),
        pytest.param(np.inf, id="positive-inf"),
        pytest.param(-np.inf, id="negative-inf"),
    ],
)
def test_invalid_measured_pick_kills_and_logs_original_without_taup(monkeypatch, pick):
    estimator = Mock(side_effect=AssertionError("estimator must not be called"))
    taup = Mock()
    monkeypatch.setattr(snr_module, "FD_snr_estimator", estimator)
    datum = _datum(pick)
    metadata_before = dict(datum)
    samples_before = np.asarray(datum.data, dtype=float).copy()
    state_before = (
        datum.npts,
        datum.t0,
        datum.dt,
        datum.tref,
        datum.number_of_stages(),
        str(datum.get_nodes()),
    )
    datum.elog.log_error("existing", "existing complaint", ErrorSeverity.Complaint)
    elog_size_before = datum.elog.size()
    elog_before = [
        (error.algorithm, error.message, error.badness)
        for error in datum.elog.get_error_log()
    ]

    result = snr_module.broadband_snr_QC(datum, taup_model=taup)

    assert result is datum
    assert result.dead()
    assert result.elog.size() == elog_size_before + 1
    errors = result.elog.get_error_log()
    assert [
        (error.algorithm, error.message, error.badness)
        for error in errors[:elog_size_before]
    ] == elog_before
    error = errors[-1]
    assert error.algorithm == "broadband_snr_QC"
    assert error.badness == ErrorSeverity.Invalid
    assert error.message == (
        "Measured arrival time key=Ptime is missing or does not contain "
        "a finite numeric value"
    )
    np.testing.assert_equal(dict(result), metadata_before)
    np.testing.assert_array_equal(result.data, samples_before)
    assert (
        result.npts,
        result.t0,
        result.dt,
        result.tref,
        result.number_of_stages(),
        str(result.get_nodes()),
    ) == state_before
    estimator.assert_not_called()
    taup.get_travel_times.assert_not_called()


@pytest.mark.parametrize("option", [None, 0, ""])
def test_only_explicit_false_selects_theoretical_arrival(monkeypatch, option):
    estimator = Mock(side_effect=_successful_estimator)
    taup = Mock()
    monkeypatch.setattr(snr_module, "FD_snr_estimator", estimator)
    datum = _datum(123.5)

    result = snr_module.broadband_snr_QC(
        datum,
        use_measured_arrival_time=option,
        taup_model=taup,
    )

    assert result is datum
    assert result.live
    assert result["Parrival"]["snr_arrival_time"] == 123.5
    taup.get_travel_times.assert_not_called()
    estimator.assert_called_once()


def test_dead_input_returns_unchanged_without_arrival_or_snr_calls(monkeypatch):
    estimator = Mock(side_effect=AssertionError("estimator must not be called"))
    taup = Mock()
    monkeypatch.setattr(snr_module, "FD_snr_estimator", estimator)
    datum = _datum()
    datum.kill()
    metadata_before = dict(datum)
    samples_before = np.asarray(datum.data, dtype=float).copy()
    elog_size_before = datum.elog.size()

    result = snr_module.broadband_snr_QC(datum, taup_model=taup)

    assert result is datum
    assert result.dead()
    assert result.elog.size() == elog_size_before
    np.testing.assert_equal(dict(result), metadata_before)
    np.testing.assert_array_equal(result.data, samples_before)
    estimator.assert_not_called()
    taup.get_travel_times.assert_not_called()


def test_false_option_alone_uses_theoretical_arrival(monkeypatch):
    estimator = Mock(side_effect=_successful_estimator)
    monkeypatch.setattr(snr_module, "FD_snr_estimator", estimator)
    taup_arrival = SimpleNamespace(time=7.5, phase=SimpleNamespace(name="P"))
    taup = Mock()
    taup.get_travel_times.return_value = [taup_arrival]
    datum = _datum(999.0)
    datum["source_lat"] = 10.0
    datum["source_lon"] = 20.0
    datum["source_depth"] = 30.0
    datum["source_time"] = 100.0
    datum["channel_lat"] = 11.0
    datum["channel_lon"] = 21.0

    result = snr_module.broadband_snr_QC(
        datum,
        use_measured_arrival_time=False,
        taup_model=taup,
    )

    assert result is datum
    assert result.live
    assert result["Parrival"]["snr_arrival_time"] == 107.5
    taup.get_travel_times.assert_called_once()
    estimator.assert_called_once()
