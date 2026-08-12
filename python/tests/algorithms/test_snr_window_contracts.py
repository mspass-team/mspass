from unittest.mock import Mock, call

import numpy as np
import pytest

import mspasspy.algorithms.snr as snr_module
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorLogger, ErrorSeverity, MsPASSError

# Exact grid tie for t0=10.3, dt=0.1, and npts=10.
_UPPER_HALF_SAMPLE_TIE = 11.25


def _live_timeseries(npts=10, dt=0.1, t0=10.3):
    datum = TimeSeries(npts)
    datum.dt = dt
    datum.t0 = t0
    for index in range(npts):
        datum.data[index] = float(index + 1)
    datum.set_live()
    return datum


@pytest.mark.parametrize("boundary", ["final", "below-half-tie"])
def test_snr_window_accepts_last_sample_and_values_below_positive_half_tie(
    boundary,
):
    datum = _live_timeseries()
    if boundary == "final":
        window_end = datum.endtime()
    else:
        window_end = np.nextafter(_UPPER_HALF_SAMPLE_TIE, -np.inf)
    window = TimeWindow(datum.t0 + datum.dt, window_end)

    assert not snr_module._window_invalid(datum, window)
    value = snr_module.snr.__wrapped__(
        datum,
        noise_window=TimeWindow(datum.t0 + datum.dt, datum.t0 + 2.0 * datum.dt),
        signal_window=window,
        noise_metric="rms",
        signal_metric="rms",
    )
    assert isinstance(value, float)
    assert np.isfinite(value)


@pytest.mark.parametrize("boundary", ["half-tie", "plus-dt"])
def test_snr_window_rejects_positive_half_tie_and_later_values(boundary):
    datum = _live_timeseries()
    if boundary == "half-tie":
        window_end = _UPPER_HALF_SAMPLE_TIE
    else:
        window_end = datum.endtime() + datum.dt
    window = TimeWindow(datum.t0 + datum.dt, window_end)

    assert snr_module._window_invalid(datum, window)
    with pytest.raises(MsPASSError) as captured:
        snr_module.snr.__wrapped__(
            datum,
            noise_window=TimeWindow(datum.t0 + datum.dt, datum.t0 + 2.0 * datum.dt),
            signal_window=window,
            noise_metric="rms",
            signal_metric="rms",
        )
    assert captured.value.severity == ErrorSeverity.Invalid


def test_snr_window_keeps_the_existing_strict_start_boundary():
    datum = _live_timeseries()

    assert snr_module._window_invalid(datum, TimeWindow(datum.t0, datum.t0 + datum.dt))


def _window_result(name, severity=None):
    result = TimeSeries(3)
    result.set_live()
    if severity is not None:
        result.elog.log_error(name, f"{name} failed", severity)
        result.kill()
    return result


@pytest.mark.parametrize(
    "noise_severity,signal_severity",
    [
        (ErrorSeverity.Complaint, None),
        (None, ErrorSeverity.Invalid),
        (ErrorSeverity.Complaint, ErrorSeverity.Invalid),
    ],
)
def test_fd_snr_returns_immediately_when_a_window_is_dead(
    monkeypatch, capsys, noise_severity, signal_severity
):
    noise = _window_result("noise-window", noise_severity)
    signal = _window_result("signal-window", signal_severity)
    window_data = Mock(side_effect=[noise, signal])
    monkeypatch.setattr(snr_module, "WindowData", window_data)
    noise_engine = Mock()
    signal_engine = Mock()
    datum = _live_timeseries()

    result = snr_module.FD_snr_estimator(
        datum,
        noise_window=TimeWindow(-1.0, -0.5),
        signal_window=TimeWindow(-0.5, 0.5),
        noise_spectrum_engine=noise_engine,
        signal_spectrum_engine=signal_engine,
    )

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == {}
    assert isinstance(result[1], ErrorLogger)
    expected_calls = [call(datum, -1.0, -0.5)]
    if noise_severity is not None:
        expected = [("noise-window", "noise-window failed", noise_severity)]
    else:
        expected_calls.append(call(datum, -0.5, 0.5))
        expected = [("signal-window", "signal-window failed", signal_severity)]
    errors = result[1].get_error_log()
    assert [(e.algorithm, e.message, e.badness) for e in errors] == expected
    assert window_data.call_args_list == expected_calls
    noise_engine.apply.assert_not_called()
    signal_engine.apply.assert_not_called()
    assert capsys.readouterr() == ("", "")


def test_fd_snr_real_dead_noise_window_merges_the_message_once():
    datum = _live_timeseries(npts=20)
    noise_engine = Mock()
    signal_engine = Mock()

    result = snr_module.FD_snr_estimator(
        datum,
        noise_window=TimeWindow(datum.t0 - 1.0, datum.t0 - 0.5),
        signal_window=TimeWindow(datum.t0 + datum.dt, datum.t0 + 2.0 * datum.dt),
        noise_spectrum_engine=noise_engine,
        signal_spectrum_engine=signal_engine,
    )

    assert len(result) == 2
    assert result[0] == {}
    errors = result[1].get_error_log()
    assert len(errors) == 1
    assert errors[0].algorithm == "WindowDataAtomic"
    assert errors[0].badness == ErrorSeverity.Invalid
    assert "Data time range is outside" in errors[0].message
    noise_engine.apply.assert_not_called()
    signal_engine.apply.assert_not_called()
