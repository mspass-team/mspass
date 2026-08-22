import ast
import inspect
from unittest.mock import Mock

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.algorithms import window as window_module


def _waveform(waveform_type, *, t0=0.0, dt=1.0):
    result = waveform_type(5)
    result.t0 = t0
    result.dt = dt
    result["marker"] = "original"
    if waveform_type is TimeSeries:
        for sample in range(result.npts):
            result.data[sample] = sample + 1.0
    else:
        for component in range(3):
            for sample in range(result.npts):
                result.data[component, sample] = 10.0 * component + sample + 1.0
    result.set_live()
    return result


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("shift", [-10.0, 0.0, 10.0])
@pytest.mark.parametrize(
    "start,end,handling,expected_start,expected_npts,padded_index",
    [
        (0.0, 2.0, "kill", 0.0, 3, None),
        (-1.0, 2.0, "pad", -1.0, 4, 0),
        (2.0, 5.0, "pad", 2.0, 4, -1),
    ],
)
def test_windowdata_applies_padding_and_shift_once(
    waveform_type,
    shift,
    start,
    end,
    handling,
    expected_start,
    expected_npts,
    padded_index,
):
    datum = _waveform(waveform_type, t0=shift)

    result = window_module.WindowData(
        datum,
        start,
        end,
        t0shift=shift,
        short_segment_handling=handling,
    )

    assert result.live
    assert result.t0 == expected_start + shift
    assert result.npts == expected_npts
    if padded_index is not None:
        samples = np.asarray(result.data)
        if waveform_type is TimeSeries:
            assert samples[padded_index] == 0.0
        else:
            assert np.array_equal(samples[:, padded_index], np.zeros(3))


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("request_time", [2.0, np.nextafter(2.5, 2.0)])
def test_windowdata_autopad_equal_endpoint_is_one_sample(waveform_type, request_time):
    datum = _waveform(waveform_type)

    result = window_module.WindowData_autopad(datum, request_time, request_time)

    assert result.live
    assert result.t0 == 2.0
    assert result.npts == 1


def _snapshot(datum):
    metadata = dict(datum)
    metadata.pop("delta", None)
    return (
        datum.live,
        datum.npts,
        datum.t0,
        datum.dt,
        metadata,
        np.array(datum.data, copy=True),
    )


def _assert_unchanged(datum, before):
    assert datum.live == before[0]
    assert datum.npts == before[1]
    assert datum.t0 == before[2]
    np.testing.assert_equal(datum.dt, before[3])
    metadata = dict(datum)
    metadata.pop("delta", None)
    assert metadata == before[4]
    assert np.array_equal(datum.data, before[5])


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
def test_windowdata_autopad_rejects_reversed_window_before_allocation(
    monkeypatch, waveform_type
):
    datum = _waveform(waveform_type)
    before = _snapshot(datum)
    window_data = Mock(side_effect=AssertionError("WindowData must not be called"))
    monkeypatch.setattr(window_module, "WindowData", window_data)

    with pytest.raises(ValueError, match="less than or equal"):
        window_module.WindowData_autopad(datum, 2.0, 1.0)

    window_data.assert_not_called()
    _assert_unchanged(datum, before)


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("dt", [0.0, -1.0, np.nan, np.inf])
def test_windowdata_autopad_rejects_invalid_dt_before_allocation(
    monkeypatch, waveform_type, dt
):
    datum = _waveform(waveform_type, dt=dt)
    before = _snapshot(datum)
    window_data = Mock(side_effect=AssertionError("WindowData must not be called"))
    monkeypatch.setattr(window_module, "WindowData", window_data)

    with pytest.raises(ValueError, match="finite and positive"):
        window_module.WindowData_autopad(datum, 1.0, 2.0)

    window_data.assert_not_called()
    _assert_unchanged(datum, before)


@pytest.mark.parametrize(
    "ensemble_type,waveform_type",
    [
        (TimeSeriesEnsemble, TimeSeries),
        (SeismogramEnsemble, Seismogram),
    ],
)
@pytest.mark.parametrize("invalid_request", ["window", "dt"])
def test_windowdata_autopad_prevalidates_ensembles_before_allocation(
    monkeypatch, ensemble_type, waveform_type, invalid_request
):
    ensemble = ensemble_type()
    ensemble.member.append(_waveform(waveform_type))
    ensemble.member.append(
        _waveform(waveform_type, dt=0.0 if invalid_request == "dt" else 1.0)
    )
    ensemble.set_live()
    before = [_snapshot(member) for member in ensemble.member]
    window_data = Mock(side_effect=AssertionError("WindowData must not be called"))
    monkeypatch.setattr(window_module, "WindowData", window_data)

    stime, etime = (2.0, 1.0) if invalid_request == "window" else (1.0, 2.0)
    with pytest.raises(ValueError):
        window_module.WindowData_autopad(ensemble, stime, etime)

    window_data.assert_not_called()
    for member, snapshot in zip(ensemble.member, before):
        _assert_unchanged(member, snapshot)


def test_window_mspasserror_calls_use_message_and_severity():
    tree = ast.parse(inspect.getsource(window_module))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "MsPASSError"
    ]

    assert calls
    for call in calls:
        assert len(call.args) == 2
        assert ast.unparse(call.args[1]).startswith("ErrorSeverity.")
