import math
from unittest.mock import Mock

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    TimeReferenceType,
    TimeSeries,
)
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.algorithms import resample as resample_module


class _ConcreteResampler(resample_module.BasicResampler):
    def resample(self, mspass_object, handles_ensembles=True):
        return mspass_object


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"dt": 0.1, "sampling_rate": 10.0},
    ],
)
def test_basic_resampler_requires_exactly_one_parameter_without_partial_state(kwargs):
    instance = object.__new__(_ConcreteResampler)

    with pytest.raises(ValueError):
        instance.__init__(**kwargs)

    assert vars(instance) == {}


@pytest.mark.parametrize("parameter", ["dt", "sampling_rate"])
@pytest.mark.parametrize(
    "value", [True, False, "0.1", object(), 0.0, -1.0, math.nan, math.inf, -math.inf]
)
def test_basic_resampler_rejects_invalid_values_without_partial_state(parameter, value):
    instance = object.__new__(_ConcreteResampler)

    with pytest.raises(ValueError):
        instance.__init__(**{parameter: value})

    assert vars(instance) == {}


@pytest.mark.parametrize(
    "kwargs,expected_dt,expected_rate",
    [
        ({"dt": 0.125}, 0.125, 8.0),
        ({"sampling_rate": 40}, 0.025, 40.0),
        ({"dt": np.float64(0.2)}, 0.2, 5.0),
    ],
)
def test_basic_resampler_stores_reciprocal_targets(kwargs, expected_dt, expected_rate):
    operator = _ConcreteResampler(**kwargs)

    assert operator.dt == expected_dt
    assert operator.samprate == expected_rate
    assert operator.target_dt() == expected_dt
    assert operator.target_samprate() == expected_rate


def _timeseries(live):
    result = TimeSeries(8)
    result.dt = 0.1
    result.t0 = 5.0
    result.tref = TimeReferenceType.UTC
    result["marker"] = "original"
    for sample in range(result.npts):
        result.data[sample] = sample + 1.0
    if live:
        result.set_live()
    else:
        result.kill()
    result.elog.log_error("setup", "existing error", ErrorSeverity.Informational)
    return result


def _seismogram(live):
    result = Seismogram(8)
    result.dt = 0.1
    result.t0 = 5.0
    result.tref = TimeReferenceType.UTC
    result["marker"] = "original"
    for component in range(3):
        for sample in range(result.npts):
            result.data[component, sample] = 10.0 * component + sample
    result.set_live()
    result.rotate(0.25)
    if not live:
        result.kill()
    result.elog.log_error("setup", "existing error", ErrorSeverity.Informational)
    return result


def _snapshot(datum):
    state = (
        datum.live,
        datum.npts,
        datum.dt,
        datum.t0,
        datum.tref,
        datum.cardinal() if isinstance(datum, Seismogram) else None,
        datum.orthogonal() if isinstance(datum, Seismogram) else None,
        dict(datum),
        tuple(
            (entry.algorithm, entry.message, entry.badness)
            for entry in datum.elog.get_error_log()
        ),
    )
    transformation = (
        np.array(datum.tmatrix, copy=True) if isinstance(datum, Seismogram) else None
    )
    return state, np.array(datum.data, copy=True), transformation


@pytest.mark.parametrize("factory", [_timeseries, _seismogram])
@pytest.mark.parametrize("live", [True, False])
def test_factor_one_decimation_returns_same_unchanged_object(
    monkeypatch, factory, live
):
    datum = factory(live)
    before = _snapshot(datum)

    decimate = Mock(
        side_effect=AssertionError(
            "scipy.signal.decimate must not be called for factor one"
        )
    )
    monkeypatch.setattr(resample_module.signal, "decimate", decimate)
    result = resample_module.ScipyDecimator(sampling_rate=10.0).resample(datum)

    assert result is datum
    decimate.assert_not_called()
    after = _snapshot(datum)
    assert after[0] == before[0]
    assert np.array_equal(after[1], before[1])
    if isinstance(datum, Seismogram):
        assert np.array_equal(after[2], before[2])


def test_factor_greater_than_one_still_calls_scipy(monkeypatch):
    datum = _timeseries(True)
    calls = []

    def fake_decimate(data, factor, **kwargs):
        calls.append((factor, kwargs))
        return np.asarray(data)[::factor]

    monkeypatch.setattr(resample_module.signal, "decimate", fake_decimate)
    result = resample_module.ScipyDecimator(sampling_rate=5.0).resample(datum)

    assert result is datum
    assert len(calls) == 1
    assert calls[0][0] == 2
    assert datum.dt == 0.2
    assert datum.npts == 4
