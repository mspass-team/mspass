from pathlib import Path

import numpy as np
import pytest

import mspasspy.ccore.algorithms.amplitudes as amplitudes_binding
from mspasspy.ccore.algorithms.amplitudes import (
    ScalingMethod,
    _scale,
    _scale_ensemble,
)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _make_datum(atomic_type, samples, live=True):
    datum = atomic_type(len(samples))
    datum.t0 = 0.0
    datum.set_dt(1.0)
    datum.tref = TimeReferenceType.Relative
    datum["calib"] = 2.0
    datum["contract_marker"] = "unchanged"
    if atomic_type is TimeSeries:
        for index, value in enumerate(samples):
            datum.data[index] = value
    else:
        for index, value in enumerate(samples):
            datum.data[0, index] = value
            datum.data[1, index] = 0.0
            datum.data[2, index] = 0.0
    if live:
        datum.set_live()
    else:
        datum.kill()
    return datum


def _samples(datum):
    return np.asarray(datum.data).copy()


def _elog_snapshot(datum):
    return [
        (entry.algorithm, entry.message, entry.badness, entry.p_id)
        for entry in datum.elog.get_error_log()
    ]


def _snapshot(datum):
    return {
        "metadata": dict(datum),
        "samples": _samples(datum),
        "live": datum.live,
        "npts": datum.npts,
        "t0": datum.t0,
        "dt": datum.dt,
        "tref": datum.tref,
        "elog": _elog_snapshot(datum),
    }


def _assert_snapshot(datum, expected):
    assert dict(datum) == expected["metadata"]
    np.testing.assert_allclose(
        _samples(datum), expected["samples"], rtol=0.0, atol=0.0, equal_nan=True
    )
    assert datum.live is expected["live"]
    assert datum.npts == expected["npts"]
    assert datum.t0 == expected["t0"]
    assert datum.dt == expected["dt"]
    assert datum.tref == expected["tref"]
    assert _elog_snapshot(datum) == expected["elog"]


def _first_sample(datum):
    if isinstance(datum, TimeSeries):
        return datum.data[0]
    return datum.data[0, 0]


def _ensemble_for(atomic_type):
    return TimeSeriesEnsemble() if atomic_type is TimeSeries else SeismogramEnsemble()


def test_scale_contract_uses_real_binding():
    assert Path(amplitudes_binding.__file__).suffix == ".so"


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
def test_atomic_scale_measures_only_the_window_intersection(atomic_type):
    interior = _make_datum(atomic_type, [100.0, 1.0, 2.0, 4.0, 3.0, 1.0])
    amplitude = _scale(interior, ScalingMethod.Peak, 2.0, TimeWindow(2.0, 4.0))
    assert amplitude == pytest.approx(4.0)
    assert _first_sample(interior) == pytest.approx(50.0)
    assert interior["calib"] == pytest.approx(4.0)

    clipped_left = _make_datum(atomic_type, [1.0, 2.0, 3.0, 4.0])
    assert _scale(
        clipped_left, ScalingMethod.Peak, 1.0, TimeWindow(-5.0, 2.0)
    ) == pytest.approx(3.0)
    clipped_right = _make_datum(atomic_type, [1.0, 2.0, 3.0, 4.0])
    assert _scale(
        clipped_right, ScalingMethod.Peak, 1.0, TimeWindow(2.0, 10.0)
    ) == pytest.approx(4.0)

    reversed_window = _make_datum(atomic_type, [100.0, 1.0, 2.0, 4.0])
    assert _scale(
        reversed_window, ScalingMethod.Peak, 1.0, TimeWindow(3.0, 2.0)
    ) == pytest.approx(100.0)


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize(
    "window",
    (
        TimeWindow(-4.0, -2.0),
        TimeWindow(10.0, 12.0),
        TimeWindow(2.0, 2.0),
        TimeWindow(-2.0, 0.0),
        TimeWindow(3.0, 5.0),
    ),
    ids=(
        "disjoint-left",
        "disjoint-right",
        "zero-width-input",
        "zero-width-left-intersection",
        "zero-width-right-intersection",
    ),
)
def test_invalid_atomic_scale_window_raises_before_mutation(atomic_type, window):
    datum = _make_datum(atomic_type, [1.0, 2.0, 3.0, 4.0])
    before = _snapshot(datum)

    with pytest.raises(MsPASSError, match="no positive-width intersection") as error:
        _scale(datum, ScalingMethod.Peak, 1.0, window)

    assert error.value.severity == ErrorSeverity.Invalid
    _assert_snapshot(datum, before)


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("use_mean", (True, False), ids=("mean", "median"))
def test_ensemble_scale_uses_only_eligible_statistic_and_one_common_gain(
    atomic_type, use_mean
):
    amplitudes = [2.0, 4.0, 16.0, 128.0, 0.0, np.nan, np.inf, 8.0]
    ensemble = _ensemble_for(atomic_type)
    for index, amplitude in enumerate(amplitudes):
        ensemble.member.append(_make_datum(atomic_type, [amplitude], index != 7))
    dead_before = _snapshot(ensemble.member[-1])
    expected_amplitude = np.power(16384.0, 0.25) if use_mean else 8.0

    returned = _scale_ensemble(ensemble, ScalingMethod.Peak, 16.0, use_mean)

    assert returned == pytest.approx(expected_amplitude)
    gain = 16.0 / expected_amplitude
    for index, original_amplitude in enumerate(amplitudes[:-1]):
        assert _first_sample(ensemble.member[index]) == pytest.approx(
            original_amplitude * gain, nan_ok=True
        )
        assert ensemble.member[index]["calib"] == pytest.approx(2.0 / gain)
    _assert_snapshot(ensemble.member[-1], dead_before)


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
def test_ensemble_scale_with_no_eligible_member_is_exact_noop(atomic_type):
    ensemble = _ensemble_for(atomic_type)
    for amplitude, live in ((0.0, True), (np.nan, True), (np.inf, True), (8.0, False)):
        ensemble.member.append(_make_datum(atomic_type, [amplitude], live))
    ensemble["ensemble_marker"] = "unchanged"
    ensemble_state = dict(ensemble)
    before = [_snapshot(member) for member in ensemble.member]

    returned = _scale_ensemble(ensemble, ScalingMethod.Peak, 8.0, True)

    assert returned == 0.0
    assert dict(ensemble) == ensemble_state
    for member, expected in zip(ensemble.member, before):
        _assert_snapshot(member, expected)


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("use_mean", (True, False), ids=("mean", "median"))
def test_ensemble_scale_handles_one_eligible_member(atomic_type, use_mean):
    ensemble = _ensemble_for(atomic_type)
    ensemble.member.append(_make_datum(atomic_type, [4.0]))
    ensemble.member.append(_make_datum(atomic_type, [0.0]))

    returned = _scale_ensemble(ensemble, ScalingMethod.Peak, 8.0, use_mean)

    assert returned == pytest.approx(4.0)
    assert _first_sample(ensemble.member[0]) == pytest.approx(8.0)
    assert _first_sample(ensemble.member[1]) == pytest.approx(0.0)
    assert ensemble.member[0]["calib"] == pytest.approx(1.0)
    assert ensemble.member[1]["calib"] == pytest.approx(1.0)
