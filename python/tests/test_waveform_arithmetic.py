import numpy as np
import pytest

from mspasspy.ccore.seismic import Seismogram, TimeReferenceType, TimeSeries
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _make_waveform(waveform_type, sample_count, t0, dt, base):
    waveform = waveform_type(sample_count)
    waveform.t0 = t0
    waveform.dt = dt
    waveform.tref = TimeReferenceType.Relative
    waveform.set_live()
    waveform["sentinel"] = "unchanged"
    if waveform_type is TimeSeries:
        for sample in range(sample_count):
            waveform.data[sample] = base + sample
    elif sample_count:
        for component in range(3):
            waveform.data[component, :] = (
                base + 10.0 * component + np.arange(sample_count)
            )
    return waveform


def _snapshot(waveform):
    return {
        "npts": waveform.npts,
        "t0": waveform.t0,
        "dt": waveform.dt,
        "tref": waveform.tref,
        "live": waveform.live,
        "error_count": waveform.elog.size(),
        "sentinel": waveform["sentinel"],
        "data": np.array(waveform.data, copy=True),
    }


def _assert_state(waveform, expected):
    assert waveform.npts == expected["npts"]
    assert waveform.t0 == expected["t0"]
    assert waveform.dt == expected["dt"]
    assert waveform.tref == expected["tref"]
    assert waveform.live == expected["live"]
    assert waveform.elog.size() == expected["error_count"]
    assert waveform["sentinel"] == expected["sentinel"]
    np.testing.assert_array_equal(waveform.data, expected["data"])


def _combine(lhs, rhs, operation):
    if operation == "add":
        lhs += rhs
    else:
        lhs -= rhs


def _verify_valid(waveform_type, operation, rhs_t0, rhs_dt, offset, lhs_dt=1.0):
    lhs = _make_waveform(waveform_type, 5, 0.0, lhs_dt, 100.0)
    rhs = _make_waveform(waveform_type, 4, rhs_t0, rhs_dt, 10.0)
    expected = _snapshot(lhs)
    sign = 1.0 if operation == "add" else -1.0
    for rhs_index in range(rhs.npts):
        lhs_index = offset + rhs_index
        if 0 <= lhs_index < lhs.npts:
            if waveform_type is TimeSeries:
                expected["data"][lhs_index] += sign * rhs.data[rhs_index]
            else:
                expected["data"][:, lhs_index] += sign * rhs.data[:, rhs_index]

    rhs_before = _snapshot(rhs)
    _combine(lhs, rhs, operation)
    _assert_state(lhs, expected)
    _assert_state(rhs, rhs_before)


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("operation", ["add", "subtract"])
def test_waveform_arithmetic_overlap(waveform_type, operation):
    cases = [
        (0.0, 1.0, 0, 1.0),
        (2.0, 1.0, 2, 1.0),
        (-2.0, 1.0, -2, 1.0),
        (5.0, 1.0, 5, 1.0),
        (-4.0, 1.0, -4, 1.0),
        (-0.0347376, 1.0, 0, 1.0),
        (0.49, 1.0, 0, 1.0),
        (-0.49, 1.0, 0, 1.0),
        (0.5, 1.0, 1, 1.0),
        (-0.5, 1.0, -1, 1.0),
        (0.51, 1.0, 1, 1.0),
        (-0.51, 1.0, -1, 1.0),
        (0.0, 0.02500124, 0, 0.025),
        (0.0, 0.025, 0, 0.02500124),
    ]
    for rhs_t0, rhs_dt, offset, lhs_dt in cases:
        _verify_valid(waveform_type, operation, rhs_t0, rhs_dt, offset, lhs_dt)


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("operation", ["add", "subtract"])
@pytest.mark.parametrize("rhs_t0", [2.1, -1.1])
def test_waveform_arithmetic_actual_no_overlap(waveform_type, operation, rhs_t0):
    lhs = _make_waveform(waveform_type, 3, 0.0, 1.0, 100.0)
    rhs = _make_waveform(waveform_type, 2, rhs_t0, 1.0, 10.0)
    expected = _snapshot(lhs)
    rhs_before = _snapshot(rhs)
    _combine(lhs, rhs, operation)
    _assert_state(lhs, expected)
    _assert_state(rhs, rhs_before)


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("operation", ["add", "subtract"])
def test_waveform_arithmetic_legacy_guards(waveform_type, operation):
    lhs = _make_waveform(waveform_type, 3, 0.0, 1.0, 100.0)
    rhs = _make_waveform(waveform_type, 2, 0.0, 1.0, 10.0)
    before = _snapshot(lhs)
    rhs.kill()
    _combine(lhs, rhs, operation)
    _assert_state(lhs, before)

    rhs.set_live()
    rhs.tref = TimeReferenceType.UTC
    with pytest.raises(MsPASSError) as error:
        _combine(lhs, rhs, operation)
    assert error.value.severity == ErrorSeverity.Invalid
    _assert_state(lhs, before)

    empty = _make_waveform(waveform_type, 0, 0.0, 1.0, 10.0)
    _combine(lhs, empty, operation)
    _assert_state(lhs, before)


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("operation", ["add", "subtract"])
def test_waveform_arithmetic_rhs_longer_than_lhs(waveform_type, operation):
    lhs = _make_waveform(waveform_type, 3, 0.0, 1.0, 100.0)
    rhs = _make_waveform(waveform_type, 6, -1.0, 1.0, 10.0)
    expected = _snapshot(lhs)
    sign = 1.0 if operation == "add" else -1.0
    if waveform_type is TimeSeries:
        expected["data"] += sign * np.array(rhs.data[1:4])
    else:
        expected["data"] += sign * np.array(rhs.data[:, 1:4])
    _combine(lhs, rhs, operation)
    _assert_state(lhs, expected)
