import math

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
    else:
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

    _combine(lhs, rhs, operation)
    _assert_state(lhs, expected)


def _verify_rejected(waveform_type, operation, rhs_t0, rhs_dt, lhs_dt=1.0):
    lhs = _make_waveform(waveform_type, 5, 0.0, lhs_dt, 100.0)
    rhs = _make_waveform(waveform_type, 4, rhs_t0, rhs_dt, 10.0)
    rhs.elog.log_error("rhs", "must not merge on rejection", ErrorSeverity.Complaint)
    before = _snapshot(lhs)

    with pytest.raises(MsPASSError) as exc_info:
        _combine(lhs, rhs, operation)

    assert exc_info.value.severity == ErrorSeverity.Invalid
    _assert_state(lhs, before)


@pytest.mark.parametrize("waveform_type", [TimeSeries, Seismogram])
@pytest.mark.parametrize("operation", ["add", "subtract"])
def test_waveform_arithmetic_grid_contract(waveform_type, operation):
    valid_cases = [
        (0.0, 1.0, 0),
        (2.0, 1.0, 2),
        (-2.0, 1.0, -2),
        (5.0, 1.0, 5),
        (-4.0, 1.0, -4),
    ]

    tolerance = 1.0e-6
    positive_offset_at_tolerance = 1.0 + tolerance
    negative_offset_at_tolerance = -1.0 - tolerance
    assert (
        abs(positive_offset_at_tolerance - round(positive_offset_at_tolerance))
        <= tolerance
    )
    assert (
        abs(negative_offset_at_tolerance - round(negative_offset_at_tolerance))
        <= tolerance
    )
    valid_cases.extend(
        [
            (positive_offset_at_tolerance, 1.0, 1),
            (negative_offset_at_tolerance, 1.0, -1),
        ]
    )

    for rhs_t0, rhs_dt, offset in valid_cases:
        _verify_valid(waveform_type, operation, rhs_t0, rhs_dt, offset)

    lhs_dt = 1.0e6
    dt_at_tolerance = lhs_dt - 1.0
    dt_beyond = math.nextafter(dt_at_tolerance, 0.0)
    assert abs(lhs_dt - dt_at_tolerance) == tolerance * max(
        abs(lhs_dt), abs(dt_at_tolerance)
    )
    assert abs(lhs_dt - dt_beyond) > tolerance * max(abs(lhs_dt), abs(dt_beyond))
    _verify_valid(waveform_type, operation, 0.0, dt_at_tolerance, 0, lhs_dt=lhs_dt)

    positive_offset_beyond = math.nextafter(positive_offset_at_tolerance, math.inf)
    negative_offset_beyond = math.nextafter(negative_offset_at_tolerance, -math.inf)
    assert abs(positive_offset_beyond - round(positive_offset_beyond)) > tolerance
    assert abs(negative_offset_beyond - round(negative_offset_beyond)) > tolerance
    for rhs_t0, rhs_dt in [
        (positive_offset_beyond, 1.0),
        (negative_offset_beyond, 1.0),
    ]:
        _verify_rejected(waveform_type, operation, rhs_t0, rhs_dt)
    _verify_rejected(waveform_type, operation, 0.0, dt_beyond, lhs_dt=lhs_dt)
