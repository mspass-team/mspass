import numpy as np
import pytest

from mspasspy.ccore.algorithms.basic import Butterworth
from mspasspy.ccore.seismic import (
    DoubleVector,
    Seismogram,
    TimeReferenceType,
    TimeSeries,
)
from mspasspy.ccore.utility import ErrorSeverity, Metadata


def _samples(dt, npts=4000, f1=2.0, f2=15.0):
    t = np.arange(npts) * dt
    return np.sin(2.0 * np.pi * f1 * t) + np.cos(2.0 * np.pi * f2 * t)


def _timeseries(dt, samples=None):
    if samples is None:
        samples = _samples(dt)
    result = TimeSeries(len(samples))
    result.dt = dt
    result.t0 = 0.0
    result.tref = TimeReferenceType.Relative
    result.data = DoubleVector(samples)
    result.set_live()
    return result


def _seismogram(dt, samples=None):
    if samples is None:
        samples = _samples(dt)
    result = Seismogram(len(samples))
    result.dt = dt
    result.t0 = 0.0
    result.tref = TimeReferenceType.Relative
    result.data[0, :] = samples
    result.data[1, :] = 2.0 * samples
    result.data[2, :] = -0.5 * samples
    result.set_live()
    return result


def _corner_metadata(filter_type, dt):
    md = Metadata()
    md["sample_interval"] = dt
    md["zerophase"] = True
    md["filter_type"] = filter_type
    md["filter_definition_method"] = "corner_pole"
    if filter_type != "lowpass":
        md["npoles_low"] = 4
        md["corner_low"] = 1.0
    if filter_type != "highpass":
        md["npoles_high"] = 4
        md["corner_high"] = 5.0
    return md


def _filter_state(filt):
    return (
        filt.dt(),
        filt.low_corner(),
        filt.high_corner(),
        filt.npoles_low(),
        filt.npoles_high(),
        filt.filter_type(),
        filt.is_zerophase(),
    )


@pytest.mark.parametrize(
    "filter_type,expected",
    [
        ("lowpass", (0, 0.0, 4, 5.0)),
        ("highpass", (4, 1.0, 0, 0.0)),
        ("bandpass", (4, 1.0, 4, 5.0)),
    ],
)
def test_metadata_corner_pole_state(filter_type, expected):
    filt = Butterworth(_corner_metadata(filter_type, 0.01))
    assert filt.filter_type() == filter_type
    assert filt.npoles_low() == expected[0]
    assert filt.low_corner() == pytest.approx(expected[1])
    assert filt.npoles_high() == expected[2]
    assert filt.high_corner() == pytest.approx(expected[3])


def test_metadata_stop_pass_matches_argument_constructor():
    md = Metadata()
    md["sample_interval"] = 0.01
    md["zerophase"] = True
    md["filter_type"] = "bandpass"
    md["filter_definition_method"] = "stop_pass"
    md["fstop_low"] = 0.5
    md["astop_low"] = 0.01
    md["fpass_low"] = 1.0
    md["apass_low"] = 0.99
    md["fpass_high"] = 5.0
    md["apass_high"] = 0.99
    md["fstop_high"] = 8.0
    md["astop_high"] = 0.01
    metadata_filter = Butterworth(md)
    argument_filter = Butterworth(
        True,
        True,
        True,
        0.5,
        0.01,
        1.0,
        0.99,
        5.0,
        0.99,
        8.0,
        0.01,
        0.01,
    )
    assert metadata_filter.npoles_low() == argument_filter.npoles_low()
    assert metadata_filter.npoles_high() == argument_filter.npoles_high()
    assert metadata_filter.low_corner() == pytest.approx(argument_filter.low_corner())
    assert metadata_filter.high_corner() == pytest.approx(argument_filter.high_corner())
    lhs = _timeseries(0.01)
    rhs = TimeSeries(lhs)
    metadata_filter.apply(lhs)
    argument_filter.apply(rhs)
    np.testing.assert_allclose(lhs.data, rhs.data, rtol=1.0e-10, atol=1.0e-12)


def test_timeseries_and_seismogram_apply_the_same_filter():
    samples = _samples(0.01)
    ts = _timeseries(0.01, samples)
    seis = _seismogram(0.01, samples)
    Butterworth(True, True, True, 4, 1.0, 4, 5.0, 0.01).apply(ts)
    Butterworth(True, True, True, 4, 1.0, 4, 5.0, 0.01).apply(seis)
    ts_data = np.asarray(ts.data)
    np.testing.assert_allclose(seis.data[0, :], ts_data, rtol=1.0e-10, atol=1.0e-12)
    np.testing.assert_allclose(
        seis.data[1, :], 2.0 * ts_data, rtol=1.0e-10, atol=1.0e-12
    )
    np.testing.assert_allclose(
        seis.data[2, :], -0.5 * ts_data, rtol=1.0e-10, atol=1.0e-12
    )


def test_reusable_filter_matches_fresh_filters_across_dt_changes():
    reusable = Butterworth(True, True, True, 4, 1.0, 4, 15.0, 0.01)
    for dt, datum_factory in [
        (0.01, _timeseries),
        (0.02, _seismogram),
        (0.01, _timeseries),
    ]:
        actual = datum_factory(dt)
        expected = datum_factory(dt)
        fresh = Butterworth(True, True, True, 4, 1.0, 4, 15.0, dt)
        reusable.apply(actual)
        fresh.apply(expected)
        np.testing.assert_allclose(
            actual.data, expected.data, rtol=1.0e-10, atol=1.0e-12
        )
        assert reusable.dt() == pytest.approx(dt)
        assert reusable.low_corner() == pytest.approx(1.0)
        assert reusable.high_corner() == pytest.approx(15.0)


@pytest.mark.parametrize("datum_factory", [_timeseries, _seismogram])
def test_unsafe_upper_corner_fallback_filters_once_and_preserves_state(
    datum_factory,
):
    samples = _samples(0.03, f1=3.0, f2=12.0)
    actual = datum_factory(0.03, samples)
    expected = datum_factory(0.03, samples)
    filt = Butterworth(True, True, True, 4, 1.0, 4, 20.0, 0.01)
    state_before = _filter_state(filt)
    Butterworth(True, True, False, 4, 1.0, 0, 0.0, 0.03).apply(expected)
    filt.apply(actual)
    np.testing.assert_allclose(actual.data, expected.data, rtol=1.0e-10, atol=1.0e-12)
    assert _filter_state(filt) == state_before
    errors = list(actual.elog.get_error_log())
    assert len(errors) == 1
    assert errors[0].badness == ErrorSeverity.Complaint
    assert "Disabling upper corner" in errors[0].message


def test_empty_bound_inputs_are_noops():
    filt = Butterworth(True, True, True, 4, 1.0, 4, 5.0, 0.01)
    state_before = _filter_state(filt)
    ts = TimeSeries()
    ts.dt = 0.1
    seis = Seismogram()
    seis.dt = 0.1
    filt.apply(ts)
    filt.apply(seis)
    assert ts.npts == 0
    assert seis.npts == 0
    assert len(list(ts.elog.get_error_log())) == 0
    assert len(list(seis.elog.get_error_log())) == 0
    assert _filter_state(filt) == state_before
