import math

import numpy as np
import pytest

from mspasspy.ccore.algorithms.basic import agc
from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _make_data(samples, dt=0.5, t0=0.0):
    data = Seismogram(len(samples))
    data.dt = dt
    data.t0 = t0
    data.set_live()
    for index, vector in enumerate(samples):
        data.data[:, index] = vector
    return data


def _expected(samples, twin, dt):
    sample_count = len(samples)
    rounded_window_samples = math.floor(twin / dt + 0.5)
    half_window = min(
        math.floor(rounded_window_samples / 2), math.floor((sample_count - 1) / 2)
    )
    gains = []
    output = np.zeros((3, sample_count))
    for index in range(sample_count):
        first = max(0, index - half_window)
        last = min(sample_count - 1, index + half_window)
        window = np.asarray(samples[first : last + 1])
        energy = np.square(window).sum()
        gain = 1.0 / math.sqrt(energy / (3 * len(window))) if energy > 0 else 0.0
        gains.append(gain)
        output[:, index] = gain * np.asarray(samples[index])
    return gains, output


@pytest.mark.parametrize(
    ("samples", "twin", "dt", "t0"),
    [
        ([[1.0, 1.0, 1.0]] * 5, 1.5, 0.5, 9.25),
        ([[0.0, 0.0, 0.0]] * 5, 1.5, 0.5, 0.0),
        ([[3.0, 0.0, 0.0]] + [[0.0, 0.0, 0.0]] * 4, 1.0, 0.5, 0.0),
        (
            [[0.0, 0.0, 0.0]] * 2 + [[0.0, -3.0, 0.0]] + [[0.0, 0.0, 0.0]] * 2,
            1.0,
            0.5,
            0.0,
        ),
        (
            [[2.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0], [0.0, 0.0, 0.0]],
            0.5,
            0.5,
            -2.0,
        ),
        ([[2.0, 0.0, 0.0]], 0.01, 0.5, 4.0),
        (
            [[0.0, 0.0, 0.0]] * 2 + [[0.0, 5.0, 0.0]] + [[0.0, 0.0, 0.0]] * 2,
            2.5,
            0.5,
            0.0,
        ),
        ([[2.0, 0.0, 0.0], [0.0, 4.0, 0.0]], 100.0, 0.5, 4.0),
    ],
)
def test_agc_binding_matches_formula(samples, twin, dt, t0):
    data = _make_data(samples, dt=dt, t0=t0)
    expected_gain, expected_output = _expected(samples, twin, dt)

    gain = agc(data, twin)

    assert gain.npts == len(samples)
    assert len(gain.data) == len(samples)
    assert gain.t0 == t0
    assert gain.dt == dt
    np.testing.assert_allclose(gain.data, expected_gain, rtol=1.0e-12, atol=1.0e-12)
    np.testing.assert_allclose(data.data, expected_output, rtol=1.0e-12, atol=1.0e-12)


@pytest.mark.parametrize("twin", [0.0, -1.0, float("inf"), float("-inf"), float("nan")])
def test_agc_binding_rejects_invalid_twin_without_mutation(twin):
    data = _make_data([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    original = np.array(data.data, copy=True)

    with pytest.raises(MsPASSError) as exc_info:
        agc(data, twin)

    assert exc_info.value.severity == ErrorSeverity.Invalid
    np.testing.assert_array_equal(data.data, original)


@pytest.mark.parametrize("dt", [0.0, -1.0, float("inf"), float("-inf"), float("nan")])
def test_agc_binding_rejects_invalid_dt_without_mutation(dt):
    data = _make_data([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dt=dt, t0=3.25)
    original = np.array(data.data, copy=True)

    with pytest.raises(MsPASSError) as exc_info:
        agc(data, 1.0)

    assert exc_info.value.severity == ErrorSeverity.Invalid
    assert data.npts == 2
    assert data.t0 == 3.25
    if math.isnan(dt):
        assert math.isnan(data.dt)
    else:
        assert data.dt == dt
    np.testing.assert_array_equal(data.data, original)


def test_agc_binding_rejects_empty_without_mutation():
    data = _make_data([], dt=0.5, t0=-8.0)

    with pytest.raises(MsPASSError) as exc_info:
        agc(data, 1.0)

    assert exc_info.value.severity == ErrorSeverity.Invalid
    assert data.npts == 0
    assert data.t0 == -8.0
    assert data.dt == 0.5
    assert data.live
