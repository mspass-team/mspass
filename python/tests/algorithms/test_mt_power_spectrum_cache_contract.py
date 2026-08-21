import numpy as np
import pytest

from mspasspy.algorithms.MTPowerSpectrumEngine import MTPowerSpectrumEngine
from mspasspy.ccore.seismic import DoubleVector, TimeSeries

WINSIZE = 32


def _samples(length=WINSIZE):
    return np.sin(2.0 * np.pi * np.arange(length) / 8.0)


def _input(kind, length=WINSIZE):
    samples = _samples(length)
    if kind == "timeseries":
        datum = TimeSeries(length)
        datum.dt = 0.1
        datum.data = DoubleVector(samples)
        datum.set_live()
        return datum
    if kind == "doublevector":
        return DoubleVector(samples)
    return samples


def _cache_snapshot(engine):
    return {
        "MTSpec_instance": engine.MTSpec_instance,
        "MTSpec_state": {
            key: np.array(value, copy=True) if isinstance(value, np.ndarray) else value
            for key, value in vars(engine.MTSpec_instance).items()
        },
        "vn": engine.vn,
        "vn_values": np.array(engine.vn, copy=True),
        "lamb": engine.lamb,
        "lamb_values": np.array(engine.lamb, copy=True),
        "nfft": engine.nfft,
    }


def _assert_cache_unchanged(engine, before):
    assert engine.MTSpec_instance is before["MTSpec_instance"]
    assert vars(engine.MTSpec_instance).keys() == before["MTSpec_state"].keys()
    for key, expected in before["MTSpec_state"].items():
        actual = vars(engine.MTSpec_instance)[key]
        if isinstance(expected, np.ndarray):
            np.testing.assert_array_equal(actual, expected)
        else:
            assert actual == expected
    assert engine.vn is before["vn"]
    np.testing.assert_array_equal(engine.vn, before["vn_values"])
    assert engine.lamb is before["lamb"]
    np.testing.assert_array_equal(engine.lamb, before["lamb_values"])
    assert engine.nfft == before["nfft"]


def _spectrum_values(spectrum):
    return np.asarray(spectrum.spectrum, dtype=float)


@pytest.mark.parametrize("kind", ["timeseries", "doublevector", "ndarray"])
def test_exact_length_is_repeatable_and_reuses_cached_tapers(kind):
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=64)
    datum = _input(kind)

    first = engine.apply(datum, dt=0.1)
    vn = engine.vn
    vn_values = np.array(engine.vn, copy=True)
    lamb = engine.lamb
    lamb_values = np.array(engine.lamb, copy=True)
    nfft = engine.nfft
    second = engine.apply(datum, dt=0.1)

    assert engine.vn is vn
    np.testing.assert_array_equal(engine.vn, vn_values)
    assert engine.lamb is lamb
    np.testing.assert_array_equal(engine.lamb, lamb_values)
    assert engine.nfft == nfft
    np.testing.assert_allclose(_spectrum_values(second), _spectrum_values(first))


@pytest.mark.parametrize("kind", ["timeseries", "doublevector", "ndarray"])
@pytest.mark.parametrize("length", [WINSIZE - 1, WINSIZE + 1])
def test_length_mismatch_preserves_cache_and_later_valid_result(kind, length):
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=64)
    valid = _input(kind)
    expected = engine.apply(valid, dt=0.1)
    before = _cache_snapshot(engine)

    with pytest.raises(ValueError, match="does not match winsize"):
        engine.apply(_input(kind, length), dt=0.1)

    _assert_cache_unchanged(engine, before)
    actual = engine.apply(valid, dt=0.1)
    np.testing.assert_allclose(_spectrum_values(actual), _spectrum_values(expected))


@pytest.mark.parametrize("bad", [object(), [1.0] * WINSIZE, "samples"])
def test_unsupported_input_preserves_cache_and_later_valid_result(bad):
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=64)
    valid = _input("ndarray")
    expected = engine.apply(valid, dt=0.1)
    before = _cache_snapshot(engine)

    with pytest.raises(TypeError, match="TimeSeries"):
        engine.apply(bad)

    _assert_cache_unchanged(engine, before)
    actual = engine.apply(valid, dt=0.1)
    np.testing.assert_allclose(_spectrum_values(actual), _spectrum_values(expected))


def test_multidimensional_array_preserves_initialized_cache():
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=64)
    expected = engine.apply(_input("ndarray"), dt=0.1)
    before = _cache_snapshot(engine)

    with pytest.raises(TypeError, match="one-dimensional"):
        engine.apply(np.zeros((4, 8)), dt=0.1)

    _assert_cache_unchanged(engine, before)
    actual = engine.apply(_input("ndarray"), dt=0.1)
    np.testing.assert_allclose(_spectrum_values(actual), _spectrum_values(expected))
