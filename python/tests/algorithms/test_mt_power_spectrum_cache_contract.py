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
@pytest.mark.parametrize("nfft", [0, 32, 64])
def test_length_changes_match_fresh_engine(kind, nfft):
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=nfft)
    for length in [WINSIZE, WINSIZE - 1, WINSIZE + 1, WINSIZE - 1, WINSIZE]:
        datum = _input(kind, length)
        actual = engine.apply(datum, dt=0.1)
        expected = MTPowerSpectrumEngine(length, 3.0, 5, nfft=nfft).apply(datum, dt=0.1)
        assert engine.winsize == length
        assert engine.vn.shape[0] == length
        assert actual["npts"] == length
        assert actual["MTSpec_npts"] == length
        assert actual["MTSpec_nfft"] == expected["MTSpec_nfft"]
        assert actual.df() == expected.df()
        np.testing.assert_allclose(_spectrum_values(actual), _spectrum_values(expected))
        assert np.all(np.isfinite(_spectrum_values(actual)))


@pytest.mark.parametrize("kind", ["timeseries", "doublevector", "ndarray"])
def test_empty_input_preserves_cache(kind):
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=64)
    engine.apply(_input(kind), dt=0.1)
    before = _cache_snapshot(engine)
    with pytest.raises(ValueError, match="empty"):
        engine.apply(_input(kind, 0), dt=0.1)
    _assert_cache_unchanged(engine, before)
    assert engine.winsize == WINSIZE


def test_failed_numerical_resize_preserves_cache_and_later_valid_result():
    engine = MTPowerSpectrumEngine(WINSIZE, 3.0, 5, nfft=64)
    valid = _input("ndarray")
    expected = engine.apply(valid, dt=0.1)
    before = _cache_snapshot(engine)

    # scipy's DPSS implementation requires NW < M/2.  This reaches the
    # numerical backend with a nonempty window too short for the configured NW.
    with pytest.raises(ValueError, match="NW must be less than M/2"):
        engine.apply(_samples(6), dt=0.1)

    assert engine.winsize == WINSIZE
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


@pytest.mark.parametrize("length", [WINSIZE - 1, WINSIZE + 1])
def test_native_timeseries_mismatch_pads_or_truncates(length):
    from mspasspy.ccore.algorithms.deconvolution import (
        MTPowerSpectrumEngine as NativeEngine,
    )
    from mspasspy.ccore.utility import MsPASSError

    engine = NativeEngine(WINSIZE, 3.0, 5, 64, 0.1)
    actual = engine.apply(_input("timeseries", length))
    assert actual.live
    padded = np.zeros(WINSIZE)
    padded[: min(length, WINSIZE)] = _samples(length)[:WINSIZE]
    expected = engine.apply(DoubleVector(padded))
    np.testing.assert_allclose(_spectrum_values(actual), expected)
    with pytest.raises(MsPASSError, match="does not match operator"):
        engine.apply(DoubleVector(_samples(length)))


def test_broadband_snr_qc_with_reused_resized_engines():
    import pickle
    from pathlib import Path
    from mspasspy.algorithms.snr import broadband_snr_QC
    from mspasspy.ccore.algorithms.basic import TimeWindow
    from mspasspy.algorithms.window import WindowData

    path = Path(__file__).parents[1] / "data" / "snrtestdata"
    with path.open("rb") as stream:
        datum = pickle.load(stream)
    datum["Ptime"] = 0.0
    noise_window = TimeWindow(-200.0, -50.0)
    signal_window = TimeWindow(-10.0, 50.0)
    noise_npts = WindowData(datum, noise_window.start, noise_window.end).npts
    signal_npts = WindowData(datum, signal_window.start, signal_window.end).npts
    noise_engine = MTPowerSpectrumEngine(noise_npts - 1, 4.0, 6)
    signal_engine = MTPowerSpectrumEngine(signal_npts + 1, 4.0, 6)
    options = dict(
        noise_window=noise_window,
        signal_window=signal_window,
        optional_metrics=[],
        kill_null_signals=False,
        save_spectra=True,
    )
    for _ in range(2):
        actual = broadband_snr_QC(
            TimeSeries(datum),
            noise_spectrum_engine=noise_engine,
            signal_spectrum_engine=signal_engine,
            **options,
        )
        expected = broadband_snr_QC(
            TimeSeries(datum),
            noise_spectrum_engine=MTPowerSpectrumEngine(noise_npts, 4.0, 6),
            signal_spectrum_engine=MTPowerSpectrumEngine(signal_npts, 4.0, 6),
            **options,
        )
        assert actual.live
        assert actual.is_defined("Parrival")
        assert actual["Parrival"].keys() == expected["Parrival"].keys()
        for key in ["low_f_band_edge", "high_f_band_edge", "bandwidth"]:
            assert actual["Parrival"][key] == expected["Parrival"][key]
    assert noise_engine.winsize == noise_npts
    assert signal_engine.winsize == signal_npts
