import numpy as np
import pytest

from mspasspy.ccore.algorithms.deconvolution import MTPowerSpectrumEngine
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum, TimeSeries
from mspasspy.ccore.utility import Metadata


@pytest.mark.parametrize("nfft", [5, 6, 7, 8])
def test_multitaper_frequency_grid_matches_fft_bins(nfft):
    dt = 0.2
    expected_df = 1.0 / (nfft * dt)
    engine = MTPowerSpectrumEngine(nfft, 0.5, 1, nfft, dt)

    signal = TimeSeries(nfft)
    signal.dt = dt
    signal.t0 = 0.0
    signal.set_live()
    for i in range(nfft):
        signal.data[i] = np.sin(2.0 * np.pi * expected_df * i * dt)

    spectrum = engine.apply(signal)
    expected_frequencies = np.arange(nfft // 2 + 1) * expected_df

    assert engine.nfft() == nfft
    assert engine.df() == pytest.approx(expected_df)
    assert spectrum.df() == pytest.approx(expected_df)
    assert spectrum.frequencies() == pytest.approx(expected_frequencies)
    assert np.argmax(spectrum.spectrum) == 1
    assert spectrum.frequency(1) == pytest.approx(expected_df)


def test_power_lookup_includes_terminal_stored_bin():
    values = [1.0, 4.0, 9.0, 16.0]
    df = 0.25
    spectrum = PowerSpectrum(
        Metadata(),
        DoubleVector(values),
        df,
        "endpoint-test",
        0.5,
        0.125,
        8,
    )
    terminal = spectrum.frequency(spectrum.nf() - 1)

    assert spectrum.power(terminal) == values[-1]
    assert spectrum.power(terminal - df / 4.0) == pytest.approx(
        values[-1] - (values[-1] - values[-2]) / 4.0
    )
    assert spectrum.power(np.nextafter(terminal, -np.inf)) == pytest.approx(values[-1])
    assert spectrum.power(np.nextafter(terminal, np.inf)) == 0.0
