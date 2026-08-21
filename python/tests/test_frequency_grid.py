import numpy as np
import pytest

from mspasspy.ccore.algorithms.deconvolution import (
    CNRDeconEngine,
    MTPowerSpectrumEngine,
)
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum, TimeSeries
from mspasspy.ccore.utility import ErrorSeverity, Metadata, MsPASSError, pfread


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


def _live_timeseries(npts, dt):
    data = TimeSeries(npts)
    data.dt = dt
    data.t0 = 0.0
    data.set_live()
    for i in range(npts):
        data.data[i] = np.sin(0.17 * i) + 0.25 * np.cos(0.07 * i)
    return data


def test_cnr_internal_noise_spectrum_contains_a_real_nyquist_bin():
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    dt = engine.get_operator_dt()
    noise = _live_timeseries(801, dt)

    spectrum = engine.compute_noise_spectrum(noise)

    assert spectrum.live
    assert spectrum.nf() > 1
    assert spectrum.frequency(spectrum.nf() - 1) == pytest.approx(1.0 / (2.0 * dt))
    assert spectrum.power(spectrum.frequency(spectrum.nf() - 1)) == pytest.approx(
        spectrum.spectrum[-1]
    )


def test_cnr_rejects_an_external_odd_grid_without_nyquist():
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    dt = engine.get_operator_dt()
    noise = _live_timeseries(801, dt)
    odd_spectrum = MTPowerSpectrumEngine(801, 2.5, 4, 801, dt).apply(noise)
    assert odd_spectrum.frequency(odd_spectrum.nf() - 1) < 1.0 / (2.0 * dt)

    with pytest.raises(MsPASSError) as caught:
        engine.initialize_inverse_operator(noise, odd_spectrum)

    assert caught.value.severity == ErrorSeverity.Invalid
    assert "does not cover operator Nyquist" in str(caught.value)
