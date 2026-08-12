import math

import pytest

from mspasspy.ccore.algorithms.amplitudes import PercAmplitude
from mspasspy.ccore.seismic import _CoreSeismogram, _CoreTimeSeries
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError

SAMPLES = (-16.0, 1.0, -32.0, 4.0, -2.0, 8.0)


def _timeseries():
    waveform = _CoreTimeSeries(len(SAMPLES))
    waveform.set_live()
    for index, sample in enumerate(SAMPLES):
        waveform.data[index] = sample
    return waveform


def _seismogram():
    waveform = _CoreSeismogram(len(SAMPLES))
    waveform.set_live()
    for index, sample in enumerate(SAMPLES):
        waveform.data[0, index] = sample
    return waveform


@pytest.mark.parametrize("factory", [_timeseries, _seismogram])
def test_percentile_amplitude_uses_lower_quantile(factory):
    waveform = factory()
    sorted_amplitudes = sorted(abs(sample) for sample in SAMPLES)

    for percentile in (0.01, 0.5, 50.0, 0.95, 95.0, 1.0, 100.0):
        fraction = percentile / 100.0 if percentile > 1.0 else percentile
        expected = sorted_amplitudes[math.floor(fraction * (len(SAMPLES) - 1))]
        assert PercAmplitude(waveform, percentile) == expected

    assert PercAmplitude(waveform, 0.5) == PercAmplitude(waveform, 50.0)
    assert PercAmplitude(waveform, 0.95) == PercAmplitude(waveform, 95.0)
    assert PercAmplitude(waveform, 1.0) == PercAmplitude(waveform, 100.0)


@pytest.mark.parametrize("factory", [_timeseries, _seismogram])
@pytest.mark.parametrize(
    "percentile", [0.0, -1.0, 100.1, float("inf"), float("-inf"), float("nan")]
)
def test_percentile_amplitude_rejects_invalid_live_input(factory, percentile):
    with pytest.raises(MsPASSError) as exc_info:
        PercAmplitude(factory(), percentile)
    assert exc_info.value.severity == ErrorSeverity.Invalid


@pytest.mark.parametrize("factory", [_timeseries, _seismogram])
def test_percentile_amplitude_returns_zero_for_dead_or_empty_input(factory):
    dead = factory()
    dead.kill()
    assert PercAmplitude(dead, 0.0) == 0.0

    empty = type(dead)(0)
    empty.set_live()
    assert PercAmplitude(empty, 0.0) == 0.0
