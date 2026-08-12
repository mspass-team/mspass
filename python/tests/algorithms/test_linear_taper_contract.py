import pytest

from mspasspy.ccore.algorithms.basic import LinearTaper
from mspasspy.ccore.seismic import Seismogram, TimeReferenceType, TimeSeries


def _timeseries():
    waveform = TimeSeries(11)
    waveform.t0 = 0.0
    waveform.dt = 1.0
    waveform.tref = TimeReferenceType.Relative
    waveform.set_live()
    for sample in range(waveform.npts):
        waveform.data[sample] = 1.0
    return waveform


def _seismogram():
    waveform = Seismogram(11)
    waveform.t0 = 0.0
    waveform.dt = 1.0
    waveform.tref = TimeReferenceType.Relative
    waveform.set_live()
    for sample in range(waveform.npts):
        for component in range(3):
            waveform.data[component, sample] = component + 1.0
    return waveform


def _head_weight(time):
    if time < 2.0:
        return 0.0
    if time < 5.0:
        return (time - 2.0) / (5.0 - 2.0)
    return 1.0


def _tail_weight(time):
    if time < 6.0:
        return 1.0
    if time <= 9.0:
        return (9.0 - time) / (9.0 - 6.0)
    return 0.0


def test_linear_taper_piecewise_formula_and_waveform_parity():
    timeseries = _timeseries()
    seismogram = _seismogram()
    taper = LinearTaper(2.0, 5.0, 6.0, 9.0)

    assert taper.apply(timeseries) == 0
    assert taper.apply(seismogram) == 0

    for sample in range(timeseries.npts):
        time = timeseries.time(sample)
        expected = _head_weight(time) * _tail_weight(time)
        assert timeseries.data[sample] == pytest.approx(expected)
        for component in range(3):
            component_scale = component + 1.0
            assert seismogram.data[component, sample] == pytest.approx(
                expected * component_scale
            )
            assert seismogram.data[
                component, sample
            ] / component_scale == pytest.approx(timeseries.data[sample])

    assert timeseries.data[2] == 0.0
    assert timeseries.data[5] == 1.0
    assert timeseries.data[6] == 1.0
    assert timeseries.data[9] == 0.0
