from copy import deepcopy
import sys
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest

sys.path.append("python/tests")
from helper import get_live_seismogram

from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.algorithms.RFdeconProcessor import RFdecon, RFdeconProcessor
from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.seismic import DoubleVector, Seismogram, TimeSeries

_SCALAR_NOISE_ALGORITHMS = (
    "MultiTaperPowerXcor",
    "MultiTaperPowerSpecDiv",
)


class _OneShotNumericIterable:
    def __init__(self, values):
        self.values = list(values)
        self.iterations = 0
        self.yields = 0

    def __iter__(self):
        self.iterations += 1
        if self.iterations > 1:
            raise AssertionError("numeric iterable was materialized more than once")
        for value in self.values:
            self.yields += 1
            yield value


def _seismogram():
    datum = get_live_seismogram(3000, 20.0)
    datum.t0 = -35.0
    return datum


def _noise_timeseries(datum):
    return ExtractComponent(WindowData(Seismogram(datum), -30.0, -5.0), 2)


def _snapshot_waveform(datum):
    return (
        datum.live,
        datum.npts,
        datum.t0,
        datum.dt,
        dict(datum),
        np.array(datum.data, copy=True),
    )


def _assert_waveform_unchanged(datum, snapshot):
    assert datum.live == snapshot[0]
    assert datum.npts == snapshot[1]
    assert datum.t0 == snapshot[2]
    assert datum.dt == snapshot[3]
    assert dict(datum) == snapshot[4]
    assert np.array_equal(datum.data, snapshot[5])


def _processor_snapshot(processor):
    return deepcopy(processor.__getstate__())


def _assert_processor_unchanged(processor, snapshot):
    current = processor.__getstate__()
    assert current.keys() == snapshot.keys()
    for key in current:
        if isinstance(current[key], np.ndarray):
            assert np.array_equal(current[key], snapshot[key])
        elif key == "md":
            assert dict(current[key]) == dict(snapshot[key])
        else:
            assert current[key] == snapshot[key]


@pytest.mark.parametrize("algorithm", _SCALAR_NOISE_ALGORITHMS)
@pytest.mark.parametrize("representation", ("timeseries", "vector"))
def test_rfdecon_scalar_noise_dispatches_samples_once(
    monkeypatch, algorithm, representation
):
    datum = _seismogram()
    noise = _noise_timeseries(datum)
    processor = RFdeconProcessor(algorithm, pf="data/pf/RFdeconProcessor.pf")
    original_loadnoise = processor.loadnoise
    calls = []

    def record_loadnoise(value, dtype="Seismogram", component=2, window=False):
        calls.append((value, dtype, component, window))
        return original_loadnoise(
            value, dtype=dtype, component=component, window=window
        )

    monkeypatch.setattr(processor, "loadnoise", record_loadnoise)
    if representation == "timeseries":
        supplied_noise = noise
    else:
        supplied_noise = _OneShotNumericIterable(noise.data)

    result = RFdecon(datum, engine=processor, noisedata=supplied_noise)

    assert isinstance(result, Seismogram)
    assert result.live
    assert np.isfinite(result.data).all()
    assert len(calls) == 1
    loaded, dtype, component, window = calls[0]
    expected_dtype = "TimeSeries" if representation == "timeseries" else "raw_vector"
    assert dtype == expected_dtype
    assert component == 2
    assert window is False
    if representation == "timeseries":
        assert loaded is noise
    else:
        assert isinstance(loaded, DoubleVector)
        assert supplied_noise.iterations == 1
        assert supplied_noise.yields == len(noise.data)
    assert np.array_equal(processor.nvector, np.asarray(noise.data))


@pytest.mark.parametrize("algorithm", _SCALAR_NOISE_ALGORITHMS)
@pytest.mark.parametrize("representation", ("timeseries", "vector"))
def test_rfdecon_scalar_noise_constructed_processor(algorithm, representation):
    datum = _seismogram()
    noise = _noise_timeseries(datum)
    if representation == "timeseries":
        supplied_noise = noise
    else:
        supplied_noise = _OneShotNumericIterable(noise.data)

    result = RFdecon(
        datum,
        alg=algorithm,
        pf="data/pf/RFdeconProcessor.pf",
        noisedata=supplied_noise,
    )

    assert isinstance(result, Seismogram)
    assert result.live
    assert np.isfinite(result.data).all()
    if representation == "vector":
        assert supplied_noise.iterations == 1
        assert supplied_noise.yields == len(noise.data)


@pytest.mark.parametrize("algorithm", _SCALAR_NOISE_ALGORITHMS)
def test_rfdecon_scalar_noise_none_keeps_component_window_path(monkeypatch, algorithm):
    datum = _seismogram()
    processor = RFdeconProcessor(algorithm, pf="data/pf/RFdeconProcessor.pf")
    original_loadnoise = processor.loadnoise
    calls = []

    def record_loadnoise(value, dtype="Seismogram", component=2, window=False):
        calls.append((value, dtype, component, window))
        return original_loadnoise(
            value, dtype=dtype, component=component, window=window
        )

    monkeypatch.setattr(processor, "loadnoise", record_loadnoise)

    result = RFdecon(datum, engine=processor, noisedata=None, ncomp=1)

    assert isinstance(result, Seismogram)
    assert result.live
    assert len(calls) == 1
    assert calls[0] == (datum, "Seismogram", 1, True)


@pytest.mark.parametrize("algorithm", _SCALAR_NOISE_ALGORITHMS)
@pytest.mark.parametrize(
    "bad_noise",
    (
        object(),
        np.ones((2, 3)),
        pd.DataFrame([[1.0, 2.0], [3.0, 4.0]], columns=[10.0, 20.0]),
    ),
)
def test_rfdecon_rejects_invalid_scalar_noise_before_mutation(
    monkeypatch, algorithm, bad_noise
):
    datum = _seismogram()
    waveform_before = _snapshot_waveform(datum)
    processor = RFdeconProcessor(algorithm, pf="data/pf/RFdeconProcessor.pf")
    processor.dvector = np.array([1.0, 2.0])
    processor.wvector = np.array([3.0, 4.0])
    processor.nvector = np.array([5.0, 6.0])
    processor_before = _processor_snapshot(processor)
    loadwavelet = Mock(side_effect=AssertionError("wavelet must not be loaded"))
    loadnoise = Mock(side_effect=AssertionError("noise must not be loaded"))
    monkeypatch.setattr(processor, "loadwavelet", loadwavelet)
    monkeypatch.setattr(processor, "loadnoise", loadnoise)

    with pytest.raises(TypeError, match="one-dimensional numeric iterable"):
        RFdecon(
            datum,
            engine=processor,
            wavelet=[1.0, 0.0],
            wavelet_t0=processor.dwin.start,
            noisedata=bad_noise,
        )

    loadwavelet.assert_not_called()
    loadnoise.assert_not_called()
    _assert_processor_unchanged(processor, processor_before)
    _assert_waveform_unchanged(datum, waveform_before)
