from unittest.mock import Mock

import matplotlib.pyplot as plt
import numpy as np
import pytest

import mspasspy.graphics as graphics
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)


@pytest.fixture(autouse=True)
def close_figures():
    plt.close("all")
    yield
    plt.close("all")


def make_atomic(datum_type):
    datum = datum_type(4)
    datum.t0 = 1.0
    datum.dt = 0.25
    if isinstance(datum, TimeSeries):
        for index, value in enumerate((1.0, 2.0, 3.0, 4.0)):
            datum.data[index] = value
    else:
        datum.data[:, :] = np.array(
            [[1.0, 2.0, 3.0, 4.0], [2.0, 3.0, 4.0, 5.0], [3.0, 4.0, 5.0, 6.0]]
        )
    return datum


@pytest.mark.parametrize("datum_type,trace_count", ((TimeSeries, 1), (Seismogram, 3)))
@pytest.mark.parametrize("style", ("wt", "wtva", "img", "wtvaimg"))
def test_sectionplotter_supports_atomic_inputs(datum_type, trace_count, style):
    plotter = graphics.SectionPlotter()
    plotter.change_style(style)
    datum = make_atomic(datum_type)

    handles = plotter.plot(datum)

    assert len(handles) == 1
    axes = handles[0].axes[0]
    expected_times = datum.t0 + np.arange(datum.npts) * datum.dt
    if style != "img":
        assert len(axes.lines) == trace_count
        for line in axes.lines:
            np.testing.assert_array_equal(line.get_ydata(), expected_times)
    if "img" in style:
        assert len(axes.images) == 1
        assert axes.images[0].get_array().shape == (datum.npts, trace_count)


@pytest.mark.parametrize("ensemble_type", (TimeSeriesEnsemble, SeismogramEnsemble))
@pytest.mark.parametrize("style", ("wt", "wtva", "img", "wtvaimg"))
def test_sectionplotter_preserves_ensemble_style_dispatch(
    monkeypatch, ensemble_type, style
):
    plotter = graphics.SectionPlotter()
    plotter.change_style(style)
    ensemble = ensemble_type()
    expected = [object()]
    wtva = Mock(return_value=expected)
    image = Mock(return_value=expected)
    monkeypatch.setattr(graphics, "wtvaplot", wtva)
    monkeypatch.setattr(graphics, "imageplot", image)

    assert plotter.plot(ensemble) is expected

    if style == "img":
        image.assert_called_once()
        assert image.call_args.args[0] is ensemble
        wtva.assert_not_called()
    else:
        wtva.assert_called_once()
        assert wtva.call_args.args[0] is ensemble
        image.assert_not_called()
