from unittest.mock import Mock

import numpy as np
import pytest

import mspasspy.graphics as graphics
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble

EXPECTED_STYLES = ("wt", "wtva", "img", "wtvaimg")
NON_STRING_STYLES = (None, 1, ["wt"], np.array(["wt", "img"]))
NON_STRING_STYLE_IDS = ("none", "integer", "list", "ndarray")


def _timeseries():
    datum = TimeSeries(2)
    datum.t0 = 0.0
    datum.dt = 1.0
    datum.data[0] = 2.0
    datum.data[1] = 4.0
    datum.set_live()
    return datum


def test_valid_style_set_is_exact():
    assert graphics._VALID_PLOT_STYLES == EXPECTED_STYLES


def test_atomic_wt_stores_only_the_documented_figure_attribute(monkeypatch):
    plotter = graphics.SeismicPlotter(style="wt")
    datum = _timeseries()
    expected = object()
    render = Mock(return_value=expected)
    monkeypatch.setattr(plotter, "_wtva_TimeSeries", render)

    plotter.plot(datum)

    assert plotter.figure is expected
    assert plotter.get_plot_gcf() is expected
    assert not hasattr(plotter, "figre")
    render.assert_called_once_with(datum, False)


@pytest.mark.parametrize("aspect", (None, 0.375), ids=("default", "custom"))
@pytest.mark.parametrize(
    "limits,expected_limits",
    (
        ((None, None), (-4.0, 4.0)),
        ((-3.0, None), (-3.0, None)),
        ((None, 5.0), (None, 5.0)),
        ((-3.0, 5.0), (-3.0, 5.0)),
    ),
    ids=("automatic", "vmin-only", "vmax-only", "both"),
)
def test_atomic_image_options_are_defined_and_forwarded(
    monkeypatch, aspect, limits, expected_limits
):
    plotter = graphics.SeismicPlotter(style="img")
    plotter._aspect = aspect
    plotter._vmin, plotter._vmax = limits
    imshow = Mock()
    figure = object()
    monkeypatch.setattr(graphics.plt, "imshow", imshow)
    monkeypatch.setattr(graphics.plt, "gcf", Mock(return_value=figure))

    result = plotter._imageplot_TimeSeries(_timeseries())

    assert result is figure
    imshow.assert_called_once()
    call = imshow.call_args
    assert call.kwargs["aspect"] == (0.25 if aspect is None else aspect)
    assert call.kwargs["vmin"] == expected_limits[0]
    assert call.kwargs["vmax"] == expected_limits[1]
    assert np.asarray(call.args[0]).shape == (1, 2)


@pytest.mark.parametrize(
    "limits,expected_limits",
    (
        ((None, None), (-4.0, 4.0)),
        ((-3.0, None), (-3.0, None)),
        ((None, 5.0), (None, 5.0)),
        ((-3.0, 5.0), (-3.0, 5.0)),
    ),
    ids=("automatic", "vmin-only", "vmax-only", "both"),
)
def test_ensemble_color_limits_are_defined_and_forwarded(
    monkeypatch, limits, expected_limits
):
    plotter = graphics.SeismicPlotter(style="img")
    plotter._vmin, plotter._vmax = limits
    imshow = Mock()
    figure = object()
    monkeypatch.setattr(graphics.plt, "imshow", imshow)
    monkeypatch.setattr(graphics.plt, "gcf", Mock(return_value=figure))
    ensemble = TimeSeriesEnsemble()
    ensemble.member.append(_timeseries())

    result = plotter._imageplot_TimeSeriesEnsemble(ensemble)

    assert result is figure
    imshow.assert_called_once()
    call = imshow.call_args
    assert call.kwargs["aspect"] == 1.0
    assert call.kwargs["vmin"] == expected_limits[0]
    assert call.kwargs["vmax"] == expected_limits[1]
    assert np.asarray(call.args[0]).shape == (1, 2)


@pytest.mark.parametrize(
    "style", (*NON_STRING_STYLES, "unknown"), ids=(*NON_STRING_STYLE_IDS, "unknown")
)
def test_seismicplotter_constructor_rejects_every_invalid_style(style):
    with pytest.raises(TypeError) as error:
        graphics.SeismicPlotter(style=style)

    for accepted in EXPECTED_STYLES:
        assert accepted in str(error.value)


@pytest.mark.parametrize(
    "plotter_type", (graphics.SectionPlotter, graphics.SeismicPlotter)
)
@pytest.mark.parametrize("style", NON_STRING_STYLES, ids=NON_STRING_STYLE_IDS)
def test_change_style_rejects_non_string_style(plotter_type, style):
    plotter = plotter_type()
    original_style = plotter.style

    with pytest.raises(TypeError) as error:
        plotter.change_style(style)

    for accepted in EXPECTED_STYLES:
        assert accepted in str(error.value)
    assert plotter.style == original_style


@pytest.mark.parametrize(
    "plotter_type", (graphics.SectionPlotter, graphics.SeismicPlotter)
)
def test_change_style_preserves_unknown_string_runtime_error(plotter_type):
    plotter = plotter_type()
    original_style = plotter.style

    with pytest.raises(RuntimeError) as error:
        plotter.change_style("unknown")

    for accepted in EXPECTED_STYLES:
        assert accepted in str(error.value)
    assert plotter.style == original_style
