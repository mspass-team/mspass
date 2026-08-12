from unittest.mock import Mock

import pytest

import mspasspy.graphics as graphics
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)


@pytest.mark.parametrize("datum_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("style", ("wt", "wtva", "img", "wtvaimg"))
def test_sectionplotter_rejects_atomic_inputs_before_plotting(
    monkeypatch, datum_type, style
):
    plotter = graphics.SectionPlotter()
    plotter.change_style(style)
    wtva = Mock()
    image = Mock()
    monkeypatch.setattr(graphics, "wtvaplot", wtva)
    monkeypatch.setattr(graphics, "imageplot", image)

    with pytest.raises(TypeError, match="use SeismicPlotter"):
        plotter.plot(datum_type())

    wtva.assert_not_called()
    image.assert_not_called()


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
