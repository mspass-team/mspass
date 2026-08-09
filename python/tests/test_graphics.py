from unittest.mock import Mock

from mspasspy.graphics import LargeEnsemblePlotter


def test_large_ensemble_plotter_ignores_empty_figure_handles(monkeypatch):
    plotter = LargeEnsemblePlotter()
    active_figure = Mock()
    monkeypatch.setattr(plotter, "get_plot_gcf", lambda: None)
    monkeypatch.setattr(
        plotter, "get_3Censemble_gcf", lambda: [None, active_figure, None]
    )

    plotter._clear_figure_canvases()

    active_figure.clear.assert_called_once_with()
