from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--decon-validation-plots",
        action="store_true",
        default=False,
        help="write deconvolution validation diagnostic plots",
    )
    parser.addoption(
        "--decon-validation-plot-dir",
        action="store",
        default="decon_validation_plots",
        help="directory used with --decon-validation-plots",
    )


@pytest.fixture
def decon_validation_plot_dir(request):
    if not request.config.getoption("--decon-validation-plots"):
        return None
    plot_dir = Path(request.config.getoption("--decon-validation-plot-dir"))
    plot_dir.mkdir(parents=True, exist_ok=True)
    return plot_dir
