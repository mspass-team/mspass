"""Rendered regressions for dead ensemble slots and all-style aspect control."""

import matplotlib

matplotlib.use("Agg", force=True)
import matplotlib.pyplot as plt
import numpy as np
import pytest

from mspasspy import graphics
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)

STYLES = ("wt", "wtva", "img", "wtvaimg")
PLOTTERS = (graphics.SectionPlotter, graphics.SeismicPlotter)


@pytest.fixture(autouse=True)
def close_figures():
    plt.close("all")
    yield
    plt.close("all")


def make_trace(kind=TimeSeries, *, empty=False):
    trace = kind(0 if empty else 5)
    trace.t0 = 10.0
    trace.dt = 0.5
    if not empty:
        values = np.array([0.0, 0.2, -0.1, 0.3, 0.0])
        if kind is TimeSeries:
            for index, value in enumerate(values):
                trace.data[index] = value
        else:
            trace.data[:, :] = np.vstack([values, values * 2, values * 3])
    trace.set_live()
    return trace


def make_ensemble(kind=TimeSeries):
    ens = TimeSeriesEnsemble() if kind is TimeSeries else SeismogramEnsemble()
    # Both a default empty tombstone and a nonempty killed member must be
    # ignored.  Their invalid dt and huge time bounds must never influence
    # the grid used to render the live data.
    ens.member.append(kind())
    ens.member.append(make_trace(kind))
    dead = make_trace(kind)
    dead.kill()
    dead.dt = 0.0
    dead.t0 = -1e12
    ens.member.append(dead)
    ens.member.append(make_trace(kind, empty=True))
    ens.member.append(make_trace(kind))
    ens.set_live()
    return ens


def figures_for(plotter, result, kind):
    if isinstance(plotter, graphics.SectionPlotter):
        return result
    if kind is Seismogram:
        return plotter.get_3Censemble_gcf()
    return [plotter.get_plot_gcf()]


@pytest.mark.parametrize("kind", (TimeSeries, Seismogram))
@pytest.mark.parametrize("style", STYLES)
@pytest.mark.parametrize("plotter_type", PLOTTERS)
def test_dead_and_empty_members_render_as_blank_slots(plotter_type, style, kind):
    ensemble = make_ensemble(kind)
    original = np.array(ensemble.member[1].data, copy=True)
    plotter = plotter_type(normalize=True)
    plotter.change_style(style)
    result = plotter.plot(ensemble)
    figures = figures_for(plotter, result, kind)
    assert len(figures) == (3 if kind is Seismogram else 1)
    for figure in figures:
        figure.canvas.draw()
        axes = figure.axes[0]
        if isinstance(plotter, graphics.SectionPlotter):
            assert axes.get_ylim() == pytest.approx((12.0, 10.0))
        else:
            assert axes.get_xlim() == pytest.approx((10.0, 12.0))
        if "img" in style:
            image = np.asarray(axes.images[0].get_array())
            if isinstance(plotter, graphics.SectionPlotter):
                assert image.shape == (5, 5)
                assert not image[:, [0, 2, 3]].any()
                assert image[:, 1].any() and image[:, 4].any()
            else:
                assert image.shape == (5, 5)
                assert not image[[0, 2, 3], :].any()
                assert image[1, :].any() and image[4, :].any()
        if style != "img" and isinstance(plotter, graphics.SeismicPlotter):
            assert len(axes.lines) == 2
            assert len(axes.collections) == (0 if style == "wt" else 2)
    np.testing.assert_array_equal(np.asarray(ensemble.member[1].data), original)
    assert ensemble.member[2].dead()
    assert ensemble.member[3].npts == 0


@pytest.mark.parametrize("kind", (TimeSeries, Seismogram))
@pytest.mark.parametrize("style", STYLES)
@pytest.mark.parametrize("plotter_type", PLOTTERS)
def test_no_usable_members_returns_without_creating_figures(plotter_type, style, kind):
    ensemble = make_ensemble(kind)
    ensemble.member[1].kill()
    ensemble.member[4].kill()
    plotter = plotter_type(normalize=True)
    plotter.change_style(style)
    assert plotter.plot(ensemble) is None
    assert plt.get_fignums() == []


@pytest.mark.parametrize("kind", (TimeSeries, Seismogram))
@pytest.mark.parametrize("style", STYLES)
@pytest.mark.parametrize("plotter_type", PLOTTERS)
def test_custom_aspect_controls_rendered_data_scale(plotter_type, style, kind):
    ensemble = make_ensemble(kind)
    # Keep this independent of dead-member handling so the aspect regression
    # detects ignored controls even when the old code can render the data.
    ensemble.member.clear()
    ensemble.member.append(make_trace(kind))
    ensemble.member.append(make_trace(kind))
    # Test the legacy attribute too: the constructor API alone must not hide
    # an ignored wiggle aspect or an imshow overlay resetting the axes.
    plotter = plotter_type()
    plotter.change_style(style)
    plotter._aspect = 0.375
    result = plotter.plot(ensemble)
    for figure in figures_for(plotter, result, kind):
        figure.canvas.draw()
        axes = figure.axes[0]
        origin, xunit, yunit = axes.transData.transform([(0, 0), (1, 0), (0, 1)])
        ratio = np.linalg.norm(yunit - origin) / np.linalg.norm(xunit - origin)
        assert ratio == pytest.approx(0.375)


@pytest.mark.parametrize("plotter_type", PLOTTERS)
def test_public_aspect_can_be_changed_and_reset(plotter_type):
    plotter = plotter_type(aspect=0.5)
    plotter.change_style("wt")
    ensemble = make_ensemble()
    result = plotter.plot(ensemble)
    figure = figures_for(plotter, result, TimeSeries)[0]
    figure.canvas.draw()
    assert figure.axes[0].get_aspect() == 0.5
    for value in (2.0, "equal", "auto", None):
        plotter.set_aspect(value)
        result = plotter.plot(ensemble)
        figure = figures_for(plotter, result, TimeSeries)[0]
        figure.canvas.draw()
        assert figure.axes[0].get_aspect() == (
            {"equal": 1.0, None: "auto"}.get(value, value)
        )


@pytest.mark.parametrize("plotter_type", PLOTTERS)
@pytest.mark.parametrize("value", (0, -1, np.nan, np.inf, "wide", [1, 2]))
def test_invalid_aspect_is_rejected_without_changing_setting(plotter_type, value):
    plotter = plotter_type(aspect=0.5)
    with pytest.raises(ValueError, match="aspect"):
        plotter.set_aspect(value)
    assert plotter._aspect == 0.5


@pytest.mark.parametrize("style", ("img", "wtvaimg"))
def test_default_section_image_keeps_long_time_windows_readable(style):
    ensemble = make_ensemble()
    for member in ensemble.member:
        if member.live and member.npts:
            member.dt = 100.0
    plotter = graphics.SectionPlotter()
    plotter.change_style(style)
    figure = plotter.plot(ensemble)[0]
    figure.canvas.draw()
    axes = figure.axes[0]
    # Default images should use the full subplot width, rather than shrink
    # to a thin strip for hundreds of seconds per trace.
    assert axes.get_window_extent().width > 0.7 * figure.bbox.width


def test_scalar_conversion_uses_only_live_samples_and_preserves_slots():
    ensemble = make_ensemble()
    t0, dt, matrix = graphics.tse2nparray(ensemble)
    assert (t0, dt) == (10.0, 0.5)
    assert matrix.shape == (5, 5)
    assert not matrix[:, [0, 2, 3]].any()
    np.testing.assert_array_equal(matrix[:, 1], np.asarray(ensemble.member[1].data))
    ensemble.member[1].kill()
    ensemble.member[4].kill()
    with pytest.raises(IndexError, match="live nonempty"):
        graphics.tse2nparray(ensemble)


@pytest.mark.parametrize("plotter_type", PLOTTERS)
@pytest.mark.parametrize("style", ("wt", "wtva"))
def test_reset_aspect_after_image_style_on_same_axes(plotter_type, style):
    plotter = plotter_type(aspect=0.5)
    ensemble = make_ensemble()
    plotter.change_style("wtvaimg")
    figure = figures_for(plotter, plotter.plot(ensemble), TimeSeries)[0]
    figure.canvas.draw()
    assert figure.axes[0].get_aspect() == 0.5
    plotter.set_aspect(None)
    plotter.change_style(style)
    second = figures_for(plotter, plotter.plot(ensemble), TimeSeries)[0]
    assert second is figure
    figure.canvas.draw()
    assert figure.axes[0].get_aspect() == "auto"


def test_section_half_sample_offsets_follow_native_sample_number():
    ensemble = TimeSeriesEnsemble()
    first = make_trace()
    first.dt = 1.0
    first.t0 = 0.0
    shifted = make_trace()
    shifted.dt = 1.0
    shifted.t0 = 0.5
    ensemble.member.append(first)
    ensemble.member.append(shifted)
    ensemble.set_live()
    t0, dt, matrix = graphics.tse2nparray(ensemble)
    times = t0 + np.arange(matrix.shape[0]) * dt
    for i, time in enumerate(times):
        if shifted.t0 <= time <= shifted.endtime():
            assert matrix[i, 1] == shifted.data[shifted.sample_number(time)]
    assert matrix[1, 1] == shifted.data[1]


def test_mixed_sample_rate_image_follows_native_sample_number():
    ensemble = TimeSeriesEnsemble()
    fine = make_trace()
    fine.t0 = 0.0
    fine.dt = 0.5
    coarse = make_trace()
    coarse.t0 = 0.0
    coarse.dt = 1.0
    ensemble.member.append(fine)
    ensemble.member.append(coarse)
    ensemble.set_live()
    plotter = graphics.SeismicPlotter(style="img")
    plotter.plot(ensemble)
    figure = plotter.get_plot_gcf()
    figure.canvas.draw()
    image = np.asarray(figure.axes[0].images[0].get_array())
    times = fine.t0 + np.arange(image.shape[1]) * fine.dt
    expected = [coarse.data[coarse.sample_number(time)] for time in times]
    np.testing.assert_array_equal(image[1], expected)


@pytest.mark.parametrize("kind", (TimeSeries, Seismogram))
@pytest.mark.parametrize("style", STYLES)
@pytest.mark.parametrize("plotter_type", PLOTTERS)
def test_atomic_aspect_controls_rendered_data_scale(plotter_type, style, kind):
    plotter = plotter_type(aspect=0.375)
    plotter.change_style(style)
    result = plotter.plot(make_trace(kind))
    figures = (
        result
        if isinstance(plotter, graphics.SectionPlotter)
        else [plotter.get_plot_gcf()]
    )
    assert len(figures) == 1
    figure = figures[0]
    figure.canvas.draw()
    axes = figure.axes[0]
    origin, xunit, yunit = axes.transData.transform([(0, 0), (1, 0), (0, 1)])
    ratio = np.linalg.norm(yunit - origin) / np.linalg.norm(xunit - origin)
    assert ratio == pytest.approx(0.375)
