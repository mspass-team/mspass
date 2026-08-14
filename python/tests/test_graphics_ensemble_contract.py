from unittest.mock import Mock

import numpy as np
import pytest

import mspasspy.graphics as graphics
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)


def _timeseries(value, *, dt=1.0, t0=0.0, live=True, npts=3):
    datum = TimeSeries(npts)
    datum.dt = dt
    datum.t0 = t0
    for sample in range(npts):
        datum.data[sample] = value + sample
    if live:
        datum.set_live()
    else:
        datum.kill()
    datum["member_id"] = value
    return datum


def _seismogram(value, *, dt=1.0, live=True):
    datum = Seismogram(3)
    datum.dt = dt
    for component in range(3):
        for sample in range(datum.npts):
            datum.data[component, sample] = value + 10 * component + sample
    if live:
        datum.set_live()
    else:
        datum.kill()
    datum["member_id"] = value
    return datum


def _ensemble(member_type, members, *, live=True):
    ensemble_type = (
        TimeSeriesEnsemble if member_type is TimeSeries else SeismogramEnsemble
    )
    ensemble = ensemble_type()
    for member in members:
        ensemble.member.append(member)
    if live:
        ensemble.set_live()
    return ensemble


@pytest.mark.parametrize("member_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("skip_the_dead", (True, False), ids=("skip", "keep-cells"))
@pytest.mark.parametrize(
    "count,members_per_frame,expected_sizes",
    (
        (1, 3, (1,)),
        (2, 3, (2,)),
        (3, 3, (3,)),
        (4, 3, (3, 1)),
        (8, 3, (3, 3, 2)),
    ),
)
def test_large_ensemble_frames_every_live_member_once(
    monkeypatch,
    member_type,
    skip_the_dead,
    count,
    members_per_frame,
    expected_sizes,
):
    members = []
    live_ids = []
    for value in range(count):
        members.append(
            _timeseries(value) if member_type is TimeSeries else _seismogram(value)
        )
        live_ids.append(value)
        if value % 2 == 0:
            members.append(
                _timeseries(100 + value, dt=np.nan, live=False)
                if member_type is TimeSeries
                else _seismogram(100 + value, dt=np.nan, live=False)
            )
    ensemble = _ensemble(member_type, members)
    plotter = graphics.LargeEnsemblePlotter(
        normalize=False, members_per_frame=members_per_frame
    )
    plotted = []
    show = Mock()
    clear = Mock()

    def record_frame(self, frame):
        assert frame.live
        plotted.append([member["member_id"] for member in frame.member])

    monkeypatch.setattr(graphics.SeismicPlotter, "plot", record_frame)
    monkeypatch.setattr(graphics.plt, "show", show)
    monkeypatch.setattr(plotter, "_clear_figure_canvases", clear)

    assert plotter.plot(ensemble, skip_the_dead=skip_the_dead) is None

    expected_ids = (
        live_ids if skip_the_dead else [member["member_id"] for member in members]
    )
    if not skip_the_dead:
        expected_sizes = tuple(
            min(members_per_frame, len(expected_ids) - offset)
            for offset in range(0, len(expected_ids), members_per_frame)
        )
    assert tuple(map(len, plotted)) == expected_sizes
    assert [member_id for frame in plotted for member_id in frame] == expected_ids
    assert show.call_count == len(expected_sizes)
    assert clear.call_count == max(0, len(expected_sizes) - 1)


@pytest.mark.parametrize("member_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("mode", ("empty", "dead-ensemble", "all-dead"))
def test_large_ensemble_without_live_members_never_plots(
    monkeypatch, member_type, mode
):
    if mode == "empty":
        ensemble = _ensemble(member_type, [], live=True)
    elif mode == "dead-ensemble":
        ensemble = _ensemble(
            member_type,
            [
                (
                    _timeseries(1, dt=np.nan)
                    if member_type is TimeSeries
                    else _seismogram(1, dt=np.nan)
                )
            ],
            live=False,
        )
    else:
        ensemble = _ensemble(
            member_type,
            [
                (
                    _timeseries(1, live=False)
                    if member_type is TimeSeries
                    else _seismogram(1, live=False)
                )
            ],
            live=True,
        )
    plotter = graphics.LargeEnsemblePlotter(normalize=False)
    render = Mock()
    show = Mock()
    figure = Mock()
    monkeypatch.setattr(graphics.SeismicPlotter, "plot", render)
    monkeypatch.setattr(graphics.plt, "show", show)
    monkeypatch.setattr(graphics.plt, "figure", figure)

    assert plotter.plot(ensemble) is None
    render.assert_not_called()
    show.assert_not_called()
    figure.assert_not_called()


@pytest.mark.parametrize("style", ("wt", "wtva", "img", "wtvaimg"))
@pytest.mark.parametrize("member_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("mode", ("empty", "dead-ensemble", "all-dead"))
def test_seismic_plotter_without_live_members_never_creates_a_figure(
    monkeypatch, style, member_type, mode
):
    members = []
    ensemble_live = True
    if mode == "dead-ensemble":
        members = [
            (
                _timeseries(1, dt=np.nan)
                if member_type is TimeSeries
                else _seismogram(1, dt=np.nan)
            )
        ]
        ensemble_live = False
    elif mode == "all-dead":
        members = [
            (
                _timeseries(1, live=False)
                if member_type is TimeSeries
                else _seismogram(1, live=False)
            )
        ]
    ensemble = _ensemble(member_type, members, live=ensemble_live)
    plotter = graphics.SeismicPlotter(style=style)
    figure = Mock()
    gcf = Mock()
    draw_line = Mock()
    imshow = Mock()
    monkeypatch.setattr(graphics.plt, "figure", figure)
    monkeypatch.setattr(graphics.plt, "gcf", gcf)
    monkeypatch.setattr(graphics.plt, "plot", draw_line)
    monkeypatch.setattr(graphics.plt, "imshow", imshow)

    assert plotter.plot(ensemble) is None

    figure.assert_not_called()
    gcf.assert_not_called()
    draw_line.assert_not_called()
    imshow.assert_not_called()


@pytest.mark.parametrize("member_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize("bad_dt", (0.0, -1.0, np.nan, np.inf, -np.inf))
def test_invalid_live_dt_is_rejected_before_plotting(monkeypatch, member_type, bad_dt):
    valid_members = (
        [_timeseries(3), _timeseries(4)]
        if member_type is TimeSeries
        else [_seismogram(3), _seismogram(4)]
    )
    invalid = (
        _timeseries(1, dt=bad_dt)
        if member_type is TimeSeries
        else _seismogram(1, dt=bad_dt)
    )
    dead = (
        _timeseries(2, dt=np.nan, live=False)
        if member_type is TimeSeries
        else _seismogram(2, dt=np.nan, live=False)
    )
    ensemble = _ensemble(member_type, [*valid_members, dead, invalid])
    plotter = graphics.LargeEnsemblePlotter(
        normalize=False, members_per_frame=len(valid_members)
    )
    render = Mock()
    show = Mock()
    monkeypatch.setattr(graphics.SeismicPlotter, "plot", render)
    monkeypatch.setattr(graphics.plt, "show", show)

    with pytest.raises(ValueError, match="finite, positive dt"):
        plotter.plot(ensemble)

    render.assert_not_called()
    show.assert_not_called()


@pytest.mark.parametrize("style", ("wt", "wtva", "img", "wtvaimg"))
@pytest.mark.parametrize("member_type", (TimeSeries, Seismogram))
def test_seismic_plotter_rejects_invalid_live_dt_before_drawing(
    monkeypatch, style, member_type
):
    invalid = (
        _timeseries(1, dt=np.nan)
        if member_type is TimeSeries
        else _seismogram(1, dt=np.nan)
    )
    ensemble = _ensemble(member_type, [invalid])
    plotter = graphics.SeismicPlotter(style=style)
    figure = Mock()
    draw_line = Mock()
    imshow = Mock()
    monkeypatch.setattr(graphics.plt, "figure", figure)
    monkeypatch.setattr(graphics.plt, "plot", draw_line)
    monkeypatch.setattr(graphics.plt, "imshow", imshow)

    with pytest.raises(ValueError, match="finite, positive dt"):
        plotter.plot(ensemble)

    figure.assert_not_called()
    draw_line.assert_not_called()
    imshow.assert_not_called()


def test_image_ensemble_uses_minimum_live_dt_and_copies_sample_zero(monkeypatch):
    dead = _timeseries(100, dt=-1.0, live=False)
    coarse = _timeseries(10, dt=1.0)
    fine = _timeseries(20, dt=0.5)
    ensemble = _ensemble(TimeSeries, [dead, coarse, fine])
    plotter = graphics.SeismicPlotter(style="img")
    imshow = Mock()
    monkeypatch.setattr(graphics.plt, "imshow", imshow)
    monkeypatch.setattr(graphics.plt, "gcf", Mock(return_value=object()))

    plotter._imageplot_TimeSeriesEnsemble(ensemble)

    matrix = np.asarray(imshow.call_args.args[0])
    assert matrix.shape == (3, 5)
    assert matrix[0].tolist() == [0.0] * 5
    assert matrix[1, 0] == coarse.data[0]
    assert matrix[2, 0] == fine.data[0]


@pytest.mark.parametrize("aspect", (None, 0.375))
def test_image_ensemble_forwards_calculated_or_custom_aspect(monkeypatch, aspect):
    ensemble = _ensemble(TimeSeries, [_timeseries(1), _timeseries(2)])
    plotter = graphics.SeismicPlotter(style="img")
    plotter._aspect = aspect
    imshow = Mock()
    monkeypatch.setattr(graphics.plt, "imshow", imshow)
    monkeypatch.setattr(graphics.plt, "gcf", Mock(return_value=object()))

    plotter._imageplot_TimeSeriesEnsemble(ensemble)

    assert imshow.call_args.kwargs["aspect"] == (1.0 if aspect is None else aspect)


def test_three_component_wiggle_forwards_fill_to_every_component(monkeypatch):
    ensemble = _ensemble(Seismogram, [_seismogram(1)])
    plotter = graphics.SeismicPlotter(style="wtva")
    fill_calls = []

    def record(component_ensemble, fill=True):
        fill_calls.append(fill)
        return object()

    monkeypatch.setattr(plotter, "_wtva", record)
    monkeypatch.setattr(graphics.plt, "figure", Mock())

    handles = plotter._wtva_SeismogramEnsemble(ensemble, False)

    assert len(handles) == 3
    assert fill_calls == [False, False, False]
