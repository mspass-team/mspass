import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest

import mspasspy.algorithms.MCXcorStacking as mcx
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.seismic import (
    DoubleVector,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import Metadata


def test_contract_module_is_loaded_from_selected_tree():
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        assert Path(mcx.__file__).resolve().is_relative_to(Path(source_root).resolve())


def _live_timeseries(npts=32, dt=1.0, t0=0.0):
    datum = TimeSeries()
    datum.set_npts(npts)
    datum.dt = dt
    datum.t0 = t0
    datum.data = DoubleVector(np.ones(npts))
    datum.set_live()
    return datum


def test_set_phases_posts_default_depth_before_model_selection():
    datum = _live_timeseries()
    datum["dist"] = 30.0
    datum["source_time"] = 1000.0
    default_depth = 42.5

    class InspectingModel:
        def get_travel_times(self, **kwargs):
            assert datum["source_depth"] == default_depth
            assert kwargs["source_depth_in_km"] == default_depth
            return [SimpleNamespace(name="P", time=12.0)]

    result = mcx._set_phases(datum, InspectingModel(), default_depth=default_depth)

    assert result is datum
    assert result["source_depth"] == default_depth
    assert result["Ptime"] == 1012.0
    assert result.elog.size() == 1
    assert mcx._get_search_range(result) == 20.0


@pytest.mark.parametrize("depth", [50.0, 150.0])
@pytest.mark.parametrize(
    "pPtime,PPtime,expected",
    [
        (112.0, None, {50.0: 12.0, 150.0: 12.0}),
        (None, 130.0, {50.0: 30.0, 150.0: 30.0}),
        (None, None, {50.0: 20.0, 150.0: 20.0}),
    ],
)
def test_search_range_phase_fallbacks(depth, pPtime, PPtime, expected):
    datum = _live_timeseries()
    datum["source_depth"] = depth
    datum["Ptime"] = 100.0
    if pPtime is not None:
        datum["pPtime"] = pPtime
    if PPtime is not None:
        datum["PPtime"] = PPtime

    assert mcx._get_search_range(datum) == expected[depth]


@pytest.mark.parametrize(
    "depth,pPtime,PPtime,expected",
    [
        (150.0, 112.0, 130.0, 12.0),
        (50.0, 112.0, 130.0, 30.0),
        (150.0, 90.0, None, 0.0),
        (50.0, None, 90.0, 0.0),
    ],
)
def test_search_range_preference_and_nonnegative_duration(
    depth, pPtime, PPtime, expected
):
    datum = _live_timeseries()
    datum["source_depth"] = depth
    datum["Ptime"] = 100.0
    if pPtime is not None:
        datum["pPtime"] = pPtime
    if PPtime is not None:
        datum["PPtime"] = PPtime

    assert mcx._get_search_range(datum) == expected


def test_search_range_treats_100_km_as_shallow():
    datum = _live_timeseries()
    datum["source_depth"] = 100.0
    datum["Ptime"] = 100.0
    datum["pPtime"] = 112.0
    datum["PPtime"] = 130.0

    assert mcx._get_search_range(datum) == 30.0


@pytest.mark.parametrize("depth", [50.0, 150.0])
@pytest.mark.parametrize("duration_undefined,expected", [(8.0, 8.0), (-8.0, 0.0)])
def test_search_range_clamps_undefined_duration(depth, duration_undefined, expected):
    datum = _live_timeseries()
    datum["source_depth"] = depth
    datum["Ptime"] = 100.0

    assert (
        mcx._get_search_range(datum, duration_undefined=duration_undefined) == expected
    )


@pytest.mark.parametrize(
    "fraction,expected_end", [(0.0, 0.0), (0.5, 10.0), (1.0, 20.0)]
)
def test_prep_applies_search_fraction_once_to_common_window(fraction, expected_end):
    datum = _live_timeseries(npts=101, t0=90.0)
    datum.tref = TimeReferenceType.UTC
    datum["Ptime"] = 100.0
    datum["PPtime"] = 120.0
    datum["source_depth"] = 10.0
    datum["Parrival"] = {"bandwidth": 1.0}
    ensemble = TimeSeriesEnsemble(Metadata(), 1)
    ensemble.member.append(datum)
    ensemble.set_live()
    coda_search_starts = []

    def record_coda_search(datum, level, search_start=None, **kwargs):
        coda_search_starts.append(search_start)
        return TimeWindow(0.0, 100.0)

    with (
        patch.object(mcx, "filter", side_effect=lambda value, **kwargs: value),
        patch.object(mcx, "MADAmplitude", return_value=1.0),
        patch.object(mcx, "_coda_duration", side_effect=record_coda_search),
    ):
        result, beam = mcx.MCXcorPrepP(
            ensemble,
            TimeWindow(-10.0, -1.0),
            model=object(),
            set_phases=False,
            low_f_corner=0.1,
            high_f_corner=1.0,
            search_window_fraction=fraction,
        )

    assert result.live
    assert beam.live
    assert coda_search_starts == [20.0]
    assert beam["correlation_window_end"] == expected_end


def test_coda_duration_clips_negative_start_and_reports_no_crossing():
    datum = _live_timeseries(npts=16)
    datum.data = DoubleVector(np.zeros(datum.npts))

    window = mcx._coda_duration(datum, level=1.0, t0=-10.0)

    assert window.start == datum.t0
    assert window.end == datum.t0
    assert window.end - window.start == 0.0

    off_grid_start = 0.25
    window = mcx._coda_duration(datum, level=1.0, t0=off_grid_start)
    assert window.start == off_grid_start
    assert window.end == off_grid_start


def test_phase_time_contract():
    datum = _live_timeseries()
    datum["Ptime"] = 1000.0
    datum["arrival_time_correction"] = 1.25
    assert mcx.phase_time(datum) == -2.0

    datum.tref = TimeReferenceType.UTC
    datum.erase("arrival_time_correction")
    assert mcx.phase_time(datum) == -1.0

    datum["arrival_time_correction"] = 1.25
    datum.erase("Ptime")
    assert mcx.phase_time(datum) == -1.0

    datum["Ptime"] = 1000.0
    assert mcx.phase_time(datum) == 1001.25
