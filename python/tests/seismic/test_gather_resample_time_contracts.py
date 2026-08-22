from copy import deepcopy
import math

import numpy as np
import pandas as pd
import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.seismic.gather import (
    Gather,
    regularize_ensemble,
    resample_ensemble,
)


def _timeseries(t0, npts, dt, offset=0.0):
    datum = TimeSeries(npts)
    datum.t0 = t0
    datum.dt = dt
    datum["marker"] = offset
    datum["sampling_rate"] = 1.0 / dt
    datum.set_live()
    for index in range(npts):
        datum.data[index] = offset + index
    return datum


def _ensemble(specifications):
    ensemble = TimeSeriesEnsemble()
    ensemble["label"] = "original"
    for specification in specifications:
        ensemble.member.append(_timeseries(*specification))
    ensemble.set_live()
    return ensemble


def _ensemble_snapshot(ensemble):
    return {
        "metadata": deepcopy(dict(ensemble)),
        "live": ensemble.live,
        "elog": [
            (error.algorithm, error.message, error.badness)
            for error in ensemble.elog.get_error_log()
        ],
        "members": [
            {
                "metadata": deepcopy(dict(member)),
                "live": member.live,
                "is_utc": member.time_is_UTC(),
                "t0": member.t0,
                "dt": member.dt,
                "npts": member.npts,
                "data": np.array(member.data, copy=True),
                "elog": [
                    (error.algorithm, error.message, error.badness)
                    for error in member.elog.get_error_log()
                ],
                "history_stages": member.number_of_stages(),
                "history_nodes": str(member.get_nodes()),
            }
            for member in ensemble.member
        ],
    }


def _assert_ensemble_matches_snapshot(ensemble, snapshot):
    assert dict(ensemble) == snapshot["metadata"]
    assert ensemble.live == snapshot["live"]
    assert [
        (error.algorithm, error.message, error.badness)
        for error in ensemble.elog.get_error_log()
    ] == snapshot["elog"]
    assert len(ensemble.member) == len(snapshot["members"])
    for member, expected in zip(ensemble.member, snapshot["members"]):
        assert dict(member) == expected["metadata"]
        assert member.live == expected["live"]
        assert member.time_is_UTC() == expected["is_utc"]
        assert member.t0 == expected["t0"]
        assert member.dt == expected["dt"]
        assert member.npts == expected["npts"]
        np.testing.assert_array_equal(member.data, expected["data"])
        assert [
            (error.algorithm, error.message, error.badness)
            for error in member.elog.get_error_log()
        ] == expected["elog"]
        assert member.number_of_stages() == expected["history_stages"]
        assert str(member.get_nodes()) == expected["history_nodes"]


@pytest.mark.parametrize("target_dt", [None, 0.1])
def test_resample_ensemble_uses_sample_interval_semantics(target_dt):
    ensemble = _ensemble([(0.0, 200, 0.1, 1.0), (0.0, 100, 0.2, 2.0)])

    result = resample_ensemble(ensemble, target_dt)

    assert result is ensemble
    for member in result.member:
        assert member.live
        assert member.dt == pytest.approx(0.1)
        assert member.npts == 200
        assert member["sampling_rate"] == pytest.approx(10.0)


@pytest.mark.parametrize(
    "target_dt", [0.0, -0.1, float("nan"), float("inf"), -float("inf"), "0.1", True]
)
def test_resample_ensemble_rejects_invalid_dt_before_mutation(target_dt):
    ensemble = _ensemble([(0.0, 20, 0.1, 1.0), (0.0, 10, 0.2, 2.0)])
    before = _ensemble_snapshot(ensemble)

    with pytest.raises(ValueError, match="finite positive"):
        resample_ensemble(ensemble, target_dt)

    _assert_ensemble_matches_snapshot(ensemble, before)


def test_regularize_ensemble_uses_the_inclusive_common_intersection():
    ensemble = _ensemble([(0.0, 6, 1.0, 0.0), (2.0, 6, 1.0, 100.0)])

    result = regularize_ensemble(ensemble)

    assert result is ensemble
    assert [(member.t0, member.endtime(), member.npts) for member in result.member] == [
        (2.0, 5.0, 4),
        (2.0, 5.0, 4),
    ]
    np.testing.assert_array_equal(result.member[0].data, [2.0, 3.0, 4.0, 5.0])
    np.testing.assert_array_equal(result.member[1].data, [100.0, 101.0, 102.0, 103.0])


def test_regularize_ensemble_rejects_disjoint_intersection_before_mutation():
    ensemble = _ensemble([(0.0, 3, 1.0, 0.0), (3.0, 3, 1.0, 100.0)])
    before = _ensemble_snapshot(ensemble)

    with pytest.raises(ValueError, match="common time interval"):
        regularize_ensemble(ensemble)

    _assert_ensemble_matches_snapshot(ensemble, before)


def test_regularize_ensemble_preserves_a_shared_endpoint_sample():
    ensemble = _ensemble([(0.0, 3, 1.0, 0.0), (2.0, 3, 1.0, 100.0)])

    result = regularize_ensemble(ensemble)

    assert result is ensemble
    assert [(member.t0, member.endtime(), member.npts) for member in result.member] == [
        (2.0, 2.0, 1),
        (2.0, 2.0, 1),
    ]
    np.testing.assert_array_equal(result.member[0].data, [2.0])
    np.testing.assert_array_equal(result.member[1].data, [100.0])


def test_regularize_ensemble_ignores_and_preserves_dead_member_spans():
    ensemble = _ensemble(
        [
            (0.0, 6, 1.0, 0.0),
            (2.0, 6, 1.0, 100.0),
            (1000.0, 1, 10.0, 999.0),
        ]
    )
    ensemble.member[2].kill()
    dead_before = _ensemble_snapshot(ensemble)["members"][2]

    result = regularize_ensemble(ensemble)

    assert [
        (member.t0, member.endtime(), member.npts) for member in result.member[:2]
    ] == [
        (2.0, 5.0, 4),
        (2.0, 5.0, 4),
    ]
    dead = result.member[2]
    assert dict(dead) == dead_before["metadata"]
    assert dead.live == dead_before["live"]
    assert dead.t0 == dead_before["t0"]
    assert dead.dt == dead_before["dt"]
    assert dead.npts == dead_before["npts"]
    np.testing.assert_array_equal(dead.data, dead_before["data"])
    assert [
        (error.algorithm, error.message, error.badness)
        for error in dead.elog.get_error_log()
    ] == dead_before["elog"]


def test_regularize_ensemble_with_no_live_members_is_an_exact_noop():
    ensemble = _ensemble([(0.0, 3, 1.0, 0.0), (10.0, 2, 2.0, 100.0)])
    for member in ensemble.member:
        member.kill()
    before = _ensemble_snapshot(ensemble)

    assert regularize_ensemble(ensemble) is ensemble

    _assert_ensemble_matches_snapshot(ensemble, before)


def test_regularize_ensemble_snaps_off_grid_members_without_changing_samples():
    ensemble = _ensemble([(0.0, 6, 1.0, 0.0), (0.25, 6, 1.0, 100.0)])
    samples_before = [np.array(member.data, copy=True) for member in ensemble.member]

    result = regularize_ensemble(ensemble)

    assert result is ensemble
    assert [(member.t0, member.endtime(), member.npts) for member in result.member] == [
        (0.0, 5.0, 6),
        (0.0, 5.0, 6),
    ]
    for member, expected in zip(result.member, samples_before):
        np.testing.assert_array_equal(member.data, expected)


def test_regularize_ensemble_interpolates_off_grid_members_when_requested():
    ensemble = _ensemble([(0.0, 6, 1.0, 0.0), (0.25, 6, 1.0, 100.0)])

    result = regularize_ensemble(ensemble, grid_alignment="interpolate")

    assert [(member.t0, member.endtime(), member.npts) for member in result.member] == [
        (1.0, 5.0, 5),
        (1.0, 5.0, 5),
    ]
    np.testing.assert_allclose(result.member[0].data, [1.0, 2.0, 3.0, 4.0, 5.0])
    np.testing.assert_allclose(
        result.member[1].data, [100.75, 101.75, 102.75, 103.75, 104.75]
    )


def test_interpolation_preserves_a_near_grid_boundary_sample():
    ensemble = _ensemble([(0.0, 6, 0.1, 0.0), (0.30000000000000004, 3, 0.1, 100.0)])

    result = regularize_ensemble(ensemble, grid_alignment="interpolate")

    assert [member.npts for member in result.member] == [3, 3]
    assert [member.t0 for member in result.member] == pytest.approx([0.3, 0.3])
    np.testing.assert_allclose(result.member[0].data, [3.0, 4.0, 5.0])
    np.testing.assert_allclose(result.member[1].data, [100.0, 101.0, 102.0])


def test_regularize_ensemble_interpolates_three_component_members():
    ensemble = SeismogramEnsemble()
    for t0, offset in ((0.0, 0.0), (0.25, 100.0)):
        member = Seismogram(6)
        member.t0 = t0
        member.dt = 1.0
        member.set_live()
        for component in range(3):
            for sample in range(6):
                member.data[component, sample] = offset + 10 * component + sample
        ensemble.member.append(member)
    ensemble.set_live()

    result = regularize_ensemble(ensemble, grid_alignment="interpolate")

    assert [(member.t0, member.endtime(), member.npts) for member in result.member] == [
        (1.0, 5.0, 5),
        (1.0, 5.0, 5),
    ]
    np.testing.assert_allclose(
        result.member[0].data,
        [
            [1.0, 2.0, 3.0, 4.0, 5.0],
            [11.0, 12.0, 13.0, 14.0, 15.0],
            [21.0, 22.0, 23.0, 24.0, 25.0],
        ],
    )
    np.testing.assert_allclose(
        result.member[1].data,
        [
            [100.75, 101.75, 102.75, 103.75, 104.75],
            [110.75, 111.75, 112.75, 113.75, 114.75],
            [120.75, 121.75, 122.75, 123.75, 124.75],
        ],
    )


def test_regularize_ensemble_rejects_invalid_alignment_mode_before_mutation():
    ensemble = _ensemble([(0.0, 6, 1.0, 0.0), (0.25, 6, 1.0, 100.0)])
    before = _ensemble_snapshot(ensemble)

    with pytest.raises(ValueError, match="grid_alignment"):
        regularize_ensemble(ensemble, grid_alignment="invalid")

    _assert_ensemble_matches_snapshot(ensemble, before)


def test_regularize_ensemble_rejects_different_sample_intervals_atomically():
    ensemble = _ensemble([(0.0, 6, 1.0, 0.0), (0.25, 12, 0.5, 100.0)])
    before = _ensemble_snapshot(ensemble)

    with pytest.raises(ValueError, match="common sample interval"):
        regularize_ensemble(ensemble)

    _assert_ensemble_matches_snapshot(ensemble, before)


def test_custom_regularizer_retains_control_of_disjoint_members():
    ensemble = _ensemble([(0.0, 3, 1.0, 0.0), (4.0, 3, 1.0, 100.0)])
    members = list(ensemble.member)
    calls = []

    def regularizer(member):
        calls.append(member)
        return member

    result = regularize_ensemble(ensemble, regularizer=regularizer)

    assert result is ensemble
    assert calls == members
    assert list(result.member) == members


def _gather(is_utc, starttime_shift=None):
    metadata = {"is_utc": is_utc, "dt": 1.0, "label": "original"}
    if starttime_shift is not None:
        metadata["starttime_shift"] = starttime_shift
    member_metadata = pd.DataFrame(
        {
            "delta": [1.0, 1.0],
            "starttime": [10.0, 20.0],
            "is_live": [True, True],
            "marker": ["left", "right"],
        }
    )
    return Gather(
        capacity=2,
        size=2,
        npts=4,
        num_components=1,
        npartitions=1,
        member_metadata=member_metadata,
        ensemble_metadata=metadata,
        array_type="numpy",
    )


def _gather_snapshot(gather):
    return {
        "member_metadata": gather.member_metadata.copy(deep=True),
        "ensemble_metadata": deepcopy(dict(gather.ensemble_metadata())),
        "member_data": np.array(gather.member_data, copy=True),
        "fields": (
            gather.capacity,
            gather.size,
            gather.npts,
            gather.num_components,
            gather.array_type,
            gather.is_parallel,
            gather.is_compact,
            gather.npartitions,
        ),
        "elog": [
            (error.algorithm, error.message, error.badness)
            for error in gather.elog.get_error_log()
        ],
    }


def _assert_metadata_values_equal(actual, expected):
    assert actual.keys() == expected.keys()
    for key, expected_value in expected.items():
        actual_value = actual[key]
        if isinstance(expected_value, float) and math.isnan(expected_value):
            assert math.isnan(actual_value)
        else:
            assert actual_value == expected_value


def _assert_gather_matches_snapshot(gather, snapshot):
    pd.testing.assert_frame_equal(gather.member_metadata, snapshot["member_metadata"])
    _assert_metadata_values_equal(
        dict(gather.ensemble_metadata()), snapshot["ensemble_metadata"]
    )
    np.testing.assert_array_equal(gather.member_data, snapshot["member_data"])
    assert (
        gather.capacity,
        gather.size,
        gather.npts,
        gather.num_components,
        gather.array_type,
        gather.is_parallel,
        gather.is_compact,
        gather.npartitions,
    ) == snapshot["fields"]
    assert [
        (error.algorithm, error.message, error.badness)
        for error in gather.elog.get_error_log()
    ] == snapshot["elog"]


def test_ator_and_rtoa_are_exact_inverses_using_persisted_shift_metadata():
    gather = _gather(is_utc=True)
    original_starttimes = gather.member_metadata["starttime"].copy()

    assert gather.ator(2.5) is gather
    assert gather.member_metadata["starttime"].tolist() == [7.5, 17.5]
    assert gather.ensemble_metadata()["starttime_shift"] == 2.5
    assert gather.ensemble_metadata()["is_utc"] is False
    assert "t0shift" not in vars(gather)

    assert gather.rtoa() is gather
    pd.testing.assert_series_equal(
        gather.member_metadata["starttime"], original_starttimes
    )
    assert gather.ensemble_metadata()["starttime_shift"] == 0.0
    assert gather.ensemble_metadata()["is_utc"] is True
    assert "t0shift" not in vars(gather)


@pytest.mark.parametrize(
    "shift", [None, "2.5", float("nan"), float("inf"), -float("inf"), True]
)
def test_ator_rejects_invalid_shift_before_mutation(shift):
    gather = _gather(is_utc=True)
    before = _gather_snapshot(gather)

    with pytest.raises(ValueError, match="finite number"):
        gather.ator(shift)

    _assert_gather_matches_snapshot(gather, before)


def test_ator_is_identity_on_already_relative_input_without_validating_shift():
    gather = _gather(is_utc=False, starttime_shift=4.0)
    before = _gather_snapshot(gather)

    assert gather.ator("not a number") is gather
    _assert_gather_matches_snapshot(gather, before)


def test_rtoa_rejects_missing_shift_before_mutation():
    gather = _gather(is_utc=False)
    before = _gather_snapshot(gather)

    with pytest.raises(ValueError, match="does not define starttime_shift"):
        gather.rtoa()

    _assert_gather_matches_snapshot(gather, before)


@pytest.mark.parametrize(
    "shift", ["2.5", float("nan"), float("inf"), -float("inf"), True]
)
def test_rtoa_rejects_invalid_stored_shift_before_mutation(shift):
    gather = _gather(is_utc=False, starttime_shift=shift)
    before = _gather_snapshot(gather)

    with pytest.raises(ValueError, match="finite number"):
        gather.rtoa()

    _assert_gather_matches_snapshot(gather, before)


@pytest.mark.parametrize("stored_shift", [None, "invalid", float("nan")])
def test_rtoa_is_identity_on_already_utc_input_without_validating_stored_shift(
    stored_shift,
):
    gather = _gather(is_utc=True, starttime_shift=stored_shift)
    if stored_shift is None:
        gather.ensemble_metadata().pop("starttime_shift", None)
    before = _gather_snapshot(gather)

    assert gather.rtoa() is gather
    _assert_gather_matches_snapshot(gather, before)
