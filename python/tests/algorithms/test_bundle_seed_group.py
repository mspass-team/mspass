from unittest.mock import Mock

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.algorithms import bundle


class _CountingList(list):
    def __init__(self, values):
        super().__init__(values)
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        return super().__iter__()


class _CountingTuple(tuple):
    def __new__(cls, values):
        return super().__new__(cls, values)

    def __init__(self, values):
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        return super().__iter__()


def _members():
    result = [TimeSeries(2), TimeSeries(2), TimeSeries(2)]
    for index, member in enumerate(result):
        member["member_index"] = index
    return result


def _bundle_members():
    result = []
    for channel, hang, vang, fill in (
        ("HHE", 90.0, 90.0, 1.0),
        ("HHN", 0.0, 90.0, 2.0),
        ("HHZ", 0.0, 0.0, 3.0),
    ):
        member = TimeSeries(4)
        member.t0 = 0.0
        member.dt = 1.0
        member.tref = TimeReferenceType.UTC
        member.force_t0_shift(0.0)
        member.set_live()
        for sample in range(member.npts):
            member.data[sample] = fill
        member["net"] = "XX"
        member["sta"] = "TEST"
        member["loc"] = ""
        member["chan"] = channel
        member["channel_hang"] = hang
        member["channel_vang"] = vang
        result.append(member)
    return result


def _assert_same_seismogram(actual, expected):
    assert actual.live == expected.live
    assert actual.npts == expected.npts
    assert actual.t0 == expected.t0
    assert actual.dt == expected.dt
    assert actual.tref == expected.tref
    assert actual.cardinal() == expected.cardinal()
    assert actual.orthogonal() == expected.orthogonal()
    assert {key: actual[key] for key in actual.keys()} == {
        key: expected[key] for key in expected.keys()
    }
    np.testing.assert_array_equal(actual.data, expected.data)
    np.testing.assert_array_equal(actual.tmatrix, expected.tmatrix)


@pytest.mark.parametrize("container_type", [list, tuple, TimeSeriesEnsemble])
def test_bundle_seed_group_accepts_documented_containers(monkeypatch, container_type):
    members = _members()
    if container_type is TimeSeriesEnsemble:
        container = TimeSeriesEnsemble()
        for member in members:
            container.member.append(member)
    else:
        container = container_type(members)
    expected = Seismogram(2)
    core = Mock(return_value=expected)
    monkeypatch.setattr(bundle, "_BundleSEEDGroup", core)

    result = bundle.BundleSEEDGroup(container, 0, 2)

    assert result is expected
    core.assert_called_once()
    sequence, i0, iend = core.call_args.args
    assert [member["member_index"] for member in sequence] == [0, 1, 2]
    if container_type is not TimeSeriesEnsemble:
        assert all(actual is original for actual, original in zip(sequence, members))
    assert (i0, iend) == (0, 2)


@pytest.mark.parametrize("container_type", [_CountingList, _CountingTuple])
def test_bundle_seed_group_materializes_sequences_once(monkeypatch, container_type):
    container = container_type(_members())
    core = Mock(return_value=Seismogram(2))
    monkeypatch.setattr(bundle, "_BundleSEEDGroup", core)

    bundle.BundleSEEDGroup(container, 0, 2)

    assert container.iterations == 1
    core.assert_called_once()


def test_bundle_seed_group_accepts_single_element_index_range(monkeypatch):
    core = Mock(return_value=Seismogram(2))
    monkeypatch.setattr(bundle, "_BundleSEEDGroup", core)

    bundle.BundleSEEDGroup([TimeSeries(2)], 0, 0)

    core.assert_called_once()


def test_bundle_seed_group_documented_containers_have_identical_core_output():
    members = _bundle_members()
    ensemble = TimeSeriesEnsemble()
    for member in members:
        ensemble.member.append(member)

    results = [
        bundle.BundleSEEDGroup(members, 0, 2),
        bundle.BundleSEEDGroup(tuple(members), 0, 2),
        bundle.BundleSEEDGroup(ensemble, 0, 2),
    ]

    expected = results[0]
    assert expected.live
    assert expected.npts == 4
    np.testing.assert_array_equal(
        expected.data, np.array([[1.0] * 4, [2.0] * 4, [3.0] * 4])
    )
    for result in results[1:]:
        _assert_same_seismogram(result, expected)


@pytest.mark.parametrize(
    "container",
    [
        pytest.param((member for member in _members()), id="generator"),
        pytest.param(set(), id="set"),
        pytest.param(None, id="none"),
    ],
)
def test_bundle_seed_group_rejects_undocumented_containers(monkeypatch, container):
    core = Mock()
    monkeypatch.setattr(bundle, "_BundleSEEDGroup", core)

    with pytest.raises(TypeError, match="TimeSeriesEnsemble, list, or tuple"):
        bundle.BundleSEEDGroup(container)

    core.assert_not_called()


def test_bundle_seed_group_rejects_non_timeseries_elements(monkeypatch):
    core = Mock()
    monkeypatch.setattr(bundle, "_BundleSEEDGroup", core)

    with pytest.raises(TypeError, match="every input element"):
        bundle.BundleSEEDGroup([TimeSeries(2), object(), TimeSeries(2)])

    core.assert_not_called()


@pytest.mark.parametrize(
    "sequence,i0,iend",
    [
        ([], 0, 2),
        (_members(), -1, 2),
        (_members(), 2, 1),
        (_members(), 0, 3),
        (_members(), 0.0, 2),
        (_members(), 0, "2"),
        (_members(), True, 2),
        (_members(), 0, False),
    ],
)
def test_bundle_seed_group_rejects_invalid_indices(monkeypatch, sequence, i0, iend):
    core = Mock()
    monkeypatch.setattr(bundle, "_BundleSEEDGroup", core)

    with pytest.raises(ValueError, match="i0 and iend|indices must satisfy"):
        bundle.BundleSEEDGroup(sequence, i0, iend)

    core.assert_not_called()
