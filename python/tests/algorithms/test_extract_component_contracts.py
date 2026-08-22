import numpy as np
import pytest

from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)


def _seismogram(marker, live=True):
    result = Seismogram(4)
    result["marker"] = marker
    for component in range(3):
        for sample in range(result.npts):
            result.data[component, sample] = marker * 100 + component * 10 + sample
    if live:
        result.set_live()
    else:
        result.kill()
    return result


def _timeseries(marker, live=True):
    result = TimeSeries(4)
    result["marker"] = marker
    for sample in range(result.npts):
        result.data[sample] = marker * 100 + sample
    if live:
        result.set_live()
    else:
        result.kill()
    return result


@pytest.mark.parametrize("live", [True, False])
def test_extract_component_returns_timeseries_for_live_and_dead_seismogram(live):
    source = _seismogram(1, live)

    result = ExtractComponent(source, 1)

    assert isinstance(result, TimeSeries)
    assert result.live is live
    if live:
        assert np.array_equal(np.asarray(result.data), [110, 111, 112, 113])
        assert result["marker"] == 1
    else:
        assert result.npts == 0


def test_extract_component_preserves_live_ensemble_and_member_states():
    source = SeismogramEnsemble()
    source["ensemble_marker"] = "source"
    source.member.append(_seismogram(1, True))
    source.member.append(_seismogram(2, False))
    source.member.append(_seismogram(3, True))
    source.set_live()

    result = ExtractComponent(source, 2)

    assert isinstance(result, TimeSeriesEnsemble)
    assert result.live
    assert result["ensemble_marker"] == "source"
    assert len(result.member) == 3
    assert [member.live for member in result.member] == [True, False, True]
    assert np.array_equal(np.asarray(result.member[0].data), [120, 121, 122, 123])
    assert result.member[1].npts == 0
    assert np.array_equal(np.asarray(result.member[2].data), [320, 321, 322, 323])
    assert [result.member[index]["marker"] for index in (0, 2)] == [1, 3]


def test_extract_component_short_circuits_dead_ensemble():
    source = SeismogramEnsemble()
    source["ensemble_marker"] = "source"
    source.member.append(_seismogram(1, True))
    source.member.append(_seismogram(2, False))
    source.kill()

    result = ExtractComponent(source, 2)

    assert isinstance(result, TimeSeriesEnsemble)
    assert result.dead()
    assert len(result.member) == 0
    assert source.dead()
    assert len(source.member) == 2
    assert [member.live for member in source.member] == [True, False]


def test_extract_component_rejects_timeseries_without_mutation():
    source = _timeseries(7, True)
    original_samples = np.asarray(source.data).copy()
    original_elog_size = source.elog.size()

    with pytest.raises(TypeError, match="only accepts Seismogram"):
        ExtractComponent(source, 0)

    assert source.live
    assert source.npts == 4
    assert source["marker"] == 7
    assert np.array_equal(np.asarray(source.data), original_samples)
    assert source.elog.size() == original_elog_size


def test_extract_component_rejects_timeseries_ensemble_without_mutation():
    source = TimeSeriesEnsemble()
    source["ensemble_marker"] = "unsupported"
    source.member.append(_timeseries(4, True))
    source.member.append(_timeseries(5, False))
    source.set_live()
    original_samples = [np.asarray(member.data).copy() for member in source.member]
    original_elog_sizes = [member.elog.size() for member in source.member]
    original_ensemble_elog_size = source.elog.size()

    with pytest.raises(TypeError, match="only accepts Seismogram"):
        ExtractComponent(source, 0)

    assert source.live
    assert source["ensemble_marker"] == "unsupported"
    assert len(source.member) == 2
    assert [member.live for member in source.member] == [True, False]
    assert [member["marker"] for member in source.member] == [4, 5]
    assert all(
        np.array_equal(np.asarray(member.data), expected)
        for member, expected in zip(source.member, original_samples)
    )
    assert [member.elog.size() for member in source.member] == original_elog_sizes
    assert source.elog.size() == original_ensemble_elog_size


def test_extract_component_rejects_python_object_without_mutation():
    class Unsupported:
        def __init__(self):
            self.values = [1, 2, 3]

    source = Unsupported()

    with pytest.raises(TypeError, match="only accepts Seismogram"):
        ExtractComponent(source, 0)

    assert source.values == [1, 2, 3]
