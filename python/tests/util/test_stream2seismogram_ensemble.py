import numpy as np
import obspy
import pytest

from mspasspy.ccore.seismic import SeismogramEnsemble
from mspasspy.util import converter


def _stream(group_count):
    result = obspy.Stream()
    channels = ("E", "N", "Z")
    for group in range(group_count):
        for component, channel in enumerate(channels):
            trace = obspy.Trace(
                data=np.full(4, group * 10 + component + 1, dtype=np.float64)
            )
            trace.stats.station = f"STA{group}"
            trace.stats.channel = channel
            trace.stats.delta = 0.1
            result.append(trace)
    return result


@pytest.mark.parametrize("group_count", [0, 1, 2])
def test_stream2seismogramensemble_converts_complete_groups_in_order(group_count):
    result = converter.Stream2SeismogramEnsemble(_stream(group_count))

    assert isinstance(result, SeismogramEnsemble)
    assert len(result.member) == group_count
    assert result.live is (group_count > 0)
    assert [member["sta"] for member in result.member] == [
        f"STA{group}" for group in range(group_count)
    ]
    assert all(member.live for member in result.member)
    for group, member in enumerate(result.member):
        expected = np.array(
            [
                [group * 10 + 1] * 4,
                [group * 10 + 2] * 4,
                [group * 10 + 3] * 4,
            ]
        )
        assert np.array_equal(np.asarray(member.data), expected)


def test_stream2seismogramensemble_rejects_incomplete_group_before_side_effects(
    monkeypatch,
):
    stream = _stream(1)
    stream.append(obspy.Trace(data=np.arange(4, dtype=np.float64)))
    calls = {"constructor": 0, "metadata": 0}

    def unexpected_constructor(*args, **kwargs):
        calls["constructor"] += 1
        raise AssertionError("conversion must not start")

    def unexpected_metadata(*args, **kwargs):
        calls["metadata"] += 1
        raise AssertionError("metadata posting must not start")

    monkeypatch.setattr(converter, "Stream2Seismogram", unexpected_constructor)
    monkeypatch.setattr(converter, "post_ensemble_metadata", unexpected_metadata)

    with pytest.raises(ValueError, match="divisible by 3"):
        converter.Stream2SeismogramEnsemble(stream)

    assert calls == {"constructor": 0, "metadata": 0}
