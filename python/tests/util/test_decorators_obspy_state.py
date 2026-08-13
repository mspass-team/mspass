import copy

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.util.decorators import (
    seismogram_as_stream,
    seismogram_ensemble_as_stream,
    timeseries_as_trace,
    timeseries_ensemble_as_stream,
)

NSAMPLES = 128
DT = 0.05
UTC_START = 1_700_000_000.0
T0_SHIFT = UTC_START - 2.5
INTERPOLATE_TIME_SHIFT = 0.375
SIDE_CHANNEL_KEYS = {
    "_mspass_time_reference",
    "_mspass_t0_shift",
    "_mspass_relative_t0",
    "_mspass_live",
    "_mspass_member_index",
    "_mspass_component_index",
}


def _make_timeseries(relative=False, live=True, phase=0.0):
    data = TimeSeries(NSAMPLES)
    data.dt = DT
    data.t0 = UTC_START
    data.tref = TimeReferenceType.UTC
    data.set_live()
    for index in range(NSAMPLES):
        data.data[index] = np.sin(0.2 * index + phase) + 0.01 * index
    data["member_label"] = phase
    data["nested"] = {"values": [phase]}
    data["processing"] = []
    if relative:
        data.ator(T0_SHIFT)
        data["starttime_shift"] = T0_SHIFT
    if not live:
        data.kill()
    return data


def _make_seismogram(relative=False, live=True, phase=0.0):
    data = Seismogram(NSAMPLES)
    data.dt = DT
    data.t0 = UTC_START
    data.tref = TimeReferenceType.UTC
    data.set_live()
    for component in range(3):
        for index in range(NSAMPLES):
            data.data[component, index] = (
                np.sin(0.2 * index + phase + component) + 0.01 * index
            )
    data["member_label"] = phase
    data["nested"] = {"values": [phase]}
    data["processing"] = []
    if relative:
        data.ator(T0_SHIFT)
        data["starttime_shift"] = T0_SHIFT
    if not live:
        data.kill()
    return data


def _make_timeseries_ensemble():
    ensemble = TimeSeriesEnsemble()
    ensemble["ensemble_label"] = "timeseries"
    ensemble.member.append(_make_timeseries(phase=0.0))
    ensemble.member.append(_make_timeseries(relative=True, phase=0.5))
    ensemble.member.append(_make_timeseries(relative=True, live=False, phase=1.0))
    ensemble.set_live()
    return ensemble


def _make_seismogram_ensemble():
    ensemble = SeismogramEnsemble()
    ensemble["ensemble_label"] = "seismogram"
    ensemble.member.append(_make_seismogram(phase=0.0))
    ensemble.member.append(_make_seismogram(relative=True, phase=0.5))
    ensemble.member.append(_make_seismogram(relative=True, live=False, phase=1.0))
    ensemble.set_live()
    return ensemble


@timeseries_as_trace
@seismogram_as_stream
@timeseries_ensemble_as_stream
@seismogram_ensemble_as_stream
def _run_obspy_operation(data, operation):
    if operation == "detrend":
        return data.detrend("linear")
    if operation == "filter":
        return data.filter("lowpass", freq=2.0)
    if operation == "interpolate":
        return data.interpolate(
            40.0, method="linear", time_shift=INTERPOLATE_TIME_SHIFT
        )
    if operation == "resample":
        return data.resample(40.0)
    raise ValueError("unknown operation")


def _time_state(data):
    return {
        "tref": data.tref,
        "shifted": data.shifted(),
        "t0_shift": data.get_t0shift(),
        "shift_defined": data.is_defined("starttime_shift"),
        "shift_value": (
            data["starttime_shift"] if data.is_defined("starttime_shift") else None
        ),
        "live": data.live,
        "t0": data.t0,
    }


def _assert_time_state(data, state, t0_offset=0.0):
    assert data.tref == state["tref"]
    assert data.shifted() == state["shifted"]
    assert data.get_t0shift() == pytest.approx(state["t0_shift"])
    assert data.is_defined("starttime_shift") == state["shift_defined"]
    if state["shift_defined"]:
        assert data["starttime_shift"] == state["shift_value"]
    assert data.live == state["live"]
    assert data.t0 == pytest.approx(state["t0"] + t0_offset)

    if data.time_is_relative() and data.shifted():
        restored = type(data)(data)
        restored.set_live()
        restored.rtoa()
        assert restored.time_is_UTC()
        assert restored.t0 == pytest.approx(data.t0 + state["t0_shift"])


def _assert_core_state_is_coherent(data):
    samples = np.array(data.data)
    assert data.npts == samples.shape[-1]
    assert data["npts"] == data.npts
    assert data["delta"] == pytest.approx(data.dt)
    assert data["starttime"] == pytest.approx(data.t0)
    assert data["endtime"] == pytest.approx(data.endtime())
    if data.is_defined("sampling_rate"):
        assert data["sampling_rate"] == pytest.approx(1.0 / data.dt)
    assert SIDE_CHANNEL_KEYS.isdisjoint(data.keys())


@pytest.mark.parametrize("factory", [_make_timeseries, _make_seismogram])
@pytest.mark.parametrize("relative", [False, True])
@pytest.mark.parametrize("live", [False, True])
@pytest.mark.parametrize("operation", ["detrend", "filter", "interpolate", "resample"])
def test_atomic_obspy_copyback_preserves_time_and_life(
    factory, relative, live, operation
):
    data = factory(relative=relative, live=live)
    original_state = _time_state(data)
    original_samples = np.array(data.data)

    _run_obspy_operation(data, operation)

    t0_offset = INTERPOLATE_TIME_SHIFT if operation == "interpolate" else 0.0
    _assert_time_state(data, original_state, t0_offset=t0_offset)
    _assert_core_state_is_coherent(data)
    if operation in ("interpolate", "resample"):
        assert data.dt == pytest.approx(0.025)
        assert data.npts != NSAMPLES
    else:
        assert data.dt == pytest.approx(DT)
        assert data.npts == NSAMPLES
    assert not np.array_equal(np.array(data.data), original_samples)


@pytest.mark.parametrize("factory", [_make_timeseries, _make_seismogram])
def test_relative_t0_is_not_interpreted_as_an_epoch(factory):
    data = factory(relative=True)
    data.t0 = 1.0e12  # outside the datetime range accepted by UTCDateTime
    original_state = _time_state(data)

    _run_obspy_operation(data, "detrend")

    _assert_time_state(data, original_state)
    _assert_core_state_is_coherent(data)


@pytest.mark.parametrize(
    "factory", [_make_timeseries_ensemble, _make_seismogram_ensemble]
)
@pytest.mark.parametrize("operation", ["detrend", "filter", "interpolate", "resample"])
def test_ensemble_obspy_copyback_preserves_each_member(factory, operation):
    ensemble = factory()
    original_members = list(ensemble.member)
    original_states = [_time_state(member) for member in ensemble.member]
    original_labels = [member["member_label"] for member in ensemble.member]

    result = _run_obspy_operation(ensemble, operation)

    assert result is ensemble
    assert len(ensemble.member) == len(original_members)
    for index, member in enumerate(ensemble.member):
        assert member is original_members[index]
        t0_offset = INTERPOLATE_TIME_SHIFT if operation == "interpolate" else 0.0
        _assert_time_state(member, original_states[index], t0_offset=t0_offset)
        _assert_core_state_is_coherent(member)
        assert member["member_label"] == original_labels[index]
        if operation in ("interpolate", "resample"):
            assert member.dt == pytest.approx(0.025)
            assert member.npts != NSAMPLES


def _metadata_snapshot(data):
    return {key: copy.deepcopy(data[key]) for key in data.keys()}


def _atomic_snapshot(data):
    return {
        "metadata": _metadata_snapshot(data),
        "samples": np.array(data.data),
        "time": _time_state(data),
        "npts": data.npts,
        "dt": data.dt,
        "elog_size": data.elog.size(),
        "history_stages": data.number_of_stages(),
    }


def _ensemble_snapshot(ensemble):
    return {
        "metadata": _metadata_snapshot(ensemble),
        "live": ensemble.live,
        "members": [_atomic_snapshot(member) for member in ensemble.member],
    }


def _assert_atomic_unchanged(data, snapshot):
    assert _metadata_snapshot(data) == snapshot["metadata"]
    np.testing.assert_array_equal(np.array(data.data), snapshot["samples"])
    assert _time_state(data) == snapshot["time"]
    assert data.npts == snapshot["npts"]
    assert data.dt == snapshot["dt"]
    assert data.elog.size() == snapshot["elog_size"]
    assert data.number_of_stages() == snapshot["history_stages"]


def _assert_ensemble_unchanged(ensemble, snapshot):
    assert _metadata_snapshot(ensemble) == snapshot["metadata"]
    assert ensemble.live == snapshot["live"]
    assert len(ensemble.member) == len(snapshot["members"])
    for member, member_snapshot in zip(ensemble.member, snapshot["members"]):
        _assert_atomic_unchanged(member, member_snapshot)


@timeseries_ensemble_as_stream
def _change_timeseries_stream_size(stream, change):
    stream[0].data[0] += 100.0
    stream[0].stats.nested["values"].append("changed")
    if change == "shorter":
        stream.pop()
    elif change == "longer":
        stream.append(stream[-1].copy())
    elif change == "reordered":
        stream.traces.reverse()
    return stream


@seismogram_ensemble_as_stream
def _change_seismogram_stream_size(stream, change):
    stream[0].data[0] += 100.0
    stream[0].stats.nested["values"].append("changed")
    if change == "shorter":
        stream.pop()
    elif change == "longer":
        stream.append(stream[-1].copy())
    elif change == "reordered":
        stream.traces[0], stream.traces[1] = stream.traces[1], stream.traces[0]
    return stream


@timeseries_ensemble_as_stream
def _mismatch_second_stream(first, second):
    first[0].data[0] += 100.0
    first[0].stats.nested["values"].append("changed")
    second.pop()
    return first


@pytest.mark.parametrize(
    "factory,wrapped",
    [
        (_make_timeseries_ensemble, _change_timeseries_stream_size),
        (_make_seismogram_ensemble, _change_seismogram_stream_size),
    ],
)
@pytest.mark.parametrize("change", ["shorter", "longer", "reordered"])
def test_ensemble_stream_mismatch_is_atomic(factory, wrapped, change):
    ensemble = factory()
    snapshot = _ensemble_snapshot(ensemble)

    with pytest.raises(ValueError, match="Processed Stream"):
        wrapped(ensemble, change)

    _assert_ensemble_unchanged(ensemble, snapshot)


def test_all_streams_are_validated_before_any_copyback():
    first = _make_timeseries_ensemble()
    second = _make_timeseries_ensemble()
    first_snapshot = _ensemble_snapshot(first)
    second_snapshot = _ensemble_snapshot(second)

    with pytest.raises(ValueError, match="member count"):
        _mismatch_second_stream(first, second)

    _assert_ensemble_unchanged(first, first_snapshot)
    _assert_ensemble_unchanged(second, second_snapshot)
