import gc
import pickle

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import AtomicType, ErrorSeverity


@pytest.mark.parametrize(
    "ensemble_type,member_type",
    [
        (TimeSeriesEnsemble, TimeSeries),
        (SeismogramEnsemble, Seismogram),
    ],
)
@pytest.mark.parametrize("protocol", [4, 5])
def test_ensemble_pickle_direct_members_and_legacy_state(
    ensemble_type, member_type, protocol
):
    atomic_type = (
        AtomicType.TIMESERIES if member_type is TimeSeries else AtomicType.SEISMOGRAM
    )
    ensemble = ensemble_type(3)
    ensemble["pickle_format"] = "direct-members"
    ensemble.elog.log_error(
        "test_ensemble_pickle",
        "preserve ensemble error log",
        ErrorSeverity.Complaint,
    )
    for index in range(3):
        member = member_type(8)
        member["member_index"] = index
        member.dt = 0.05
        member.t0 = float(index)
        member.tref = TimeReferenceType.UTC
        np.asarray(member.data)[...] = index + 0.5
        member.set_as_origin(
            "native-ensemble-pickle", "1", f"member-{index}", atomic_type
        )
        member.elog.log_error(
            "native-ensemble-pickle",
            f"member error {index}",
            ErrorSeverity.Complaint,
        )
        member.set_live()
        if index == 1:
            member.kill()
        ensemble.member.append(member)
    ensemble.set_live()

    state = ensemble.__getstate__()
    assert len(state) == 4
    assert isinstance(state[3], list)
    assert [member["member_index"] for member in state[3]] == [0, 1, 2]

    # The returned state owns an independent member snapshot.  References
    # into the source vector would become invalid when the vector reallocates.
    np.asarray(state[3][0].data)[...] = 17.5
    assert np.all(np.asarray(ensemble.member[0].data) == 0.5)
    for _ in range(8):
        ensemble.member.append(member_type(1))
    assert np.all(np.asarray(state[3][0].data) == 17.5)
    for _ in range(8):
        ensemble.member.pop()

    restored = pickle.loads(pickle.dumps(ensemble, protocol=protocol))
    _assert_ensemble_state(restored, ensemble, ensemble_type)

    legacy_state = list(ensemble.__getstate__())
    legacy_state[3] = pickle.dumps(legacy_state[3], protocol=protocol)
    legacy = ensemble_type.__new__(ensemble_type)
    legacy.__setstate__(tuple(legacy_state))
    _assert_ensemble_state(legacy, ensemble, ensemble_type)

    consumed_state = ensemble.__getstate__()
    external_member_alias = consumed_state[3][0]
    consumed = ensemble_type.__new__(ensemble_type)
    consumed.__setstate__(consumed_state)
    assert consumed_state[3] == [None, None, None]
    assert external_member_alias["member_index"] == 0
    assert np.all(np.asarray(external_member_alias.data) == 0.5)

    lifetime_state = ensemble.__getstate__()
    del ensemble
    gc.collect()
    after_source_collection = ensemble_type.__new__(ensemble_type)
    after_source_collection.__setstate__(lifetime_state)
    assert [member["member_index"] for member in after_source_collection.member] == [
        0,
        1,
        2,
    ]


def _assert_ensemble_state(actual, expected, ensemble_type):
    assert type(actual) is ensemble_type
    assert actual.live
    assert actual["pickle_format"] == "direct-members"
    assert actual.elog.size() == 1
    assert [member.live for member in actual.member] == [True, False, True]
    for index, member in enumerate(actual.member):
        expected_member = expected.member[index]
        assert member["member_index"] == index
        assert member.dt == 0.05
        assert member.t0 == float(index)
        assert member.tref == TimeReferenceType.UTC
        assert member.number_of_stages() == expected_member.number_of_stages()
        assert member.current_nodedata().uuid == expected_member.current_nodedata().uuid
        assert member.elog.size() == 1
        assert np.all(np.asarray(member.data) == index + 0.5)


@pytest.mark.parametrize(
    "ensemble_type,other_member_type",
    [
        (TimeSeriesEnsemble, Seismogram),
        (SeismogramEnsemble, TimeSeries),
    ],
)
def test_ensemble_pickle_rejects_invalid_member_payload(
    ensemble_type, other_member_type
):
    ensemble = ensemble_type()
    state = list(ensemble.__getstate__())

    for invalid_payload in (None, (), {}, [other_member_type()]):
        malformed = ensemble_type.__new__(ensemble_type)
        state[3] = invalid_payload
        with pytest.raises(ValueError, match="Invalid .*Ensemble pickle state"):
            malformed.__setstate__(tuple(state))

    empty = ensemble_type.__new__(ensemble_type)
    state[3] = []
    empty.__setstate__(tuple(state))
    assert empty.dead()
    assert len(empty.member) == 0
