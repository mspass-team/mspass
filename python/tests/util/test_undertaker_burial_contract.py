import itertools

import pytest

from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.util.Undertaker import Undertaker


class _FakeDatabase:
    def __init__(self):
        self.elog_calls = []
        self.history_calls = []

    def _save_elog(self, datum, collection, data_tag=None):
        self.elog_calls.append(
            (datum["member_id"], collection, data_tag, datum.elog.size())
        )
        return "cemetery-{}".format(len(self.elog_calls))

    def _save_history(self, datum, alg_name):
        self.history_calls.append((datum["member_id"], alg_name))


class _SpyUndertaker(Undertaker):
    def bury(self, mspass_object, save_history=False, mummify_atomic_data=True):
        self.burial_calls.append((mspass_object, save_history, mummify_atomic_data))
        return super().bury(
            mspass_object,
            save_history=save_history,
            mummify_atomic_data=mummify_atomic_data,
        )


def _make_undertaker():
    undertaker = _SpyUndertaker.__new__(_SpyUndertaker)
    undertaker.db = _FakeDatabase()
    undertaker.regular_data_collection = "cemetery"
    undertaker.aborted_data_collection = "abortions"
    undertaker.data_tag = "undertaker-contract"
    undertaker.burial_calls = []
    return undertaker


_DEAD_INDEXES = {
    0: (),
    1: (1,),
    2: (1, 3),
    3: (0, 2, 4),
}


def _make_mixed_ensemble(dead_count):
    dead_indexes = _DEAD_INDEXES[dead_count]
    ensemble = TimeSeriesEnsemble()
    ensemble["ensemble_marker"] = "preserve-me"
    expected_samples = {}

    for member_id in range(5):
        datum = TimeSeries(3)
        datum.dt = 0.1
        datum.t0 = float(member_id)
        datum["member_id"] = member_id
        datum["is_abortion"] = False
        datum["member_marker"] = "member-{}".format(member_id)
        for sample_number in range(datum.npts):
            datum.data[sample_number] = 10.0 * member_id + sample_number
        datum.set_live()
        expected_samples[member_id] = list(datum.data)
        if member_id in dead_indexes:
            datum.elog.log_error(
                "test",
                "dead member {}".format(member_id),
                ErrorSeverity.Invalid,
            )
            datum.kill()
        ensemble.member.append(datum)

    ensemble.set_live()

    live_ids = [i for i in range(5) if i not in dead_indexes]
    return ensemble, live_ids, list(dead_indexes), expected_samples


_OPTION_PAIRS = list(itertools.product((False, True), repeat=2))


def _assert_burial_calls(
    undertaker, dead_ids, save_history, mummify_atomic_data, ensemble_calls
):
    atomic_calls = [
        call for call in undertaker.burial_calls if isinstance(call[0], TimeSeries)
    ]
    actual_ensemble_calls = [
        call
        for call in undertaker.burial_calls
        if isinstance(call[0], TimeSeriesEnsemble)
    ]

    assert len(actual_ensemble_calls) == ensemble_calls
    assert [call[1:] for call in actual_ensemble_calls] == [
        (save_history, mummify_atomic_data)
    ] * ensemble_calls
    assert [call[0]["member_id"] for call in atomic_calls] == dead_ids
    assert [call[1:] for call in atomic_calls] == [
        (save_history, mummify_atomic_data)
    ] * len(dead_ids)


def _assert_database_writes(undertaker, dead_ids, save_history):
    assert [call[0] for call in undertaker.db.elog_calls] == dead_ids
    assert all(
        call[1:] == ("cemetery", "undertaker-contract", 1)
        for call in undertaker.db.elog_calls
    )
    expected_history_ids = dead_ids if save_history else []
    assert [call[0] for call in undertaker.db.history_calls] == expected_history_ids
    assert all(call[1] == "Undertaker.bury" for call in undertaker.db.history_calls)


def _assert_live_members(ensemble, live_ids, expected_samples):
    assert ensemble.live
    assert ensemble["ensemble_marker"] == "preserve-me"
    assert [member["member_id"] for member in ensemble.member] == live_ids
    for member in ensemble.member:
        member_id = member["member_id"]
        assert member.live
        assert member["member_marker"] == "member-{}".format(member_id)
        assert list(member.data) == expected_samples[member_id]


@pytest.mark.parametrize("dead_count", (0, 1, 2, 3))
@pytest.mark.parametrize(("save_history", "mummify_atomic_data"), _OPTION_PAIRS)
def test_bury_ensemble_calls_atomic_path_once_per_dead_member(
    dead_count, save_history, mummify_atomic_data
):
    undertaker = _make_undertaker()
    ensemble, live_ids, dead_ids, expected_samples = _make_mixed_ensemble(dead_count)

    result = undertaker.bury(
        ensemble,
        save_history=save_history,
        mummify_atomic_data=mummify_atomic_data,
    )

    _assert_burial_calls(
        undertaker,
        dead_ids,
        save_history,
        mummify_atomic_data,
        ensemble_calls=1,
    )
    _assert_database_writes(undertaker, dead_ids, save_history)
    _assert_live_members(result, live_ids, expected_samples)
    for member in ensemble.member:
        if member["member_id"] in dead_ids:
            assert member.npts == (0 if mummify_atomic_data else 3)


@pytest.mark.parametrize("dead_count", (0, 1, 2, 3))
@pytest.mark.parametrize(("save_history", "mummify_atomic_data"), _OPTION_PAIRS)
def test_bring_out_your_dead_calls_atomic_path_once_per_dead_member(
    dead_count, save_history, mummify_atomic_data
):
    undertaker = _make_undertaker()
    ensemble, live_ids, dead_ids, expected_samples = _make_mixed_ensemble(dead_count)

    live_ensemble, bodies = undertaker.bring_out_your_dead(
        ensemble,
        bury=True,
        save_history=save_history,
        mummify_atomic_data=mummify_atomic_data,
    )

    _assert_burial_calls(
        undertaker,
        dead_ids,
        save_history,
        mummify_atomic_data,
        ensemble_calls=0,
    )
    _assert_database_writes(undertaker, dead_ids, save_history)
    _assert_live_members(live_ensemble, live_ids, expected_samples)
    assert [member["member_id"] for member in bodies.member] == dead_ids
    assert all(member.dead() for member in bodies.member)
    for member in ensemble.member:
        if member["member_id"] in dead_ids:
            assert member.npts == (0 if mummify_atomic_data else 3)


def test_is_abortion_logs_original_message_and_severity():
    undertaker = _make_undertaker()
    datum = TimeSeries(1)
    datum.set_live()
    datum.kill()
    expected_message = (
        "Undertaker._is_abortion: Warning:  dead datum has is_abortion attribute undefined - "
        "assumed False\nMsPASS readers should always set this attribute"
    )

    assert undertaker._is_abortion(datum) is False
    assert datum.elog.size() == 1
    entry = datum.elog.get_error_log()[0]
    assert entry.message == expected_message
    assert entry.badness == ErrorSeverity.Complaint
