import copy

import pytest

from mspasspy.ccore.seismic import (
    Seismogram,
    SeismogramEnsemble,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
import mspasspy.db.normalize as normalize_module
from mspasspy.db.normalize import ArrivalDBMatcher


class FakeCursor(list):
    def close(self):
        self.closed = True


class FakeCollection:
    def __init__(self, documents):
        self.documents = copy.deepcopy(documents)
        self.count_queries = []
        self.find_queries = []

    @staticmethod
    def _matches(document, query):
        for key, expected in query.items():
            if isinstance(expected, dict):
                value = document.get(key)
                if value is None:
                    return False
                if "$gte" in expected and value < expected["$gte"]:
                    return False
                if "$lte" in expected and value > expected["$lte"]:
                    return False
            elif document.get(key) != expected:
                return False
        return True

    def _matching_documents(self, query):
        return [doc for doc in self.documents if self._matches(doc, query)]

    def count_documents(self, query):
        self.count_queries.append(copy.deepcopy(query))
        return len(self._matching_documents(query))

    def find(self, query):
        self.find_queries.append(copy.deepcopy(query))
        return FakeCursor(copy.deepcopy(self._matching_documents(query)))


@pytest.fixture(autouse=True)
def patch_database_matcher_constructor(monkeypatch):
    def initialize(
        instance,
        db,
        collection,
        attributes_to_load=None,
        load_if_defined=None,
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=False,
    ):
        instance.dbhandle = db
        instance.collection = collection
        instance.attributes_to_load = list(attributes_to_load or [])
        instance.load_if_defined = list(load_if_defined or [])
        instance.aliases = dict(aliases or {})
        instance.require_unique_match = require_unique_match
        instance.prepend_collection_name = prepend_collection_name

    monkeypatch.setattr(normalize_module.DatabaseMatcher, "__init__", initialize)


def _ensemble(ensemble_class, *, start_key="starttime", end_key="endtime"):
    ensemble = ensemble_class()
    member = TimeSeries(1) if ensemble_class is TimeSeriesEnsemble else Seismogram(1)
    member.set_live()
    ensemble.member.append(member)
    ensemble[start_key] = 100.0
    ensemble[end_key] = 110.0
    ensemble["sta"] = "AAA"
    ensemble["net"] = "XX"
    ensemble.set_live()
    return ensemble


def _atomic(ensemble_class):
    datum = TimeSeries(11) if ensemble_class is TimeSeriesEnsemble else Seismogram(11)
    datum.t0 = 100.0
    datum.dt = 1.0
    datum["sta"] = "AAA"
    datum["net"] = "XX"
    datum.set_live()
    return datum


def _ensemble_state(ensemble):
    return {
        "metadata": dict(ensemble),
        "dead": ensemble.dead(),
        "member_metadata": [dict(member) for member in ensemble.member],
        "member_dead": [member.dead() for member in ensemble.member],
        "member_sample_state": [
            (member.npts, member.t0, member.dt) for member in ensemble.member
        ],
    }


@pytest.mark.parametrize("ensemble_class", [TimeSeriesEnsemble, SeismogramEnsemble])
def test_default_and_custom_interval_queries_are_exact_and_inclusive(ensemble_class):
    collection = FakeCollection([])
    default_matcher = ArrivalDBMatcher(collection, query={"phase": "P"})
    default_ensemble = _ensemble(ensemble_class)
    original_predicate = copy.deepcopy(default_matcher.query)
    default_state = _ensemble_state(default_ensemble)

    assert default_matcher.query_generator(default_ensemble) == {
        "phase": "P",
        "time": {"$gte": 100.0, "$lte": 110.0},
        "sta": "AAA",
        "net": "XX",
    }
    assert default_matcher.query == original_predicate
    assert _ensemble_state(default_ensemble) == default_state

    custom_matcher = ArrivalDBMatcher(
        collection,
        ensemble_starttime_key="window_open",
        ensemble_endtime_key="window_close",
    )
    custom_ensemble = _ensemble(
        ensemble_class, start_key="window_open", end_key="window_close"
    )
    custom_ensemble.erase("net")
    custom_state = _ensemble_state(custom_ensemble)
    assert custom_matcher.query_generator(custom_ensemble) == {
        "time": {"$gte": 100.0, "$lte": 110.0},
        "sta": "AAA",
    }
    assert _ensemble_state(custom_ensemble) == custom_state


@pytest.mark.parametrize("ensemble_class", [TimeSeriesEnsemble, SeismogramEnsemble])
@pytest.mark.parametrize("require_unique_match", [False, True])
@pytest.mark.parametrize("match_count", [0, 1, 2])
def test_find_one_uses_the_atomic_database_matcher_contract(
    ensemble_class, require_unique_match, match_count
):
    documents = [
        {"phase": "P", "time": 100.0, "sta": "AAA", "net": "XX"},
        {"phase": "S", "time": 110.0, "sta": "AAA", "net": "XX"},
    ][:match_count]
    collection = FakeCollection(documents)
    matcher = ArrivalDBMatcher(
        collection,
        require_unique_match=require_unique_match,
        prepend_collection_name=True,
    )
    ensemble = _ensemble(ensemble_class)
    ensemble_state = _ensemble_state(ensemble)
    matcher_query = copy.deepcopy(matcher.query)

    if match_count == 2 and require_unique_match:
        with pytest.raises(MsPASSError) as exc:
            matcher.find_one(ensemble)
        assert exc.value.severity == ErrorSeverity.Fatal
    else:
        result, elog = matcher.find_one(ensemble)
        if match_count == 0:
            assert result is None
            assert elog is not None
        else:
            assert dict(result) == {
                "arrival_phase": documents[0]["phase"],
                "arrival_time": documents[0]["time"],
            }
            assert (elog is not None) == (match_count == 2)

    expected_query = {
        "time": {"$gte": 100.0, "$lte": 110.0},
        "sta": "AAA",
        "net": "XX",
    }
    assert collection.count_queries == [expected_query]
    assert collection.find_queries == ([] if match_count == 0 else [expected_query])
    assert _ensemble_state(ensemble) == ensemble_state
    assert matcher.query == matcher_query


@pytest.mark.parametrize("ensemble_class", [TimeSeriesEnsemble, SeismogramEnsemble])
def test_ensemble_and_atomic_paths_return_identical_output_mapping(ensemble_class):
    documents = [{"phase": "P", "time": 105.0, "sta": "AAA", "net": "XX"}]
    collection = FakeCollection(documents)
    matcher = ArrivalDBMatcher(collection, prepend_collection_name=True)
    ensemble = _ensemble(ensemble_class)
    atomic = _atomic(ensemble_class)
    ensemble_state = _ensemble_state(ensemble)
    atomic_state = dict(atomic)

    ensemble_result, ensemble_elog = matcher.find_one(ensemble)
    atomic_result, atomic_elog = matcher.find_one(atomic)

    assert dict(ensemble_result) == dict(atomic_result)
    assert dict(ensemble_result) == {"arrival_phase": "P", "arrival_time": 105.0}
    assert ensemble_elog is None
    assert atomic_elog is None
    assert _ensemble_state(ensemble) == ensemble_state
    assert dict(atomic) == atomic_state


@pytest.mark.parametrize("ensemble_class", [TimeSeriesEnsemble, SeismogramEnsemble])
@pytest.mark.parametrize(
    "invalid_state", ["dead", "empty", "missing_start", "missing_end", "missing_sta"]
)
def test_invalid_ensembles_return_before_any_database_query(
    ensemble_class, invalid_state
):
    collection = FakeCollection(
        [{"phase": "P", "time": 105.0, "sta": "AAA", "net": "XX"}]
    )
    matcher = ArrivalDBMatcher(collection)
    ensemble = _ensemble(ensemble_class)
    if invalid_state == "dead":
        ensemble.kill()
    elif invalid_state == "empty":
        ensemble.member.clear()
    elif invalid_state == "missing_start":
        ensemble.erase("starttime")
    elif invalid_state == "missing_end":
        ensemble.erase("endtime")
    else:
        ensemble.erase("sta")
    ensemble_state = _ensemble_state(ensemble)

    result, _ = matcher.find(ensemble)

    assert result is None
    assert matcher.query_generator(ensemble) is None
    assert collection.count_queries == []
    assert collection.find_queries == []
    assert _ensemble_state(ensemble) == ensemble_state
