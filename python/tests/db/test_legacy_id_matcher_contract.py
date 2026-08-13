import os
from pathlib import Path
from unittest.mock import patch

import pytest

from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.db import matcher as matcher_module
from mspasspy.db.matcher import ID_matcher, NMF

SOURCE_PYTHON_ROOT = Path(
    os.environ.get("MSPASS_TEST_SOURCE_ROOT", Path(__file__).resolve().parents[2])
)


class ReturnProbe(NMF):
    def __init__(self, result):
        super().__init__()
        self.result = result
        self.normalize_calls = []

    def get_document(self, datum):
        return None

    def normalize(self, datum):
        self.normalize_calls.append(datum)
        return self.result


class FakeCollection:
    def __init__(self, documents):
        self.documents = documents
        self.queries = []

    def find_one(self, query):
        self.queries.append(query)
        return self.documents.get(query["_id"])


class FakeDatabase:
    def __init__(self, collection):
        self.collection = collection

    def __getitem__(self, name):
        assert name == "source"
        return self.collection


def _live_datum(source_id=None):
    datum = TimeSeries(1)
    datum.set_live()
    if source_id is not None:
        datum["source_id"] = source_id
    return datum


def test_contract_suite_loads_matcher_from_selected_worktree():
    expected = SOURCE_PYTHON_ROOT / "mspasspy/db/matcher.py"
    assert Path(matcher_module.__file__).resolve() == expected.resolve()


def test_nmf_callable_returns_the_distinct_normalize_result_once():
    datum = object()
    normalized = object()
    matcher = ReturnProbe(normalized)

    result = matcher(datum)

    assert result is normalized
    assert matcher.normalize_calls == [datum]


def test_callable_returns_the_normalize_result_and_invokes_it_once():
    collection = FakeCollection({1: {"_id": 1, "lat": 42.0}})
    matcher = ID_matcher(FakeDatabase(collection), "source", ["lat"])
    datum = _live_datum(1)

    with patch.object(matcher, "normalize", wraps=matcher.normalize) as normalize_spy:
        result = matcher(datum)

    assert result is datum
    normalize_spy.assert_called_once_with(datum)
    assert datum["lat"] == 42.0
    assert datum.elog.size() == 0
    assert collection.queries == [{"_id": 1}]


@pytest.mark.parametrize("kill_on_failure", [False, True])
@pytest.mark.parametrize("failure", ["missing", "unmatched"])
def test_callable_failures_log_once_and_honor_kill_policy(kill_on_failure, failure):
    collection = FakeCollection({})
    matcher = ID_matcher(
        FakeDatabase(collection),
        "source",
        ["lat"],
        kill_on_failure=kill_on_failure,
    )
    datum = _live_datum(None if failure == "missing" else 99)

    with patch.object(matcher, "normalize", wraps=matcher.normalize) as normalize_spy:
        result = matcher(datum)

    assert result is datum
    normalize_spy.assert_called_once_with(datum)
    errors = datum.elog.get_error_log()
    assert len(errors) == 1
    assert errors[0].algorithm == "ID_matcher"
    assert errors[0].badness == ErrorSeverity.Invalid
    if failure == "missing":
        assert "is not defined" in errors[0].message
        assert collection.queries == []
    else:
        assert "No matching _id" in errors[0].message
        assert collection.queries == [{"_id": 99}]
    assert datum.dead() is kill_on_failure


@pytest.mark.parametrize("kill_on_failure", [False, True])
@pytest.mark.parametrize("failure", ["missing", "unmatched"])
def test_normalize_failures_have_one_invalid_log_and_exact_life_state(
    kill_on_failure, failure
):
    collection = FakeCollection({})
    matcher = ID_matcher(
        FakeDatabase(collection),
        "source",
        ["lat"],
        kill_on_failure=kill_on_failure,
    )
    datum = _live_datum(None if failure == "missing" else 99)

    result = matcher.normalize(datum)

    assert result is datum
    errors = datum.elog.get_error_log()
    assert len(errors) == 1
    assert errors[0].badness == ErrorSeverity.Invalid
    assert datum.dead() is kill_on_failure
    assert collection.queries == ([] if failure == "missing" else [{"_id": 99}])
