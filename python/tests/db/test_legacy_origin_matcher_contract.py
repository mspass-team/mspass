import copy
import os
import subprocess
import uuid
from importlib.metadata import distribution, version
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.db import matcher as matcher_module
from mspasspy.db.matcher import origin_time_source_matcher


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


class RecordingCollection:
    def __init__(self, documents=()):
        self.documents = copy.deepcopy(list(documents))
        self.count_queries = []
        self.find_queries = []

    @staticmethod
    def _matches(document, query):
        interval = query["time"]
        value = document["time"]
        return interval["$gte"] <= value <= interval["$lte"]

    def count_documents(self, query):
        self.count_queries.append(copy.deepcopy(query))
        return sum(self._matches(document, query) for document in self.documents)

    def find_one(self, query):
        self.find_queries.append(copy.deepcopy(query))
        for document in self.documents:
            if self._matches(document, query):
                return copy.deepcopy(document)
        return None


class RecordingDatabase:
    def __init__(self, documents=()):
        self.source = RecordingCollection(documents)

    def __getitem__(self, collection):
        assert collection == "source"
        return self.source


def make_datum(starttime=100.0):
    datum = TimeSeries(1)
    datum.t0 = starttime
    datum.set_live()
    return datum


def error_log(datum):
    return list(datum.elog.get_error_log())


def test_contract_suite_loads_matcher_from_selected_build():
    _assert_module_from_selected_build(matcher_module, Path("mspasspy/db/matcher.py"))


def test_query_uses_exact_inclusive_mongo_operators_and_offsets():
    database = RecordingDatabase([{"time": 100.0}])
    matcher = origin_time_source_matcher(
        database,
        t0offset=2.0,
        tolerance=3.0,
        attributes_to_load=[],
        verbose=False,
    )
    datum = make_datum(105.0)

    assert matcher.get_document(datum) == {"time": 100.0}

    expected = {"time": {"$gte": 100.0, "$lte": 106.0}}
    assert database.source.count_queries == [expected]
    assert database.source.find_queries == [expected]


@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_zero_match_normalize_logs_once_and_honors_kill(kill_on_failure):
    database = RecordingDatabase()
    matcher = origin_time_source_matcher(
        database,
        attributes_to_load=[],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )
    datum = make_datum()

    assert matcher.get_document(make_datum()) is None
    assert matcher.normalize(datum) is datum

    errors = error_log(datum)
    assert len(errors) == 1
    assert errors[0].badness == ErrorSeverity.Invalid
    assert "No matching document was found" in errors[0].message
    assert datum.dead() is kill_on_failure
    assert len(database.source.count_queries) == 2
    assert database.source.find_queries == []


def test_one_match_returns_exact_document_once():
    document = {"time": 99.0, "lat": 1.0}
    database = RecordingDatabase([document])
    matcher = origin_time_source_matcher(
        database,
        tolerance=1.0,
        attributes_to_load=[],
        verbose=True,
    )
    datum = make_datum(100.0)

    assert matcher.get_document(datum) == document

    assert len(database.source.count_queries) == 1
    assert len(database.source.find_queries) == 1
    assert error_log(datum) == []
    assert datum.live


@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_multiple_matches_select_once_and_complain_without_killing(
    verbose, kill_on_failure
):
    documents = [{"time": 99.0, "tag": "first"}, {"time": 101.0, "tag": "second"}]
    database = RecordingDatabase(documents)
    matcher = origin_time_source_matcher(
        database,
        tolerance=2.0,
        attributes_to_load=[],
        kill_on_failure=kill_on_failure,
        verbose=verbose,
    )
    datum = make_datum(100.0)

    assert matcher.get_document(datum) == documents[0]

    assert len(database.source.count_queries) == 1
    assert len(database.source.find_queries) == 1
    errors = error_log(datum)
    assert len(errors) == int(verbose)
    if verbose:
        assert errors[0].badness == ErrorSeverity.Complaint
        assert "multiple source documents" in errors[0].message
    assert datum.live


@pytest.fixture
def mongo_database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError as error:
        client.close()
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    name = "test_legacy_origin_matcher_" + uuid.uuid4().hex
    database = client[name]
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def test_real_mongo_zero_one_and_multiple_matches(mongo_database):
    matcher = origin_time_source_matcher(
        mongo_database,
        tolerance=1.0,
        attributes_to_load=[],
        kill_on_failure=False,
        verbose=True,
    )

    zero = make_datum(100.0)
    assert matcher.get_document(zero) is None
    assert zero.elog.size() == 0

    first_id = mongo_database.source.insert_one(
        {"time": 99.0, "tag": "first"}
    ).inserted_id
    one = make_datum(100.0)
    result = matcher.get_document(one)
    assert result["_id"] == first_id
    assert one.elog.size() == 0

    second_id = mongo_database.source.insert_one(
        {"time": 101.0, "tag": "second"}
    ).inserted_id
    multiple = make_datum(100.0)
    result = matcher.get_document(multiple)
    assert result["_id"] in {first_id, second_id}
    errors = error_log(multiple)
    assert len(errors) == 1
    assert errors[0].badness == ErrorSeverity.Complaint
    assert multiple.live
