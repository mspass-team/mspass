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
from mspasspy.db.matcher import css30_arrival_interval_matcher


class RecordingCollection:
    def __init__(self, documents=()):
        self.documents = copy.deepcopy(list(documents))
        self.count_queries = []
        self.find_one_queries = []
        self.find_queries = []

    @staticmethod
    def _matches(document, query):
        for key, expected in query.items():
            if isinstance(expected, dict):
                value = document.get(key)
                if not expected["$gte"] <= value <= expected["$lte"]:
                    return False
            elif document.get(key) != expected:
                return False
        return True

    def count_documents(self, query):
        self.count_queries.append(copy.deepcopy(query))
        return sum(self._matches(document, query) for document in self.documents)

    def find_one(self, query):
        self.find_one_queries.append(copy.deepcopy(query))
        for document in self.documents:
            if self._matches(document, query):
                return copy.deepcopy(document)
        return None

    def find(self, query):
        self.find_queries.append(copy.deepcopy(query))
        return [
            copy.deepcopy(document)
            for document in self.documents
            if self._matches(document, query)
        ]


class RecordingDatabase:
    def __init__(self, documents=(), collection="arrival"):
        self.collection_name = collection
        self.collection = RecordingCollection(documents)

    def __getitem__(self, collection):
        assert collection == self.collection_name
        return self.collection


def make_datum(starttime=100.0, npts=11, dt=1.0):
    datum = TimeSeries(npts)
    datum.t0 = starttime
    datum.dt = dt
    datum.set_live()
    return datum


def errors(datum):
    return list(datum.elog.get_error_log())


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


def test_contract_suite_loads_matcher_from_selected_build():
    _assert_module_from_selected_build(matcher_module, Path("mspasspy/db/matcher.py"))


def test_query_is_closed_waveform_interval_and_constructor_fields_exist():
    document = {"phase": "P", "time": 100.0}
    database = RecordingDatabase([document], collection="custom_arrival")
    matcher = css30_arrival_interval_matcher(
        database,
        startime_offset=3.0,
        attributes_to_load=[],
        load_if_defined=[],
        arrival_collection_name="custom_arrival",
    )
    datum = make_datum()

    assert matcher.get_document(datum) == document

    assert matcher.startime_offset == 3.0
    assert matcher.arrival_collection_name == "custom_arrival"
    assert matcher.collection == "custom_arrival"
    expected = {"phase": "P", "time": {"$gte": 100.0, "$lte": 110.0}}
    assert database.collection.count_queries == [expected]
    assert database.collection.find_one_queries == [expected]


def test_multiple_matches_select_nearest_to_start_plus_offset():
    documents = [
        {"phase": "P", "time": 101.0, "tag": "early"},
        {"phase": "P", "time": 104.0, "tag": "target"},
        {"phase": "P", "time": 109.0, "tag": "late"},
    ]
    database = RecordingDatabase(documents)
    matcher = css30_arrival_interval_matcher(
        database,
        startime_offset=4.0,
        attributes_to_load=[],
        load_if_defined=[],
    )

    assert matcher.get_document(make_datum())["tag"] == "target"

    assert len(database.collection.count_queries) == 1
    assert database.collection.find_one_queries == []
    assert len(database.collection.find_queries) == 1


@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_zero_match_logs_one_invalid_and_honors_kill(kill_on_failure):
    database = RecordingDatabase()
    matcher = css30_arrival_interval_matcher(
        database,
        attributes_to_load=[],
        load_if_defined=[],
        kill_on_failure=kill_on_failure,
    )
    datum = make_datum()

    assert matcher.normalize(datum) is datum

    log = errors(datum)
    assert len(log) == 1
    assert log[0].badness == ErrorSeverity.Invalid
    assert "arrival collection" in log[0].message
    assert datum.dead() is kill_on_failure


@pytest.mark.parametrize("kill_on_failure", [False, True])
@pytest.mark.parametrize("prepend", [False, True])
def test_required_fields_copy_or_log_exact_invalid(kill_on_failure, prepend):
    document = {"phase": "P", "time": 102.0, "required": 7}
    collection_name = "custom_arrival"
    database = RecordingDatabase([document], collection=collection_name)
    matcher = css30_arrival_interval_matcher(
        database,
        attributes_to_load=["required", "missing_required"],
        load_if_defined=[],
        kill_on_failure=kill_on_failure,
        prepend_collection_name=prepend,
        arrival_collection_name=collection_name,
    )
    datum = make_datum()

    assert matcher.normalize(datum) is datum

    output_key = "custom_arrival_required" if prepend else "required"
    missing_output_key = (
        "custom_arrival_missing_required" if prepend else "missing_required"
    )
    assert datum[output_key] == 7
    assert not datum.is_defined(missing_output_key)
    log = errors(datum)
    assert len(log) == 1
    assert log[0].badness == ErrorSeverity.Invalid
    assert "key=missing_required" in log[0].message
    assert "collection=custom_arrival" in log[0].message
    assert datum.dead() is kill_on_failure


@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("prepend", [False, True])
def test_optional_fields_copy_and_missing_are_informational_only(verbose, prepend):
    document = {"phase": "P", "time": 102.0, "present_optional": 9}
    collection_name = "custom_arrival"
    database = RecordingDatabase([document], collection=collection_name)
    matcher = css30_arrival_interval_matcher(
        database,
        attributes_to_load=[],
        load_if_defined=["present_optional", "missing_optional"],
        kill_on_failure=True,
        prepend_collection_name=prepend,
        verbose=verbose,
        arrival_collection_name=collection_name,
    )
    datum = make_datum()

    assert matcher.normalize(datum) is datum

    output_key = "custom_arrival_present_optional" if prepend else "present_optional"
    missing_output_key = (
        "custom_arrival_missing_optional" if prepend else "missing_optional"
    )
    assert datum[output_key] == 9
    assert not datum.is_defined(missing_output_key)
    log = errors(datum)
    assert len(log) == int(verbose)
    if verbose:
        assert log[0].badness == ErrorSeverity.Informational
        assert "optional load key=missing_optional" in log[0].message
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
    name = "test_css_arrival_matcher_" + uuid.uuid4().hex
    database = client[name]
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def test_real_mongo_zero_one_and_multiple_selection(mongo_database):
    matcher = css30_arrival_interval_matcher(
        mongo_database,
        startime_offset=4.0,
        attributes_to_load=[],
        load_if_defined=[],
    )

    assert matcher.get_document(make_datum()) is None

    one_id = mongo_database.arrival.insert_one(
        {"phase": "P", "time": 100.0}
    ).inserted_id
    assert matcher.get_document(make_datum())["_id"] == one_id

    mongo_database.arrival.delete_many({})
    upper_endpoint_id = mongo_database.arrival.insert_one(
        {"phase": "P", "time": 110.0}
    ).inserted_id
    assert matcher.get_document(make_datum())["_id"] == upper_endpoint_id

    mongo_database.arrival.delete_many({})

    target_id = mongo_database.arrival.insert_one(
        {"phase": "P", "time": 104.0}
    ).inserted_id
    mongo_database.arrival.insert_one({"phase": "P", "time": 109.0})
    assert matcher.get_document(make_datum())["_id"] == target_id
