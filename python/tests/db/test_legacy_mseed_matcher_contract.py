import copy
import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import pytest

from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.db import matcher as matcher_module
from mspasspy.db.matcher import mseed_channel_matcher, mseed_site_matcher


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


class CountingTimeSeries(TimeSeries):
    def __init__(self):
        super().__init__(1)
        self.kill_calls = 0

    def kill(self):
        self.kill_calls += 1
        return super().kill()


class RecordingCollection:
    def __init__(self, documents):
        self.documents = copy.deepcopy(documents)
        self.count_queries = []
        self.find_queries = []

    @staticmethod
    def _matches(document, query):
        for key, expected in query.items():
            if isinstance(expected, dict):
                value = document.get(key)
                if "$lt" in expected and not value < expected["$lt"]:
                    return False
                if "$gt" in expected and not value > expected["$gt"]:
                    return False
            elif document.get(key) != expected:
                return False
        return True

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
    def __init__(self, channel=(), site=()):
        self.collections = {
            "channel": RecordingCollection(channel),
            "site": RecordingCollection(site),
        }

    def __getitem__(self, collection):
        return self.collections[collection]


def make_datum(**metadata):
    datum = CountingTimeSeries()
    datum.set_live()
    datum.t0 = 50.0
    for key, value in metadata.items():
        datum[key] = value
    return datum


def assert_one_invalid(datum, expected_text, killed):
    errors = list(datum.elog.get_error_log())
    assert len(errors) == 1
    assert errors[0].badness == ErrorSeverity.Invalid
    assert expected_text in errors[0].message
    assert datum.dead() is killed
    assert datum.kill_calls == int(killed)


def test_contract_suite_loads_matcher_from_selected_build():
    _assert_module_from_selected_build(matcher_module, Path("mspasspy/db/matcher.py"))


@pytest.mark.parametrize("missing_key", ["net", "sta", "chan"])
@pytest.mark.parametrize("with_location", [False, True])
@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_channel_missing_required_key_logs_once_without_query(
    missing_key, with_location, kill_on_failure
):
    metadata = {"net": "XX", "sta": "AAA", "chan": "BHZ"}
    metadata.pop(missing_key)
    if with_location:
        metadata["loc"] = "00"
    database = RecordingDatabase()
    matcher = mseed_channel_matcher(
        database,
        attributes_to_load=["lat"],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )
    log_calls = []
    original_log_error = matcher.log_error

    def recording_log_error(*args, **kwargs):
        log_calls.append((args, kwargs))
        return original_log_error(*args, **kwargs)

    matcher.log_error = recording_log_error
    datum = make_datum(**metadata)

    assert matcher.normalize(datum) is datum

    assert_one_invalid(datum, missing_key, kill_on_failure)
    collection = database.collections["channel"]
    assert collection.count_queries == []
    assert collection.find_queries == []
    assert len(log_calls) == 1
    assert log_calls[0][0][3] is kill_on_failure


@pytest.mark.parametrize("missing_key", ["net", "sta"])
@pytest.mark.parametrize("with_location", [False, True])
@pytest.mark.parametrize("channel", [None, "BHZ", "DIFFERENT"])
@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_site_missing_required_key_logs_once_without_query(
    missing_key, with_location, channel, kill_on_failure
):
    metadata = {"net": "XX", "sta": "AAA"}
    metadata.pop(missing_key)
    if with_location:
        metadata["loc"] = "00"
    if channel is not None:
        metadata["chan"] = channel
    database = RecordingDatabase()
    matcher = mseed_site_matcher(
        database,
        attributes_to_load=["lat"],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )
    log_calls = []
    original_log_error = matcher.log_error

    def recording_log_error(*args, **kwargs):
        log_calls.append((args, kwargs))
        return original_log_error(*args, **kwargs)

    matcher.log_error = recording_log_error
    datum = make_datum(**metadata)

    assert matcher.normalize(datum) is datum

    assert_one_invalid(datum, missing_key, kill_on_failure)
    collection = database.collections["site"]
    assert collection.count_queries == []
    assert collection.find_queries == []
    assert len(log_calls) == 1
    assert log_calls[0][0][3] is kill_on_failure


@pytest.mark.parametrize("matcher_kind", ["channel", "site"])
@pytest.mark.parametrize("with_location", [False, True])
@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_location_is_optional(matcher_kind, with_location, kill_on_failure):
    metadata = {"net": "XX", "sta": "AAA"}
    document = {
        "net": "XX",
        "sta": "AAA",
        "lat": 12.5,
        "starttime": 0.0,
        "endtime": 100.0,
    }
    if matcher_kind == "channel":
        metadata["chan"] = "BHZ"
        document["chan"] = "BHZ"
    if with_location:
        metadata["loc"] = "00"
        document["loc"] = "00"
    database = RecordingDatabase(**{matcher_kind: [document]})
    matcher_class = (
        mseed_channel_matcher if matcher_kind == "channel" else mseed_site_matcher
    )
    matcher = matcher_class(
        database,
        attributes_to_load=["lat"],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )
    datum = make_datum(**metadata)

    assert matcher.normalize(datum) is datum

    assert datum.live
    assert datum.elog.size() == 0
    assert datum[f"{matcher_kind}_lat"] == 12.5
    query = database.collections[matcher_kind].count_queries[0]
    assert ("loc" in query) is with_location


@pytest.mark.parametrize("kill_on_failure", [False, True])
@pytest.mark.parametrize("with_location", [False, True])
def test_site_channel_metadata_never_changes_query_or_result(
    kill_on_failure, with_location
):
    database = RecordingDatabase(
        site=[
            {
                "net": "XX",
                "sta": "AAA",
                "lat": 12.5,
                "starttime": 0.0,
                "endtime": 100.0,
            }
        ]
    )
    if with_location:
        database.collections["site"].documents[0]["loc"] = "00"
    matcher = mseed_site_matcher(
        database,
        attributes_to_load=["lat"],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )

    for channel in (None, "BHZ", "DIFFERENT"):
        metadata = {"net": "XX", "sta": "AAA"}
        if with_location:
            metadata["loc"] = "00"
        if channel is not None:
            metadata["chan"] = channel
        datum = make_datum(**metadata)
        assert matcher.normalize(datum) is datum
        assert datum["site_lat"] == 12.5
        assert datum.elog.size() == 0

    collection = database.collections["site"]
    assert collection.count_queries == [collection.count_queries[0]] * 3
    assert collection.find_queries == [collection.find_queries[0]] * 3
    assert "chan" not in collection.count_queries[0]


@pytest.mark.parametrize(
    "matcher_class,missing_key",
    [
        (mseed_channel_matcher, "net"),
        (mseed_channel_matcher, "sta"),
        (mseed_channel_matcher, "chan"),
        (mseed_site_matcher, "net"),
        (mseed_site_matcher, "sta"),
    ],
)
@pytest.mark.parametrize("entrypoint", ["get_document", "normalize"])
@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_missing_input_public_entrypoints_do_not_duplicate_errors(
    matcher_class, missing_key, entrypoint, kill_on_failure
):
    metadata = {"net": "XX", "sta": "AAA", "chan": "BHZ"}
    metadata.pop(missing_key)
    database = RecordingDatabase()
    matcher = matcher_class(
        database,
        attributes_to_load=["lat"],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )
    datum = make_datum(**metadata)

    result = getattr(matcher, entrypoint)(datum)

    if entrypoint == "get_document":
        assert result is None
    else:
        assert result is datum
    assert_one_invalid(datum, missing_key, kill_on_failure)
    collection_name = "channel" if matcher_class is mseed_channel_matcher else "site"
    collection = database.collections[collection_name]
    assert collection.count_queries == []
    assert collection.find_queries == []


@pytest.mark.parametrize("matcher_kind", ["channel", "site"])
def test_network_is_always_part_of_the_match(matcher_kind):
    common = {"sta": "AAA", "starttime": 0.0, "endtime": 100.0}
    metadata = {"net": "YY", "sta": "AAA"}
    documents = []
    for network, latitude in (("XX", 1.0), ("YY", 2.0)):
        document = {**common, "net": network, "lat": latitude}
        if matcher_kind == "channel":
            document["chan"] = "BHZ"
            metadata["chan"] = "BHZ"
        documents.append(document)
    database = RecordingDatabase(**{matcher_kind: documents})
    matcher_class = (
        mseed_channel_matcher if matcher_kind == "channel" else mseed_site_matcher
    )
    matcher = matcher_class(
        database,
        attributes_to_load=["lat"],
        kill_on_failure=False,
        verbose=False,
    )
    datum = make_datum(**metadata)

    assert matcher.normalize(datum) is datum

    assert datum[f"{matcher_kind}_lat"] == 2.0
    collection = database.collections[matcher_kind]
    assert collection.count_queries[0]["net"] == "YY"
    assert collection.find_queries[0]["net"] == "YY"


@pytest.mark.parametrize("matcher_kind", ["channel", "site"])
@pytest.mark.parametrize("kill_on_failure", [False, True])
def test_missing_output_field_names_collection_and_field_once(
    matcher_kind, kill_on_failure
):
    document = {
        "net": "XX",
        "sta": "AAA",
        "starttime": 0.0,
        "endtime": 100.0,
    }
    metadata = {"net": "XX", "sta": "AAA"}
    if matcher_kind == "channel":
        document["chan"] = "BHZ"
        metadata["chan"] = "BHZ"
    database = RecordingDatabase(**{matcher_kind: [document]})
    matcher_class = (
        mseed_channel_matcher if matcher_kind == "channel" else mseed_site_matcher
    )
    matcher = matcher_class(
        database,
        attributes_to_load=["missing_field"],
        kill_on_failure=kill_on_failure,
        verbose=False,
    )
    datum = make_datum(**metadata)

    assert matcher.normalize(datum) is datum

    assert_one_invalid(
        datum,
        f"key=missing_field in document returned from collection={matcher_kind}",
        kill_on_failure,
    )
    collection = database.collections[matcher_kind]
    assert len(collection.count_queries) == 1
    assert len(collection.find_queries) == 1
