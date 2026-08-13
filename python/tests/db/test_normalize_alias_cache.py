import copy
import os
import uuid
from pathlib import Path

import pandas as pd
import pytest
from bson import ObjectId

import mspasspy.db.normalize as normalize_module
from mspasspy.ccore.utility import Metadata
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.db.normalize import (
    DataFrameCacheMatcher,
    DictionaryCacheMatcher,
    EqualityDBMatcher,
    EqualityMatcher,
    OriginTimeMatcher,
)

SOURCE_PYTHON_ROOT = Path(
    os.environ.get("MSPASS_TEST_SOURCE_ROOT", Path(__file__).resolve().parents[2])
)
EXPECTED_NORMALIZE_MODULE = SOURCE_PYTHON_ROOT / "mspasspy" / "db" / "normalize.py"


class _EqualityDictionaryMatcher(DictionaryCacheMatcher):
    """Concrete dictionary-cache matcher used to compare backend contracts."""

    def __init__(
        self,
        source,
        collection,
        match_keys,
        attributes_to_load,
        load_if_defined,
        aliases,
    ):
        self.match_keys = match_keys
        super().__init__(
            source,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=True,
            prepend_collection_name=False,
        )

    def cache_id(self, mspass_object):
        if all(mspass_object.is_defined(key) for key in self.match_keys):
            return repr(tuple(mspass_object[key] for key in self.match_keys))
        return None

    def db_make_cache_id(self, document):
        source_keys = self.match_keys.values()
        if all(key in document for key in source_keys):
            return repr(tuple(document[key] for key in source_keys))
        return None


@pytest.fixture
def normalization_sources():
    assert Path(normalize_module.__file__).resolve() == EXPECTED_NORMALIZE_MODULE
    client = DBClient("127.0.0.1")
    database_name = "issue_823_" + uuid.uuid4().hex
    database = Database(client, database_name)
    client.admin.command("ping")
    rows = [
        {
            "_id": ObjectId(),
            "match_only": "A",
            "version_only": 1,
            "source_value": 11,
            "optional_source": "first",
        },
        {
            "_id": ObjectId(),
            "match_only": "B",
            "version_only": 2,
            "source_value": 22,
            "optional_source": "second",
        },
    ]
    database["records"].insert_many(copy.deepcopy(rows))
    yield database, rows
    client.drop_database(database_name)
    client.close()


def _matcher_configuration():
    match_keys = {"lookup": "match_only", "version": "version_only"}
    attributes_to_load = ["source_value"]
    load_if_defined = ["optional_source"]
    aliases = {
        "match_only": "lookup",
        "version_only": "version",
        "source_value": "target_value",
        "optional_source": "target_optional",
    }
    return match_keys, attributes_to_load, load_if_defined, aliases


def _make_matchers(database, rows):
    match_keys, attributes_to_load, load_if_defined, aliases = _matcher_configuration()
    frame = pd.DataFrame(copy.deepcopy(rows))
    dataframe_matcher = EqualityMatcher(
        frame,
        "records",
        match_keys,
        attributes_to_load,
        load_if_defined=load_if_defined,
        aliases=aliases,
        prepend_collection_name=False,
    )
    dictionary_matcher = _EqualityDictionaryMatcher(
        database,
        "records",
        match_keys,
        attributes_to_load,
        load_if_defined,
        aliases,
    )
    database_matcher = EqualityDBMatcher(
        database,
        "records",
        match_keys,
        attributes_to_load,
        load_if_defined=load_if_defined,
        aliases=aliases,
        require_unique_match=True,
        prepend_collection_name=False,
    )
    return frame, dataframe_matcher, dictionary_matcher, database_matcher


def _dictionary_cache_snapshot(matcher):
    return {
        key: [dict(metadata) for metadata in values]
        for key, values in matcher.normcache.items()
    }


def _database_snapshot(database):
    return sorted(
        [copy.deepcopy(document) for document in database["records"].find({})],
        key=lambda document: str(document["_id"]),
    )


def test_match_only_columns_are_cached_but_not_returned(normalization_sources):
    database, rows = normalization_sources
    source_frame = pd.DataFrame(copy.deepcopy(rows))
    frame_before = source_frame.copy(deep=True)
    match_keys, attributes_to_load, load_if_defined, aliases = _matcher_configuration()
    configuration_before = copy.deepcopy(
        (match_keys, attributes_to_load, load_if_defined, aliases)
    )
    matcher = EqualityMatcher(
        source_frame,
        "records",
        match_keys,
        attributes_to_load,
        load_if_defined=load_if_defined,
        aliases=aliases,
        prepend_collection_name=False,
    )
    caller = Metadata({"lookup": "A", "version": 1, "unchanged": True})
    caller_before = dict(caller)

    result, elog = matcher.find_one(caller)

    assert elog is None
    assert dict(result) == {"target_value": 11, "target_optional": "first"}
    assert list(matcher.cache.columns) == [
        "source_value",
        "optional_source",
        "match_only",
        "version_only",
    ]
    assert "target_value" not in matcher.cache.columns
    assert "match_only" not in result and "version_only" not in result
    assert dict(caller) == caller_before
    assert (match_keys, attributes_to_load, load_if_defined, aliases) == (
        configuration_before
    )
    pd.testing.assert_frame_equal(source_frame, frame_before)
    assert _database_snapshot(database) == sorted(
        copy.deepcopy(rows), key=lambda document: str(document["_id"])
    )


def test_three_backends_return_identical_aliased_output(normalization_sources):
    database, rows = normalization_sources
    frame, dataframe_matcher, dictionary_matcher, database_matcher = _make_matchers(
        database, rows
    )
    frame_before = frame.copy(deep=True)
    dataframe_cache_before = dataframe_matcher.cache.copy(deep=True)
    dictionary_before = _dictionary_cache_snapshot(dictionary_matcher)
    database_before = _database_snapshot(database)
    caller = Metadata({"lookup": "B", "version": 2, "unchanged": "caller"})
    caller_before = dict(caller)

    results = [
        matcher.find_one(caller)
        for matcher in (
            dataframe_matcher,
            dictionary_matcher,
            database_matcher,
        )
    ]

    assert all(elog is None for _, elog in results)
    mappings = [dict(metadata) for metadata, _ in results]
    assert (
        mappings
        == [
            {"target_value": 22, "target_optional": "second"},
        ]
        * 3
    )
    assert dict(caller) == caller_before
    pd.testing.assert_frame_equal(frame, frame_before)
    pd.testing.assert_frame_equal(dataframe_matcher.cache, dataframe_cache_before)
    assert _dictionary_cache_snapshot(dictionary_matcher) == dictionary_before
    assert _database_snapshot(database) == database_before


def test_missing_match_keys_leave_caller_and_backends_unchanged(
    normalization_sources,
):
    database, rows = normalization_sources
    frame, dataframe_matcher, dictionary_matcher, database_matcher = _make_matchers(
        database, rows
    )
    dataframe_cache_before = dataframe_matcher.cache.copy(deep=True)
    dictionary_before = _dictionary_cache_snapshot(dictionary_matcher)
    database_before = _database_snapshot(database)
    caller = Metadata({"lookup": "A", "unchanged": 7})
    caller_before = dict(caller)

    for matcher in (dataframe_matcher, dictionary_matcher, database_matcher):
        result, elog = matcher.find_one(caller)
        assert result is None
        assert elog is not None

    assert dict(caller) == caller_before
    pd.testing.assert_frame_equal(dataframe_matcher.cache, dataframe_cache_before)
    assert _dictionary_cache_snapshot(dictionary_matcher) == dictionary_before
    assert _database_snapshot(database) == database_before
    assert set(frame.columns) == set(rows[0])


def test_invalid_match_keys_fail_before_cache_construction(monkeypatch):
    frame = pd.DataFrame([{"source_value": 1, "match_only": "A"}])
    frame_before = frame.copy(deep=True)
    cache_load_calls = 0

    def count_cache_load(*args, **kwargs):
        nonlocal cache_load_calls
        cache_load_calls += 1

    monkeypatch.setattr(
        DataFrameCacheMatcher, "_load_dataframe_cache", count_cache_load
    )

    with pytest.raises(TypeError, match="matchkeys.*python dictionary"):
        EqualityMatcher(
            frame,
            "records",
            ["match_only"],
            ["source_value"],
        )

    assert cache_load_calls == 0
    pd.testing.assert_frame_equal(frame, frame_before)


def test_origin_time_find_doc_reads_values_from_source_keys():
    sources = pd.DataFrame(
        [
            {
                "source_time": 100.0,
                "source_value": 41,
                "optional_source": "present",
            }
        ]
    )
    sources_before = sources.copy(deep=True)
    waveform_document = {"starttime": 100.0, "unchanged": True}
    document_before = copy.deepcopy(waveform_document)
    matcher = OriginTimeMatcher(
        sources,
        tolerance=1.0,
        attributes_to_load=["source_time", "source_value"],
        load_if_defined=["optional_source"],
        aliases={
            "source_value": "target_value",
            "optional_source": "target_optional",
        },
        prepend_collection_name=False,
        source_time_key="source_time",
    )

    result = matcher.find_doc(waveform_document)

    assert result == {
        "source_time": 100.0,
        "target_value": 41,
        "target_optional": "present",
    }
    assert waveform_document == document_before
    pd.testing.assert_frame_equal(sources, sources_before)
