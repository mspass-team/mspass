"""Cache construction regressions for issue #996 (no MongoDB server needed)."""

from copy import deepcopy
from unittest.mock import Mock

import pandas as pd
import pytest
from pymongo import MongoClient
from pymongo.collection import Collection

from mspasspy.ccore.utility import Metadata
from mspasspy.db.normalize import DataFrameCacheMatcher, EqualityMatcher


class _CacheMatcher(DataFrameCacheMatcher):
    def subset(self, datum):
        return self.cache[self.cache["orid"] == datum["orid"]]


@pytest.mark.parametrize("matcher_class", [_CacheMatcher, EqualityMatcher])
@pytest.mark.parametrize("query", [None, {}, {"evid": {"$in": [42]}}])
def test_database_query_is_applied_before_caching(monkeypatch, matcher_class, query):
    """Forward the unchanged query once, while preserving generic matching."""
    client = MongoClient(connect=False)
    rows = [
        {"evid": 42, "orid": 101, "lat": 34.5, "label": "preferred"},
        {"evid": 42, "orid": 102, "lat": 34.5, "label": "alternate"},
    ]
    find = Mock(return_value=iter(rows))
    monkeypatch.setattr(Collection, "find", find)
    original_query = deepcopy(query)
    options = {"cache_columns": ["orid"]}
    if matcher_class is EqualityMatcher:
        options = {"match_keys": {"orid": "orid", "lat": "lat"}}
    try:
        matcher = matcher_class(
            client["catalog"],
            "source",
            attributes_to_load=["lat", "label"],
            query=query,
            **options,
        )
        find.assert_called_once_with({} if query is None else query)
        assert query == original_query
        result, error = matcher.find_one(Metadata({"orid": 102, "lat": 34.5}))
        assert error is None
        assert result["label"] == "alternate"
        assert result["lat"] == 34.5
        assert not result.is_defined("orid")
        # Matching uses the cache, without issuing another database query.
        assert (
            matcher.find_one(Metadata({"orid": 101, "lat": 34.5}))[0]["label"]
            == "preferred"
        )
        assert find.call_count == 1
    finally:
        client.close()


def test_dataframe_query_is_ignored_and_source_preserved():
    """MongoDB query dictionaries do not subset already materialized frames."""
    frame = pd.DataFrame(
        [
            {"orid": 101, "evid": 42, "lat": 34.5},
            {"orid": 102, "evid": 43, "lat": 35.5},
        ]
    )
    original = frame.copy(deep=True)
    matcher = EqualityMatcher(
        frame,
        "source",
        match_keys={"orid": "orid", "lat": "lat"},
        attributes_to_load=["evid"],
        query={"evid": 42},
    )
    result, error = matcher.find_one(Metadata({"orid": 102, "lat": 35.5}))
    assert error is None
    assert result["evid"] == 43
    pd.testing.assert_frame_equal(frame, original)
