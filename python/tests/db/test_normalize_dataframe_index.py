"""Regression tests for indexed DataFrame normalization and optional fields."""

import pandas as pd
import pytest

from mspasspy.ccore.utility import Metadata
from mspasspy.db.normalize import EqualityMatcher


@pytest.fixture
def sources():
    return pd.DataFrame(
        [
            {
                "net": "XX",
                "sta": "AAA",
                "coords": [10.0, 20.0],
                "optional": "present",
            },
            {
                "net": "XX",
                "sta": "BBB",
                "coords": [30.0, 40.0],
                "optional": pd.NA,
            },
        ]
    )


@pytest.mark.parametrize("index_keys", [[], ["net"], ["net", "sta"]])
def test_index_fields_and_array_attributes_are_preserved(sources, index_keys):
    frame = sources.set_index(index_keys) if index_keys else sources
    original = frame.copy(deep=True)
    matcher = EqualityMatcher(
        frame,
        "site",
        {"net": "net", "sta": "sta"},
        ["net", "sta", "coords"],
        load_if_defined=["optional"],
        require_unique_match=True,
    )

    metadata, elog = matcher.find_one(Metadata({"net": "XX", "sta": "AAA"}))

    assert elog is None
    assert metadata["net"] == "XX"
    assert metadata["sta"] == "AAA"
    assert list(metadata["coords"]) == [10.0, 20.0]
    assert metadata["optional"] == "present"
    pd.testing.assert_frame_equal(frame, original)
    for key in index_keys:
        assert key in matcher.cache.index.names
        assert key not in matcher.cache.columns


@pytest.mark.parametrize("index_keys", [[], ["net"], ["net", "sta"]])
def test_null_optional_field_is_omitted(sources, index_keys):
    frame = sources.set_index(index_keys) if index_keys else sources
    matcher = EqualityMatcher(
        frame,
        "site",
        {"net": "net", "sta": "sta"},
        ["sta", "coords"],
        load_if_defined=["optional"],
        require_unique_match=True,
    )
    metadata, elog = matcher.find_one(Metadata({"net": "XX", "sta": "BBB"}))

    assert elog is None
    assert metadata["sta"] == "BBB"
    assert list(metadata["coords"]) == [30.0, 40.0]
    assert "optional" not in metadata


def test_missing_match_key_returns_a_diagnostic(sources):
    matcher = EqualityMatcher(
        sources.set_index(["net", "sta"]),
        "site",
        {"net": "net", "sta": "sta"},
        ["net", "sta"],
    )
    result, elog = matcher.find_one(Metadata({"net": "XX"}))

    assert result is None
    assert elog is not None


def test_underscore_prefixed_source_column_keeps_its_name():
    matcher = EqualityMatcher(
        pd.DataFrame([{"_id": "record-1", "net": "XX"}]),
        "site",
        {"net": "net"},
        ["_id", "net"],
        prepend_collection_name=True,
    )

    result, elog = matcher.find_one(Metadata({"net": "XX"}))

    assert elog is None
    assert dict(result) == {"site_id": "record-1", "site_net": "XX"}
