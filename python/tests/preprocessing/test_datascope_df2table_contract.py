from pathlib import Path

import pandas as pd
import pytest

from mspasspy.preprocessing.css30.datascope import DatascopeDatabase

KEYS = ["ival", "fval", "sval"]
NULLS = {"ival": -1, "fval": -99.0, "sval": "-"}
FORMATS = [["ival", "%d"], ["fval", "%.1f"], ["sval", "%s"]]


def _database():
    database = object.__new__(DatascopeDatabase)
    database.dbname = "output"
    database._get_line_format_pf = lambda table: FORMATS
    database._parse_attribute_name_tbl = lambda table: ({}, {}, NULLS, {})
    return database


def _target(tmp_path):
    return Path(tmp_path) / "output.test"


@pytest.mark.parametrize("row_count", [0, 1, 3])
def test_zero_column_dataframe_uses_schema_nulls(tmp_path, row_count):
    database = _database()
    frame = pd.DataFrame(index=[11, 17, 23][:row_count])
    original = frame.copy(deep=True)

    result = database.df2table(frame, table="test", dir=str(tmp_path), append=False)

    assert list(result.columns) == KEYS
    assert list(result.index) == list(frame.index)
    assert result["ival"].tolist() == [-1] * row_count
    assert result["fval"].tolist() == [-99.0] * row_count
    assert result["sval"].tolist() == ["-"] * row_count
    pd.testing.assert_frame_equal(frame, original)
    expected = "" if row_count == 0 else "-1 -99.0 -\n" * row_count
    assert _target(tmp_path).read_text(encoding="utf-8") == expected


def test_missing_columns_align_to_the_caller_index(tmp_path):
    database = _database()
    frame = pd.DataFrame(
        {"fval": [1.5, 2.5], "sval": ["a", "b"]},
        index=[4, 7],
    )
    original = frame.copy(deep=True)

    result = database.df2table(frame, table="test", dir=str(tmp_path), append=False)

    assert list(result.columns) == KEYS
    assert list(result.index) == [4, 7]
    assert result["ival"].tolist() == [-1, -1]
    assert result["fval"].tolist() == [1.5, 2.5]
    assert result["sval"].tolist() == ["a", "b"]
    pd.testing.assert_frame_equal(frame, original)
    assert _target(tmp_path).read_text(encoding="utf-8") == "-1 1.5 a\n-1 2.5 b\n"


def test_zero_rows_preserve_append_and_overwrite_behavior(tmp_path):
    database = _database()
    target = _target(tmp_path)
    target.write_bytes(b"existing\n")
    empty = pd.DataFrame(columns=["sval", "ival"])

    appended = database.df2table(empty, table="test", dir=str(tmp_path), append=True)

    assert list(appended.columns) == KEYS
    assert appended.empty
    assert target.read_bytes() == b"existing\n"

    overwritten = database.df2table(
        empty, table="test", dir=str(tmp_path), append=False
    )

    assert list(overwritten.columns) == KEYS
    assert overwritten.empty
    assert target.read_bytes() == b""


def test_matching_schema_preserves_the_existing_fast_path(tmp_path):
    database = _database()
    frame = pd.DataFrame({"ival": [1], "fval": [2.5], "sval": ["a"]})
    original_dtypes = frame.dtypes.copy()

    result = database.df2table(frame, table="test", dir=str(tmp_path), append=False)

    assert result is frame
    pd.testing.assert_series_equal(result.dtypes, original_dtypes)
    assert _target(tmp_path).read_text(encoding="utf-8") == "1 2.5 a\n"


def test_extra_columns_keep_the_existing_print_and_drop_behavior(tmp_path, capsys):
    database = _database()
    frame = pd.DataFrame({"ival": [1], "fval": [2.5], "sval": ["a"], "extra": [9]})

    result = database.df2table(frame, table="test", dir=str(tmp_path), append=False)

    output = capsys.readouterr().out
    assert "extra" in output
    assert list(result.columns) == KEYS
    assert _target(tmp_path).read_text(encoding="utf-8") == "1 2.5 a\n"
