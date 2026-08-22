import warnings
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
def test_zero_column_dataframe_uses_typed_schema_nulls(tmp_path, row_count):
    database = _database()
    frame = pd.DataFrame(index=[11, 17, 23][:row_count])
    original = frame.copy(deep=True)

    result = database.df2table(frame, table="test", dir=str(tmp_path), append=False)

    assert list(result.columns) == KEYS
    assert list(result.index) == list(frame.index)
    assert len(result) == row_count
    for row in frame.index:
        for key in KEYS:
            assert result.loc[row, key] == NULLS[key]
            assert type(result.loc[row, key]) is type(NULLS[key])
    pd.testing.assert_frame_equal(frame, original)
    expected = "" if row_count == 0 else "-1 -99.0 -\n" * row_count
    assert _target(tmp_path).read_text(encoding="utf-8") == expected


def test_zero_rows_append_preserves_file_and_overwrite_truncates(tmp_path):
    database = _database()
    target = _target(tmp_path)
    target.write_bytes(b"existing\n")
    empty = pd.DataFrame(columns=["sval", "ival"])
    original = empty.copy(deep=True)

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
    pd.testing.assert_frame_equal(empty, original)


@pytest.mark.parametrize("append", [False, True])
def test_missing_extra_and_reordered_columns_share_one_transformation(tmp_path, append):
    database = _database()
    frame = pd.DataFrame(
        {
            "zeta": [8, 9],
            "sval": ["a", "b"],
            "alpha": [1, 2],
            "fval": [1.5, 2.5],
        },
        index=[4, 7],
    )
    original = frame.copy(deep=True)

    with pytest.warns(UserWarning) as captured:
        result = database.df2table(
            frame, table="test", dir=str(tmp_path), append=append
        )

    assert len(captured) == 1
    assert captured[0].category is UserWarning
    assert str(captured[0].message) == (
        "The following input DataFrame columns are not defined in the schema "
        "for table test and were dropped: alpha, zeta"
    )
    assert list(result.columns) == KEYS
    assert list(result.index) == [4, 7]
    assert result["ival"].tolist() == [-1, -1]
    assert all(type(value) is int for value in result["ival"])
    assert result["fval"].tolist() == [1.5, 2.5]
    assert result["sval"].tolist() == ["a", "b"]
    pd.testing.assert_frame_equal(frame, original)
    assert _target(tmp_path).read_text(encoding="utf-8") == ("-1 1.5 a\n-1 2.5 b\n")


def test_schema_null_rows_are_reordered_without_warning(tmp_path):
    database = _database()
    frame = pd.DataFrame({"sval": ["-"], "fval": [-99.0], "ival": [-1]})

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        result = database.df2table(frame, table="test", dir=str(tmp_path), append=False)

    assert captured == []
    assert list(result.columns) == KEYS
    assert result.iloc[0].tolist() == [-1, -99.0, "-"]
    assert _target(tmp_path).read_bytes() == b"-1 -99.0 -\n"
