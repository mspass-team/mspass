import ast
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.preprocessing.css30.datascope import DatascopeDatabase


def _database(snetsta=None):
    database = object.__new__(DatascopeDatabase)
    database.parse_snetsta = Mock(return_value={} if snetsta is None else snetsta)
    database.get_table = Mock(return_value=pd.DataFrame())
    database.get_nulls = Mock(return_value={})
    return database


@pytest.mark.parametrize("xref", [{}, {"X_STA": ("X", "STA")}])
def test_wfdisc2doclist_accepts_dict_without_row_processing(xref):
    database = _database()

    result = database.wfdisc2doclist(snetsta_xref=xref, verbose=False)

    assert result == []
    database.parse_snetsta.assert_not_called()
    database.get_table.assert_called_once_with("wfdisc")


def test_wfdisc2doclist_accepts_none_and_parses_xref_once():
    database = _database({"X_STA": ("X", "STA")})

    result = database.wfdisc2doclist(snetsta_xref=None, verbose=False)

    assert result == []
    database.parse_snetsta.assert_called_once_with()
    database.get_table.assert_called_once_with("wfdisc")


@pytest.mark.parametrize("xref", [[], (), "bad", 3])
def test_wfdisc2doclist_rejects_invalid_xref_before_rows(xref):
    database = _database()

    with pytest.raises(MsPASSError) as captured:
        database.wfdisc2doclist(snetsta_xref=xref, verbose=False)

    error = captured.value
    expected_message = (
        "DatascopeDatabase.wfdisc2doclist: snetsta_xref has invalid "
        f"type={type(xref)}.  It must be a dict or None."
    )
    assert error.severity == ErrorSeverity.Fatal
    assert error.message == expected_message
    database.parse_snetsta.assert_not_called()
    database.get_table.assert_not_called()
    database.get_nulls.assert_not_called()


def test_datascope_mspass_errors_use_message_and_error_severity():
    source_path = (
        Path(__file__).resolve().parents[2]
        / "mspasspy/preprocessing/css30/datascope.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "MsPASSError"
    ]

    assert calls
    for call in calls:
        assert len(call.args) == 2
        assert not call.keywords
        assert ast.unparse(call.args[1]).startswith("ErrorSeverity.")
