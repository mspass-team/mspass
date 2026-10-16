"""Preferred-origin catalog views use real CSS parsing and joins."""

from pathlib import Path

import pandas as pd

from mspasspy.preprocessing.css30.datascope import DatascopeDatabase


ROOT = Path(__file__).resolve().parents[3]


def test_preferred_view_on_repository_catalog():
    db = DatascopeDatabase(
        str(ROOT / "python/tests/data/datascope/testdb"),
        pffile=str(ROOT / "data/pf/DatascopeDatabase.pf"),
    )
    all_origins = db.CSS30Catalog2df()
    preferred = db.CSS30Catalog2df(preferred_only=True)
    expected = all_origins.loc[all_origins["orid"].eq(all_origins["prefor"])].copy()
    assert len(preferred) > 0
    pd.testing.assert_frame_equal(preferred, expected)
    pd.testing.assert_frame_equal(db.CSS30Catalog2df(preferred_only=False), all_origins)


def test_multiple_origins_and_unassociated_arrivals(monkeypatch):
    """Filtering survives right joins that retain nonpreferred/orphan arrivals."""
    db = DatascopeDatabase("unused", pffile=str(ROOT / "data/pf/DatascopeDatabase.pf"))
    tables = {
        "event": pd.DataFrame({"evid": [1, 2], "prefor": [12, 99]}),
        "origin": pd.DataFrame({"evid": [1, 1, 2], "orid": [11, 12, 21]}),
        "assoc": pd.DataFrame({"orid": [11, 12, 12, 21], "arid": [101, 102, 103, 104]}),
        "arrival": pd.DataFrame(
            {"arid": [101, 102, 103, 104, 105], "sta": ["A", "B", "C", "D", "E"]}
        ),
    }
    # Mock only the table-reading boundary; exercise production join and filter.
    monkeypatch.setattr(db, "get_table", lambda table: tables[table].copy(deep=True))
    all_origins = db.CSS30Catalog2df()
    assert set(all_origins["arid"]) == {101, 102, 103, 104, 105}
    preferred = db.CSS30Catalog2df(preferred_only=True)
    assert list(preferred["arid"]) == [102, 103]
    assert set(preferred["evid"]) == {1}
    assert set(preferred["orid"]) == {12}
    assert list(tables["origin"]["orid"]) == [11, 12, 21]
