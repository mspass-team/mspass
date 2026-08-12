import builtins

import pytest
from bson import ObjectId

from mspasspy.db.script import normalize_mseed as normalize_mseed_cli


@pytest.fixture
def successful_cli(monkeypatch):
    dbclient = object()
    database = object()
    calls = {"dbclient": 0, "database": [], "normalize": []}

    def make_dbclient():
        calls["dbclient"] += 1
        return dbclient

    def make_database(client, name):
        calls["database"].append((client, name))
        return database

    def run_normalize(db, **kwargs):
        calls["normalize"].append((db, kwargs))
        return (0, 0, 0)

    monkeypatch.setattr(normalize_mseed_cli, "DBClient", make_dbclient)
    monkeypatch.setattr(normalize_mseed_cli, "Database", make_database)
    monkeypatch.setattr(normalize_mseed_cli, "normalize_mseed", run_normalize)
    return dbclient, database, calls


@pytest.mark.parametrize(
    ("query_args", "expected_query"),
    [
        ([], {}),
        (["--wfquery", ""], {}),
        (
            ["--wfquery", '{"net":"IU","npts":{"$gte":100}}'],
            {"net": "IU", "npts": {"$gte": 100}},
        ),
        (
            [
                "--wfquery",
                '{"_id":{"$oid":"64b000000000000000000001"}}',
            ],
            {"_id": ObjectId("64b000000000000000000001")},
        ),
    ],
)
def test_wfquery_is_parsed_before_normalization(
    query_args, expected_query, successful_cli
):
    dbclient, database, calls = successful_cli

    normalize_mseed_cli.main(["testdb", *query_args])

    assert calls["dbclient"] == 1
    assert calls["database"] == [(dbclient, "testdb")]
    assert calls["normalize"] == [
        (
            database,
            {
                "wfquery": expected_query,
                "blocksize": 1000,
                "normalize_site": False,
            },
        )
    ]


@pytest.mark.parametrize(
    ("query_text", "diagnostic"),
    [
        (
            "{'net': __import__('os').getcwd()}",
            "--wfquery must be valid BSON Extended JSON",
        ),
        (
            '{"_id":{"$oid":"not-an-object-id"}}',
            "--wfquery must be valid BSON Extended JSON",
        ),
        ("42", "--wfquery must decode to a JSON object"),
        ('["IU"]', "--wfquery must decode to a JSON object"),
    ],
)
def test_invalid_wfquery_exits_before_database_construction(
    query_text, diagnostic, monkeypatch, capsys
):
    eval_calls = []
    constructor_calls = []

    def record_eval(*args, **kwargs):
        eval_calls.append((args, kwargs))
        return {}

    def record_construction(*args, **kwargs):
        constructor_calls.append((args, kwargs))
        return object()

    monkeypatch.setattr(builtins, "eval", record_eval)
    monkeypatch.setattr(normalize_mseed_cli, "DBClient", record_construction)
    monkeypatch.setattr(normalize_mseed_cli, "Database", record_construction)

    with pytest.raises(SystemExit) as excinfo:
        normalize_mseed_cli.main(["testdb", "--wfquery", query_text])

    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert captured.out == ""
    assert captured.err.endswith(f"normalize_mseed: error: {diagnostic}\n")
    assert eval_calls == []
    assert constructor_calls == []
