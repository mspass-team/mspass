import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import pytest
from bson import ObjectId

from mspasspy.db.script import dbverify


class LinkDatabase:
    def __init__(self, broken, undefined):
        self.broken = list(broken)
        self.undefined = list(undefined)
        self.calls = []

    def _check_links(self, **kwargs):
        self.calls.append(kwargs.copy())
        limit = kwargs["error_limit"]
        return self.broken[:limit], self.undefined[:limit]

    def __getitem__(self, collection):
        assert collection == "wf_TimeSeries"
        return object()


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


def test_contract_suite_loads_dbverify_from_selected_build():
    _assert_module_from_selected_build(dbverify, Path("mspasspy/db/script/dbverify.py"))


@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("has_broken", [False, True])
@pytest.mark.parametrize("has_undefined", [False, True])
def test_broken_and_undefined_four_way_matrix(
    monkeypatch, capsys, verbose, has_broken, has_undefined
):
    broken = [ObjectId()] if has_broken else []
    undefined = [ObjectId()] if has_undefined else []
    database = LinkDatabase(broken, undefined)
    printed_records = []
    monkeypatch.setattr(
        dbverify,
        "print_bad_wf_docs",
        lambda collection, ids: printed_records.append(list(ids)),
    )

    dbverify.run_check_links(
        database,
        "wf_TimeSeries",
        ["site_id"],
        7,
        verbose,
        no_cursor_timeout=True,
    )

    output = capsys.readouterr().out
    assert database.calls == [
        {
            "xref_key": "site_id",
            "collection": "wf_TimeSeries",
            "error_limit": 7,
            "no_cursor_timeout": True,
        }
    ]
    assert ("no broken links" in output) is not has_broken
    assert ("no undefined linking key" in output) is not has_undefined
    if verbose:
        expected_records = []
        if has_broken:
            expected_records.append(broken)
        if has_undefined:
            expected_records.append(undefined)
        assert printed_records == expected_records
    else:
        assert printed_records == []
        assert ("Found broken links in  1 documents checked" in output) is has_broken
        assert (
            "Found undefined link keys in  1 documents checked" in output
        ) is has_undefined


@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("error_limit", [1, 2, 3, 4])
def test_error_limit_is_forwarded_once_without_second_limiting(
    monkeypatch, capsys, verbose, error_limit
):
    broken = [ObjectId(), ObjectId()]
    undefined = [ObjectId(), ObjectId(), ObjectId()]
    database = LinkDatabase(broken, undefined)
    printed_records = []
    monkeypatch.setattr(
        dbverify,
        "print_bad_wf_docs",
        lambda collection, ids: printed_records.append(list(ids)),
    )

    dbverify.run_check_links(
        database, "wf_TimeSeries", ["source_id"], error_limit, verbose
    )

    output = capsys.readouterr().out
    assert len(database.calls) == 1
    assert database.calls[0]["error_limit"] == error_limit
    returned_broken = broken[:error_limit]
    returned_undefined = undefined[:error_limit]
    if verbose:
        assert printed_records == [returned_broken, returned_undefined]
    else:
        assert printed_records == []
        assert (
            f"Found broken links in  {len(returned_broken)} documents checked" in output
        )
        assert (
            "Found undefined link keys in  "
            f"{len(returned_undefined)} documents checked" in output
        )
