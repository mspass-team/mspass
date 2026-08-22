import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest
import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "code-format.yml"
README_PATH = REPOSITORY_ROOT / "README.md"
CHECK_SCRIPT = """\
shopt -s globstar nullglob
notebooks=(docs/**/*.ipynb)
black --workers 1 --check --diff python/mspasspy python/tests "${notebooks[@]}"
"""
FORMAT_SCRIPT = """\
shopt -s globstar nullglob
notebooks=(docs/**/*.ipynb)
black --workers 1 python/mspasspy python/tests "${notebooks[@]}"
"""


def _load_workflow():
    with WORKFLOW_PATH.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _run_black(black, *arguments, cwd):
    return subprocess.run(
        [black, "--workers", "1", *arguments],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def _write_notebook(path, source):
    path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "execution_count": None,
                        "metadata": {},
                        "outputs": [],
                        "source": [source],
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        )
    )


def test_black_workflow_is_pinned_read_only_and_stably_named():
    workflow = _load_workflow()
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["jobs"]) == {"black-format"}

    job = workflow["jobs"]["black-format"]
    assert job["name"] == "black-format"
    assert job["runs-on"] == "ubuntu-latest"
    assert "permissions" not in job
    assert all("continue-on-error" not in step for step in job["steps"])
    assert [step.get("uses", step.get("name")) for step in job["steps"]] == [
        "actions/checkout@v4",
        "actions/setup-python@v5",
        "Install pinned Black",
        "Verify the formatting policy",
        "Check formatting without modifying files",
    ]

    install = next(
        step for step in job["steps"] if step.get("name") == "Install pinned Black"
    )
    assert (
        install["run"] == "python -m pip install 'black[jupyter]==25.1.0' pytest pyyaml"
    )
    policy = next(
        step
        for step in job["steps"]
        if step.get("name") == "Verify the formatting policy"
    )
    assert policy["run"] == "pytest -q python/tests/test_black_workflow.py"
    check = next(
        step
        for step in job["steps"]
        if step.get("name") == "Check formatting without modifying files"
    )
    assert check["shell"] == "bash"
    assert check["run"] == CHECK_SCRIPT
    assert "push" not in workflow["on"]
    assert all(
        "create-pull-request" not in str(step.get("uses", "")) for step in job["steps"]
    )


def test_readme_documents_the_exact_check_and_format_commands():
    readme = README_PATH.read_text(encoding="utf-8")
    assert "python -m pip install 'black[jupyter]==25.1.0'" in readme
    assert CHECK_SCRIPT in readme
    assert FORMAT_SCRIPT in readme


def test_read_only_check_rejects_python_and_notebook_without_mutation(tmp_path):
    black = shutil.which("black")
    if not black:
        pytest.skip("Black is installed by the black-format workflow")
    version = subprocess.run(
        [black, "--version"], capture_output=True, text=True, check=True
    )
    if "25.1.0" not in version.stdout:
        pytest.skip("integration contract requires the workflow's Black 25.1.0")

    package = tmp_path / "python" / "mspasspy"
    tests = tmp_path / "python" / "tests"
    notebooks = tmp_path / "docs" / "guide"
    package.mkdir(parents=True)
    tests.mkdir(parents=True)
    notebooks.mkdir(parents=True)
    (package / "good.py").write_text("answer = 42\n")
    bad_python = tests / "bad.py"
    bad_python.write_text("result=  [1,2,3]\n")
    bad_notebook = notebooks / "demo.ipynb"
    _write_notebook(bad_notebook, "result=  [1,2,3]\n")

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    before_status = subprocess.run(
        ["git", "status", "--short"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    before_python = bad_python.read_bytes()
    before_notebook = bad_notebook.read_bytes()

    rejected_python = _run_black(black, "--check", "--diff", bad_python, cwd=tmp_path)
    rejected_notebook = _run_black(
        black, "--check", "--diff", bad_notebook, cwd=tmp_path
    )
    assert rejected_python.returncode != 0
    assert rejected_notebook.returncode != 0
    diagnostics = (
        rejected_python.stdout
        + rejected_python.stderr
        + rejected_notebook.stdout
        + rejected_notebook.stderr
    )
    assert "bad.py" in diagnostics
    assert "demo.ipynb" in diagnostics
    assert bad_python.read_bytes() == before_python
    assert bad_notebook.read_bytes() == before_notebook
    assert (
        subprocess.run(
            ["git", "status", "--short"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        == before_status
    )

    for path in (bad_python, bad_notebook):
        formatted = _run_black(black, path, cwd=tmp_path)
        assert formatted.returncode == 0, formatted.stdout + formatted.stderr
        accepted = _run_black(black, "--check", "--diff", path, cwd=tmp_path)
        assert accepted.returncode == 0, accepted.stdout + accepted.stderr


def test_read_only_check_accepts_formatted_tree_without_notebooks(tmp_path):
    black = shutil.which("black")
    if not black:
        pytest.skip("Black is installed by the black-format workflow")
    version = subprocess.run(
        [black, "--version"], capture_output=True, text=True, check=True
    )
    if "25.1.0" not in version.stdout:
        pytest.skip("integration contract requires the workflow's Black 25.1.0")

    (tmp_path / "python" / "mspasspy").mkdir(parents=True)
    (tmp_path / "python" / "tests").mkdir(parents=True)
    (tmp_path / "python" / "mspasspy" / "good.py").write_text("answer = 42\n")
    (tmp_path / "python" / "tests" / "good.py").write_text("assert True\n")

    for path in (
        tmp_path / "python" / "mspasspy" / "good.py",
        tmp_path / "python" / "tests" / "good.py",
    ):
        accepted = _run_black(black, "--check", "--diff", path, cwd=tmp_path)
        assert accepted.returncode == 0, accepted.stdout + accepted.stderr
