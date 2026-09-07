import ast
import json
import os
from pathlib import Path
import shutil
import signal
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


def _run_workflow_script(script, cwd, environment):
    with subprocess.Popen(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        cwd=cwd,
        env=environment,
        start_new_session=True,
    ) as process:
        try:
            returncode = process.wait(timeout=30)
        finally:
            # Black uses workers when it receives multiple input files.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
    assert returncode == 0


def test_black_workflow_generates_bounded_source_branch_fixes():
    workflow = _load_workflow()
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["on"]) == {"pull_request"}
    assert "closed" in workflow["on"]["pull_request"]["types"]
    assert workflow["concurrency"]["group"] == (
        "black-format-${{ github.event.pull_request.number }}"
    )
    assert workflow["concurrency"]["cancel-in-progress"] is False

    job = workflow["jobs"]["black-format"]
    assert job["name"] == "black-format"
    assert job["permissions"] == {"contents": "write", "pull-requests": "write"}
    assert all("continue-on-error" not in step for step in job["steps"])
    checkout = job["steps"][0]["with"]
    assert checkout["ref"] == "${{ github.event.pull_request.head.sha }}"
    assert checkout["persist-credentials"] is False

    install = next(
        step for step in job["steps"] if step.get("name") == "Install pinned Black"
    )
    assert (
        install["run"] == "python -m pip install 'black[jupyter]==25.1.0' pytest pyyaml"
    )
    source = next(step for step in job["steps"] if step.get("id") == "source")
    assert "head.repo.full_name == github.repository" in source["if"]
    assert "!startsWith(github.event.pull_request.head.ref, 'black-formatting/')" in (
        source["if"]
    )
    fix = next(step for step in job["steps"] if step.get("id") == "fix")
    assert fix["if"] == "steps.source.outputs.current == 'true'"
    assert fix["with"]["base"] == "${{ github.event.pull_request.head.ref }}"
    assert fix["with"]["branch"] == (
        "black-formatting/pr-${{ github.event.pull_request.number }}"
    )
    assert fix["with"]["delete-branch"] is True
    assert "branch-suffix" not in fix["with"]
    assert fix["with"]["add-paths"].splitlines() == [
        "python/mspasspy",
        "python/tests",
        "docs",
    ]

    report = next(
        step for step in job["steps"] if step.get("name") == "Report formatting changes"
    )
    assert report["if"] == "steps.format.outputs.changed == 'true'"
    assert "exit 1" in report["run"]
    cleanup = workflow["jobs"]["cleanup"]
    assert "github.event.action == 'closed'" in cleanup["if"]
    assert "!startsWith(github.event.pull_request.head.ref, 'black-formatting/')" in (
        cleanup["if"]
    )


def test_readme_documents_the_exact_check_and_format_commands():
    readme = README_PATH.read_text(encoding="utf-8")
    assert "python -m pip install 'black[jupyter]==25.1.0'" in readme
    assert CHECK_SCRIPT in readme
    assert FORMAT_SCRIPT in readme


@pytest.mark.parametrize(
    "state,sha,branch,repo,expected",
    [
        ("open", "expected", "feature", "owner/repo", True),
        ("open", "new-commit", "feature", "owner/repo", False),
        ("closed", "expected", "feature", "owner/repo", False),
        ("open", "expected", "renamed", "owner/repo", False),
        ("open", "expected", "feature", "fork/repo", False),
    ],
)
def test_publishing_rechecks_source_pr_before_writing(
    state, sha, branch, repo, expected
):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is provided by the GitHub Actions runner")
    script = next(
        step["with"]["script"]
        for step in _load_workflow()["jobs"]["black-format"]["steps"]
        if step.get("id") == "source"
    )
    current = {
        "state": state,
        "head": {"sha": sha, "ref": branch, "repo": {"full_name": repo}},
    }
    harness = """
const [script, current] = process.argv.slice(1);
const context = {
  repo: {owner: 'owner', repo: 'repo'}, issue: {number: 1033},
  payload: {pull_request: {head: {sha: 'expected', ref: 'feature'}}}
};
const github = {rest: {pulls: {get: async () => ({data: JSON.parse(current)})}}};
const core = {setOutput: (name, value) => process.stdout.write(JSON.stringify(value))};
const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
new AsyncFunction('github', 'context', 'core', script)(github, context, core);
"""
    result = subprocess.run(
        [node, "-e", harness, script, json.dumps(current)],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    assert json.loads(result.stdout) is expected


@pytest.mark.parametrize("with_notebook", [True, False])
def test_workflow_formats_real_files_and_emits_applicable_patch(
    tmp_path, with_notebook
):
    black = shutil.which("black")
    if not black:
        pytest.skip("Black is installed by the black-format workflow")
    version = subprocess.run(
        [black, "--version"], capture_output=True, text=True, check=True, timeout=10
    )
    if "25.1.0" not in version.stdout:
        pytest.skip("integration contract requires the workflow's Black 25.1.0")

    package = tmp_path / "python" / "mspasspy"
    tests = tmp_path / "python" / "tests"
    notebooks = tmp_path / "docs" / "guide with spaces"
    for directory in (package, tests, notebooks):
        directory.mkdir(parents=True)
    source = package / "sample.py"
    # Reproduce #1033: useful comments have trailing spaces, but no code change.
    original = "# Explain the file byte offset.  \nresult=  [1,2,3]\n"
    source.write_text(original)
    (tests / "sample.py").write_text("assert 1==1\n")
    notebook = notebooks / "example notebook.ipynb"
    if with_notebook:
        _write_notebook(notebook, "result=  [1,2,3]\n")
    (tmp_path / "docs" / "README.md").write_text("Documentation\n")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True, timeout=10)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, timeout=10)

    runner_temp = tmp_path / "runner"
    runner_temp.mkdir()
    output = runner_temp / "output"
    environment = dict(
        os.environ, RUNNER_TEMP=str(runner_temp), GITHUB_OUTPUT=str(output)
    )
    script = next(
        step["run"]
        for step in _load_workflow()["jobs"]["black-format"]["steps"]
        if step.get("id") == "format"
    )
    _run_workflow_script(script, tmp_path, environment)
    assert "changed=true" in output.read_text()
    assert ast.dump(ast.parse(source.read_text())) == ast.dump(ast.parse(original))
    assert "# Explain the file byte offset.\n" in source.read_text()
    if with_notebook:
        assert "result = [1, 2, 3]" in "".join(
            json.loads(notebook.read_text())["cells"][0]["source"]
        )
    patch = runner_temp / "black-format.patch"
    subprocess.run(
        ["git", "apply", "--reverse", "--check", str(patch)],
        cwd=tmp_path,
        check=True,
        timeout=10,
    )
    # Model merging the fix into the contributor's branch and running again.
    # Exercise the action's literal pathspecs, including a tree with no notebooks.
    paths = next(
        step["with"]["add-paths"].splitlines()
        for step in _load_workflow()["jobs"]["black-format"]["steps"]
        if step.get("id") == "fix"
    )
    subprocess.run(["git", "add", "--", *paths], cwd=tmp_path, check=True, timeout=10)
    output.write_text("")
    _run_workflow_script(script, tmp_path, environment)
    assert output.read_text().strip() == "changed=false"
    assert patch.read_bytes() == b""


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
