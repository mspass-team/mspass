import os
import subprocess
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "support-matrix.yml"


def _load_workflow():
    with WORKFLOW_PATH.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def test_support_matrix_is_the_exact_advertised_cross_product():
    workflow = _load_workflow()
    assert workflow["on"] == {
        "pull_request": {"branches": ["master"]},
        "workflow_dispatch": None,
    }
    assert set(workflow["jobs"]) == {"support", "support-matrix"}

    support = workflow["jobs"]["support"]
    assert support["name"] == (
        "CPython ${{ matrix.python-version }} / ${{ matrix.arch }}"
    )
    combinations = support["strategy"]["matrix"]["include"]
    assert len(combinations) == 8
    assert {
        (entry["python-version"], entry["arch"], entry["runner"])
        for entry in combinations
    } == {
        (version, arch, runner)
        for version in ("3.10", "3.11", "3.12", "3.13")
        for arch, runner in (
            ("amd64", "ubuntu-latest"),
            ("arm64", "ubuntu-24.04-arm"),
        )
    }
    assert support["runs-on"] == "${{ matrix.runner }}"
    assert support["strategy"]["fail-fast"] is False
    assert "if" not in support
    assert "continue-on-error" not in support


def test_every_support_job_builds_imports_and_runs_the_workflow_test():
    support = _load_workflow()["jobs"]["support"]
    steps = support["steps"]
    runs = [step.get("run") for step in steps]

    install = next(
        step for step in steps if step.get("name") == "Build and install MsPASS"
    )
    assert (
        "python -m pip install --no-cache-dir --no-build-isolation ."
        in install["run"].splitlines()
    )
    assert "python -c 'import mspasspy, mspasspy.ccore'" in runs
    assert "pytest -q python/tests/test_workflow.py" in runs
    assert all("if" not in step for step in steps)
    assert all("continue-on-error" not in step for step in steps)
    setup_python = next(
        step for step in steps if step.get("name", "").startswith("Set up Python")
    )
    assert setup_python["uses"].startswith("actions/setup-python@")
    assert setup_python["with"]["python-version"] == "${{ matrix.python-version }}"


def test_support_matrix_aggregate_fails_unless_all_eight_jobs_succeed():
    aggregate = _load_workflow()["jobs"]["support-matrix"]

    assert aggregate == {
        "name": "support-matrix",
        "if": "${{ always() }}",
        "needs": "support",
        "runs-on": "ubuntu-latest",
        "steps": [
            {
                "name": "Require all advertised combinations",
                "env": {"SUPPORT_RESULT": "${{ needs.support.result }}"},
                "run": 'test "$SUPPORT_RESULT" = success',
            }
        ],
    }
    command = aggregate["steps"][0]["run"]
    for result in ("success", "failure", "cancelled", "skipped"):
        completed = subprocess.run(
            ["bash", "-c", command],
            env={**os.environ, "SUPPORT_RESULT": result},
            check=False,
        )
        assert completed.returncode == (0 if result == "success" else 1)


def test_workflow_has_read_only_permissions_and_no_branch_protection_mutation():
    workflow = _load_workflow()

    assert workflow["permissions"] == {"contents": "read"}
    serialized = WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "branches/master/protection" not in serialized
    assert "statuses:" not in serialized
