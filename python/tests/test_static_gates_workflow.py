import os
from pathlib import Path
import shlex
import subprocess

import pytest
import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
WORKFLOW_PATH = REPOSITORY_ROOT / ".github/workflows/static-gates.yml"


@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _step(job, name):
    return next(step for step in job["steps"] if step.get("name") == name)


def test_ruff_gate_is_exact_and_has_no_waiver(workflow):
    assert workflow["on"] == {
        "push": {"branches": ["master"]},
        "pull_request": {"branches": ["master"]},
        "workflow_dispatch": None,
    }
    ruff = workflow["jobs"]["ruff"]
    assert _step(ruff, "Install Ruff")["run"] == "python -m pip install ruff==0.12.8"

    command = _step(ruff, "Check fatal Python errors")["run"]
    assert shlex.split(command) == [
        "ruff",
        "check",
        "--select",
        "E9,F63,F7,F82",
        "python/mspasspy",
        "python/tests",
    ]
    assert "ignore" not in command.lower()
    assert "noqa" not in command.lower()


def test_native_path_filter_and_full_sanitizer_suites(workflow):
    jobs = workflow["jobs"]
    path_filter = next(
        step for step in jobs["changes"]["steps"] if step.get("id") == "filter"
    )
    assert yaml.safe_load(path_filter["with"]["filters"]) == {"native": ["cxx/**"]}

    expected = {
        "asan": (
            "address",
            "Configure ASan",
            "Run full ASan test suite",
            {"ASAN_OPTIONS": "halt_on_error=1:abort_on_error=1:detect_leaks=1"},
        ),
        "ubsan": (
            "undefined",
            "Configure UBSan",
            "Run full UBSan test suite",
            {"UBSAN_OPTIONS": "halt_on_error=1:print_stacktrace=1"},
        ),
    }
    cache_keys = set()
    for job_name, (
        sanitizer,
        configure_step_name,
        test_step_name,
        sanitizer_environment,
    ) in expected.items():
        job = jobs[job_name]
        assert job["needs"] == "changes"
        assert job["if"] == "${{ needs.changes.outputs.native == 'true' }}"
        assert job["env"]["CC"] == "clang"
        assert job["env"]["CXX"] == "clang++"
        assert job["env"]["BUILD_DIRECTORY"] == f"build-{job_name}"
        cache = _step(job, "Cache Boost 1.86")
        assert cache["uses"] == "actions/cache@v5"
        assert cache["with"]["path"] == "${{ runner.temp }}/mspass-boost-1.86.0"
        cache_key = cache["with"]["key"]
        assert "mspass-boost-1.86.0" in cache_key
        assert "${{ runner.os }}" in cache_key
        assert "${{ runner.arch }}" in cache_key
        assert "${{ hashFiles('**/boost-download.cmake') }}" in cache_key
        cache_keys.add(cache_key)
        assert _step(job, "Configure Boost 1.86 cache")["run"] == (
            'echo "MSPASS_BOOST_ROOT=${RUNNER_TEMP}/mspass-boost-1.86.0" '
            '>> "$GITHUB_ENV"'
        )
        configure = _step(job, configure_step_name)["run"]
        assert "-DBUILD_TESTING=ON" in configure
        assert "-DCMAKE_BUILD_TYPE=RelWithDebInfo" in configure
        assert configure.count(f"-fsanitize={sanitizer}") == 2
        if job_name == "ubsan":
            assert "-fno-sanitize-recover=undefined" in configure
        build = _step(job, f"Build {configure_step_name.removeprefix('Configure ')}")[
            "run"
        ]
        assert build == (
            'cmake --build "$BUILD_DIRECTORY" --config RelWithDebInfo --parallel 2'
        )
        assert "--target" not in build
        test_step = _step(job, test_step_name)
        assert test_step["run"] == "ctest --output-on-failure"
        assert test_step["env"] == sanitizer_environment
        assert all("continue-on-error" not in step for step in job["steps"])
    assert len(cache_keys) == 1
    serialized = str(workflow).lower()
    for forbidden in (
        "suppressions=",
        "sanitize-ignorelist",
        "sanitize-blacklist",
        "detect_leaks=0",
    ):
        assert forbidden not in serialized


def _run_aggregate(script, **results):
    environment = os.environ.copy()
    environment.update(results)
    return subprocess.run(
        ["bash", "-c", script],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    ).returncode


def test_static_gates_accepts_only_complete_or_not_applicable_states(workflow):
    aggregate = workflow["jobs"]["static-gates"]
    assert aggregate["name"] == "static-gates"
    assert aggregate["if"] == "${{ always() }}"
    assert aggregate["needs"] == ["changes", "ruff", "asan", "ubsan"]
    script = _step(aggregate, "Require every applicable gate")["run"]

    assert (
        _run_aggregate(
            script,
            CHANGES_RESULT="success",
            NATIVE_CHANGED="true",
            RUFF_RESULT="success",
            ASAN_RESULT="success",
            UBSAN_RESULT="success",
        )
        == 0
    )
    assert (
        _run_aggregate(
            script,
            CHANGES_RESULT="success",
            NATIVE_CHANGED="false",
            RUFF_RESULT="success",
            ASAN_RESULT="skipped",
            UBSAN_RESULT="skipped",
        )
        == 0
    )

    failures = [
        {"RUFF_RESULT": "failure"},
        {"CHANGES_RESULT": "failure"},
        {"ASAN_RESULT": "failure"},
        {"UBSAN_RESULT": "failure"},
    ]
    for failure in failures:
        results = {
            "CHANGES_RESULT": "success",
            "NATIVE_CHANGED": "true",
            "RUFF_RESULT": "success",
            "ASAN_RESULT": "success",
            "UBSAN_RESULT": "success",
        }
        results.update(failure)
        assert _run_aggregate(script, **results) != 0

    assert (
        _run_aggregate(
            script,
            CHANGES_RESULT="success",
            NATIVE_CHANGED="false",
            RUFF_RESULT="success",
            ASAN_RESULT="success",
            UBSAN_RESULT="success",
        )
        != 0
    )
    for invalid_native_state in ("", "unknown"):
        assert (
            _run_aggregate(
                script,
                CHANGES_RESULT="success",
                NATIVE_CHANGED=invalid_native_state,
                RUFF_RESULT="success",
                ASAN_RESULT="skipped",
                UBSAN_RESULT="skipped",
            )
            != 0
        )


def test_branch_protection_is_a_post_merge_admin_step(workflow):
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["jobs"]["changes"]["permissions"] == {
        "contents": "read",
        "pull-requests": "read",
    }
    serialized = WORKFLOW_PATH.read_text(encoding="utf-8")
    assert "branches/master/protection" not in serialized
    assert "statuses:" not in serialized

    guide = (
        REPOSITORY_ROOT / "docs/source/getting_started/static_gates_ci.rst"
    ).read_text(encoding="utf-8")
    normalized = " ".join(guide.split())
    assert "must not require ``static-gates`` until this workflow" in normalized
    assert "merged into ``master``" in normalized
    assert "add exactly ``static-gates``" in normalized
    assert "branch-protection API" in normalized
