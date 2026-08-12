import json
import os
from pathlib import Path
import subprocess
import sys

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
import pytest
import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
SUPPORTED_PYTHON_VERSIONS = ("3.10", "3.11", "3.12", "3.13")


@pytest.fixture(scope="module")
def distribution_requirements(tmp_path_factory):
    report_path = tmp_path_factory.mktemp("dependency-metadata") / "pip-report.json"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--dry-run",
            "--ignore-installed",
            "--no-deps",
            "--no-build-isolation",
            "--report",
            str(report_path),
            ".[test,seisbench]",
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr

    report = json.loads(report_path.read_text(encoding="utf-8"))
    packages = [
        item
        for item in report["install"]
        if item["metadata"]["name"].lower() == "mspasspy"
    ]
    assert len(packages) == 1
    return [Requirement(raw) for raw in packages[0]["metadata"]["requires_dist"]]


def _applies(requirement, python_version, extra):
    environment = default_environment()
    environment.update(
        python_version=python_version,
        python_full_version=f"{python_version}.0",
        extra=extra,
    )
    return requirement.marker is None or requirement.marker.evaluate(environment)


def test_moto_is_exposed_only_by_the_test_extra(distribution_requirements):
    moto_requirements = [
        requirement
        for requirement in distribution_requirements
        if requirement.name.lower() == "moto"
    ]
    assert len(moto_requirements) == 1
    moto = moto_requirements[0]
    assert moto.specifier == SpecifierSet(">=5.2.2,<5.3.0")

    for python_version in SUPPORTED_PYTHON_VERSIONS:
        assert not _applies(moto, python_version, "")
        assert _applies(moto, python_version, "test")
        test_only = [
            requirement
            for requirement in distribution_requirements
            if _applies(requirement, python_version, "test")
            and not _applies(requirement, python_version, "")
        ]
        assert [requirement.name.lower() for requirement in test_only] == ["moto"]


def test_seisbench_has_one_reachable_unconditional_branch(distribution_requirements):
    seisbench_requirements = [
        requirement
        for requirement in distribution_requirements
        if requirement.name.lower() == "seisbench"
    ]
    assert len(seisbench_requirements) == 1
    requirement = seisbench_requirements[0]
    assert requirement.specifier == SpecifierSet("<=0.10.2")
    assert "python_version" not in str(requirement.marker)

    for python_version in SUPPORTED_PYTHON_VERSIONS:
        assert not _applies(requirement, python_version, "")
        reachable = [
            candidate
            for candidate in seisbench_requirements
            if _applies(candidate, python_version, "seisbench")
        ]
        assert reachable == [requirement]


def test_generated_dependency_manifests_are_aligned():
    result = subprocess.run(
        [sys.executable, "scripts/sync_dependencies.py", "--check"],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "moto" not in (REPOSITORY_ROOT / "meta.yaml").read_text(encoding="utf-8")


def test_test_runners_install_the_test_extra():
    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github/workflows/python-package.yml").read_text(
            encoding="utf-8"
        )
    )
    install_step = next(
        step
        for step in workflow["jobs"]["build"]["steps"]
        if step.get("name") == "Install"
    )
    assert "'.[seisbench,test]'" in install_step["run"]

    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    runtime_package = dockerfile.split("FROM mspass-source AS runtime-package", 1)[
        1
    ].split("FROM mspass-source AS dev-package", 1)[0]
    dev_package = dockerfile.split("FROM mspass-source AS dev-package", 1)[1].split(
        "FROM runtime-package AS runtime-common", 1
    )[0]
    assert "pip3 install /mspass -v" in runtime_package
    assert "/mspass[" not in runtime_package
    assert "pip3 install '/mspass[seisbench,test]' -v" in dev_package
