import os
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "publish-pypi.yml"
README_PATH = REPOSITORY_ROOT / "README.md"


def _load_workflow():
    with WORKFLOW_PATH.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def _step(job, name):
    return next(step for step in job["steps"] if step.get("name") == name)


def test_wheel_job_has_exactly_four_linux_x86_64_cpython_targets():
    workflow = _load_workflow()
    wheel_job = workflow["jobs"]["build-wheels"]
    build = _step(wheel_job, "Build, repair, and clean-test Linux x86_64 wheels")
    environment = build["env"]

    assert wheel_job["runs-on"] == "ubuntu-latest"
    assert environment["CIBW_PLATFORM"] == "linux"
    assert environment["CIBW_ARCHS_LINUX"] == "x86_64"
    assert environment["CIBW_BUILD"].split() == [
        "cp310-manylinux_x86_64",
        "cp311-manylinux_x86_64",
        "cp312-manylinux_x86_64",
        "cp313-manylinux_x86_64",
    ]
    assert environment["CIBW_MANYLINUX_X86_64_IMAGE"] == "manylinux_2_28"
    assert environment["CIBW_BEFORE_ALL_LINUX"] == (
        "dnf install -y gcc-gfortran gsl-devel lapack-devel && "
        "tar -xJf /opt/_internal/static-libs-for-embedding-only.tar.xz "
        "-C /opt/_internal"
    )
    assert environment["CIBW_ENVIRONMENT"] == "MSPASS_CMAKE_BUILD_TYPE=Release"
    serialized = str(wheel_job).lower()
    for unsupported in ("macos", "windows", "aarch64", "arm64", "musllinux"):
        assert unsupported not in serialized


def test_every_wheel_is_repaired_inspected_checked_and_clean_tested():
    wheel_job = _load_workflow()["jobs"]["build-wheels"]
    install = _step(wheel_job, "Install wheel build and inspection tools")["run"]
    build = _step(wheel_job, "Build, repair, and clean-test Linux x86_64 wheels")
    inspect = _step(wheel_job, "Inspect and validate every repaired wheel")["run"]

    assert "cibuildwheel==4.1.1" in install
    assert build["run"] == "python -m cibuildwheel --output-dir wheelhouse"
    assert (
        build["env"]["CIBW_REPAIR_WHEEL_COMMAND_LINUX"]
        == "auditwheel repair -w {dest_dir} {wheel}"
    )
    assert "import mspasspy, mspasspy.ccore" in build["env"]["CIBW_TEST_COMMAND"]
    assert "TimeSeries(3)" in build["env"]["CIBW_TEST_COMMAND"]
    assert "PeakAmplitude(d) == 2.5" in build["env"]["CIBW_TEST_COMMAND"]
    assert 'test "${#wheels[@]}" -eq 4' in inspect
    assert 'python -m twine check "${wheels[@]}"' in inspect
    assert 'auditwheel show "$wheel"' in inspect
    assert "delocate" not in str(wheel_job).lower()
    assert "delvewheel" not in str(wheel_job).lower()
    upload = _step(wheel_job, "Upload wheel artifacts")
    assert upload["with"] == {
        "name": "pypi-wheels-linux-x86_64",
        "path": "wheelhouse/*.whl",
    }


def test_only_release_tags_trusted_publish_all_five_artifacts():
    workflow = _load_workflow()
    assert workflow["on"]["pull_request"] == {"branches": ["master"]}
    assert workflow["on"]["push"] == {"tags": ["v*.*.*"]}

    sdist = workflow["jobs"]["build-sdist"]
    sdist_build = _step(sdist, "Build source distribution")["run"]
    assert "python -m build --sdist" in sdist_build
    assert "python -m twine check dist/*" in sdist_build
    assert _step(sdist, "Upload distribution artifact")["with"] == {
        "name": "pypi-sdist",
        "path": "dist/",
    }

    publish = workflow["jobs"]["publish-pypi"]
    assert workflow["permissions"] == {"contents": "read"}
    assert set(publish["needs"]) == {"build-sdist", "build-wheels"}
    assert publish["if"] == "startsWith(github.ref, 'refs/tags/v')"
    assert publish["permissions"] == {"id-token": "write"}
    assert _step(publish, "Download distribution artifact")["with"]["name"] == (
        "pypi-sdist"
    )
    assert _step(publish, "Download wheel artifacts")["with"]["name"] == (
        "pypi-wheels-linux-x86_64"
    )
    artifact_guard = _step(publish, "Require one sdist and exactly four wheels")["run"]
    assert "-name '*.tar.gz'" in artifact_guard
    assert "-name '*.whl'" in artifact_guard
    assert '" -eq 1' in artifact_guard
    assert '" -eq 4' in artifact_guard
    assert '" -eq 5' in artifact_guard
    publisher = _step(publish, "Publish package distributions to PyPI")
    assert publisher["uses"] == "pypa/gh-action-pypi-publish@release/v1"
    assert publisher["with"] == {"packages-dir": "dist/"}
    for name, job in workflow["jobs"].items():
        if name != "publish-pypi":
            assert job.get("permissions", {}).get("id-token") != "write"
            assert "gh-action-pypi-publish" not in str(job)


def test_documentation_limits_binary_wheels_to_supported_targets():
    readme = README_PATH.read_text(encoding="utf-8")
    assert "Linux x86_64" in readme
    for version in ("3.10", "3.11", "3.12", "3.13"):
        assert version in readme
    assert "macOS, Windows, and Linux arm64 wheels are not" in readme
    assert "Source package: [PyPI]" not in readme
