import os
from pathlib import Path
import re
import subprocess
from uuid import uuid4

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "conda-build.yml"
DOCKERFILE_PATH = REPOSITORY_ROOT / "scripts" / "Dockerfile_conda_build"
UPLOAD_SCRIPT = REPOSITORY_ROOT / "scripts" / "upload_conda_package.sh"


def _secret_paths(value, path=()):
    paths = []
    if isinstance(value, dict):
        for key, child in value.items():
            paths.extend(_secret_paths(child, path + (str(key),)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            paths.extend(_secret_paths(child, path + (str(index),)))
    elif isinstance(value, str) and "secrets.CONDA_TOKEN" in value:
        paths.append(path)
    return paths


def test_docker_build_and_export_have_no_credential_input():
    dockerfile = DOCKERFILE_PATH.read_text()
    assert "ANACONDA_API_TOKEN" not in dockerfile
    assert "CONDA_TOKEN" not in dockerfile
    assert not re.search(r"\banaconda\b.*\bupload\b", dockerfile)
    assert not re.search(r"^ARG\s+\S*TOKEN", dockerfile, flags=re.MULTILINE)
    assert dockerfile.rstrip().endswith(
        "FROM scratch AS package\nCOPY --from=build /conda-package/ /"
    )

    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    for job_name in ("conda-build-arm64", "conda-build-amd64"):
        steps = workflow["jobs"][job_name]["steps"]
        build_step = next(
            step for step in steps if step.get("uses") == "docker/build-push-action@v7"
        )
        serialized_build = yaml.safe_dump(build_step)
        assert "CONDA_TOKEN" not in serialized_build
        assert "ANACONDA_API_TOKEN" not in serialized_build
        assert build_step["with"]["target"] == "package"
        assert build_step["with"]["outputs"].startswith("type=local,dest=")
        assert build_step["with"]["build-args"].strip().startswith("PYTHON_VERSION=")


def test_conda_secret_is_scoped_only_to_host_upload_steps():
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    secret_paths = _secret_paths(workflow)
    assert len(secret_paths) == 3

    for job_name, job in workflow["jobs"].items():
        assert "env" not in job
        upload_steps = [
            step for step in job["steps"] if step.get("name", "").startswith("Upload ")
        ]
        assert len(upload_steps) == 1, job_name
        upload_step = upload_steps[0]
        assert upload_step["env"] == {
            "ANACONDA_API_TOKEN": "${{ secrets.CONDA_TOKEN }}"
        }
        assert "upload_conda_package.sh" in upload_step["run"]

        for step in job["steps"]:
            if step is upload_step:
                continue
            assert "secrets.CONDA_TOKEN" not in yaml.safe_dump(step)
            assert "ANACONDA_API_TOKEN" not in yaml.safe_dump(step)


def test_each_package_is_built_before_its_upload_step():
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    for job_name, job in workflow["jobs"].items():
        steps = job["steps"]
        upload_index = next(
            index
            for index, step in enumerate(steps)
            if step.get("name", "").startswith("Upload ")
        )
        if job_name == "conda-build-osx":
            build_index = next(
                index
                for index, step in enumerate(steps)
                if step.get("name") == "Build conda package"
            )
            assert "conda build" in steps[build_index]["run"]
            assert "export PYTHON_VERSION=" in steps[build_index]["run"]
            assert (
                '"${{ steps.build_package.outputs.package_path }}"'
                in steps[upload_index]["run"]
            )
        else:
            build_index = next(
                index
                for index, step in enumerate(steps)
                if step.get("uses") == "docker/build-push-action@v7"
            )
            assert steps[build_index]["with"]["outputs"] == (
                "type=local,dest=${{ runner.temp }}/conda-package"
            )
            assert '"${RUNNER_TEMP}/conda-package"' in steps[upload_index]["run"]
        assert build_index < upload_index
        assert "conda build" not in steps[upload_index]["run"]


def _fake_anaconda(tmp_path):
    executable = tmp_path / "anaconda"
    executable.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        '[[ "$ANACONDA_API_TOKEN" == "$EXPECTED_TOKEN" ]]\n'
        'printf \'%s\\n\' "$@" > "$CAPTURE_FILE"\n'
    )
    executable.chmod(0o755)
    return executable


@pytest.mark.parametrize(
    "python_version,expected_labels,use_directory",
    (
        ("3.12", ["--label", "py3.12"], False),
        ("3.13", ["--label", "main", "--label", "py3.13"], True),
    ),
)
def test_upload_script_calls_a_mock_client_without_logging_the_token(
    tmp_path, python_version, expected_labels, use_directory
):
    sentinel = "mspass-conda-secret-" + uuid4().hex
    package_dir = tmp_path / "artifact"
    package_dir.mkdir()
    package = package_dir / "mspass-package.conda"
    package.write_bytes(b"credential-free package")
    (package_dir / "build-metadata.txt").write_text("credential-free metadata")
    assert sentinel.encode() not in package.read_bytes()
    source = package_dir if use_directory else package
    capture = tmp_path / "client-arguments"
    environment = os.environ.copy()
    environment.update(
        ANACONDA_API_TOKEN=sentinel,
        EXPECTED_TOKEN=sentinel,
        CAPTURE_FILE=str(capture),
    )

    result = subprocess.run(
        [
            str(UPLOAD_SCRIPT),
            str(source),
            python_version,
            str(_fake_anaconda(tmp_path)),
        ],
        env=environment,
        text=True,
        capture_output=True,
        check=True,
    )

    assert sentinel not in result.stdout
    assert sentinel not in result.stderr
    assert capture.read_text().splitlines() == [
        "upload",
        str(package),
        *expected_labels,
        "--force",
    ]
    assert sentinel not in capture.read_text()


@pytest.mark.parametrize("package_count", (0, 2))
def test_upload_script_rejects_an_ambiguous_artifact_before_client_call(
    tmp_path, package_count
):
    package_dir = tmp_path / "artifact"
    package_dir.mkdir()
    for index in range(package_count):
        (package_dir / f"package-{index}.conda").write_bytes(b"package")
    capture = tmp_path / "client-arguments"
    environment = os.environ.copy()
    environment.update(
        ANACONDA_API_TOKEN="test-token",
        EXPECTED_TOKEN="test-token",
        CAPTURE_FILE=str(capture),
    )

    result = subprocess.run(
        [
            str(UPLOAD_SCRIPT),
            str(package_dir),
            "3.13",
            str(_fake_anaconda(tmp_path)),
        ],
        env=environment,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    assert f"expected exactly one conda package, found {package_count}" in result.stderr
    assert not capture.exists()


def test_upload_script_rejects_a_non_package_file_before_client_call(tmp_path):
    wrong_artifact = tmp_path / "not-a-package.txt"
    wrong_artifact.write_bytes(b"not a conda package")
    capture = tmp_path / "client-arguments"
    environment = os.environ.copy()
    environment.update(
        ANACONDA_API_TOKEN="test-token",
        EXPECTED_TOKEN="test-token",
        CAPTURE_FILE=str(capture),
    )

    result = subprocess.run(
        [
            str(UPLOAD_SCRIPT),
            str(wrong_artifact),
            "3.13",
            str(_fake_anaconda(tmp_path)),
        ],
        env=environment,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1
    assert "package source is not a conda package" in result.stderr
    assert not capture.exists()


def test_upload_script_requires_the_token_before_client_call(tmp_path):
    package = tmp_path / "package.conda"
    package.write_bytes(b"package")
    capture = tmp_path / "client-arguments"
    environment = os.environ.copy()
    environment.pop("ANACONDA_API_TOKEN", None)
    environment["CAPTURE_FILE"] = str(capture)

    result = subprocess.run(
        [
            str(UPLOAD_SCRIPT),
            str(package),
            "3.13",
            str(_fake_anaconda(tmp_path)),
        ],
        env=environment,
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "ANACONDA_API_TOKEN is required" in result.stderr
    assert not capture.exists()
