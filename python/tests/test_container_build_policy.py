import fnmatch
import hashlib
import re
import shutil
import subprocess
from pathlib import Path, PurePosixPath

import pytest
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = REPOSITORY_ROOT / "Dockerfile"
CONDA_DOCKERFILE = REPOSITORY_ROOT / "scripts" / "Dockerfile_conda_build"
CONDA_RECIPE = REPOSITORY_ROOT / "meta.yaml"
DOCKERIGNORE = REPOSITORY_ROOT / ".dockerignore"
VERIFY_DOWNLOAD = REPOSITORY_ROOT / "scripts" / "verify-container-download.sh"


def _dockerignore_match(pattern, path):
    pattern = pattern.lstrip("/").rstrip("/")
    path = path.strip("/")
    if pattern == "**":
        return True
    if pattern.endswith("/**"):
        prefix = pattern[:-3].rstrip("/")
        return path == prefix or path.startswith(prefix + "/")
    if "/" not in pattern:
        return any(fnmatch.fnmatchcase(part, pattern) for part in path.split("/"))
    if fnmatch.fnmatchcase(path, pattern):
        return True
    return any(
        fnmatch.fnmatchcase(str(parent), pattern)
        for parent in PurePosixPath(path).parents
        if str(parent) != "."
    )


def _is_in_build_context(path):
    excluded = False
    for raw_line in DOCKERIGNORE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        include = line.startswith("!")
        pattern = line[1:] if include else line
        if _dockerignore_match(pattern, path):
            excluded = not include
    return not excluded


def _dockerfile_instructions(path):
    instructions = []
    current = ""
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        current = (current + " " + line).strip()
        if current.endswith("\\"):
            current = current[:-1].rstrip()
            continue
        instructions.append(current)
        current = ""
    assert not current
    return instructions


def _external_base_images(path):
    arguments = {}
    internal_stages = set()
    external_images = []
    for instruction in _dockerfile_instructions(path):
        keyword, _, value = instruction.partition(" ")
        if keyword.upper() == "ARG" and "=" in value:
            name, default = value.split("=", 1)
            arguments[name] = default
        if keyword.upper() != "FROM":
            continue
        parts = value.split()
        image = parts[0]
        match = re.fullmatch(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", image)
        if match:
            image = arguments[match.group(1)]
        if image != "scratch" and image not in internal_stages:
            external_images.append(image)
        if len(parts) == 3 and parts[1].upper() == "AS":
            internal_stages.add(parts[2])
    return external_images


def _load_workflow(path):
    with path.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def test_docker_context_is_allowlisted_and_excludes_sensitive_sentinels():
    included = [
        "Dockerfile",
        "LICENSE",
        "README.md",
        "cxx/CMakeLists.txt",
        "data/yaml/mspass.yaml",
        "docker-entrypoint.sh",
        "docs/requirements.txt",
        "meta.yaml",
        "pyproject.toml",
        "cxx/test/mseed/test.msd",
        "python/mspasspy/client.py",
        "python/mspasspy/db/database.py",
        "python/tests/data/3channels.mseed",
        "python/tests/data/MCXcor_testdata.pickle",
        "python/tests/data/MCXcorStacking_data_generator.ipynb",
        "python/tests/db/test_database.py",
        "scripts/Dockerfile_conda_build",
        "scripts/conda_build.sh",
        "scripts/start-mspass.sh",
        "scripts/start-mspass-geolab.sh",
        "scripts/start-mspass-geolab-entrypoint.sh",
        "scripts/verify-container-download.sh",
    ]
    excluded = [
        ".git/config",
        ".env",
        "credentials.json",
        "untracked-local-file.txt",
        "python/mspasspy/.env.local",
        "python/mspasspy/cloud-credentials.json",
        "python/mspasspy/__pycache__/module.pyc",
        "python/mspasspy/build/extension.so",
        "cxx/cmake-build-debug/CMakeCache.txt",
        "docs/_build/index.html",
        "python/mspasspy/user-data/private-trace.mseed",
        "scripts/aws_lambda_examples/base.zip",
        "scripts/IU_examples/python/test.py",
        "scripts/untracked-local-file.txt",
        ".docker-build-assets/spark.tgz",
        "db/WiredTiger",
        "logs/mongo.log",
    ]

    assert all(_is_in_build_context(path) for path in included)
    assert not any(_is_in_build_context(path) for path in excluded)


def test_no_container_layer_copies_git_or_sensitive_context():
    for path in (DOCKERFILE, CONDA_DOCKERFILE):
        for instruction in _dockerfile_instructions(path):
            keyword, _, value = instruction.partition(" ")
            if keyword.upper() in {"ADD", "COPY"}:
                assert ".git" not in value.split()
    assert not _is_in_build_context(".git/HEAD")


def test_docker_context_layer_excludes_sensitive_sentinels(tmp_path):
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker is not installed")
    try:
        daemon = subprocess.run(
            [docker, "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        pytest.skip("Docker daemon is not responding")
    if daemon.returncode != 0:
        pytest.skip("Docker daemon is not available")

    context = tmp_path / "context"
    context.mkdir()
    shutil.copyfile(DOCKERIGNORE, context / ".dockerignore")
    sentinels = {
        "Dockerfile": "FROM scratch\nCOPY . /context\n",
        "python/mspasspy/included.py": "included = True\n",
        ".git/config": "must not enter the context\n",
        ".env": "must not enter the context\n",
        "credentials.json": "must not enter the context\n",
        "python/mspasspy/__pycache__/module.pyc": "must not enter the context\n",
        "db/WiredTiger": "must not enter the context\n",
    }
    for relative_path, content in sentinels.items():
        sentinel = context / relative_path
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text(content, encoding="utf-8")

    output = tmp_path / "image-root"
    build = subprocess.run(
        [
            docker,
            "buildx",
            "build",
            "--progress=plain",
            "--output",
            f"type=local,dest={output}",
            str(context),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    exported_context = output / "context"
    assert (exported_context / "python/mspasspy/included.py").is_file()
    for excluded_path in sentinels.keys() - {
        "Dockerfile",
        "python/mspasspy/included.py",
    }:
        assert not (exported_context / excluded_path).exists()


def test_remote_download_and_base_image_policy():
    digest_pattern = re.compile(r"@sha256:[0-9a-f]{64}$")
    for path in (DOCKERFILE, CONDA_DOCKERFILE):
        for image in _external_base_images(path):
            assert digest_pattern.search(image), image

        for instruction in _dockerfile_instructions(path):
            keyword, _, value = instruction.partition(" ")
            if keyword.upper() == "ADD":
                assert not re.search(r"https?://", value)
            if keyword.upper() == "RUN":
                assert not re.search(r"\b(?:curl|wget)\b[^;]*\|", value)

    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    for required_digest in (
        "SPARK_SHA512",
        "TINI_SHA256_AMD64",
        "TINI_SHA256_ARM64",
        "JSYAML_SHA256",
        "PYBIND11_SHA256",
    ):
        assert re.search(rf"ARG {required_digest}=[0-9a-f]+", dockerfile)
    assert dockerfile.count("verify-container-download.sh") >= 6
    assert (
        "MONGODB_KEY_FINGERPRINT=39BD841E4BE5FB195A65400E6A26B1AE64C3C388" in dockerfile
    )
    assert dockerfile.index(
        'verify-container-download.sh sha512 "${SPARK_SHA512}"'
    ) < dockerfile.index("tar -xzf /tmp/spark.tgz")
    assert dockerfile.index(
        'verify-container-download.sh sha256 "${tini_sha256}"'
    ) < dockerfile.index("chmod 0755 /tini")
    assert dockerfile.index(
        'verify-container-download.sh sha256 "${PYBIND11_SHA256}"'
    ) < dockerfile.index("tar -xzf /tmp/pybind11.tar.gz")
    assert dockerfile.count(
        'test "${actual_fingerprint}" = "${MONGODB_KEY_FINGERPRINT}"'
    ) == dockerfile.count("gpg --batch --dearmor")


def test_checksum_verifier_rejects_corrupt_content(tmp_path):
    artifact = tmp_path / "artifact"
    artifact.write_bytes(b"verified build input\n")
    correct_digest = hashlib.sha256(artifact.read_bytes()).hexdigest()

    valid = subprocess.run(
        ["sh", str(VERIFY_DOWNLOAD), "sha256", correct_digest, str(artifact)],
        capture_output=True,
        text=True,
        check=False,
    )
    corrupt = subprocess.run(
        ["sh", str(VERIFY_DOWNLOAD), "sha256", "0" * 64, str(artifact)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert valid.returncode == 0, valid.stderr
    assert corrupt.returncode != 0


def test_release_targets_receive_and_verify_explicit_provenance():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    provenance_requirement = (
        "setuptools_scm==8.3.1 "
        "--hash=sha256:332ca0d43791b818b841213e76b1971b7711a960761c5bea5fc5cdb5196fbce3"
    )
    assert "ADD .git" not in dockerfile
    assert dockerfile.count("SETUPTOOLS_SCM_PRETEND_VERSION_FOR_MSPASSPY") == 3
    assert dockerfile.count("md.version('mspasspy')") == 2
    assert "'mspasspy':'${MSPASS_VERSION}'" in dockerfile
    assert dockerfile.count("org.opencontainers.image.version") == 2
    assert dockerfile.count("org.opencontainers.image.revision") == 2
    assert dockerfile.count('test -n "${MSPASS_VCS_REF}"') == 3

    docker_workflow_path = (
        REPOSITORY_ROOT / ".github" / "workflows" / "docker-publish.yml"
    )
    docker_workflow_text = docker_workflow_path.read_text(encoding="utf-8")
    assert docker_workflow_text.count(provenance_requirement) == 1
    assert "--require-hashes" in docker_workflow_text
    workflow = _load_workflow(docker_workflow_path)
    build_targets = set()
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            if step.get("uses") != "docker/build-push-action@v7":
                continue
            needs = job.get("needs", [])
            if isinstance(needs, str):
                needs = [needs]
            assert "download-spark" in needs
            options = step["with"]
            build_targets.add(options["target"])
            build_arguments = options.get("build-args", "")
            assert "MSPASS_VERSION=" in build_arguments
            assert "MSPASS_VCS_REF=" in build_arguments
            for build_argument in build_arguments.splitlines():
                if build_argument.startswith("MSPASS_BASE_IMAGE="):
                    assert re.search(r"@sha256:[0-9a-f]{64}$", build_argument)
    assert {"runtime", "dev", "geolab"}.issubset(build_targets)

    conda_workflow_path = REPOSITORY_ROOT / ".github" / "workflows" / "conda-build.yml"
    conda_workflow_text = conda_workflow_path.read_text(encoding="utf-8")
    assert conda_workflow_text.count(provenance_requirement) == 2
    assert conda_workflow_text.count("--require-hashes") == 2
    conda_workflow = _load_workflow(conda_workflow_path)
    conda_container_steps = [
        step
        for job in conda_workflow["jobs"].values()
        for step in job.get("steps", [])
        if step.get("uses") == "docker/build-push-action@v7"
    ]
    assert conda_container_steps
    assert all(
        "MSPASS_VERSION=" in step["with"].get("build-args", "")
        and "MSPASS_VCS_REF=" in step["with"].get("build-args", "")
        for step in conda_container_steps
    )
    assert "ADD .git" not in CONDA_DOCKERFILE.read_text(encoding="utf-8")

    conda_recipe = CONDA_RECIPE.read_text(encoding="utf-8")
    assert "environ.get('MSPASS_VERSION')" in conda_recipe
    assert "environ.get('MSPASS_VCS_REF')" in conda_recipe
    assert "SETUPTOOLS_SCM_PRETEND_VERSION_FOR_MSPASSPY" in conda_recipe
