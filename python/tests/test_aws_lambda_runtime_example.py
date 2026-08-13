import importlib.util
from email.parser import BytesParser
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import zipfile

import boto3
from botocore.stub import ANY, Stubber
import pytest
import yaml

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
EXAMPLE_ROOT = REPOSITORY_ROOT / "scripts/aws_lambda_examples"
ARCHIVE_PATH = EXAMPLE_ROOT / "base.zip"
REQUIREMENTS_PATH = EXAMPLE_ROOT / "bundle-requirements.txt"


def _load_client_module():
    module_path = EXAMPLE_ROOT / "AwsLambdaClient.py"
    spec = importlib.util.spec_from_file_location(
        "aws_lambda_runtime_client", module_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _normalized_distribution_name(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def _locked_versions():
    text = REQUIREMENTS_PATH.read_text(encoding="utf-8")
    return {
        _normalized_distribution_name(name): version
        for name, version in re.findall(
            r"^([A-Za-z0-9_.-]+)==([^\s\\]+)", text, flags=re.MULTILINE
        )
    }


def test_create_function_uses_python_313_bundle_contract(tmp_path, monkeypatch):
    module = _load_client_module()
    client = module.AwsLambdaClient(
        "test-access-key",
        "test-secret-key",
        "lambda-upload-bucket",
        "arn:aws:iam::123456789012:role/lambda-test",
        "us-west-2",
    )

    with zipfile.ZipFile(tmp_path / "base.zip", "w") as archive:
        archive.writestr("process.py", "def handler(event, context): return event\n")
    shutil.copy2(EXAMPLE_ROOT / "aws_lambda_func_def.py", tmp_path)
    monkeypatch.chdir(tmp_path)

    credentials = {
        "region_name": "us-west-2",
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    s3_client = boto3.client("s3", **credentials)
    lambda_client = boto3.client("lambda", **credentials)
    clients = {"s3": s3_client, "lambda": lambda_client}
    monkeypatch.setattr(client, "create_aws_client", clients.__getitem__)

    expected_code = {
        "S3Bucket": "lambda-upload-bucket",
        "S3Key": "base.zip",
    }
    with Stubber(s3_client) as s3_stubber, Stubber(lambda_client) as lambda_stubber:
        s3_stubber.add_response(
            "put_object",
            {},
            {
                "Key": "base.zip",
                "Bucket": "lambda-upload-bucket",
                "Body": ANY,
            },
        )
        lambda_stubber.add_response(
            "create_function",
            {},
            {
                "FunctionName": "runtime-contract-test",
                "Runtime": "python3.13",
                "Role": "arn:aws:iam::123456789012:role/lambda-test",
                "Handler": "process.handler",
                "Code": expected_code,
                "Description": "",
                "Timeout": 300,
                "MemorySize": 1024,
                "Publish": True,
            },
        )
        client.create_lambda_function("runtime-contract-test")
        s3_stubber.assert_no_pending_responses()
        lambda_stubber.assert_no_pending_responses()


def test_bundle_is_python_313_native_and_fully_locked():
    locked_versions = _locked_versions()
    assert locked_versions
    with zipfile.ZipFile(ARCHIVE_PATH) as archive:
        members = archive.infolist()
        names = [member.filename for member in members]
        records = [
            archive.read(name) for name in names if name.endswith(".dist-info/RECORD")
        ]
        metadata = {}
        for name in names:
            if name.count("/") == 1 and name.endswith(".dist-info/METADATA"):
                parsed = BytesParser().parsebytes(archive.read(name))
                metadata[_normalized_distribution_name(parsed["Name"])] = parsed[
                    "Version"
                ]

    lowered_names = [name.lower() for name in names]
    assert not any(name.endswith(".pyc") for name in names)
    assert not any(
        marker in name
        for name in lowered_names
        for marker in ("python3.7", "python-37", "py3.7", "cpython-37")
    )
    tagged_extensions = [name for name in lowered_names if ".cpython-" in name]
    assert tagged_extensions
    assert all(".cpython-313-" in name for name in tagged_extensions)
    assert metadata == locked_versions
    assert records
    assert all(b"../../bin/" not in record for record in records)
    assert {"process.py", "aws_lambda_func_def.py"}.issubset(names)
    for package in ("numpy", "obspy", "scipy"):
        assert any(
            name.startswith(f"{package}/") and name.endswith(".so") for name in names
        )
    assert sum(member.file_size for member in members) <= 250 * 1024 * 1024


def test_build_uses_pinned_lambda_image_and_runs_abi_smoke():
    dockerfile = (EXAMPLE_ROOT / "Dockerfile.bundle").read_text(encoding="utf-8")
    assert (
        "public.ecr.aws/lambda/python:3.13@sha256:"
        "c9b4d923d571121a1b00e2e5b43ea046d163b6e88a0d2fbcd0bc3be465593545" in dockerfile
    )
    for option in ("--only-binary=:all:", "--require-hashes", "--no-compile"):
        assert option in dockerfile
    assert "python /tmp/bundle.py build /asset /base.zip" in dockerfile

    build_script = (EXAMPLE_ROOT / "build_base_zip.sh").read_text(encoding="utf-8")
    assert "docker buildx build" in build_script
    assert "--platform linux/amd64" in build_script
    dockerignore = (EXAMPLE_ROOT / ".dockerignore").read_text(encoding="utf-8")
    assert dockerignore.splitlines()[0] == "*"
    assert "base.zip" not in {
        line.removeprefix("!")
        for line in dockerignore.splitlines()
        if line.startswith("!")
    }

    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github/workflows/aws-lambda-bundle.yml").read_text(
            encoding="utf-8"
        )
    )
    steps = workflow["jobs"]["build-bundle"]["steps"]
    commands = [step.get("run", "") for step in steps]
    assert any("build_base_zip.sh /tmp/base.zip" in command for command in commands)
    comparison = next(command for command in commands if "diff --recursive" in command)
    assert "scripts/aws_lambda_examples/base.zip" in comparison
    assert "/tmp/base.zip" in comparison


@pytest.mark.skipif(
    sys.version_info[:2] != (3, 13), reason="the deployment ABI is Python 3.13"
)
def test_extracted_bundle_imports_all_modules_and_native_mseed(tmp_path):
    asset_root = tmp_path / "asset"
    with zipfile.ZipFile(ARCHIVE_PATH) as archive:
        archive.extractall(asset_root)

    verifier = tmp_path / "bundle.py"
    shutil.copy2(EXAMPLE_ROOT / "bundle.py", verifier)
    environment = os.environ.copy()
    environment.update(
        {
            "MPLCONFIGDIR": str(tmp_path / "matplotlib"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONNOUSERSITE": "1",
            "PYTHONPATH": str(asset_root),
        }
    )
    result = subprocess.run(
        [sys.executable, str(verifier), "verify", str(asset_root), str(ARCHIVE_PATH)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
