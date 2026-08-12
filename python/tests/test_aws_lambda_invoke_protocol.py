import base64
import importlib.util
import io
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest
from botocore.response import StreamingBody
from botocore.stub import Stubber

REPOSITORY_ROOT = Path(
    os.environ.get("MSPASS_TEST_REPOSITORY_ROOT", Path(__file__).resolve().parents[2])
)
EXAMPLE_DIR = REPOSITORY_ROOT / "scripts" / "aws_lambda_examples"


def _load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, EXAMPLE_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def client_module():
    return _load_module("mspass_aws_lambda_client_test", "AwsLambdaClient.py")


@pytest.fixture
def process_module(monkeypatch):
    function_module = _load_module("aws_lambda_func_def", "aws_lambda_func_def.py")
    monkeypatch.setitem(sys.modules, "aws_lambda_func_def", function_module)
    return _load_module("mspass_aws_lambda_process_test", "process.py")


class TrackingStream(io.BytesIO):
    def __init__(self, value):
        super().__init__(value)
        self.was_closed = False

    def close(self):
        self.was_closed = True
        super().close()


class RaisingStream:
    def __init__(self):
        self.was_closed = False

    def read(self):
        raise OSError("read failed")

    def close(self):
        self.was_closed = True


class LambdaClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def _client(client_module, response):
    client = client_module.AwsLambdaClient(
        "access",
        "secret",
        "upload-bucket",
        "role",
        "eu-central-1",
    )
    lambda_client = LambdaClient(response)
    client.create_aws_client = lambda service: lambda_client
    return client, lambda_client


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {"ret_type": "content", "ret_value": base64.b64encode(b"abc").decode()},
            {"ret_type": "content", "ret_value": b"abc"},
        ),
        (
            {"ret_type": "key", "ret_value": "results/output.mseed"},
            {"ret_type": "key", "ret_value": "results/output.mseed"},
        ),
    ],
)
def test_client_accepts_exact_response_protocol_and_closes_stream(
    client_module, payload, expected
):
    stream = TrackingStream(json.dumps(payload).encode("utf-8"))
    client, lambda_client = _client(client_module, {"Payload": stream})

    assert client.call_lambda_function("window", {"duration": 10}) == expected
    assert stream.was_closed
    assert lambda_client.calls == [
        {
            "FunctionName": "window",
            "InvocationType": "RequestResponse",
            "LogType": "Tail",
            "Payload": json.dumps({"duration": 10}),
        }
    ]


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {"ret_type": "content", "ret_value": "YQ=="},
            {"ret_type": "content", "ret_value": b"a"},
        ),
        (
            {"ret_type": "key", "ret_value": "outputs/a.mseed"},
            {"ret_type": "key", "ret_value": "outputs/a.mseed"},
        ),
    ],
)
def test_real_stubber_response_types_in_alternate_region(
    client_module, payload, expected
):
    client = client_module.AwsLambdaClient(
        "access", "secret", "bucket", "role", "ap-southeast-2"
    )
    lambda_client = client.create_aws_client("lambda")
    assert lambda_client.meta.region_name == "ap-southeast-2"
    request = {"duration": 10}
    raw_payload = json.dumps(payload).encode("utf-8")
    raw_stream = io.BytesIO(raw_payload)
    stream = StreamingBody(raw_stream, len(raw_payload))
    with Stubber(lambda_client) as stubber:
        stubber.add_response(
            "invoke",
            {"StatusCode": 200, "Payload": stream},
            {
                "FunctionName": "window",
                "InvocationType": "RequestResponse",
                "LogType": "Tail",
                "Payload": json.dumps(request),
            },
        )
        client.create_aws_client = lambda service: lambda_client
        assert client.call_lambda_function("window", request) == expected
    assert raw_stream.closed


@pytest.mark.parametrize(
    ("response", "match"),
    [
        (
            {
                "Payload": TrackingStream(b'{"ret_type":"content","ret_value":"YQ=="}'),
                "FunctionError": "Unhandled",
            },
            "FunctionError",
        ),
        ({"Payload": TrackingStream(b"\xff")}, "UTF-8/JSON"),
        ({"Payload": TrackingStream(b"not json")}, "UTF-8/JSON"),
        ({"Payload": TrackingStream(b"[]")}, "malformed response"),
        (
            {"Payload": TrackingStream(b'{"ret_type":"content"}')},
            "malformed response",
        ),
        (
            {
                "Payload": TrackingStream(
                    b'{"return_type":"content","ret_value":"YQ=="}'
                )
            },
            "malformed response",
        ),
        (
            {"Payload": TrackingStream(b'{"ret_type":"content","ret_value":1}')},
            "non-string base64",
        ),
        (
            {
                "Payload": TrackingStream(
                    b'{"ret_type":"content","ret_value":"not base64"}'
                )
            },
            "invalid base64",
        ),
        (
            {"Payload": TrackingStream(b'{"ret_type":"key","ret_value":""}')},
            "invalid S3 key",
        ),
        (
            {"Payload": TrackingStream(b'{"ret_type":"key","ret_value":1}')},
            "invalid S3 key",
        ),
        (
            {"Payload": TrackingStream(b'{"ret_type":"value","ret_value":"YQ=="}')},
            "unknown ret_type",
        ),
        (
            {
                "Payload": TrackingStream(
                    b'{"ret_type":"key","ret_value":"x","extra":1}'
                )
            },
            "malformed response",
        ),
    ],
)
def test_client_rejects_every_malformed_or_failed_response(
    client_module, response, match
):
    client, _ = _client(client_module, response)
    stream = response["Payload"]

    with pytest.raises(RuntimeError, match=match):
        client.call_lambda_function("window", {})
    assert stream.was_closed


def test_client_wraps_payload_read_failure_and_closes_stream(client_module):
    stream = RaisingStream()
    client, _ = _client(client_module, {"Payload": stream})

    with pytest.raises(RuntimeError, match="Failed to read.*window"):
        client.call_lambda_function("window", {})
    assert stream.was_closed


def test_client_rejects_missing_payload_stream(client_module):
    client, _ = _client(client_module, {})

    with pytest.raises(RuntimeError, match="no readable Payload"):
        client.call_lambda_function("window", {})


def test_client_uses_the_configured_session_region(client_module, monkeypatch):
    calls = []

    class Session:
        def __init__(self, **kwargs):
            calls.append(("session", kwargs))

        def client(self, service):
            calls.append(("client", service))
            return object()

    monkeypatch.setattr(client_module.boto3, "Session", Session)
    client = client_module.AwsLambdaClient(
        "access", "secret", "bucket", "role", "ap-southeast-2"
    )

    client.create_aws_client("s3")
    assert calls == [
        (
            "session",
            {
                "aws_access_key_id": "access",
                "aws_secret_access_key": "secret",
                "region_name": "ap-southeast-2",
            },
        ),
        ("client", "s3"),
    ]


def test_lambda_archive_stream_is_closed_after_upload(
    client_module, monkeypatch, tmp_path
):
    archive = tmp_path / "base.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("process.py", "old process")
        bundle.writestr("aws_lambda_func_def.py", "old function")
        bundle.writestr("dependency.txt", "keep dependency")
    (tmp_path / "process.py").write_text("new process", encoding="utf-8")
    (tmp_path / "aws_lambda_func_def.py").write_text("new function", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    uploaded = {}

    class S3:
        def put_object(self, **kwargs):
            uploaded.update(kwargs)
            assert not kwargs["Body"].closed
            uploaded["BodyBytes"] = kwargs["Body"].read()

    class Lambda:
        def create_function(self, **kwargs):
            pass

    archive_updates = []
    client = client_module.AwsLambdaClient(
        "access", "secret", "bucket", "role", "eu-central-1"
    )
    update_archive = client._updateZip

    def tracked_update(zip_path, filename):
        archive_updates.append((zip_path, filename))
        update_archive(zip_path, filename)

    client._updateZip = tracked_update
    client.create_aws_client = lambda service: S3() if service == "s3" else Lambda()

    client.create_lambda_function("window")
    assert archive_updates == [
        ("base.zip", "process.py"),
        ("base.zip", "aws_lambda_func_def.py"),
    ]
    assert uploaded["Body"].closed
    with zipfile.ZipFile(io.BytesIO(uploaded["BodyBytes"])) as uploaded_archive:
        assert uploaded_archive.read("process.py") == b"new process"
        assert uploaded_archive.read("aws_lambda_func_def.py") == b"new function"
        assert uploaded_archive.read("dependency.txt") == b"keep dependency"

    failed_upload = {}

    class FailingS3:
        def put_object(self, **kwargs):
            failed_upload.update(kwargs)
            raise RuntimeError("archive upload failed")

    client.create_aws_client = lambda service: (
        FailingS3() if service == "s3" else Lambda()
    )
    with pytest.raises(RuntimeError, match="archive upload failed"):
        client.create_lambda_function("window")
    assert failed_upload["Body"].closed


class S3Client:
    def __init__(self, source=b"source", failure=None):
        self.source = source
        self.failure = failure
        self.download_paths = []
        self.uploads = []

    def download_file(self, bucket, key, path):
        self.download_paths.append((bucket, key, path))
        if self.failure == "download":
            raise RuntimeError("download failed")
        Path(path).write_bytes(self.source)

    def upload_file(self, path, bucket, key):
        self.uploads.append((path, bucket, key, Path(path).read_bytes()))
        if self.failure == "upload":
            raise RuntimeError("upload failed")


class Session:
    def __init__(self, s3, region="eu-west-3"):
        self.s3 = s3
        self.region_name = region
        self.services = []

    def client(self, service):
        self.services.append(service)
        return self.s3


def _request(**updates):
    request = {
        "src_bucket": "source-bucket",
        "dst_bucket": "destination-bucket",
        "src_key": "input/source.mseed",
        "dst_key": "output/result.mseed",
    }
    request.update(updates)
    return request


def _install_process_fakes(monkeypatch, process_module, s3, output, failure=None):
    session = Session(s3)
    output_paths = []

    def lambda_function(input_path, event):
        assert Path(input_path).read_bytes() == s3.source
        if failure == "process":
            raise RuntimeError("process failed")
        handle = tempfile.NamedTemporaryFile(delete=False)
        handle.write(output)
        handle.close()
        output_paths.append(handle.name)
        return handle.name

    monkeypatch.setattr(process_module.boto3, "Session", lambda: session)
    monkeypatch.setattr(process_module, "lambda_func", lambda_function)
    return session, output_paths


def _assert_owned_files_removed(s3, output_paths):
    assert s3.download_paths
    assert not Path(s3.download_paths[0][2]).exists()
    assert all(not Path(path).exists() for path in output_paths)


def test_process_uses_session_region_returns_content_and_cleans_files(
    process_module, monkeypatch
):
    s3 = S3Client()
    session, output_paths = _install_process_fakes(
        monkeypatch, process_module, s3, b"processed"
    )

    response = process_module.process(_request())
    assert response == {
        "ret_type": "content",
        "ret_value": base64.b64encode(b"processed").decode("utf-8"),
    }
    assert session.region_name == "eu-west-3"
    assert session.services == ["s3"]
    assert s3.uploads == []
    _assert_owned_files_removed(s3, output_paths)


def test_process_uploads_oversized_final_json_and_returns_only_the_key(
    process_module, monkeypatch
):
    s3 = S3Client()
    session, output_paths = _install_process_fakes(
        monkeypatch, process_module, s3, b"x" * 4_499_971
    )

    response = process_module.process(_request())
    assert response == {"ret_type": "key", "ret_value": "output/result.mseed"}
    assert session.services == ["s3"]
    assert len(s3.uploads) == 1
    assert s3.uploads[0][1:] == (
        "destination-bucket",
        "output/result.mseed",
        b"x" * 4_499_971,
    )
    _assert_owned_files_removed(s3, output_paths)


def test_process_keeps_largest_reachable_inline_content(process_module, monkeypatch):
    s3 = S3Client()
    _, output_paths = _install_process_fakes(
        monkeypatch, process_module, s3, b"x" * 4_499_970
    )

    response = process_module.process(_request())
    serialized = process_module._serialized_response(response)
    assert len(serialized) == 5_999_997
    assert response["ret_type"] == "content"
    assert s3.uploads == []
    _assert_owned_files_removed(s3, output_paths)


@pytest.mark.parametrize(
    ("size", "expected_type"),
    [(5_999_999, "content"), (6_000_000, "content"), (6_000_001, "key")],
)
def test_final_serialized_invoke_boundaries(
    process_module, monkeypatch, size, expected_type
):
    s3 = S3Client()
    _, output_paths = _install_process_fakes(
        monkeypatch, process_module, s3, b"processed"
    )
    monkeypatch.setattr(
        process_module, "_serialized_response", lambda response: b"x" * size
    )

    response = process_module.process(_request())

    assert response["ret_type"] == expected_type
    assert len(s3.uploads) == (expected_type == "key")
    _assert_owned_files_removed(s3, output_paths)


def test_content_response_serialization_is_exactly_compact_utf8(process_module):
    assert (
        process_module._serialized_response(
            {"ret_type": "content", "ret_value": "YQ=="}
        )
        == b'{"ret_type":"content","ret_value":"YQ=="}'
    )
    assert process_module._serialized_response(
        {"ret_type": "key", "ret_value": "r\N{LATIN SMALL LETTER E WITH ACUTE}sultat"}
    ) == '{"ret_type":"key","ret_value":"résultat"}'.encode("utf-8")


@pytest.mark.parametrize("failure", ["download", "process", "serialization", "upload"])
def test_process_removes_owned_files_on_every_failure(
    process_module, monkeypatch, failure
):
    s3 = S3Client(failure=failure if failure in {"download", "upload"} else None)
    _, output_paths = _install_process_fakes(
        monkeypatch,
        process_module,
        s3,
        b"processed",
        failure=failure,
    )
    request = _request(save_to_s3=failure == "upload")
    if failure == "serialization":
        monkeypatch.setattr(
            process_module,
            "_serialized_response",
            lambda response: (_ for _ in ()).throw(
                RuntimeError("serialization failed")
            ),
        )

    with pytest.raises(RuntimeError, match=f"{failure} failed"):
        process_module.process(request)
    _assert_owned_files_removed(s3, output_paths)


def test_process_removes_input_if_temporary_file_close_fails(
    process_module, monkeypatch
):
    s3 = S3Client()
    session = Session(s3)
    real_named_temporary_file = tempfile.NamedTemporaryFile
    input_paths = []

    class CloseFailure:
        def __init__(self, *args, **kwargs):
            self._handle = real_named_temporary_file(*args, **kwargs)
            self.name = self._handle.name
            input_paths.append(self.name)

        def close(self):
            self._handle.close()
            raise RuntimeError("temporary file close failed")

    monkeypatch.setattr(process_module.boto3, "Session", lambda: session)
    monkeypatch.setattr(process_module.tempfile, "NamedTemporaryFile", CloseFailure)

    with pytest.raises(RuntimeError, match="temporary file close failed"):
        process_module.process(_request())

    assert len(input_paths) == 1
    assert not Path(input_paths[0]).exists()
