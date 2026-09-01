import json

import pytest

import mspasspy.db.database as database_module
from mspasspy.ccore.utility import MsPASSError
from mspasspy.db.database import Database, _managed_response_stream


class FakeBody:
    def __init__(self, payload=b"", read_error=None, close_error=None):
        self.payload = payload
        self.read_error = read_error
        self.close_error = close_error
        self.close_calls = 0

    def read(self):
        if self.read_error is not None:
            raise self.read_error
        return self.payload

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class FakeS3Client:
    def __init__(self, body):
        self.body = body
        self.close_calls = 0

    def get_object(self, **kwargs):
        return {"Body": self.body}

    def close(self):
        self.close_calls += 1


@pytest.mark.parametrize("error", [RuntimeError("read failed"), KeyboardInterrupt()])
def test_managed_response_stream_preserves_active_error_when_close_fails(error):
    close_error = RuntimeError("close failed")
    body = FakeBody(read_error=error, close_error=close_error)

    with pytest.raises(BaseException) as raised:
        with _managed_response_stream(body) as stream:
            stream.read()

    assert raised.value is error
    assert body.close_calls == 1


def test_managed_response_stream_closes_on_success_and_reports_cleanup_failure():
    body = FakeBody(payload=b"payload")
    with _managed_response_stream(body) as stream:
        assert stream.read() == b"payload"
    assert body.close_calls == 1

    close_error = RuntimeError("close failed")
    body = FakeBody(close_error=close_error)
    with pytest.raises(RuntimeError) as raised:
        with _managed_response_stream(body):
            pass
    assert raised.value is close_error


def test_s3_continuous_closes_body_on_obspy_decode_failure(monkeypatch):
    body = FakeBody(payload=b"not miniseed")
    client = FakeS3Client(body)
    decode_error = RuntimeError("decode failed")
    monkeypatch.setattr(database_module.boto3, "client", lambda *args, **kwargs: client)
    monkeypatch.setattr(
        database_module.obspy,
        "read",
        lambda *args, **kwargs: (_ for _ in ()).throw(decode_error),
    )
    datum = {"year": "2014", "day_of_year": "001", "format": "mseed"}

    with pytest.raises(MsPASSError) as raised:
        Database._read_data_from_s3_continuous(datum)

    assert raised.value.__cause__ is decode_error
    assert body.close_calls == 1
    assert client.close_calls == 0


def test_s3_event_cache_closes_body_without_closing_caller_client(tmp_path):
    body = FakeBody(payload=b"complete object")
    client = FakeS3Client(body)
    destination = tmp_path / "cached.ms"

    Database._cache_s3_object(client, "bucket", "key", str(destination))

    assert destination.read_bytes() == b"complete object"
    assert body.close_calls == 1
    assert client.close_calls == 0


def test_continuous_index_closes_body_on_obspy_decode_failure(monkeypatch):
    body = FakeBody(payload=b"not miniseed")
    client = FakeS3Client(body)
    decode_error = RuntimeError("decode failed")
    monkeypatch.setattr(
        database_module.obspy,
        "read",
        lambda *args, **kwargs: (_ for _ in ()).throw(decode_error),
    )

    class FakeDatabase:
        def __getitem__(self, name):
            return object()

    with pytest.raises(MsPASSError) as raised:
        Database.index_mseed_s3_continuous(FakeDatabase(), client, 2014, 1)

    assert raised.value.__cause__ is decode_error
    assert body.close_calls == 1
    assert client.close_calls == 0


def test_lambda_closes_payload_on_json_decode_failure(monkeypatch):
    body = FakeBody(payload=b"not json")

    class FakeLambdaClient:
        def invoke(self, **kwargs):
            return {"Payload": body}

    monkeypatch.setattr(
        database_module.boto3, "client", lambda *args, **kwargs: FakeLambdaClient()
    )

    with pytest.raises(json.JSONDecodeError):
        Database._download_windowed_mseed_file(None, None, 2014, 1)

    assert body.close_calls == 1


def test_lambda_key_result_closes_both_response_streams_on_decode_failure(monkeypatch):
    payload = FakeBody(
        payload=json.dumps(
            {"ret_type": "key", "ret_value": "output-bucket::output-key"}
        ).encode()
    )
    output = FakeBody(payload=b"not miniseed")
    s3_client = FakeS3Client(output)

    class FakeLambdaClient:
        def invoke(self, **kwargs):
            return {"Payload": payload}

    def client_factory(*args, **kwargs):
        service = kwargs.get("service_name", args[0] if args else None)
        return FakeLambdaClient() if service == "lambda" else s3_client

    decode_error = RuntimeError("decode failed")
    monkeypatch.setattr(database_module.boto3, "client", client_factory)
    monkeypatch.setattr(
        database_module.obspy,
        "read",
        lambda *args, **kwargs: (_ for _ in ()).throw(decode_error),
    )

    with pytest.raises(MsPASSError) as raised:
        Database._download_windowed_mseed_file(None, None, 2014, 1)

    assert raised.value.__cause__ is decode_error
    assert payload.close_calls == 1
    assert output.close_calls == 1
    assert s3_client.close_calls == 0


def test_url_response_closes_before_obspy_decode(monkeypatch):
    response = FakeBody(payload=b"not miniseed")
    decode_error = RuntimeError("decode failed")
    monkeypatch.setattr(database_module.urllib.request, "urlopen", lambda url: response)
    monkeypatch.setattr(
        database_module.obspy,
        "read",
        lambda *args, **kwargs: (_ for _ in ()).throw(decode_error),
    )

    with pytest.raises(RuntimeError) as raised:
        Database._read_data_from_url({}, "https://example.invalid/waveform")

    assert raised.value is decode_error
    assert response.close_calls == 1


def test_url_read_error_is_preserved_when_close_also_fails(monkeypatch):
    read_error = RuntimeError("read failed")
    response = FakeBody(read_error=read_error, close_error=RuntimeError("close failed"))
    monkeypatch.setattr(database_module.urllib.request, "urlopen", lambda url: response)

    with pytest.raises(MsPASSError) as raised:
        Database._read_data_from_url({}, "https://example.invalid/waveform")

    assert raised.value.__cause__ is read_error
    assert response.close_calls == 1
