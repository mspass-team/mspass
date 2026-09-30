"""Offline S3 worker lifecycle tests; no AWS credentials or network are used."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.config import Config
from botocore import UNSIGNED

import mspasspy.io.s3client as s3


@pytest.fixture(autouse=True)
def offline_clients(monkeypatch):
    s3.fetch_s3_client.cache_clear()
    monkeypatch.setattr(
        s3.boto3, "Session", Mock(side_effect=AssertionError("unmocked Session"))
    )
    monkeypatch.setattr(
        s3.boto3, "client", Mock(side_effect=AssertionError("unmocked client"))
    )
    monkeypatch.setattr(
        s3, "EarthScopeClient", Mock(side_effect=AssertionError("unmocked SDK"))
    )
    yield
    s3.fetch_s3_client.cache_clear()


def test_serial_default_session_cached(monkeypatch):
    session = Mock()
    factory = Mock(return_value=session)
    monkeypatch.setattr(s3.boto3, "Session", factory)
    assert s3.fetch_s3_client(parallel=False) is session.client.return_value
    assert s3.fetch_s3_client(parallel=False) is session.client.return_value
    factory.assert_called_once_with()
    session.client.assert_called_once()
    args, kwargs = session.client.call_args
    assert args == ("s3",)
    assert kwargs["config"].request_checksum_calculation == "when_required"
    assert kwargs["config"].response_checksum_validation == "when_required"


def test_serial_custom_session_and_config():
    session = Mock()
    config = Config(region_name="us-west-2")
    first = s3.fetch_s3_client(False, session, config)
    assert s3.fetch_s3_client(False, session, config) is first
    session.client.assert_called_once_with("s3", config=config)
    alternate = Mock()
    assert s3.fetch_s3_client(False, alternate, config) is alternate.client.return_value
    alternate.client.assert_called_once_with("s3", config=config)


def test_parallel_looks_up_current_worker_and_registration(monkeypatch):
    first, second, replacement = Mock(), Mock(), Mock()
    workers = [
        SimpleNamespace(data={"geolab_s3client": first}),
        SimpleNamespace(data={"geolab_s3client": second}),
    ]
    get_worker = Mock(side_effect=workers)
    monkeypatch.setattr(s3, "get_worker", get_worker)
    assert s3.fetch_s3_client() is first
    assert s3.fetch_s3_client() is second
    monkeypatch.setattr(s3, "get_worker", lambda: workers[1])
    workers[1].data["geolab_s3client"] = replacement
    assert s3.fetch_s3_client() is replacement
    workers[1].data["custom"] = first
    # Ignored serial parameters need not be hashable in worker mode.
    assert (
        s3.fetch_s3_client(session={}, session_config={}, worker_data_key="custom")
        is first
    )


def test_parallel_errors_include_cause_and_key(monkeypatch):
    failure = RuntimeError("outside worker")
    monkeypatch.setattr(s3, "get_worker", Mock(side_effect=failure))
    with pytest.raises(ValueError, match="Dask worker context") as error:
        s3.fetch_s3_client()
    assert error.value.__cause__ is failure
    monkeypatch.setattr(s3, "get_worker", lambda: SimpleNamespace(data={}))
    with pytest.raises(ValueError, match="name=missing") as error:
        s3.fetch_s3_client(worker_data_key="missing")
    assert isinstance(error.value.__cause__, KeyError)


@pytest.mark.parametrize("plugin_type", [s3.AnonymousS3Client, s3.StockS3Client])
def test_standard_plugin_configuration_and_lifecycle(monkeypatch, plugin_type):
    client = Mock()
    factory = Mock(return_value=client)
    monkeypatch.setattr(s3.boto3, "client", factory)
    plugin = plugin_type(key="custom", region="us-west-1")
    worker = SimpleNamespace(data={"unrelated": object()})
    plugin.teardown(worker)
    plugin.setup(worker)
    assert worker.data["custom"] is client
    args, kwargs = factory.call_args
    assert args == ("s3",)
    assert kwargs["region_name"] == "us-west-1"
    if plugin_type is s3.AnonymousS3Client:
        assert kwargs["config"].signature_version is UNSIGNED
    else:
        assert "config" not in kwargs
    plugin.teardown(worker)
    plugin.teardown(worker)
    client.close.assert_called_once_with()
    assert "custom" not in worker.data
    assert "unrelated" in worker.data


class RejectWrites(dict):
    def __setitem__(self, key, value):
        raise RuntimeError("registration failed")


@pytest.mark.parametrize("plugin_type", [s3.AnonymousS3Client, s3.StockS3Client])
def test_standard_plugin_setup_failures(monkeypatch, plugin_type):
    plugin = plugin_type()
    worker = SimpleNamespace(data=RejectWrites())
    client = Mock()
    factory = Mock(return_value=client)
    monkeypatch.setattr(s3.boto3, "client", factory)
    with pytest.raises(RuntimeError, match="registration failed"):
        plugin.setup(worker)
    client.close.assert_called_once_with()
    plugin.teardown(worker)
    factory.side_effect = RuntimeError("creation failed")
    with pytest.raises(RuntimeError, match="creation failed"):
        plugin.setup(worker)
    plugin.teardown(worker)


def geolab_mocks(monkeypatch):
    sdk = Mock()
    session = sdk.user.get_boto3_session.return_value
    client = session.client.return_value
    monkeypatch.setattr(s3, "EarthScopeClient", Mock(return_value=sdk))
    return sdk, session, client


def test_geolab_lifecycle_and_refresh_provider(monkeypatch):
    sdk, session, client = geolab_mocks(monkeypatch)
    plugin = s3.GeoLabS3Worker(key="custom")
    worker = SimpleNamespace(data={})
    plugin.teardown(worker)
    plugin.setup(worker)
    assert worker.data["custom"] is client
    sdk.user.get_boto3_session.assert_called_once_with()
    session.client.assert_called_once()
    sdk.close.assert_not_called()
    client.close.assert_not_called()
    plugin.teardown(worker)
    plugin.teardown(worker)
    sdk.close.assert_called_once_with()
    client.close.assert_called_once_with()
    assert "custom" not in worker.data


@pytest.mark.parametrize("stage", ["session", "client", "registration"])
def test_geolab_setup_failure_releases_acquired_resources(monkeypatch, stage):
    sdk, session, client = geolab_mocks(monkeypatch)
    worker = SimpleNamespace(data=RejectWrites() if stage == "registration" else {})
    if stage == "session":
        sdk.user.get_boto3_session.side_effect = RuntimeError("failed")
    elif stage == "client":
        session.client.side_effect = RuntimeError("failed")
    plugin = s3.GeoLabS3Worker()
    with pytest.raises(RuntimeError):
        plugin.setup(worker)
    sdk.close.assert_called_once_with()
    if stage == "registration":
        client.close.assert_called_once_with()
    else:
        client.close.assert_not_called()
    plugin.teardown(worker)
    assert not worker.data


def test_geolab_cleanup_continues_when_s3_close_fails(monkeypatch):
    sdk, session, client = geolab_mocks(monkeypatch)
    plugin = s3.GeoLabS3Worker()
    worker = SimpleNamespace(data={})
    plugin.setup(worker)
    client.close.side_effect = RuntimeError("close failed")
    with pytest.raises(RuntimeError, match="close failed"):
        plugin.teardown(worker)
    sdk.close.assert_called_once_with()
    plugin.teardown(worker)
    assert not worker.data


def test_geolab_does_not_reuse_closed_serial_cached_client(monkeypatch):
    sdk, session, client = geolab_mocks(monkeypatch)
    closed = s3.fetch_s3_client(False, session)
    closed.close()
    fresh = Mock()
    session.client.return_value = fresh
    plugin = s3.GeoLabS3Worker()
    worker = SimpleNamespace(data={})
    plugin.setup(worker)
    assert worker.data["geolab_s3client"] is fresh
    plugin.teardown(worker)
    fresh.close.assert_called_once_with()
    assert session.client.call_count == 2


@pytest.mark.parametrize(
    "plugin_type", [s3.AnonymousS3Client, s3.StockS3Client, s3.GeoLabS3Worker]
)
def test_teardown_preserves_replacement_client(monkeypatch, plugin_type):
    if plugin_type is s3.GeoLabS3Worker:
        _, _, client = geolab_mocks(monkeypatch)
    else:
        client = Mock()
        monkeypatch.setattr(s3.boto3, "client", Mock(return_value=client))
    plugin = plugin_type()
    worker = SimpleNamespace(data={})
    plugin.setup(worker)
    replacement = Mock()
    worker.data[plugin.worker_key] = replacement
    plugin.teardown(worker)
    assert worker.data[plugin.worker_key] is replacement
    replacement.close.assert_not_called()
    client.close.assert_called_once_with()


def test_serial_client_reads_existing_miniseed_via_stubbed_s3(monkeypatch):
    from io import BytesIO
    from pathlib import Path

    import boto3
    import numpy as np
    from botocore.response import StreamingBody
    from botocore.stub import Stubber
    from obspy import read

    payload = (Path(__file__).parents[1] / "data" / "3channels.mseed").read_bytes()
    expected = read(BytesIO(payload), format="MSEED")
    # Explicit test credentials keep boto3 away from the credential-provider chain.
    session = boto3.session.Session(
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    client = s3.fetch_s3_client(False, session)
    with Stubber(client) as stubber:
        stubber.add_response(
            "get_object",
            {
                "Body": StreamingBody(BytesIO(payload), len(payload)),
                "ContentLength": len(payload),
            },
            {"Bucket": "test-waveforms", "Key": "3channels.mseed"},
        )
        response = s3.fetch_s3_client(False, session).get_object(
            Bucket="test-waveforms", Key="3channels.mseed"
        )
        actual = read(BytesIO(response["Body"].read()), format="MSEED")
        stubber.assert_no_pending_responses()
    client.close()
    assert len(actual) == len(expected) == 3
    for result, reference in zip(actual, expected):
        assert result.stats == reference.stats
        np.testing.assert_array_equal(result.data, reference.data)
