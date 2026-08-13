import io
import multiprocessing
import os
import subprocess
import time
import urllib.error
import uuid
from importlib.metadata import distribution, version
from pathlib import Path

import botocore.exceptions
import numpy as np
import obspy
import pymongo
import pytest

import mspasspy.db.database as database_module
from mspasspy.ccore.seismic import TimeSeries
from mspasspy.ccore.utility import ErrorSeverity
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


class FakeBody:
    def __init__(
        self,
        payload=b"",
        error=None,
        barrier=None,
        delay=0.0,
    ):
        self.payload = payload
        self.error = error
        self.barrier = barrier
        self.delay = delay

    def read(self):
        if self.barrier is not None:
            self.barrier.wait()
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.payload


class FakeS3Client:
    def __init__(
        self,
        payload=b"",
        error=None,
        read_error=None,
        barrier=None,
        delay=0.0,
    ):
        self.payload = payload
        self.error = error
        self.read_error = read_error
        self.barrier = barrier
        self.delay = delay
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {
            "Body": FakeBody(
                self.payload,
                error=self.read_error,
                barrier=self.barrier,
                delay=self.delay,
            )
        }


def make_event_timeseries(live=True):
    datum = TimeSeries()
    datum["year"] = "2017"
    datum["day_of_year"] = "005"
    datum["filename"] = "37780584"
    if live:
        datum.set_live()
    else:
        datum.kill()
    return datum


def s3_client_error(code, status):
    return botocore.exceptions.ClientError(
        {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "GetObject",
    )


def http_error(status):
    return urllib.error.HTTPError(
        "https://example.invalid/object",
        status,
        "test response",
        None,
        None,
    )


def temporary_cache_files(cache_path):
    return list(cache_path.parent.glob(f".{cache_path.name}.*.tmp"))


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    probe = pymongo.MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        probe.admin.command("ping")
    except pymongo.errors.PyMongoError as error:
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    finally:
        probe.close()

    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    name = "test_s3_event_contract_" + uuid.uuid4().hex
    try:
        database = Database(client, name)
        yield database
    finally:
        client.drop_database(name)
        client.close()


@pytest.fixture(scope="session", autouse=True)
def assert_database_module_loaded_from_selected_build():
    _assert_module_from_selected_build(database_module, Path("mspasspy/db/database.py"))


@pytest.mark.parametrize(
    "error_factory",
    [
        pytest.param(lambda: s3_client_error("NoSuchKey", 400), id="aws-nosuchkey"),
        pytest.param(lambda: http_error(404), id="http-404"),
    ],
)
@pytest.mark.parametrize("initially_live", [True, False])
def test_event_reader_not_found_returns_one_invalid_dead_datum(
    monkeypatch, tmp_path, error_factory, initially_live
):
    datum = make_event_timeseries(live=initially_live)
    datum.elog.log_error("existing", "keep this entry", ErrorSeverity.Complaint)
    initial_elog_size = datum.elog.size()
    error = error_factory()
    client = FakeS3Client(error=error)
    monkeypatch.setattr(database_module.boto3, "client", lambda *args, **kwargs: client)
    cache_path = tmp_path / "event.ms"

    result = Database._read_data_from_s3_event(
        Database,
        datum,
        str(tmp_path),
        cache_path.name,
        0,
        nbytes=1,
        format="mseed",
    )

    assert result is datum
    assert result.dead()
    assert result.elog.size() == initial_elog_size + 1
    assert result.elog[0].algorithm == "existing"
    entry = result.elog[initial_elog_size]
    assert entry.algorithm == "Database._read_data_from_s3_event"
    assert entry.badness == ErrorSeverity.Invalid
    assert "event_waveforms/2017/2017_005/37780584.ms" in entry.message
    assert not cache_path.exists()
    assert temporary_cache_files(cache_path) == []


@pytest.mark.parametrize(
    "error_location,error_factory",
    [
        pytest.param(
            "get_object",
            lambda: s3_client_error("AccessDenied", 403),
            id="aws-non-404",
        ),
        pytest.param(
            "body",
            lambda: urllib.error.URLError("network interrupted"),
            id="network",
        ),
        pytest.param(
            "client",
            lambda: TimeoutError("client setup timed out"),
            id="client-network-error",
        ),
    ],
)
@pytest.mark.parametrize("initially_live", [True, False])
def test_event_reader_rethrows_original_error_without_mutation_or_cache_damage(
    monkeypatch,
    tmp_path,
    error_location,
    error_factory,
    initially_live,
):
    datum = make_event_timeseries(live=initially_live)
    error = error_factory()
    if error_location == "get_object":
        client = FakeS3Client(error=error)
    else:
        client = FakeS3Client(read_error=error)
    if error_location == "client":

        def raise_client_error(*args, **kwargs):
            raise error

        monkeypatch.setattr(database_module.boto3, "client", raise_client_error)
    else:
        monkeypatch.setattr(
            database_module.boto3, "client", lambda *args, **kwargs: client
        )
    cache_path = tmp_path / "event.ms"
    original_cache = b"existing complete cache"
    cache_path.write_bytes(original_cache)

    real_exists = os.path.exists
    monkeypatch.setattr(
        database_module.os.path,
        "exists",
        lambda path: False if os.fspath(path) == str(cache_path) else real_exists(path),
    )

    with pytest.raises(type(error)) as raised:
        Database._read_data_from_s3_event(
            Database,
            datum,
            str(tmp_path),
            cache_path.name,
            0,
            nbytes=1,
            format="mseed",
        )

    assert raised.value is error
    assert datum.live is initially_live
    assert datum.elog.size() == 0
    assert cache_path.read_bytes() == original_cache
    assert temporary_cache_files(cache_path) == []


@pytest.mark.parametrize(
    "error_location,error_factory",
    [
        pytest.param(
            "get_object",
            lambda: s3_client_error("AccessDenied", 403),
            id="aws-non-404",
        ),
        pytest.param(
            "body",
            lambda: urllib.error.URLError("network interrupted"),
            id="network",
        ),
        pytest.param(
            "body",
            lambda: TimeoutError("network stream timed out"),
            id="generic-network-error",
        ),
    ],
)
def test_event_index_rethrows_original_error_and_preserves_existing_cache(
    database, tmp_path, error_location, error_factory
):
    error = error_factory()
    cache_path = tmp_path / "event.ms"
    original_cache = b"previous complete cache"
    cache_path.write_bytes(original_cache)
    if error_location == "get_object":
        client = FakeS3Client(error=error)
    else:
        client = FakeS3Client(read_error=error)

    with pytest.raises(type(error)) as raised:
        database.index_mseed_s3_event(
            client,
            2017,
            5,
            37780584,
            cache_path.name,
            dir=tmp_path,
            collection="wf_miniseed",
        )

    assert raised.value is error
    assert cache_path.read_bytes() == original_cache
    assert temporary_cache_files(cache_path) == []
    assert database["wf_miniseed"].count_documents({}) == 0


def test_cache_download_fsyncs_same_directory_temp_before_publish(
    monkeypatch, tmp_path
):
    cache_path = tmp_path / "event.ms"
    payload = b"complete cache payload"
    fsync_calls = []
    real_fsync = os.fsync

    def recording_fsync(descriptor):
        fsync_calls.append(descriptor)
        return real_fsync(descriptor)

    monkeypatch.setattr(database_module.os, "fsync", recording_fsync)

    Database._cache_s3_object(
        FakeS3Client(payload=payload), "bucket", "key", str(cache_path)
    )

    assert cache_path.read_bytes() == payload
    assert len(fsync_calls) == 1
    assert temporary_cache_files(cache_path) == []


@pytest.mark.parametrize(
    "error_factory",
    [
        pytest.param(lambda: s3_client_error("404", 404), id="aws-404"),
        pytest.param(lambda: http_error(404), id="http-404"),
    ],
)
def test_event_index_not_found_returns_without_cache_or_documents(
    database, tmp_path, error_factory
):
    client = FakeS3Client(error=error_factory())
    cache_path = tmp_path / "event.ms"

    result = database.index_mseed_s3_event(
        client,
        2017,
        5,
        37780584,
        cache_path.name,
        dir=tmp_path,
        collection="wf_miniseed",
    )

    assert result is None
    assert not cache_path.exists()
    assert temporary_cache_files(cache_path) == []
    assert database["wf_miniseed"].count_documents({}) == 0


def test_public_event_index_then_read_dispatches_and_recaches(
    database, monkeypatch, tmp_path
):
    source_path = SOURCE_PYTHON_ROOT / "tests" / "data" / "37780584.ms"
    payload = source_path.read_bytes()
    client = FakeS3Client(payload=payload)
    cache_path = tmp_path / "event.ms"

    database.index_mseed_s3_event(
        client,
        2017,
        5,
        37780584,
        cache_path.name,
        dir=tmp_path,
        collection="wf_miniseed",
    )
    documents = list(database["wf_miniseed"].find({}).sort("foff", 1))
    assert len(documents) == 344
    assert documents[0]["foff"] == 0
    assert documents[-1]["foff"] + documents[-1]["nbytes"] == len(payload)
    assert all(document["storage_mode"] == "s3_event" for document in documents)
    cache_path.unlink()
    monkeypatch.setattr(database_module.boto3, "client", lambda *args, **kwargs: client)

    for document in (documents[0], documents[-1]):
        result = database.read_data(document, collection="wf_miniseed")

        assert result.live
        assert result["is_abortion"] is False
        expected = obspy.read(
            io.BytesIO(
                payload[document["foff"] : document["foff"] + document["nbytes"]]
            ),
            format="MSEED",
        )
        expected.merge()
        np.testing.assert_allclose(
            np.asarray(result.data), expected[0].data.astype("float64")
        )
    assert cache_path.read_bytes() == payload
    assert temporary_cache_files(cache_path) == []
    expected_call = {
        "Bucket": "scedc-pds",
        "Key": "event_waveforms/2017/2017_005/37780584.ms",
    }
    assert client.calls == [expected_call, expected_call]


def cache_worker(cache_path, payload, barrier, delay):
    client = FakeS3Client(payload=payload, barrier=barrier, delay=delay)
    Database._cache_s3_object(client, "bucket", "key", cache_path)


def test_multiprocess_cache_publication_is_atomic_and_cleans_all_temps(tmp_path):
    assert hasattr(Database, "_cache_s3_object")
    context = multiprocessing.get_context("spawn")
    cache_path = tmp_path / "event.ms"
    payloads = [bytes([value]) * (256 * 1024) for value in range(1, 5)]
    barrier = context.Barrier(len(payloads))
    processes = [
        context.Process(
            target=cache_worker,
            args=(str(cache_path), payload, barrier, 0.0 if index == 0 else 0.2),
        )
        for index, payload in enumerate(payloads)
    ]
    for process in processes:
        process.start()

    observations = []
    deadline = time.monotonic() + 30.0
    try:
        while any(process.is_alive() for process in processes):
            if time.monotonic() > deadline:
                pytest.fail("S3 cache contention workers did not finish")
            try:
                observed = cache_path.read_bytes()
            except FileNotFoundError:
                time.sleep(0.001)
                continue
            observations.append(observed)
            assert observed in payloads
            time.sleep(0.001)
    finally:
        for process in processes:
            process.join(timeout=1.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=1.0)

    for process in processes:
        assert process.exitcode == 0

    assert observations
    final_payload = cache_path.read_bytes()
    assert final_payload in payloads
    assert all(observed == final_payload for observed in observations)
    assert temporary_cache_files(cache_path) == []
