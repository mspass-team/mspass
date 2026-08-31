from dataclasses import replace
from io import BytesIO
from pathlib import Path

from botocore.exceptions import ClientError
import dask.distributed as ddist
import numpy as np
from obspy import Stream, Trace, UTCDateTime
import pytest

from mspasspy.workflow import sliding_window_pipeline
from scripts.earthscope_s3_example import workflow
from scripts.earthscope_s3_example import worker as worker_support
from scripts.earthscope_s3_example.worker import EarthScopeS3Worker


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        yield from self.pages


class FakeIndexS3:
    def __init__(self, pages):
        self.paginator = FakePaginator(pages)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self.paginator


class FakeCollection:
    def __init__(self, documents=()):
        self.documents = [dict(document) for document in documents]

    def find(self, query):
        return [
            document
            for document in self.documents
            if all(document.get(key) == value for key, value in query.items())
        ]

    def replace_one(self, query, document, upsert=False):
        assert upsert
        self.documents = [
            existing
            for existing in self.documents
            if not all(existing.get(key) == value for key, value in query.items())
        ]
        self.documents.append(dict(document))


class FakeBody:
    def __init__(self, payload=b"miniSEED", error=None, close_error=None):
        self.payload = payload
        self.error = error
        self.close_error = close_error
        self.closed = False

    def read(self):
        if self.error is not None:
            raise self.error
        return self.payload

    def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeObjectS3:
    def __init__(self, body):
        self.body = body

    def head_object(self, **kwargs):
        return {}

    def get_object(self, **kwargs):
        return {"Body": self.body}


class FakeRecord:
    def __init__(self, name, live=True, npts=1):
        self.name = name
        self.live = live
        self.npts = npts

    def dead(self):
        return not self.live


class FakeUploadS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, filename, bucket, key, Config=None):
        self.uploads.append((bucket, key, Path(filename).read_bytes(), Config))


class FakeWorker:
    def __init__(self):
        self.data = {}


class CloseTracker:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class WorkerOnlyPayload:
    def __reduce__(self):
        raise RuntimeError("worker-only payload crossed the process boundary")


def make_worker_only_payload(item):
    return item, WorkerOnlyPayload()


def consume_worker_only_payload(payload):
    item, large_value = payload
    assert isinstance(large_value, WorkerOnlyPayload)
    return {"ok": True, "item": item}


def _holding(net, sta, year, jday, key):
    return {"net": net, "sta": sta, "year": year, "jday": jday, "s3key": key}


def test_paginated_index_is_complete_deduplicated_and_deterministic():
    prefix = "miniseed/TA/2014/001/"
    first = [{"Key": f"{prefix}S{i}.TA.2014.001"} for i in range(1000)]
    second = [
        {"Key": f"{prefix}S1000.TA.2014.001#2"},
        {"Key": f"{prefix}S1001.TA.2014.001"},
        {"Key": f"{prefix}S1000.TA.2014.001#1"},
    ]
    client = FakeIndexS3([{"Contents": first}, {"Contents": second}])

    keys = workflow.list_station_day_keys(client, "TA", 2014, 1)

    assert len(keys) == 1002
    assert keys == sorted(keys)
    assert keys.count(f"{prefix}S1000.TA.2014.001") == 1
    assert len(client.paginator.calls) == 1

    collection = FakeCollection()
    result = workflow.index_station_days(
        client, collection, [None, "", "TA"], [(2014, 1)]
    )
    assert result == {"indexed": 1002}
    assert len(collection.documents) == 1002


def test_index_materializes_one_shot_days_for_every_network():
    client = FakeIndexS3([{"Contents": []}])
    collection = FakeCollection()

    workflow.index_station_days(
        client, collection, ["IU", "TA"], ((2014, day) for day in (1, 2))
    )

    assert len(client.paginator.calls) == 4


def test_cross_midnight_batches_have_no_omissions_or_duplicate_arrivals():
    day = UTCDateTime(year=2014, julday=1)
    arrivals = [
        {"arid": 1, "net": "TA", "sta": "TEST", "time": float(day + 43200)},
        {"arid": 2, "net": "TA", "sta": "TEST", "time": float(day + 86390)},
    ]
    holdings = FakeCollection(
        [
            _holding("TA", "TEST", 2014, 1, "day-1"),
            _holding("TA", "TEST", 2014, 2, "day-2"),
        ]
    )

    batches = workflow.build_station_batches(
        arrivals, holdings, -240, 300, pad=100, max_arrivals_per_batch=32
    )

    assert len(batches) == 1
    batch = batches[0]
    assert [request.arrival_id for request in batch.arrivals] == ["1", "2"]
    assert batch.object_keys == ("day-1", "day-2")
    assert batch.missing_days == ()

    only_crossing = workflow.build_station_batches(
        [arrivals[1]], holdings, -240, 300, pad=100
    )
    assert [request.arrival_id for request in only_crossing[0].arrivals] == ["2"]


def test_year_and_station_preparation_boundaries():
    query = workflow.year_query(2014)
    assert "$lt" in query["Ptime"]
    assert "$lte" not in query["Ptime"]
    assert query["Ptime"]["$lt"] == float(UTCDateTime(2015, 1, 1))
    days = list(workflow.index_days_for_year(2014))
    assert days[0] == (2013, 365)
    assert days[-1] == (2015, 1)
    assert workflow.normalized_networks(["TA", None, "", "IU", "TA"]) == [
        "IU",
        "TA",
    ]
    normalized = workflow.normalize_station(
        {"sta": "ORIGINAL", "arid": 1}, {}, default_network="TA"
    )
    assert normalized["net"] == "TA"
    assert normalized["sta"] == "ORIGINAL"
    matched = workflow.normalize_station(
        {"sta": "ALIAS", "arid": 2}, {"ALIAS": ("IU", "ANMO")}
    )
    assert matched["net"] == "IU"
    assert matched["sta"] == "ANMO"

    year_boundary = UTCDateTime(year=2016, julday=366) + 86399
    assert workflow.days_for_interval(
        float(year_boundary), float(year_boundary + 2)
    ) == ((2016, 366), (2017, 1))


def test_object_body_closes_and_errors_are_not_masked():
    body = FakeBody()
    marker = object()
    result = workflow.read_versioned_miniseed(
        FakeObjectS3(body), "base", stream_reader=lambda *args, **kwargs: marker
    )
    assert result is marker
    assert body.closed

    original = RuntimeError("read failed")
    body = FakeBody(error=original, close_error=RuntimeError("close failed"))
    try:
        workflow.read_versioned_miniseed(FakeObjectS3(body), "base")
    except RuntimeError as error:
        assert error is original
    else:
        raise AssertionError("read error was not propagated")
    assert body.closed

    interrupt = KeyboardInterrupt("stop")
    body = FakeBody(error=interrupt)
    try:
        workflow.read_versioned_miniseed(FakeObjectS3(body), "base")
    except KeyboardInterrupt as error:
        assert error is interrupt
    else:
        raise AssertionError("control exception was not propagated")
    assert body.closed


def test_non_missing_head_error_propagates():
    error = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}}, "HeadObject"
    )

    class DeniedS3:
        def head_object(self, **kwargs):
            raise error

    try:
        workflow.read_versioned_miniseed(DeniedS3(), "base")
    except ClientError as raised:
        assert raised is error
    else:
        raise AssertionError("access error was not propagated")


def test_versioned_reader_checks_base_then_descending_suffixes():
    body = FakeBody(payload=b"selected-version")

    class VersionedS3:
        def __init__(self):
            self.head_calls = []
            self.get_calls = []

        def head_object(self, **kwargs):
            self.head_calls.append(kwargs["Key"])
            if kwargs["Key"] != "base#2":
                raise ClientError(
                    {"Error": {"Code": "404", "Message": "missing"}},
                    "HeadObject",
                )
            return {}

        def get_object(self, **kwargs):
            self.get_calls.append(kwargs["Key"])
            return {"Body": body}

    client = VersionedS3()
    result = workflow.read_versioned_miniseed(
        client,
        "base",
        max_version=3,
        stream_reader=lambda buffer, **kwargs: buffer.read(),
    )

    assert client.head_calls == ["base", "base#3", "base#2"]
    assert client.get_calls == ["base#2"]
    assert result == b"selected-version"
    assert body.closed


def test_multirate_stream_converts_once_and_detrends_decoded_data():
    stream = Stream(
        [
            Trace(np.arange(8, dtype=float), {"channel": "BHZ", "sampling_rate": 1}),
            Trace(np.arange(8, dtype=float), {"channel": "BHN", "sampling_rate": 2}),
        ]
    )
    decoded = object()
    converted = []
    detrended = []

    def converter(merged):
        converted.append(merged)
        return decoded

    def apply_detrend(data, **kwargs):
        detrended.append((data, kwargs))
        return data

    result = workflow.prepare_stream(
        stream,
        detrend_type="simple",
        converter=converter,
        detrend_function=apply_detrend,
    )

    assert result is decoded
    assert len(converted) == 1
    assert {trace.stats.sampling_rate for trace in converted[0]} == {1, 2}
    assert detrended == [(decoded, {"type": "simple"})]


def test_writer_filters_records_streams_incrementally_and_is_idempotent(monkeypatch):
    request = workflow.WindowRequest("1", 1.0, 0.0, 2.0, {})
    batch = workflow.StationBatch(
        2014, 1, "TA", "TEST", 0, (request,), ("input",), (), "B*"
    )
    upload = FakeUploadS3()
    monkeypatch.setattr(workflow, "fetch_s3_client", lambda **kwargs: upload)

    station_stream = workflow.StationRecordStream(
        batch,
        iter(
            [
                FakeRecord("dead", live=False),
                FakeRecord("empty", npts=0),
                FakeRecord("kept"),
            ]
        ),
    )
    status = workflow.write_station_batch_records(
        station_stream, output_bucket="output", multipart_chunk_bytes=1024
    )

    assert status["ok"]
    assert status["records"] == 1
    assert len(upload.uploads) == 1
    bucket, key, payload, config = upload.uploads[0]
    assert bucket == "output"
    assert key == workflow.station_batch_object_key(batch)
    assert config.max_request_concurrency == 1
    objects = list(workflow.iter_record_stream(BytesIO(payload)))
    assert objects[0]["format"] == "mspass-earthscope-station-batch-v1"
    assert [record.name for record in objects[1:]] == ["kept"]

    assert workflow.station_batch_object_key(batch) == key
    assert workflow.station_batch_object_key(replace(batch, channel_select="H*")) != key


def test_writer_reports_missing_holding_with_initialized_zero_counts(monkeypatch):
    request = workflow.WindowRequest("1", 1.0, 0.0, 2.0, {})
    batch = workflow.StationBatch(
        2014,
        1,
        "TA",
        "TEST",
        0,
        (request,),
        (),
        ((2014, 2),),
        "B*",
    )
    upload = FakeUploadS3()
    monkeypatch.setattr(workflow, "fetch_s3_client", lambda **kwargs: upload)

    status = workflow.write_station_batch_records(
        workflow.StationRecordStream(batch, iter(())), output_bucket="output"
    )

    assert status["ok"] is False
    assert status["records"] == 0
    assert status["missing_days"] == 1


def test_run_station_batches_forwards_nondefault_version(monkeypatch):
    captured = {}

    def capture(*args, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(workflow, "sliding_window_pipeline", capture)
    assert (
        workflow.run_station_batches(
            [], object(), output_bucket="output", max_version=3
        )
        == []
    )
    assert captured["pfunc_kwargs"]["max_version"] == 3
    assert captured["sliding_window_size"] == 1
    assert captured["completion_on_worker"] is True
    assert captured["retain_results"] is True


def test_failed_worker_setup_has_safe_teardown(monkeypatch):
    worker = FakeWorker()
    plugin = EarthScopeS3Worker()

    def fail_setup():
        raise RuntimeError("setup failed")

    monkeypatch.setattr(worker_support, "create_earthscope_s3_client", fail_setup)
    with pytest.raises(RuntimeError, match="setup failed"):
        plugin.setup(worker)
    plugin.teardown(worker)

    client = CloseTracker()
    worker.data[plugin.key] = client
    plugin.teardown(worker)
    assert client.closed
    assert plugin.key not in worker.data


def test_worker_completion_keeps_unpickleable_payload_in_process():
    with ddist.LocalCluster(
        n_workers=1,
        threads_per_worker=1,
        processes=True,
        dashboard_address=None,
    ) as cluster:
        with ddist.Client(cluster) as client:
            result = sliding_window_pipeline(
                [7],
                make_worker_only_payload,
                client,
                sliding_window_size=1,
                completion_function=consume_worker_only_payload,
                completion_on_worker=True,
            )

    assert result == [{"ok": True, "item": 7}]
