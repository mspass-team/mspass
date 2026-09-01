import io
import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
from bson import ObjectId

import mspasspy.ccore.seismic as seismic_binding
import mspasspy.db.database as database_module
from mspasspy.ccore.seismic import DoubleVector, Seismogram, TimeSeries
from mspasspy.ccore.utility import AtomicType, ErrorSeverity, dmatrix
from mspasspy.db.database import Database

NPTS = 3


class _TrackingBytesIO(io.BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.length = len(payload)
        self.read_sizes = []
        self.close_calls = 0

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)

    def close(self):
        self.close_calls += 1
        super().close()


def _datum_with_sentinel_samples(atomic_type):
    datum = atomic_type()
    datum.npts = NPTS
    datum.set_live()
    datum["contract_marker"] = atomic_type.__name__
    history_type = (
        AtomicType.TIMESERIES if atomic_type is TimeSeries else AtomicType.SEISMOGRAM
    )
    datum.set_as_origin("contract-seed", "issue-812", "seed-uuid", history_type)
    if atomic_type is TimeSeries:
        for index in range(NPTS):
            datum.data[index] = -10.0 - index
    else:
        for component in range(3):
            for index in range(NPTS):
                datum.data[component, index] = -10.0 * component - index
    return datum


def _exact_payload(atomic_type):
    sample_count = NPTS if atomic_type is TimeSeries else 3 * NPTS
    values = np.arange(sample_count, dtype=np.float64) + 0.25
    return values, values.tobytes()


def _metadata_and_history_snapshot(datum):
    node = datum.current_nodedata()
    return (
        dict(datum),
        datum.is_origin(),
        (
            node.algorithm,
            node.algid,
            node.uuid,
            node.stage,
            node.status,
            node.type,
        ),
    )


def _assert_metadata_and_history_unchanged(datum, snapshot):
    metadata, is_origin, node_data = snapshot
    assert dict(datum) == metadata
    assert datum.is_origin() is is_origin
    node = datum.current_nodedata()
    assert (
        node.algorithm,
        node.algid,
        node.uuid,
        node.stage,
        node.status,
        node.type,
    ) == node_data


def _assert_read_is_bounded(payload_reader, expected_nbytes):
    assert payload_reader.read_sizes
    assert all(size >= 0 for size in payload_reader.read_sizes)
    assert sum(payload_reader.read_sizes) == expected_nbytes
    assert max(payload_reader.read_sizes) <= database_module._GRIDFS_IO_CHUNK_BYTES


def _read_payload(monkeypatch, datum, payload):
    database = object()
    gridfs_id = ObjectId()
    gridfs_handle = Mock()
    payload_reader = _TrackingBytesIO(payload)
    gridfs_handle.get.return_value = payload_reader

    def fake_gridfs(actual_database):
        assert actual_database is database
        return gridfs_handle

    monkeypatch.setattr(database_module.gridfs, "GridFS", fake_gridfs)
    Database._read_data_from_gridfs(database, datum, gridfs_id)
    gridfs_handle.get.assert_called_once_with(file_id=gridfs_id)
    return payload_reader


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


def test_contract_suite_uses_selected_build_and_real_binding():
    _assert_module_from_selected_build(database_module, "mspasspy/db/database.py")
    assert Path(seismic_binding.__file__).suffix == ".so"


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
def test_exact_gridfs_payload_loads_all_samples(monkeypatch, atomic_type):
    datum = _datum_with_sentinel_samples(atomic_type)
    state_before = _metadata_and_history_snapshot(datum)
    values, payload = _exact_payload(atomic_type)

    payload_reader = _read_payload(monkeypatch, datum, payload)

    _assert_read_is_bounded(payload_reader, len(payload))
    assert payload_reader.close_calls == 1
    assert datum.live
    assert datum.npts == NPTS
    assert datum.elog.size() == 0
    _assert_metadata_and_history_unchanged(datum, state_before)
    if atomic_type is TimeSeries:
        assert len(datum.data) == NPTS
        assert np.array_equal(np.asarray(datum.data), values)
    else:
        assert datum.data.rows() == 3
        assert datum.data.columns() == NPTS
        assert np.array_equal(np.asarray(datum.data), values.reshape(3, NPTS))


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
@pytest.mark.parametrize(
    "payload_case",
    ("one_double_short", "one_double_long", "unaligned", "empty"),
)
def test_invalid_gridfs_payload_is_atomic_dead_and_logged(
    monkeypatch, atomic_type, payload_case
):
    datum = _datum_with_sentinel_samples(atomic_type)
    original_samples = np.asarray(datum.data).copy()
    state_before = _metadata_and_history_snapshot(datum)
    _, exact_payload = _exact_payload(atomic_type)
    if payload_case == "one_double_short":
        payload = exact_payload[:-8]
    elif payload_case == "one_double_long":
        payload = exact_payload + np.float64(99.0).tobytes()
    elif payload_case == "unaligned":
        payload = exact_payload + b"\x00"
    else:
        payload = b""

    payload_reader = _read_payload(monkeypatch, datum, payload)

    assert payload_reader.read_sizes == []
    assert payload_reader.close_calls == 1
    assert datum.dead()
    assert datum.npts == NPTS
    assert np.array_equal(np.asarray(datum.data), original_samples)
    _assert_metadata_and_history_unchanged(datum, state_before)
    if atomic_type is TimeSeries:
        assert len(datum.data) == NPTS
    else:
        assert datum.data.rows() == 3
        assert datum.data.columns() == NPTS
    errors = datum.elog.get_error_log()
    assert len(errors) == 1
    assert errors[0].badness == ErrorSeverity.Invalid
    assert "Size mismatch in sample data" in errors[0].message


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
def test_zero_length_gridfs_payload_preserves_empty_dead_datum(
    monkeypatch, atomic_type
):
    datum = atomic_type()
    datum.npts = 0
    datum.set_live()

    payload_reader = _read_payload(monkeypatch, datum, b"")

    assert payload_reader.read_sizes == []
    assert payload_reader.close_calls == 1
    assert datum.dead()
    assert datum.npts == 0
    assert datum.elog.size() == 0


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
def test_native_sample_reader_preserves_layout_for_unaligned_reads(atomic_type):
    datum = _datum_with_sentinel_samples(atomic_type)
    values, expected = _exact_payload(atomic_type)
    if atomic_type is TimeSeries:
        datum.data = DoubleVector(values)
    else:
        datum.data = dmatrix(values.reshape(3, NPTS))
    reader = database_module._NativeSampleReader(datum.data)
    payload = bytearray()
    request_sizes = (1, 3, 7, 13)
    request_index = 0
    while True:
        chunk = reader.read(request_sizes[request_index % len(request_sizes)])
        if not chunk:
            break
        payload.extend(chunk)
        request_index += 1

    assert bytes(payload) == expected


@pytest.mark.parametrize("atomic_type", (TimeSeries, Seismogram))
def test_gridfs_save_streams_from_native_reader(monkeypatch, atomic_type):
    datum = _datum_with_sentinel_samples(atomic_type)
    values, expected = _exact_payload(atomic_type)
    if atomic_type is TimeSeries:
        datum.data = DoubleVector(values)
    else:
        datum.data = dmatrix(values.reshape(3, NPTS))
    gridfs_id = ObjectId()
    captured = {}

    class FakeGridFS:
        def put(self, source, **kwargs):
            captured["source"] = source
            captured["kwargs"] = kwargs
            payload = bytearray()
            while True:
                chunk = source.read(11)
                if not chunk:
                    break
                payload.extend(chunk)
            captured["payload"] = bytes(payload)
            return kwargs.get("_id", gridfs_id)

    monkeypatch.setattr(database_module.gridfs, "GridFS", lambda database: FakeGridFS())

    result = Database._save_sample_data_to_gridfs(object(), datum, gridfs_id=gridfs_id)

    assert result is datum
    assert isinstance(captured["source"], database_module._NativeSampleReader)
    assert captured["kwargs"] == {"_id": gridfs_id}
    assert captured["payload"] == expected
    assert datum["gridfs_id"] == gridfs_id
    assert datum["storage_mode"] == "gridfs"


def test_gridfs_stream_failure_removes_partial_blob_and_preserves_error(monkeypatch):
    datum = _datum_with_sentinel_samples(TimeSeries)
    write_error = RuntimeError("injected streaming failure")
    deleted_ids = []
    written_id = None

    class FailingGridFS:
        def put(self, source, **kwargs):
            nonlocal written_id
            written_id = kwargs["_id"]
            assert source.read(8)
            raise write_error

        def delete(self, gridfs_id):
            deleted_ids.append(gridfs_id)
            raise RuntimeError("injected cleanup failure")

    monkeypatch.setattr(
        database_module.gridfs, "GridFS", lambda database: FailingGridFS()
    )

    with pytest.raises(RuntimeError) as raised:
        Database._save_sample_data_to_gridfs(object(), datum)

    assert raised.value is write_error
    assert deleted_ids == [written_id]
    assert "gridfs_id" not in datum
    assert "storage_mode" not in datum
