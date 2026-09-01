import json
import os
import resource
import subprocess
import sys

import numpy as np
import pytest
from bson import ObjectId

import mspasspy.db.database as database_module
from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.db.database import Database

PAYLOAD_BYTES = 32 * 1024 * 1024
MAX_TRANSIENT_BYTES = 12 * 1024 * 1024


def _peak_rss_bytes():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024


def _make_datum(atomic_type):
    samples_per_point = 1 if atomic_type is TimeSeries else 3
    npts = PAYLOAD_BYTES // (8 * samples_per_point)
    datum = atomic_type(npts)
    np.asarray(datum.data).fill(1.0)
    datum.set_live()
    return datum


class _DiscardingGridFS:
    def put(self, source, **kwargs):
        byte_count = 0
        while True:
            chunk = source.read(database_module._GRIDFS_IO_CHUNK_BYTES)
            if not chunk:
                break
            byte_count += len(chunk)
        self.byte_count = byte_count
        return ObjectId()


class _ZeroGridOut:
    def __init__(self, length):
        self.length = length
        self.remaining = length

    def read(self, size):
        byte_count = min(size, self.remaining)
        self.remaining -= byte_count
        return bytes(byte_count)

    def close(self):
        pass


class _ReadingGridFS:
    def __init__(self, length):
        self.reader = _ZeroGridOut(length)

    def get(self, file_id):
        return self.reader


def _run_probe(operation, atomic_name):
    atomic_type = TimeSeries if atomic_name == "TimeSeries" else Seismogram
    datum = _make_datum(atomic_type)
    baseline_peak = _peak_rss_bytes()
    database = object()
    if operation == "write":
        storage = _DiscardingGridFS()
        database_module.gridfs.GridFS = lambda actual_database: storage
        Database._save_sample_data_to_gridfs(database, datum)
        byte_count = storage.byte_count
    else:
        byte_count = datum.npts * (8 if atomic_type is TimeSeries else 24)
        storage = _ReadingGridFS(byte_count)
        database_module.gridfs.GridFS = lambda actual_database: storage
        Database._read_data_from_gridfs(database, datum, ObjectId())
    print(
        json.dumps(
            {
                "operation": operation,
                "atomic_type": atomic_name,
                "payload_bytes": byte_count,
                "peak_transient_bytes": max(0, _peak_rss_bytes() - baseline_peak),
            }
        )
    )


@pytest.mark.parametrize("operation", ("read", "write"))
@pytest.mark.parametrize("atomic_name", ("TimeSeries", "Seismogram"))
def test_large_gridfs_conversion_has_bounded_rss(operation, atomic_name):
    environment = os.environ.copy()
    completed = subprocess.run(
        [sys.executable, __file__, "--probe", operation, atomic_name],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stderr
    measurement = json.loads(completed.stdout.strip().splitlines()[-1])

    assert measurement["payload_bytes"] >= PAYLOAD_BYTES - 24
    assert measurement["peak_transient_bytes"] <= MAX_TRANSIENT_BYTES


if __name__ == "__main__" and sys.argv[1:2] == ["--probe"]:
    _run_probe(sys.argv[2], sys.argv[3])
