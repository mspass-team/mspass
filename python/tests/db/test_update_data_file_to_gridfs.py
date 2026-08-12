import os
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import gridfs
import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    DoubleVector,
    Seismogram,
    TimeReferenceType,
    TimeSeries,
)
from mspasspy.ccore.utility import (
    AtomicType,
    ErrorSeverity,
    MsPASSError,
    dmatrix,
)
from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as error:
        client.close()
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    name = "test_update_file_gridfs_" + uuid.uuid4().hex
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def make_timeseries(values):
    datum = TimeSeries(len(values))
    datum.data = DoubleVector(values)
    datum.set_as_origin("test", "0", "0", AtomicType.TIMESERIES)
    return finish_datum(datum, len(values))


def make_seismogram(values):
    datum = Seismogram(len(values))
    samples = dmatrix(3, len(values))
    for component in range(3):
        for sample, value in enumerate(values):
            samples[component, sample] = value + 10.0 * component
    datum.data = samples
    datum.set_as_origin("test", "0", "0", AtomicType.SEISMOGRAM)
    return finish_datum(datum, len(values))


def finish_datum(datum, npts):
    datum.dt = 0.1
    datum.t0 = 10.0
    datum.tref = TimeReferenceType.UTC
    datum.set_live()
    datum["npts"] = npts
    datum["sampling_rate"] = 10.0
    datum["delta"] = 0.1
    datum["calib"] = 1.0
    return datum


def sample_array(datum):
    return np.asarray(datum.data)


def storage_reference(document):
    keys = ("storage_mode", "dir", "dfile", "foff", "format", "nbytes", "gridfs_id")
    return {key: document[key] for key in keys if key in document}


@pytest.mark.parametrize(
    "factory,collection",
    [
        (make_timeseries, "wf_TimeSeries"),
        (make_seismogram, "wf_Seismogram"),
    ],
)
def test_file_to_gridfs_update_round_trips_new_samples(
    database, tmp_path, factory, collection
):
    original = factory([1.0, 2.0, 3.0, 4.0])
    datum = database.save_data(
        original,
        mode="promiscuous",
        storage_mode="file",
        dir=str(tmp_path),
        dfile="samples.bin",
        save_history=False,
        return_data=True,
    )
    replacement = factory([5.0, 6.0, 7.0, 8.0])
    datum.data = replacement.data

    result = database.update_data(datum, mode="promiscuous")

    document = database[collection].find_one({"_id": result["_id"]})
    assert document["storage_mode"] == "gridfs"
    assert document["gridfs_id"] == result["gridfs_id"]
    assert gridfs.GridFS(database).exists(document["gridfs_id"])
    reread = database.read_data(result["_id"], collection=collection)
    assert reread.live
    np.testing.assert_array_equal(sample_array(reread), sample_array(replacement))


@pytest.mark.parametrize(
    "factory,collection",
    [
        (make_timeseries, "wf_TimeSeries"),
        (make_seismogram, "wf_Seismogram"),
    ],
)
@pytest.mark.parametrize("failure_mode", ["exception", "unmatched"])
def test_failed_waveform_update_keeps_file_readable_and_removes_new_gridfs_data(
    database, tmp_path, factory, collection, failure_mode
):
    original = factory([1.0, 2.0, 3.0, 4.0])
    original_samples = sample_array(original).copy()
    datum = database.save_data(
        original,
        mode="promiscuous",
        storage_mode="file",
        dir=str(tmp_path),
        dfile="samples.bin",
        save_history=False,
        return_data=True,
    )
    replacement = factory([5.0, 6.0, 7.0, 8.0])
    datum.data = replacement.data
    original_document = database[collection].find_one({"_id": datum["_id"]})
    original_reference = storage_reference(original_document)
    assert database["fs.files"].count_documents({}) == 0
    assert database["fs.chunks"].count_documents({}) == 0
    original_update_one = Collection.update_one
    injected_error = RuntimeError("injected waveform update failure")

    def fail_waveform_reference_update(self, query, update, *args, **kwargs):
        if "gridfs_id" in update.get("$set", {}):
            if failure_mode == "exception":
                raise injected_error
            return SimpleNamespace(matched_count=0)
        return original_update_one(self, query, update, *args, **kwargs)

    with patch.object(Collection, "update_one", fail_waveform_reference_update):
        if failure_mode == "exception":
            with pytest.raises(RuntimeError) as error:
                database.update_data(datum, mode="promiscuous")
            assert error.value is injected_error
        else:
            with pytest.raises(MsPASSError) as error:
                database.update_data(datum, mode="promiscuous")
            assert error.value.severity == ErrorSeverity.Invalid

    assert datum["storage_mode"] == "file"
    assert "gridfs_id" not in datum
    document = database[collection].find_one({"_id": datum["_id"]})
    assert storage_reference(document) == original_reference
    assert database["fs.files"].count_documents({}) == 0
    assert database["fs.chunks"].count_documents({}) == 0
    reread = database.read_data(datum["_id"], collection=collection)
    assert reread.live
    np.testing.assert_array_equal(sample_array(reread), original_samples)
