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
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import AtomicType, ErrorSeverity, MsPASSError, dmatrix
from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database
from mspasspy.io.distributed import write_distributed_data


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except Exception as error:
        client.close()
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    name = "test_gridfs_overwrite_" + uuid.uuid4().hex
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


def samples(datum):
    return np.asarray(datum.data)


def save_original(database, factory):
    return database.save_data(
        factory([1.0, 2.0, 3.0, 4.0]),
        mode="promiscuous",
        storage_mode="gridfs",
        save_history=False,
        return_data=True,
    )


CASES = [
    (make_timeseries, "wf_TimeSeries"),
    (make_seismogram, "wf_Seismogram"),
]


def replace_samples(database, datum, entrypoint):
    if entrypoint == "update_data":
        return database.update_data(datum, mode="promiscuous")
    return database.save_data(
        datum,
        mode="promiscuous",
        storage_mode="gridfs",
        overwrite=True,
        save_history=False,
        return_data=True,
    )


@pytest.mark.parametrize("factory,collection", CASES)
@pytest.mark.parametrize("entrypoint", ["update_data", "save_data"])
def test_successful_overwrite_commits_new_reference_before_removing_old_blob(
    database, factory, collection, entrypoint
):
    datum = save_original(database, factory)
    old_gridfs_id = datum["gridfs_id"]
    replacement = factory([5.0, 6.0, 7.0, 8.0])
    datum.data = replacement.data

    result = replace_samples(database, datum, entrypoint)

    document = database[collection].find_one({"_id": datum["_id"]})
    new_gridfs_id = document["gridfs_id"]
    assert result is datum
    assert database[collection].count_documents({}) == 1
    if entrypoint == "save_data":
        assert database["history_object"].count_documents({}) == 0
    assert new_gridfs_id == datum["gridfs_id"]
    assert new_gridfs_id != old_gridfs_id
    storage = gridfs.GridFS(database)
    assert not storage.exists(old_gridfs_id)
    assert storage.exists(new_gridfs_id)
    reread = database.read_data(document, collection=collection)
    assert reread.live
    np.testing.assert_allclose(samples(reread), samples(replacement))


@pytest.mark.parametrize("factory,collection", CASES)
@pytest.mark.parametrize("entrypoint", ["update_data", "save_data"])
def test_new_put_failure_preserves_old_reference_and_blob(
    database, factory, collection, entrypoint
):
    datum = save_original(database, factory)
    old_gridfs_id = datum["gridfs_id"]
    old_document = database[collection].find_one({"_id": datum["_id"]})
    old_samples = samples(datum).copy()
    datum.data = factory([5.0, 6.0, 7.0, 8.0]).data
    failure = RuntimeError("injected GridFS put failure")
    original_put = gridfs.GridFS.put

    def put_then_fail(self, data, *args, **kwargs):
        original_put(self, data, *args, **kwargs)
        raise failure

    with patch.object(gridfs.GridFS, "put", put_then_fail):
        with pytest.raises(RuntimeError) as error:
            replace_samples(database, datum, entrypoint)

    assert error.value is failure
    assert datum["gridfs_id"] == old_gridfs_id
    document = database[collection].find_one({"_id": datum["_id"]})
    assert document["gridfs_id"] == old_gridfs_id
    assert database["fs.files"].count_documents({}) == 1
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    reread = database.read_data(old_document, collection=collection)
    assert reread.live
    np.testing.assert_allclose(samples(reread), old_samples)


@pytest.mark.parametrize("factory,collection", CASES)
@pytest.mark.parametrize("entrypoint", ["update_data", "save_data"])
def test_reference_update_failure_removes_new_blob_and_rethrows_original_error(
    database, factory, collection, entrypoint
):
    datum = save_original(database, factory)
    old_gridfs_id = datum["gridfs_id"]
    old_document = database[collection].find_one({"_id": datum["_id"]})
    old_samples = samples(datum).copy()
    datum.data = factory([5.0, 6.0, 7.0, 8.0]).data
    failure = RuntimeError("injected waveform reference failure")
    original_update_one = Collection.update_one

    def fail_reference_update(self, query, update, *args, **kwargs):
        gridfs_id = update.get("$set", {}).get("gridfs_id")
        if gridfs_id is not None and gridfs_id != old_gridfs_id:
            raise failure
        return original_update_one(self, query, update, *args, **kwargs)

    with patch.object(Collection, "update_one", fail_reference_update):
        with pytest.raises(RuntimeError) as error:
            replace_samples(database, datum, entrypoint)

    assert error.value is failure
    assert datum["gridfs_id"] == old_gridfs_id
    document = database[collection].find_one({"_id": datum["_id"]})
    assert document["gridfs_id"] == old_gridfs_id
    assert database["fs.files"].count_documents({}) == 1
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    reread = database.read_data(old_document, collection=collection)
    assert reread.live
    np.testing.assert_allclose(samples(reread), old_samples)


@pytest.mark.parametrize("factory,collection", CASES)
@pytest.mark.parametrize("entrypoint", ["update_data", "save_data"])
def test_reference_compare_miss_removes_new_blob_without_overwriting_current_reference(
    database, factory, collection, entrypoint
):
    datum = save_original(database, factory)
    waveform_id = datum["_id"]
    old_gridfs_id = datum["gridfs_id"]
    datum.data = factory([5.0, 6.0, 7.0, 8.0]).data
    final_queries = []
    original_update_one = Collection.update_one

    def miss_reference_update(self, query, update, *args, **kwargs):
        gridfs_id = update.get("$set", {}).get("gridfs_id")
        if gridfs_id is not None:
            assert gridfs_id != old_gridfs_id
            final_queries.append(query)
            return SimpleNamespace(matched_count=0)
        return original_update_one(self, query, update, *args, **kwargs)

    with patch.object(Collection, "update_one", miss_reference_update):
        with pytest.raises(MsPASSError) as error:
            replace_samples(database, datum, entrypoint)

    assert error.value.severity == ErrorSeverity.Invalid
    assert final_queries == [{"_id": waveform_id, "gridfs_id": old_gridfs_id}]
    assert datum["gridfs_id"] == old_gridfs_id
    document = database[collection].find_one({"_id": waveform_id})
    assert document["gridfs_id"] == old_gridfs_id
    assert database["fs.files"].count_documents({}) == 1
    assert gridfs.GridFS(database).exists(old_gridfs_id)


@pytest.mark.parametrize("factory,collection", CASES)
@pytest.mark.parametrize("entrypoint", ["update_data", "save_data"])
def test_old_delete_failure_keeps_committed_new_data_and_logs_one_complaint(
    database, factory, collection, entrypoint
):
    datum = save_original(database, factory)
    old_gridfs_id = datum["gridfs_id"]
    replacement = factory([5.0, 6.0, 7.0, 8.0])
    datum.data = replacement.data
    old_log_size = datum.elog.size()
    failure = RuntimeError("injected old GridFS delete failure")
    original_delete = gridfs.GridFS.delete

    def fail_old_delete(self, gridfs_id, *args, **kwargs):
        if gridfs_id == old_gridfs_id:
            raise failure
        return original_delete(self, gridfs_id, *args, **kwargs)

    with patch.object(gridfs.GridFS, "delete", fail_old_delete):
        result = replace_samples(database, datum, entrypoint)

    document = database[collection].find_one({"_id": datum["_id"]})
    new_gridfs_id = document["gridfs_id"]
    assert result is datum
    assert new_gridfs_id == datum["gridfs_id"]
    assert new_gridfs_id != old_gridfs_id
    storage = gridfs.GridFS(database)
    assert storage.exists(old_gridfs_id)
    assert storage.exists(new_gridfs_id)
    assert database["fs.files"].count_documents({}) == 2
    assert datum.elog.size() == old_log_size + 1
    complaint = datum.elog.get_error_log()[-1]
    assert complaint.badness == ErrorSeverity.Complaint
    assert "previous sample object could not be deleted" in complaint.message
    assert str(failure) in complaint.message
    reread = database.read_data(document, collection=collection)
    assert reread.live
    np.testing.assert_allclose(samples(reread), samples(replacement))


@pytest.mark.parametrize(
    "factory,ensemble_type,collection",
    [
        (make_timeseries, TimeSeriesEnsemble, "wf_TimeSeries"),
        (make_seismogram, SeismogramEnsemble, "wf_Seismogram"),
    ],
)
def test_save_data_overwrite_updates_existing_ensemble_member_references(
    database, factory, ensemble_type, collection
):
    ensemble = ensemble_type()
    ensemble.member.append(factory([1.0, 2.0, 3.0, 4.0]))
    ensemble.member.append(factory([11.0, 12.0, 13.0, 14.0]))
    ensemble.set_live()
    ensemble = database.save_data(
        ensemble,
        storage_mode="gridfs",
        save_history=False,
        return_data=True,
    )
    waveform_ids = [d["_id"] for d in ensemble.member]
    old_gridfs_ids = [d["gridfs_id"] for d in ensemble.member]
    replacements = [
        factory([5.0, 6.0, 7.0, 8.0]),
        factory([15.0, 16.0, 17.0, 18.0]),
    ]
    for datum, replacement in zip(ensemble.member, replacements):
        datum.data = replacement.data

    result = database.save_data(
        ensemble,
        mode="promiscuous",
        storage_mode="gridfs",
        overwrite=True,
        save_history=False,
        return_data=True,
    )

    ensemble = result
    assert database[collection].count_documents({}) == 2
    storage = gridfs.GridFS(database)
    for index, datum in enumerate(ensemble.member):
        assert datum["_id"] == waveform_ids[index]
        assert datum["gridfs_id"] != old_gridfs_ids[index]
        assert not storage.exists(old_gridfs_ids[index])
        assert storage.exists(datum["gridfs_id"])
        reread = database.read_data(waveform_ids[index], collection=collection)
        np.testing.assert_allclose(samples(reread), samples(replacements[index]))


@pytest.mark.parametrize("storage_mode", ["gridfs", "file"])
def test_write_distributed_data_rejects_overwrite_before_execution(storage_mode):
    database = object.__new__(Database)

    with pytest.raises(ValueError, match="does not support overwrite=True"):
        write_distributed_data(
            None, database, storage_mode=storage_mode, overwrite=True
        )
