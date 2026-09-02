import os
import copy
import pickle
import threading
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import gridfs
import dask
import dask.bag
import numpy as np
import pymongo
import pytest
from bson import Binary, ObjectId

from mspasspy.ccore.seismic import (
    DoubleVector,
    Seismogram,
    SeismogramEnsemble,
    TimeReferenceType,
    TimeSeries,
    TimeSeriesEnsemble,
)
from mspasspy.ccore.utility import (
    AtomicType,
    ErrorSeverity,
    MsPASSError,
    ProcessingHistory,
    dmatrix,
)
import mspasspy.db.database as database_module
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
    assert database["gridfs_staging"].count_documents({}) == 0
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
def test_update_legacy_gridfs_document_without_storage_mode_removes_old_blob(
    database, factory, collection
):
    original = save_original(database, factory)
    waveform_id = original["_id"]
    old_gridfs_id = original["gridfs_id"]
    database[collection].update_one(
        {"_id": waveform_id}, {"$unset": {"storage_mode": ""}}
    )
    legacy_document = database[collection].find_one({"_id": waveform_id})
    assert "storage_mode" not in legacy_document
    datum = database.read_data(legacy_document, collection=collection)
    datum.data = factory([5.0, 6.0, 7.0, 8.0]).data

    database.update_data(datum, mode="promiscuous", save_history=False)

    document = database[collection].find_one({"_id": waveform_id})
    assert document["storage_mode"] == "gridfs"
    assert document["gridfs_id"] != old_gridfs_id
    storage = gridfs.GridFS(database)
    assert not storage.exists(old_gridfs_id)
    assert storage.exists(document["gridfs_id"])


@pytest.mark.parametrize("factory,collection", CASES)
def test_delete_legacy_gridfs_document_without_storage_mode_removes_blob(
    database, factory, collection
):
    datum = save_original(database, factory)
    waveform_id = datum["_id"]
    gridfs_id = datum["gridfs_id"]
    database[collection].update_one(
        {"_id": waveform_id}, {"$unset": {"storage_mode": ""}}
    )

    database.delete_data(
        waveform_id,
        "TimeSeries" if factory is make_timeseries else "Seismogram",
        collection=collection,
    )

    assert database[collection].find_one({"_id": waveform_id}) is None
    assert not gridfs.GridFS(database).exists(gridfs_id)


@pytest.mark.parametrize("factory,collection", CASES)
def test_update_uses_persisted_gridfs_id_when_caller_metadata_loses_it(
    database, factory, collection
):
    datum = save_original(database, factory)
    old_gridfs_id = datum["gridfs_id"]
    datum.erase("gridfs_id")
    datum.data = factory([5.0, 6.0, 7.0, 8.0]).data

    database.update_data(datum, mode="promiscuous", save_history=False)

    document = database[collection].find_one({"_id": datum["_id"]})
    assert document["gridfs_id"] == datum["gridfs_id"]
    assert document["gridfs_id"] != old_gridfs_id
    storage = gridfs.GridFS(database)
    assert not storage.exists(old_gridfs_id)
    assert storage.exists(document["gridfs_id"])


def test_update_rejects_stale_secondary_object_store_owner_before_side_effects(
    database, monkeypatch
):
    waveform_id = ObjectId()
    location = {
        "provider": "s3",
        "bucket": "existing-bucket",
        "object_name": "existing-object.bin",
        "encoding": "float64-le-v1",
    }
    database["wf_TimeSeries"].insert_one(
        {
            "_id": waveform_id,
            "storage_mode": "object_store",
            "object_store": location,
        }
    )
    datum = make_timeseries([5.0, 6.0, 7.0, 8.0])
    datum["_id"] = waveform_id
    configured_database = Database(
        database.client,
        database.name,
        read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
        write_concern=pymongo.write_concern.WriteConcern(w=1),
    )
    original_find_one = Collection.find_one
    owner_reads = []

    def stale_secondary_find(collection, query, *args, **kwargs):
        if collection.name == "wf_TimeSeries" and query == {"_id": waveform_id}:
            owner_reads.append(collection.read_preference)
            if collection.read_preference != pymongo.ReadPreference.PRIMARY:
                return None
        return original_find_one(collection, query, *args, **kwargs)

    monkeypatch.setattr(Collection, "find_one", stale_secondary_find)
    counts_before = {
        name: database[name].count_documents({})
        for name in ("wf_TimeSeries", "history_object", "elog", "fs.files")
    }

    with pytest.raises(ValueError, match="persisted object_store"):
        configured_database.update_data(datum, save_history=True)

    assert owner_reads == [pymongo.ReadPreference.PRIMARY]
    assert database["wf_TimeSeries"].find_one({"_id": waveform_id}) == {
        "_id": waveform_id,
        "storage_mode": "object_store",
        "object_store": location,
    }
    assert {
        name: database[name].count_documents({})
        for name in ("wf_TimeSeries", "history_object", "elog", "fs.files")
    } == counts_before


def test_update_rejects_nonexistent_waveform_before_side_effects(database):
    datum = make_timeseries([5.0, 6.0, 7.0, 8.0])
    datum["_id"] = ObjectId()
    datum["storage_mode"] = "gridfs"
    counts_before = {
        name: database[name].count_documents({})
        for name in ("wf_TimeSeries", "history_object", "elog", "fs.files")
    }

    with pytest.raises(ValueError, match="could not find the persisted waveform"):
        database.update_data(datum, mode="promiscuous", save_history=True)

    assert datum["storage_mode"] == "gridfs"
    assert "gridfs_id" not in datum
    assert {
        name: database[name].count_documents({})
        for name in ("wf_TimeSeries", "history_object", "elog", "fs.files")
    } == counts_before


@pytest.mark.parametrize(
    "persisted_mode,pointers",
    [
        ("file", {"dir": "/tmp", "dfile": "old.dat", "foff": 32}),
        ("url", {"url": "https://example.invalid/old-data"}),
    ],
)
def test_update_transition_uses_gridfs_mode_despite_tampered_caller(
    database, persisted_mode, pointers
):
    waveform_id = ObjectId()
    database["wf_TimeSeries"].insert_one(
        {"_id": waveform_id, "storage_mode": persisted_mode, **pointers}
    )
    datum = make_timeseries([5.0, 6.0, 7.0, 8.0])
    datum["_id"] = waveform_id
    datum["storage_mode"] = "gridfs"

    database.update_data(datum, mode="promiscuous", save_history=False)

    document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
    assert document["storage_mode"] == "gridfs"
    assert document["gridfs_id"] == datum["gridfs_id"]
    for pointer in ("object_store", "dir", "dfile", "foff", "url"):
        assert pointer not in document
    assert gridfs.GridFS(database).exists(document["gridfs_id"])


def test_gridfs_stage_insert_failure_restores_update_caller(database, monkeypatch):
    waveform_id = ObjectId()
    database["wf_TimeSeries"].insert_one(
        {
            "_id": waveform_id,
            "storage_mode": "file",
            "dir": "/old",
            "dfile": "old.dat",
            "foff": 12,
        }
    )
    datum = make_timeseries([5.0, 6.0, 7.0, 8.0])
    datum["_id"] = waveform_id
    datum["storage_mode"] = "file"
    datum["dir"] = "/old"
    datum["dfile"] = "old.dat"
    datum["foff"] = 12
    original_insert_one = Collection.insert_one

    def fail_stage_insert(collection, document, *args, **kwargs):
        if collection.name == "gridfs_staging":
            raise RuntimeError("injected stage insert failure")
        return original_insert_one(collection, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "insert_one", fail_stage_insert)
    with pytest.raises(RuntimeError, match="stage insert failure"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert datum["storage_mode"] == "file"
    assert datum["dir"] == "/old"
    assert datum["dfile"] == "old.dat"
    assert datum["foff"] == 12
    assert "gridfs_id" not in datum
    assert database["fs.files"].count_documents({}) == 0


def test_gridfs_stage_insert_failure_restores_new_save_caller(database, monkeypatch):
    datum = make_timeseries([5.0, 6.0, 7.0, 8.0])
    datum["storage_mode"] = "file"
    datum["dir"] = "/old"
    datum["dfile"] = "old.dat"
    datum["foff"] = 12
    original_insert_one = Collection.insert_one

    def fail_stage_insert(collection, document, *args, **kwargs):
        if collection.name == "gridfs_staging":
            raise RuntimeError("injected stage insert failure")
        return original_insert_one(collection, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "insert_one", fail_stage_insert)
    with pytest.raises(RuntimeError, match="stage insert failure"):
        database.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    assert datum["storage_mode"] == "file"
    assert datum["dir"] == "/old"
    assert datum["dfile"] == "old.dat"
    assert datum["foff"] == 12
    assert "gridfs_id" not in datum
    assert "_id" not in datum
    assert database["fs.files"].count_documents({}) == 0


def test_initial_gridfs_put_uncertainty_detaches_source_owner(database, monkeypatch):
    storage = gridfs.GridFS(database)
    old_waveform_id = ObjectId()
    old_gridfs_id = storage.put(b"old samples")
    old_history_id = ObjectId()
    old_elog_id = ObjectId()
    old_delete_token = ObjectId()
    old_waveform = {
        "_id": old_waveform_id,
        "storage_mode": "gridfs",
        "gridfs_id": old_gridfs_id,
        "history_object_id": old_history_id,
        "elog_id": old_elog_id,
        "_mspass_delete_token": old_delete_token,
    }
    database["wf_TimeSeries"].insert_one(copy.deepcopy(old_waveform))
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    for key, value in old_waveform.items():
        datum[key] = value
    original_put = gridfs.GridFS.put

    def put_then_lose_result(handle, *args, **kwargs):
        original_put(handle, *args, **kwargs)
        raise pymongo.errors.AutoReconnect("initial GridFS put result unknown")

    monkeypatch.setattr(gridfs.GridFS, "put", put_then_lose_result)
    with pytest.raises(MsPASSError, match="staged GridFS write committed"):
        database.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    stage = database["gridfs_staging"].find_one(
        {"waveform_id": datum["_id"], "new_gridfs_id": datum["gridfs_id"]}
    )
    assert stage is not None
    assert datum["_id"] != old_waveform_id
    assert datum["storage_mode"] == "gridfs"
    assert datum["gridfs_id"] != old_gridfs_id
    assert "history_object_id" not in datum
    assert "elog_id" not in datum
    assert "_mspass_delete_token" not in datum
    assert database["wf_TimeSeries"].find_one({"_id": old_waveform_id}) == old_waveform
    assert storage.exists(old_gridfs_id)
    assert storage.exists(datum["gridfs_id"])

    report = database.reconcile_gridfs_staging(delete_uncommitted=True)
    assert report["deleted"] == [str(stage["new_gridfs_id"])]
    assert database["gridfs_staging"].find_one({"_id": stage["_id"]}) is None
    assert storage.exists(old_gridfs_id)
    assert not storage.exists(stage["new_gridfs_id"])


def test_delete_data_prefers_literal_collection_over_default_alias(database):
    saved = database.save_data(
        make_timeseries([1.0, 2.0, 3.0, 4.0]),
        collection="wf",
        storage_mode="gridfs",
        mode="promiscuous",
        save_history=False,
        return_data=True,
    )
    storage = gridfs.GridFS(database)
    literal_gridfs_id = saved["gridfs_id"]
    default_gridfs_id = storage.put(b"default owner samples")
    default_owner = {
        "_id": saved["_id"],
        "storage_mode": "gridfs",
        "gridfs_id": default_gridfs_id,
    }
    database["wf_TimeSeries"].insert_one(copy.deepcopy(default_owner))

    database.delete_data(saved["_id"], "TimeSeries", collection="wf")

    assert database["wf"].find_one({"_id": saved["_id"]}) is None
    assert database["wf_TimeSeries"].find_one({"_id": saved["_id"]}) == default_owner
    assert not storage.exists(literal_gridfs_id)
    assert storage.exists(default_gridfs_id)


def test_delete_data_default_alias_fallback(database):
    saved = database.save_data(
        make_timeseries([1.0, 2.0, 3.0, 4.0]),
        storage_mode="gridfs",
        mode="promiscuous",
        save_history=False,
        return_data=True,
    )
    storage = gridfs.GridFS(database)
    saved_gridfs_id = saved["gridfs_id"]

    database.delete_data(saved["_id"], "TimeSeries", collection="wf")

    assert database["wf_TimeSeries"].find_one({"_id": saved["_id"]}) is None
    assert not storage.exists(saved_gridfs_id)


def test_update_gridfs_pointer_lifecycle_forces_primary_and_majority(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    configured_database = Database(
        database.client,
        database.name,
        read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
        write_concern=pymongo.write_concern.WriteConcern(w=1),
    )
    original_find_one = Collection.find_one
    original_update_one = Collection.update_one
    original_put = gridfs.GridFS.put
    owner_read_options = []
    cas_options = []
    gridfs_write_options = []

    def capture_find(collection, query, *args, **kwargs):
        if (
            collection.name == "wf_TimeSeries"
            and query == {"_id": datum["_id"]}
            and args
            and "storage_mode" in args[0]
        ):
            owner_read_options.append(
                (collection.read_preference, collection.read_concern.level)
            )
        return original_find_one(collection, query, *args, **kwargs)

    def capture_update(collection, query, update, *args, **kwargs):
        new_gridfs_id = update.get("$set", {}).get("gridfs_id")
        if collection.name == "wf_TimeSeries" and new_gridfs_id is not None:
            cas_options.append(
                (
                    collection.read_preference,
                    collection.read_concern.level,
                    collection.write_concern.document,
                )
            )
        return original_update_one(collection, query, update, *args, **kwargs)

    def capture_put(handle, *args, **kwargs):
        gridfs_write_options.append(
            (
                handle._files.read_preference,
                handle._files.read_concern.level,
                handle._files.write_concern.document,
            )
        )
        return original_put(handle, *args, **kwargs)

    monkeypatch.setattr(Collection, "find_one", capture_find)
    monkeypatch.setattr(Collection, "update_one", capture_update)
    monkeypatch.setattr(gridfs.GridFS, "put", capture_put)

    configured_database.update_data(datum, mode="promiscuous", save_history=False)

    assert owner_read_options == [(pymongo.ReadPreference.PRIMARY, "majority")]
    assert cas_options == [
        (pymongo.ReadPreference.PRIMARY, "majority", {"w": "majority"})
    ]
    assert gridfs_write_options == [
        (pymongo.ReadPreference.PRIMARY, "majority", {"w": "majority"})
    ]
    document = database["wf_TimeSeries"].find_one({"_id": datum["_id"]})
    assert document["gridfs_id"] == datum["gridfs_id"]
    assert document["gridfs_id"] != old_gridfs_id


@pytest.mark.parametrize("eventual_commit", [True, False])
def test_update_uncertain_cas_reconciles_samples_history_and_elog(
    database, monkeypatch, eventual_commit
):
    datum = save_original(database, make_timeseries)
    waveform_id = datum["_id"]
    old_gridfs_id = datum["gridfs_id"]
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    datum.elog.log_error("test", "uncertain update elog", ErrorSeverity.Complaint)
    original_update_one = Collection.update_one
    release_update = threading.Event()
    update_started = threading.Event()
    update_threads = []

    def delayed_uncertain_update(collection, query, update, *args, **kwargs):
        new_gridfs_id = update.get("$set", {}).get("gridfs_id")
        if (
            collection.name == "wf_TimeSeries"
            and new_gridfs_id is not None
            and new_gridfs_id != old_gridfs_id
        ):

            def finish_update():
                update_started.set()
                if release_update.wait(timeout=10):
                    original_update_one(collection, query, update, *args, **kwargs)

            if eventual_commit:
                thread = threading.Thread(target=finish_update)
                thread.start()
                update_threads.append(thread)
                assert update_started.wait(timeout=5)
            raise pymongo.errors.AutoReconnect("GridFS CAS result unknown")
        return original_update_one(collection, query, update, *args, **kwargs)

    monkeypatch.setattr(Collection, "update_one", delayed_uncertain_update)
    try:
        with pytest.raises(
            MsPASSError, match="could not determine whether the GridFS replacement"
        ) as error:
            database.update_data(datum, mode="promiscuous", save_history=True)

        assert error.value.severity == ErrorSeverity.Fatal
        staged_gridfs_id = datum["gridfs_id"]
        assert staged_gridfs_id != old_gridfs_id
        storage = gridfs.GridFS(database)
        assert storage.exists(old_gridfs_id)
        assert storage.exists(staged_gridfs_id)
        stage = database["gridfs_staging"].find_one(
            {"waveform_id": waveform_id, "new_gridfs_id": staged_gridfs_id}
        )
        assert stage is not None
        history_id = stage["auxiliary_documents"]["history"]["_id"]
        elog_id = stage["auxiliary_documents"]["elog"]["_id"]
        assert database["history_object"].find_one({"_id": history_id})
        assert database["elog"].find_one({"_id": elog_id})
        assert (
            database["wf_TimeSeries"].find_one({"_id": waveform_id})["gridfs_id"]
            == old_gridfs_id
        )

        if eventual_commit:
            release_update.set()
            for thread in update_threads:
                thread.join(timeout=10)
                assert not thread.is_alive()

        report = database.reconcile_gridfs_staging(delete_uncommitted=True)
        document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
        if eventual_commit:
            assert report["committed"] == [str(staged_gridfs_id)]
            assert document["gridfs_id"] == staged_gridfs_id
            assert not storage.exists(old_gridfs_id)
            assert storage.exists(staged_gridfs_id)
            assert document["history_object_id"] == history_id
            assert document["elog_id"] == elog_id
            assert (
                database["history_object"].find_one({"_id": history_id})[
                    "wf_TimeSeries_id"
                ]
                == waveform_id
            )
            assert (
                database["elog"].find_one({"_id": elog_id})["wf_TimeSeries_id"]
                == waveform_id
            )
        else:
            assert report["deleted"] == [str(staged_gridfs_id)]
            assert document["gridfs_id"] == old_gridfs_id
            assert storage.exists(old_gridfs_id)
            assert not storage.exists(staged_gridfs_id)
            assert database["history_object"].find_one({"_id": history_id}) is None
            assert database["elog"].find_one({"_id": elog_id}) is None
        assert database["gridfs_staging"].find_one({"_id": stage["_id"]}) is None
    finally:
        release_update.set()
        for thread in update_threads:
            thread.join(timeout=10)


def test_update_uncertain_gridfs_put_stages_before_auxiliary_writes(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    datum.elog.log_error("test", "put uncertainty elog", ErrorSeverity.Complaint)
    original_put = gridfs.GridFS.put

    def put_then_lose_result(handle, *args, **kwargs):
        original_put(handle, *args, **kwargs)
        raise pymongo.errors.AutoReconnect("GridFS put result unknown")

    monkeypatch.setattr(gridfs.GridFS, "put", put_then_lose_result)
    with pytest.raises(MsPASSError, match="GridFS replacement"):
        database.update_data(datum, mode="promiscuous", save_history=True)

    staged_gridfs_id = datum["gridfs_id"]
    stage = database["gridfs_staging"].find_one({"new_gridfs_id": staged_gridfs_id})
    assert stage is not None
    assert database["history_object"].count_documents({}) == 0
    assert database["elog"].count_documents({}) == 0
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    assert gridfs.GridFS(database).exists(staged_gridfs_id)

    report = database.reconcile_gridfs_staging()
    assert report["uncommitted"] == [str(staged_gridfs_id)]
    assert gridfs.GridFS(database).exists(staged_gridfs_id)
    assert database["gridfs_staging"].find_one({"_id": stage["_id"]})

    report = database.reconcile_gridfs_staging(delete_uncommitted=True)
    assert report["deleted"] == [str(staged_gridfs_id)]
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    assert not gridfs.GridFS(database).exists(staged_gridfs_id)
    assert database["gridfs_staging"].find_one({"_id": stage["_id"]}) is None


def test_missing_old_elog_fallback_uses_staged_id_and_cleans_on_cas_miss(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    missing_elog_id = ObjectId()
    datum["elog_id"] = missing_elog_id
    database["wf_TimeSeries"].update_one(
        {"_id": datum["_id"]}, {"$set": {"elog_id": missing_elog_id}}
    )
    datum.elog.log_error("test", "missing old elog", ErrorSeverity.Complaint)
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_update_one = Collection.update_one

    def miss_sample_cas(collection, query, update, *args, **kwargs):
        if collection.name == "wf_TimeSeries" and "gridfs_id" in update.get("$set", {}):
            return SimpleNamespace(matched_count=0)
        return original_update_one(collection, query, update, *args, **kwargs)

    monkeypatch.setattr(Collection, "update_one", miss_sample_cas)
    with pytest.raises(MsPASSError, match="could not commit the new GridFS"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert database["elog"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0
    assert (
        database["wf_TimeSeries"].find_one({"_id": datum["_id"]})["gridfs_id"]
        == old_gridfs_id
    )
    assert gridfs.GridFS(database).exists(old_gridfs_id)


def test_missing_old_elog_uncertain_fallback_insert_is_reconcilable(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    missing_elog_id = ObjectId()
    datum["elog_id"] = missing_elog_id
    database["wf_TimeSeries"].update_one(
        {"_id": datum["_id"]}, {"$set": {"elog_id": missing_elog_id}}
    )
    datum.elog.log_error("test", "uncertain elog fallback", ErrorSeverity.Complaint)
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_insert_one = Collection.insert_one

    def insert_elog_then_lose_result(collection, document, *args, **kwargs):
        result = original_insert_one(collection, document, *args, **kwargs)
        if collection.name == "elog":
            raise pymongo.errors.AutoReconnect("elog insert result unknown")
        return result

    monkeypatch.setattr(Collection, "insert_one", insert_elog_then_lose_result)
    with pytest.raises(MsPASSError, match="GridFS replacement"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    stage = database["gridfs_staging"].find_one({"waveform_id": datum["_id"]})
    assert stage is not None
    planned_elog_id = stage["auxiliary_documents"]["elog"]["_id"]
    assert database["elog"].find_one({"_id": planned_elog_id})
    assert database["elog"].count_documents({}) == 1
    report = database.reconcile_gridfs_staging(delete_uncommitted=True)
    assert report["deleted"] == [str(stage["new_gridfs_id"])]
    assert database["elog"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0
    assert gridfs.GridFS(database).exists(old_gridfs_id)


def test_durable_elog_replace_detects_concurrent_delete(database, monkeypatch):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    old_elog_id = database["elog"].insert_one({"logdata": []}).inserted_id
    datum["elog_id"] = old_elog_id
    database["wf_TimeSeries"].update_one(
        {"_id": datum["_id"]}, {"$set": {"elog_id": old_elog_id}}
    )
    datum.elog.log_error("test", "concurrent elog delete", ErrorSeverity.Complaint)
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_replace_one = Collection.replace_one
    original_delete_one = Collection.delete_one

    def delete_before_replace(collection, query, document, *args, **kwargs):
        if collection.name == "elog":
            original_delete_one(collection, query)
        return original_replace_one(collection, query, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "replace_one", delete_before_replace)
    with pytest.raises(MsPASSError, match="could not replace the existing elog"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert database["elog"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0
    assert (
        database["wf_TimeSeries"].find_one({"_id": datum["_id"]})["gridfs_id"]
        == old_gridfs_id
    )
    assert gridfs.GridFS(database).exists(old_gridfs_id)


def test_new_gridfs_save_uncertain_delayed_waveform_insert_reconciles(
    database, monkeypatch
):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum.elog.log_error("test", "new save uncertainty", ErrorSeverity.Complaint)
    original_insert_one = Collection.insert_one
    release_insert = threading.Event()
    insert_started = threading.Event()
    insert_threads = []

    def delayed_waveform_insert(collection, document, *args, **kwargs):
        if collection.name == "wf_TimeSeries":

            def finish_insert():
                insert_started.set()
                if release_insert.wait(timeout=10):
                    original_insert_one(collection, document, *args, **kwargs)

            thread = threading.Thread(target=finish_insert)
            thread.start()
            insert_threads.append(thread)
            assert insert_started.wait(timeout=5)
            raise pymongo.errors.AutoReconnect("waveform insert result unknown")
        return original_insert_one(collection, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "insert_one", delayed_waveform_insert)
    try:
        with pytest.raises(MsPASSError, match="staged GridFS MongoDB save"):
            database.save_data(
                datum,
                mode="promiscuous",
                storage_mode="gridfs",
                save_history=True,
                return_data=True,
            )

        waveform_id = datum["_id"]
        new_gridfs_id = datum["gridfs_id"]
        stage = database["gridfs_staging"].find_one(
            {"waveform_id": waveform_id, "new_gridfs_id": new_gridfs_id}
        )
        assert stage is not None
        assert gridfs.GridFS(database).exists(new_gridfs_id)
        assert database["wf_TimeSeries"].find_one({"_id": waveform_id}) is None

        release_insert.set()
        for thread in insert_threads:
            thread.join(timeout=10)
            assert not thread.is_alive()

        report = database.reconcile_gridfs_staging(delete_uncommitted=True)
        assert report["committed"] == [str(new_gridfs_id)]
        document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
        assert document["gridfs_id"] == new_gridfs_id
        assert gridfs.GridFS(database).exists(new_gridfs_id)
        assert database["gridfs_staging"].find_one({"_id": stage["_id"]}) is None
    finally:
        release_insert.set()
        for thread in insert_threads:
            thread.join(timeout=10)


def test_known_partial_gridfs_put_failure_removes_chunks_and_stage(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data

    def write_chunk_then_fail(handle, *args, **kwargs):
        staged_id = kwargs["_id"]
        handle._chunks.insert_one(
            {"files_id": staged_id, "n": 0, "data": Binary(b"partial")}
        )
        raise RuntimeError("known partial GridFS failure")

    monkeypatch.setattr(gridfs.GridFS, "put", write_chunk_then_fail)
    with pytest.raises(RuntimeError, match="known partial GridFS failure"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert datum["gridfs_id"] == old_gridfs_id
    assert (
        database["fs.chunks"].count_documents({"files_id": {"$ne": old_gridfs_id}}) == 0
    )
    assert database["gridfs_staging"].count_documents({}) == 0
    assert gridfs.GridFS(database).exists(old_gridfs_id)


def test_compensation_delete_failure_retains_gridfs_recovery_identity(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_update_one = Collection.update_one
    original_delete = gridfs.GridFS.delete
    staged_ids = []

    def miss_reference_update(collection, query, update, *args, **kwargs):
        new_gridfs_id = update.get("$set", {}).get("gridfs_id")
        if collection.name == "wf_TimeSeries" and new_gridfs_id is not None:
            staged_ids.append(new_gridfs_id)
            return SimpleNamespace(matched_count=0)
        return original_update_one(collection, query, update, *args, **kwargs)

    def fail_staged_delete(handle, gridfs_id, *args, **kwargs):
        if gridfs_id != old_gridfs_id:
            raise RuntimeError("injected compensation delete failure")
        return original_delete(handle, gridfs_id, *args, **kwargs)

    monkeypatch.setattr(Collection, "update_one", miss_reference_update)
    monkeypatch.setattr(gridfs.GridFS, "delete", fail_staged_delete)
    with pytest.raises(MsPASSError, match="could not fully compensate.*GridFS stage"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert len(staged_ids) == 1
    staged_gridfs_id = staged_ids[0]
    assert datum["gridfs_id"] == staged_gridfs_id
    stage = database["gridfs_staging"].find_one({"new_gridfs_id": staged_gridfs_id})
    assert stage is not None
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    assert gridfs.GridFS(database).exists(staged_gridfs_id)


@pytest.mark.parametrize("factory,collection", CASES)
def test_update_gridfs_cas_rejects_concurrent_reference_change(
    database, monkeypatch, factory, collection
):
    datum = save_original(database, factory)
    waveform_id = datum["_id"]
    old_gridfs_id = datum["gridfs_id"]
    datum.data = factory([5.0, 6.0, 7.0, 8.0]).data
    storage = gridfs.GridFS(database)
    concurrent_gridfs_id = storage.put(b"concurrent samples")
    original_update_one = Collection.update_one
    concurrent_change_applied = False

    def change_reference_before_cas(self, query, update, *args, **kwargs):
        nonlocal concurrent_change_applied
        new_gridfs_id = update.get("$set", {}).get("gridfs_id")
        if (
            self.name == collection
            and new_gridfs_id is not None
            and new_gridfs_id not in (old_gridfs_id, concurrent_gridfs_id)
            and not concurrent_change_applied
        ):
            concurrent_change_applied = True
            original_update_one(
                self,
                {"_id": waveform_id},
                {"$set": {"gridfs_id": concurrent_gridfs_id}},
            )
        return original_update_one(self, query, update, *args, **kwargs)

    monkeypatch.setattr(Collection, "update_one", change_reference_before_cas)
    with pytest.raises(MsPASSError, match="could not commit the new GridFS"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert concurrent_change_applied
    document = database[collection].find_one({"_id": waveform_id})
    assert document["gridfs_id"] == concurrent_gridfs_id
    assert datum["gridfs_id"] == old_gridfs_id
    assert storage.exists(old_gridfs_id)
    assert storage.exists(concurrent_gridfs_id)
    assert database["fs.files"].count_documents({}) == 2


def test_gridfs_replacement_and_delete_preserve_shared_reference(database):
    datum = save_original(database, make_timeseries)
    old_gridfs_id = datum["gridfs_id"]
    shared_waveform_id = (
        database["wf_TimeSeries"]
        .insert_one(
            {
                "storage_mode": "gridfs",
                "gridfs_id": old_gridfs_id,
                "npts": datum.npts,
                "starttime": datum.t0,
                "delta": datum.dt,
            }
        )
        .inserted_id
    )
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data

    database.update_data(datum, mode="promiscuous", save_history=False)

    assert gridfs.GridFS(database).exists(old_gridfs_id)
    assert (
        database["wf_TimeSeries"].find_one({"_id": shared_waveform_id})["gridfs_id"]
        == old_gridfs_id
    )
    database.delete_data(shared_waveform_id, "TimeSeries")
    assert database["wf_TimeSeries"].find_one({"_id": shared_waveform_id}) is None
    assert not gridfs.GridFS(database).exists(old_gridfs_id)


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
    assert final_queries == [
        {
            "_id": waveform_id,
            "_mspass_delete_token": {"$exists": False},
            "storage_mode": "gridfs",
            "object_store": {"$exists": False},
            "gridfs_id": old_gridfs_id,
            "dir": {"$exists": False},
            "dfile": {"$exists": False},
            "foff": {"$exists": False},
            "url": {"$exists": False},
            "history_object_id": {"$exists": False},
            "elog_id": {"$exists": False},
        }
    ]
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


def _history_snapshot(datum):
    history = ProcessingHistory(datum)
    return pickle.dumps(
        (
            history.get_nodes(),
            history.current_nodedata(),
            history.id(),
            history.stage(),
            history.is_origin(),
        )
    )


def test_save_schema_preflight_precedes_sample_write(database, monkeypatch):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    failure = RuntimeError("injected schema preflight failure")
    sample_writes = []

    def fail_preflight(*args, **kwargs):
        raise failure

    def record_sample_write(*args, **kwargs):
        sample_writes.append(True)

    monkeypatch.setattr(database_module, "md2doc", fail_preflight)
    monkeypatch.setattr(Database, "_save_sample_data", record_sample_write)

    with pytest.raises(RuntimeError) as error:
        database.save_data(datum, storage_mode="gridfs", return_data=True)

    assert error.value is failure
    assert sample_writes == []


def test_gridfs_schema_preflight_rejects_excluded_pointer(database):
    with pytest.raises(ValueError, match="gridfs_id is excluded"):
        database.save_data(
            make_timeseries([1.0, 2.0, 3.0, 4.0]),
            storage_mode="gridfs",
            exclude_keys=["gridfs_id"],
            mode="promiscuous",
            return_data=True,
        )

    assert database["wf_TimeSeries"].count_documents({}) == 0
    assert database["fs.files"].count_documents({}) == 0
    assert database["fs.chunks"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0


def test_gridfs_schema_preflight_rejects_incapable_custom_schema(database):
    custom_schema = copy.deepcopy(database.metadata_schema)
    custom_schema.TimeSeries._main_dic.pop("gridfs_id")
    custom_name = "test_gridfs_incapable_schema_" + uuid.uuid4().hex
    custom_database = Database(
        database.client,
        custom_name,
        db_schema=database.database_schema,
        md_schema=custom_schema,
    )
    try:
        with pytest.raises(ValueError, match="gridfs_id is undefined"):
            custom_database.save_data(
                make_timeseries([1.0, 2.0, 3.0, 4.0]),
                storage_mode="gridfs",
                mode="cautious",
                return_data=True,
            )
        assert custom_database["wf_TimeSeries"].count_documents({}) == 0
        assert custom_database["fs.files"].count_documents({}) == 0
        assert custom_database["gridfs_staging"].count_documents({}) == 0
    finally:
        database.client.drop_database(custom_name)


@pytest.mark.parametrize("factory,collection", CASES)
def test_update_failure_preserves_history_and_removes_new_children(
    database, monkeypatch, factory, collection
):
    datum = save_original(database, factory)
    old_gridfs_id = datum["gridfs_id"]
    history_before = _history_snapshot(datum)
    replacement = factory([5.0, 6.0, 7.0, 8.0])
    datum.data = replacement.data
    failure = RuntimeError("injected waveform reference failure")
    original_update_one = Collection.update_one

    def fail_waveform_reference(self, query, update, *args, **kwargs):
        if self.name == collection and "gridfs_id" in update.get("$set", {}):
            raise failure
        return original_update_one(self, query, update, *args, **kwargs)

    monkeypatch.setattr(Collection, "update_one", fail_waveform_reference)

    with pytest.raises(RuntimeError) as error:
        database.update_data(datum, mode="promiscuous", save_history=True)

    assert error.value is failure
    assert _history_snapshot(datum) == history_before
    assert database["history_object"].count_documents({}) == 0
    document = database[collection].find_one({"_id": datum["_id"]})
    assert document["gridfs_id"] == old_gridfs_id
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    assert database["fs.files"].count_documents({}) == 1


@pytest.mark.parametrize("factory,collection", CASES)
@pytest.mark.parametrize("entrypoint", ["save", "update"])
def test_history_resets_only_after_durable_waveform_references(
    database, monkeypatch, factory, collection, entrypoint
):
    if entrypoint == "save":
        datum = factory([1.0, 2.0, 3.0, 4.0])
    else:
        datum = save_original(database, factory)
        datum.data = factory([5.0, 6.0, 7.0, 8.0]).data
    observations = []
    original_reset = Database._reset_processing_history

    def observe_reset(datum, alg_name, alg_id, save_uuid):
        waveform = database[collection].find_one({"_id": datum["_id"]})
        history_id = waveform["history_object_id"]
        history = database["history_object"].find_one({"_id": history_id})
        assert history[collection + "_id"] == datum["_id"]
        assert gridfs.GridFS(database).exists(waveform["gridfs_id"])
        observations.append(waveform)
        original_reset(datum, alg_name, alg_id, save_uuid)

    monkeypatch.setattr(
        Database, "_reset_processing_history", staticmethod(observe_reset)
    )

    if entrypoint == "save":
        result = database.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=True,
            return_data=True,
        )
    else:
        result = database.update_data(datum, mode="promiscuous", save_history=True)

    assert result is datum
    assert len(observations) == 1
    assert datum.is_origin()


@pytest.mark.parametrize("factory,collection", CASES)
def test_save_history_is_prelinked_before_waveform_insert(
    database, monkeypatch, factory, collection
):
    datum = factory([1.0, 2.0, 3.0, 4.0])
    original_insert_one = Collection.insert_one
    observed_history_documents = []

    def capture_insert(target, document, *args, **kwargs):
        if target.name == "history_object":
            assert collection + "_id" in document
            assert (
                database[collection].find_one({"_id": document[collection + "_id"]})
                is None
            )
            observed_history_documents.append(document.copy())
        return original_insert_one(target, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "insert_one", capture_insert)

    result = database.save_data(
        datum,
        mode="promiscuous",
        storage_mode="gridfs",
        save_history=True,
        return_data=True,
    )

    assert result is datum
    assert len(observed_history_documents) == 1
    document = database[collection].find_one({"_id": datum["_id"]})
    assert document["history_object_id"] == observed_history_documents[0]["_id"]
    assert observed_history_documents[0][collection + "_id"] == datum["_id"]


@pytest.mark.parametrize("factory,collection", CASES)
def test_save_elog_failure_removes_known_created_resources(
    database, monkeypatch, factory, collection
):
    datum = factory([1.0, 2.0, 3.0, 4.0])
    datum.elog.log_error("test", "injected elog", ErrorSeverity.Complaint)
    history_before = _history_snapshot(datum)
    failure = RuntimeError("injected elog save failure")

    def fail_elog_save(*args, **kwargs):
        raise failure

    monkeypatch.setattr(Database, "_save_elog", fail_elog_save)

    with pytest.raises(RuntimeError) as error:
        database.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=True,
            return_data=True,
        )

    assert error.value is failure
    assert _history_snapshot(datum) == history_before
    assert "gridfs_id" not in datum
    assert database[collection].count_documents({}) == 0
    assert database["history_object"].count_documents({}) == 0
    assert database["elog"].count_documents({}) == 0
    assert database["fs.files"].count_documents({}) == 0


def test_save_history_can_defer_reset_without_database_io(monkeypatch):
    database = object.__new__(Database)
    database.database_schema = SimpleNamespace(default_name=lambda name: name)
    inserted_documents = []

    def insert_one(document):
        inserted_documents.append(document)
        return SimpleNamespace(inserted_id="history-id")

    collection = SimpleNamespace(insert_one=insert_one)
    monkeypatch.setattr(Database, "__getitem__", lambda self, name: collection)
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    history_before = _history_snapshot(datum)

    history_id = database._save_history(datum, reset_history=False)

    assert history_id == "history-id"
    assert len(inserted_documents) == 1
    assert _history_snapshot(datum) == history_before


def test_new_gridfs_save_does_not_inherit_auxiliary_pointers(database):
    stale_history_id = database["history_object"].insert_one({"old": True}).inserted_id
    stale_elog_id = database["elog"].insert_one({"logdata": []}).inserted_id
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum["history_object_id"] = stale_history_id
    datum["elog_id"] = stale_elog_id

    saved = database.save_data(
        datum,
        storage_mode="gridfs",
        save_history=False,
        return_data=True,
    )

    waveform = database["wf_TimeSeries"].find_one({"_id": saved["_id"]})
    assert "history_object_id" not in waveform
    assert "elog_id" not in waveform
    assert "history_object_id" not in saved
    assert "elog_id" not in saved
    assert database["history_object"].find_one({"_id": stale_history_id})
    assert database["elog"].find_one({"_id": stale_elog_id})


def test_update_uses_persisted_auxiliary_owners(database):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum.elog.log_error("test", "original elog", ErrorSeverity.Complaint)
    datum = database.save_data(
        datum,
        storage_mode="gridfs",
        save_history=True,
        return_data=True,
    )
    waveform_before = database["wf_TimeSeries"].find_one({"_id": datum["_id"]})
    original_history_id = waveform_before["history_object_id"]
    original_elog_id = waveform_before["elog_id"]
    unrelated_history_id = (
        database["history_object"].insert_one({"other": True}).inserted_id
    )
    unrelated_elog = {"logdata": [{"marker": "unrelated"}]}
    unrelated_elog_id = database["elog"].insert_one(unrelated_elog).inserted_id
    datum["history_object_id"] = unrelated_history_id
    datum["elog_id"] = unrelated_elog_id
    datum.elog.clear()
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data

    database.update_data(datum, mode="promiscuous", save_history=False)

    waveform = database["wf_TimeSeries"].find_one({"_id": datum["_id"]})
    assert waveform["history_object_id"] == original_history_id
    assert waveform["elog_id"] == original_elog_id
    assert datum["history_object_id"] == original_history_id
    assert datum["elog_id"] == original_elog_id
    assert database["history_object"].find_one({"_id": unrelated_history_id}) == {
        "_id": unrelated_history_id,
        "other": True,
    }
    assert database["elog"].find_one({"_id": unrelated_elog_id})["logdata"] == [
        {"marker": "unrelated"}
    ]


@pytest.mark.parametrize("entrypoint", ["update", "overwrite"])
def test_zero_length_replacement_preserves_old_gridfs_data(database, entrypoint):
    datum = save_original(database, make_timeseries)
    waveform_id = datum["_id"]
    old_gridfs_id = datum["gridfs_id"]
    old_document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
    datum.data = DoubleVector()
    datum.npts = 0
    datum.set_live()

    if entrypoint == "update":
        with pytest.raises(MsPASSError, match="zero-length"):
            database.update_data(datum, mode="promiscuous", save_history=False)
    else:
        result = database.save_data(
            datum,
            storage_mode="gridfs",
            overwrite=True,
            save_history=False,
            return_data=True,
        )
        assert result.dead()

    assert database["wf_TimeSeries"].find_one({"_id": waveform_id}) == old_document
    assert gridfs.GridFS(database).exists(old_gridfs_id)
    assert database["gridfs_staging"].count_documents({}) == 0


def test_ensemble_overwrite_preserves_each_member_ownership(database):
    members = []
    original_ids = []
    original_aux = []
    for offset in (0.0, 10.0):
        datum = make_timeseries([1.0 + offset, 2.0 + offset, 3.0 + offset])
        datum.elog.log_error("test", "member", ErrorSeverity.Complaint)
        datum = database.save_data(
            datum,
            storage_mode="gridfs",
            save_history=True,
            return_data=True,
        )
        datum.elog.clear()
        members.append(datum)
        original_ids.append(datum["_id"])
        original_aux.append((datum["history_object_id"], datum["elog_id"]))
    ensemble = TimeSeriesEnsemble()
    for datum in members:
        ensemble.member.append(datum)
    ensemble.set_live()
    ensemble["_id"] = members[0]["_id"]
    ensemble["gridfs_id"] = members[0]["gridfs_id"]
    ensemble["storage_mode"] = "gridfs"
    ensemble["history_object_id"] = members[0]["history_object_id"]
    ensemble["elog_id"] = members[0]["elog_id"]
    replacements = ([20.0, 21.0, 22.0], [30.0, 31.0, 32.0])
    for datum, values in zip(ensemble.member, replacements):
        datum.data = make_timeseries(values).data

    database.save_data(
        ensemble,
        storage_mode="gridfs",
        overwrite=True,
        save_history=False,
        return_data=True,
    )

    for index, waveform_id in enumerate(original_ids):
        waveform = database["wf_TimeSeries"].find_one({"_id": waveform_id})
        assert waveform["history_object_id"] == original_aux[index][0]
        assert waveform["elog_id"] == original_aux[index][1]
        reread = database.read_data(waveform_id, collection="wf_TimeSeries")
        np.testing.assert_allclose(samples(reread), replacements[index])


@pytest.mark.parametrize("entrypoint", ["update", "overwrite"])
def test_gridfs_replacement_schema_preflight_rejects_excluded_pointer(
    database, entrypoint
):
    datum = save_original(database, make_timeseries)
    old_document = database["wf_TimeSeries"].find_one({"_id": datum["_id"]})
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data

    with pytest.raises(ValueError, match="gridfs_id is excluded"):
        if entrypoint == "update":
            database.update_data(datum, mode="promiscuous", exclude_keys=["gridfs_id"])
        else:
            database.save_data(
                datum,
                storage_mode="gridfs",
                overwrite=True,
                mode="promiscuous",
                exclude_keys=["gridfs_id"],
            )

    assert database["wf_TimeSeries"].find_one({"_id": datum["_id"]}) == old_document
    assert database["fs.files"].count_documents({}) == 1
    assert database["gridfs_staging"].count_documents({}) == 0


@pytest.mark.parametrize("entrypoint", ["update", "overwrite"])
def test_gridfs_replacement_rejects_incapable_custom_schema(database, entrypoint):
    custom_schema = copy.deepcopy(database.metadata_schema)
    custom_schema.TimeSeries._main_dic.pop("gridfs_id")
    custom_name = "test_gridfs_update_schema_" + uuid.uuid4().hex
    custom_database = Database(
        database.client,
        custom_name,
        db_schema=database.database_schema,
        md_schema=custom_schema,
    )
    try:
        datum = custom_database.save_data(
            make_timeseries([1.0, 2.0, 3.0, 4.0]),
            storage_mode="gridfs",
            mode="promiscuous",
            save_history=False,
            return_data=True,
        )
        old_document = custom_database["wf_TimeSeries"].find_one({"_id": datum["_id"]})
        datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data

        with pytest.raises(ValueError, match="gridfs_id is undefined"):
            if entrypoint == "update":
                custom_database.update_data(datum, mode="cautious", save_history=False)
            else:
                custom_database.save_data(
                    datum,
                    storage_mode="gridfs",
                    overwrite=True,
                    mode="cautious",
                    save_history=False,
                )

        assert (
            custom_database["wf_TimeSeries"].find_one({"_id": datum["_id"]})
            == old_document
        )
        assert custom_database["fs.files"].count_documents({}) == 1
        assert custom_database["gridfs_staging"].count_documents({}) == 0
    finally:
        database.client.drop_database(custom_name)


def test_gridfs_cleanup_stage_query_failure_retains_recovery_state(
    database, monkeypatch
):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    sample_failure = RuntimeError("injected sample failure")
    query_failure = pymongo.errors.AutoReconnect("stage query unavailable")
    original_find_one = Collection.find_one

    def fail_sample_save(*args, **kwargs):
        raise sample_failure

    def fail_stage_query(self, *args, **kwargs):
        if self.name == "gridfs_staging":
            raise query_failure
        return original_find_one(self, *args, **kwargs)

    monkeypatch.setattr(Database, "_save_sample_data_to_gridfs", fail_sample_save)
    monkeypatch.setattr(Collection, "find_one", fail_stage_query)

    with pytest.raises(MsPASSError, match="could not fully compensate.*GridFS stage"):
        database.save_data(
            datum,
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    assert "_id" in datum
    assert "gridfs_id" in datum
    assert datum["storage_mode"] == "gridfs"


def test_gridfs_cleanup_restores_caller_before_stage_cleanup_failure(
    database, monkeypatch
):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum["storage_mode"] = "file"
    datum["dir"] = "/original"
    datum["dfile"] = "original.dat"
    snapshot = database._snapshot_object_store_metadata(datum)
    waveform_id = ObjectId()
    new_gridfs_id = ObjectId()
    staged_auxiliary = {
        "history": {"collection": "history_object", "_id": ObjectId()},
        "elog": {"collection": "elog", "_id": ObjectId()},
    }
    stage = database._create_gridfs_stage(
        "insert",
        waveform_id,
        "wf_TimeSeries",
        new_gridfs_id,
        staged_auxiliary,
    )
    database._gridfs_lifecycle_handle().put(b"staged", _id=new_gridfs_id)
    for entry in staged_auxiliary.values():
        database[entry["collection"]].insert_one({"_id": entry["_id"]})
    datum["_id"] = waveform_id
    datum["storage_mode"] = "gridfs"
    datum["gridfs_id"] = new_gridfs_id
    Database._normalize_storage_pointers(datum, "gridfs")
    original_delete_one = Collection.delete_one

    def fail_stage_delete(collection, query, *args, **kwargs):
        if collection.name == "gridfs_staging":
            return SimpleNamespace(deleted_count=0)
        return original_delete_one(collection, query, *args, **kwargs)

    monkeypatch.setattr(Collection, "delete_one", fail_stage_delete)

    failures, retained = database._cleanup_gridfs_stage(stage, datum, snapshot)

    assert failures
    assert not retained
    assert database._snapshot_object_store_metadata(datum) == snapshot
    assert not gridfs.GridFS(database).exists(new_gridfs_id)
    assert database["gridfs_staging"].find_one({"_id": stage["_id"]})
    for entry in staged_auxiliary.values():
        assert database[entry["collection"]].find_one({"_id": entry["_id"]}) is None

    monkeypatch.undo()
    report = database.reconcile_gridfs_staging(delete_uncommitted=True)
    assert report["deleted"] == [str(new_gridfs_id)]
    assert database["gridfs_staging"].find_one({"_id": stage["_id"]}) is None


def test_durable_elog_failure_never_deletes_shared_gridfs_data(database, monkeypatch):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum.elog.log_error("test", "force elog", ErrorSeverity.Complaint)
    failure = RuntimeError("injected elog failure after shared reference")

    def create_shared_reference_then_fail(self, target, *args, **kwargs):
        database["wf_TimeSeries"].insert_one(
            {
                "storage_mode": "gridfs",
                "gridfs_id": target["gridfs_id"],
            }
        )
        raise failure

    monkeypatch.setattr(Database, "_save_elog", create_shared_reference_then_fail)

    with pytest.raises(MsPASSError, match="could not fully compensate"):
        database.save_data(
            datum,
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    shared = database["wf_TimeSeries"].find_one({"gridfs_id": datum["gridfs_id"]})
    assert shared is not None
    assert gridfs.GridFS(database).exists(datum["gridfs_id"])
    assert database["gridfs_staging"].count_documents({}) == 1


def test_committed_stage_is_resolved_before_next_update(database, monkeypatch):
    monkeypatch.setattr(Database, "_complete_gridfs_stage", lambda *args: False)
    datum = database.save_data(
        make_timeseries([1.0, 2.0, 3.0, 4.0]),
        storage_mode="gridfs",
        save_history=False,
        return_data=True,
    )
    assert database["gridfs_staging"].count_documents({}) == 1
    monkeypatch.undo()
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data

    database.update_data(datum, mode="promiscuous", save_history=False)

    assert database["gridfs_staging"].count_documents({}) == 0
    reread = database.read_data(datum["_id"], collection="wf_TimeSeries")
    np.testing.assert_allclose(samples(reread), [5.0, 6.0, 7.0, 8.0])


def test_delete_resolves_committed_stage_and_honors_clear_flags(database, monkeypatch):
    monkeypatch.setattr(Database, "_complete_gridfs_stage", lambda *args: False)
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum.elog.log_error("test", "retain me", ErrorSeverity.Complaint)
    datum = database.save_data(
        datum,
        storage_mode="gridfs",
        save_history=True,
        return_data=True,
    )
    history_id = datum["history_object_id"]
    elog_id = datum["elog_id"]
    assert database["gridfs_staging"].count_documents({}) == 1
    monkeypatch.undo()

    database.delete_data(
        datum["_id"],
        "TimeSeries",
        clear_history=False,
        clear_elog=False,
    )

    assert database["wf_TimeSeries"].find_one({"_id": datum["_id"]}) is None
    assert database["history_object"].find_one({"_id": history_id})
    assert database["elog"].find_one({"_id": elog_id})
    assert database["gridfs_staging"].count_documents({}) == 0


def test_delete_claim_blocks_update_after_auxiliary_cleanup(database, monkeypatch):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum.elog.log_error("test", "delete claim elog", ErrorSeverity.Complaint)
    datum = database.save_data(
        datum,
        storage_mode="gridfs",
        save_history=True,
        return_data=True,
    )
    waveform_id = datum["_id"]
    history_id = datum["history_object_id"]
    elog_id = datum["elog_id"]
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_delete = Database._delete_gridfs_id_if_unreferenced
    update_was_rejected = False

    def attempt_update_after_auxiliary_cleanup(handle, *args, **kwargs):
        nonlocal update_was_rejected
        assert database["history_object"].find_one({"_id": history_id}) is None
        assert database["elog"].find_one({"_id": elog_id}) is None
        with pytest.raises(MsPASSError, match="claimed by delete_data"):
            database.update_data(datum, mode="promiscuous", save_history=False)
        update_was_rejected = True
        return original_delete(handle, *args, **kwargs)

    monkeypatch.setattr(
        Database,
        "_delete_gridfs_id_if_unreferenced",
        attempt_update_after_auxiliary_cleanup,
    )

    database.delete_data(waveform_id, "TimeSeries")

    assert update_was_rejected
    assert database["wf_TimeSeries"].find_one({"_id": waveform_id}) is None
    assert database["fs.files"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0


def test_update_metadata_cannot_inject_or_bypass_delete_claim(database):
    datum = save_original(database, make_timeseries)
    waveform_id = datum["_id"]
    datum["_mspass_delete_token"] = ObjectId()

    with pytest.raises(ValueError, match="cannot change sample ownership"):
        database.update_metadata(datum, mode="promiscuous")
    database.update_metadata(
        datum,
        mode="promiscuous",
        _lifecycle_managed=True,
    )

    document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
    assert "_mspass_delete_token" not in document


def test_file_metadata_repairs_remain_backward_compatible(database):
    waveform_id = (
        database["wf_TimeSeries"]
        .insert_one(
            {
                "storage_mode": "file",
                "dir": "/old",
                "dfile": "old.dat",
                "foff": 0,
                "history_object_id": ObjectId(),
            }
        )
        .inserted_id
    )
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum["_id"] = waveform_id
    datum["storage_mode"] = "file"
    datum["dir"] = "/old"
    datum["dfile"] = "old.dat"
    datum["foff"] = 0
    datum["history_object_id"] = ObjectId()
    datum.clear_modified()
    repaired_history_id = ObjectId()
    datum["dir"] = "/repaired"
    datum["dfile"] = "repaired.dat"
    datum["foff"] = 128
    datum["history_object_id"] = repaired_history_id

    database.update_metadata(datum, mode="promiscuous")

    document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
    assert document["dir"] == "/repaired"
    assert document["dfile"] == "repaired.dat"
    assert document["foff"] == 128
    assert document["history_object_id"] == repaired_history_id


def test_new_waveform_does_not_inherit_delete_claim(database):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    datum["_mspass_delete_token"] = ObjectId()

    saved = database.save_data(
        datum,
        storage_mode="gridfs",
        mode="promiscuous",
        save_history=False,
        return_data=True,
    )

    document = database["wf_TimeSeries"].find_one({"_id": saved["_id"]})
    assert "_mspass_delete_token" not in document
    assert "_mspass_delete_token" not in saved


def test_serial_gridfs_cremate_discards_live_zero_length(database):
    datum = TimeSeries(0)
    datum.set_live()

    result = database.save_data(
        datum,
        storage_mode="gridfs",
        mode="promiscuous",
        cremate=True,
        save_history=False,
        return_data=True,
    )

    assert result.dead()
    assert database["cemetery"].count_documents({}) == 0
    assert database["wf_TimeSeries"].count_documents({}) == 0
    assert database["fs.files"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0


def test_atomic_gridfs_zero_length_restores_source_owner(database):
    datum = TimeSeries(0)
    datum.set_live()
    original_owner = {
        "_id": ObjectId(),
        "storage_mode": "file",
        "dir": "/source",
        "dfile": "source.bin",
        "foff": 128,
        "history_object_id": ObjectId(),
        "elog_id": ObjectId(),
        "_mspass_delete_token": ObjectId(),
    }
    for key, value in original_owner.items():
        datum[key] = value

    result = database.save_data(
        datum,
        storage_mode="gridfs",
        mode="promiscuous",
        cremate=True,
        save_history=False,
        return_data=True,
    )

    assert result.dead()
    for key, value in original_owner.items():
        assert result[key] == value
    assert "gridfs_id" not in result
    assert database["wf_TimeSeries"].count_documents({}) == 0
    assert database["fs.files"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0


def test_atomic_gridfs_metadata_rejection_restores_source_owner(database, monkeypatch):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    original_owner = {
        "_id": ObjectId(),
        "storage_mode": "url",
        "url": "https://example.invalid/source",
        "history_object_id": ObjectId(),
        "elog_id": ObjectId(),
        "_mspass_delete_token": ObjectId(),
    }
    for key, value in original_owner.items():
        datum[key] = value

    def reject_metadata(*args, **kwargs):
        return dict(datum), False, database_module.ErrorLogger()

    monkeypatch.setattr(database_module, "md2doc", reject_metadata)
    result = database.save_data(
        datum,
        storage_mode="gridfs",
        mode="pedantic",
        cremate=True,
        save_history=False,
        return_data=True,
    )

    assert result.dead()
    for key, value in original_owner.items():
        assert result[key] == value
    assert "gridfs_id" not in result
    assert database["wf_TimeSeries"].count_documents({}) == 0
    assert database["fs.files"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0


def test_serial_gridfs_cremate_removes_new_zero_length_ensemble_member(database):
    ensemble = TimeSeriesEnsemble()
    zero_length = TimeSeries(0)
    zero_length.set_live()
    zero_length["member_marker"] = "zero"
    live_member = make_timeseries([1.0, 2.0, 3.0, 4.0])
    live_member["member_marker"] = "saved"
    ensemble.member.extend([zero_length, live_member])
    ensemble.set_live()

    result = database.save_data(
        ensemble,
        storage_mode="gridfs",
        mode="promiscuous",
        cremate=True,
        save_history=False,
        return_data=True,
    )

    assert result.live
    assert len(result.member) == 1
    assert result.member[0].live
    assert result.member[0]["member_marker"] == "saved"
    assert database["cemetery"].count_documents({}) == 0
    assert database["wf_TimeSeries"].count_documents({}) == 1
    assert database["fs.files"].count_documents({}) == 1


def test_serial_gridfs_cremate_removes_new_schema_rejection(database, monkeypatch):
    ensemble = TimeSeriesEnsemble()
    rejected = make_timeseries([1.0, 2.0, 3.0, 4.0])
    rejected["member_marker"] = "reject"
    saved_member = make_timeseries([5.0, 6.0, 7.0, 8.0])
    saved_member["member_marker"] = "saved"
    ensemble.member.extend([rejected, saved_member])
    ensemble.set_live()
    original_md2doc = database_module.md2doc

    def reject_marked(datum, *args, **kwargs):
        if "member_marker" in datum and datum["member_marker"] == "reject":
            return dict(datum), False, database_module.ErrorLogger()
        return original_md2doc(datum, *args, **kwargs)

    monkeypatch.setattr(database_module, "md2doc", reject_marked)

    result = database.save_data(
        ensemble,
        storage_mode="gridfs",
        mode="pedantic",
        cremate=True,
        save_history=False,
        return_data=True,
    )

    assert result.live
    assert len(result.member) == 1
    assert result.member[0].live
    assert result.member[0]["member_marker"] == "saved"
    assert database["cemetery"].count_documents({}) == 0
    assert database["wf_TimeSeries"].count_documents({}) == 1
    assert database["fs.files"].count_documents({}) == 1


def test_gridfs_ensemble_failure_restores_unattempted_member_storage(
    database, monkeypatch
):
    ensemble = TimeSeriesEnsemble()
    for offset in range(3):
        ensemble.member.append(
            make_timeseries([1.0 + offset, 2.0 + offset, 3.0 + offset])
        )
    ensemble.set_live()
    ensemble.member[0]["storage_mode"] = "file"
    ensemble.member[0]["dir"] = "/first"
    ensemble.member[0]["dfile"] = "first.dat"
    ensemble.member[1]["storage_mode"] = "url"
    ensemble.member[1]["url"] = "https://example.invalid/second"
    ensemble.member[2]["storage_mode"] = "object_store"
    ensemble.member[2]["object_store"] = {
        "provider": "s3",
        "bucket": "third-bucket",
        "object_name": "third.bin",
        "encoding": "float64-le-v1",
    }
    ownership_keys = database_module._OBJECT_STORE_STORAGE_KEYS

    def snapshot(datum):
        return {
            key: (True, copy.deepcopy(datum[key])) if key in datum else (False, None)
            for key in ownership_keys
        }

    before_second = snapshot(ensemble.member[1])
    before_third = snapshot(ensemble.member[2])
    original_put = gridfs.GridFS.put
    put_count = 0

    def fail_second_put(handle, *args, **kwargs):
        nonlocal put_count
        put_count += 1
        if put_count == 2:
            raise RuntimeError("known second-member GridFS failure")
        return original_put(handle, *args, **kwargs)

    monkeypatch.setattr(gridfs.GridFS, "put", fail_second_put)

    with pytest.raises(RuntimeError, match="second-member"):
        database.save_data(
            ensemble,
            storage_mode="gridfs",
            mode="promiscuous",
            save_history=False,
            return_data=True,
        )

    assert put_count == 2
    assert snapshot(ensemble.member[1]) == before_second
    assert snapshot(ensemble.member[2]) == before_third


def test_file_delete_forces_primary_and_majority_lifecycle_options(
    database, monkeypatch, tmp_path
):
    sample_file = Path(tmp_path) / "file-delete-majority.bin"
    sample_file.write_bytes(b"samples")
    history_id = database["history_object"].insert_one({"history": True}).inserted_id
    elog_id = database["elog"].insert_one({"elog": True}).inserted_id
    waveform_id = (
        database["wf_TimeSeries"]
        .insert_one(
            {
                "storage_mode": "file",
                "dir": str(tmp_path),
                "dfile": sample_file.name,
                "foff": 0,
                "history_object_id": history_id,
                "elog_id": elog_id,
            }
        )
        .inserted_id
    )
    configured_database = Database(
        database.client,
        database.name,
        read_preference=pymongo.ReadPreference.SECONDARY_PREFERRED,
        write_concern=pymongo.write_concern.WriteConcern(w=1),
    )
    original_delete_one = Collection.delete_one
    original_delete_many = Collection.delete_many
    original_count_documents = Collection.count_documents
    delete_options = []
    reference_reads = []

    def capture_delete_one(collection, *args, **kwargs):
        delete_options.append((collection.name, collection.write_concern.document))
        return original_delete_one(collection, *args, **kwargs)

    def capture_delete_many(collection, *args, **kwargs):
        delete_options.append((collection.name, collection.write_concern.document))
        return original_delete_many(collection, *args, **kwargs)

    def capture_count(collection, *args, **kwargs):
        if collection.name.startswith("wf_"):
            reference_reads.append(collection.read_preference)
        return original_count_documents(collection, *args, **kwargs)

    monkeypatch.setattr(Collection, "delete_one", capture_delete_one)
    monkeypatch.setattr(Collection, "delete_many", capture_delete_many)
    monkeypatch.setattr(Collection, "count_documents", capture_count)

    configured_database.delete_data(
        waveform_id,
        "TimeSeries",
        remove_unreferenced_files=True,
    )

    assert not sample_file.exists()
    assert {name for name, _ in delete_options} >= {
        "wf_TimeSeries",
        "history_object",
        "elog",
    }
    assert all(options.get("w") == "majority" for _, options in delete_options)
    assert reference_reads
    assert all(
        preference == pymongo.ReadPreference.PRIMARY for preference in reference_reads
    )


def test_metadata_update_started_before_delete_cannot_recreate_waveform(
    database, monkeypatch
):
    datum = save_original(database, make_timeseries)
    waveform_id = datum["_id"]
    datum["concurrent_metadata"] = "must-not-upsert"
    original_sync = Database._sync_metadata_before_update
    delete_completed = False

    def delete_between_primary_read_and_write(handle, target):
        nonlocal delete_completed
        if not delete_completed:
            delete_completed = True
            database.delete_data(waveform_id, "TimeSeries")
        return original_sync(target)

    monkeypatch.setattr(
        Database,
        "_sync_metadata_before_update",
        delete_between_primary_read_and_write,
    )

    with pytest.raises(MsPASSError, match="could not find the persisted waveform"):
        database.update_metadata(datum, mode="promiscuous")

    assert delete_completed
    assert database["wf_TimeSeries"].find_one({"_id": waveform_id}) is None
    assert database["fs.files"].count_documents({}) == 0


def test_gridfs_replacement_cas_binds_persisted_auxiliary_pointers(
    database, monkeypatch
):
    datum = database.save_data(
        make_timeseries([1.0, 2.0, 3.0, 4.0]),
        storage_mode="gridfs",
        save_history=True,
        return_data=True,
    )
    waveform_id = datum["_id"]
    old_gridfs_id = datum["gridfs_id"]
    concurrent_history_id = (
        database["history_object"].insert_one({"concurrent": True}).inserted_id
    )
    datum.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_update_one = Collection.update_one
    concurrent_change_applied = False

    def change_auxiliary_pointer_before_sample_cas(
        collection, query, update, *args, **kwargs
    ):
        nonlocal concurrent_change_applied
        new_gridfs_id = update.get("$set", {}).get("gridfs_id")
        if (
            collection.name == "wf_TimeSeries"
            and new_gridfs_id is not None
            and new_gridfs_id != old_gridfs_id
            and not concurrent_change_applied
        ):
            concurrent_change_applied = True
            original_update_one(
                collection,
                {"_id": waveform_id},
                {"$set": {"history_object_id": concurrent_history_id}},
            )
        return original_update_one(collection, query, update, *args, **kwargs)

    monkeypatch.setattr(
        Collection, "update_one", change_auxiliary_pointer_before_sample_cas
    )

    with pytest.raises(MsPASSError, match="could not commit the new GridFS"):
        database.update_data(datum, mode="promiscuous", save_history=False)

    assert concurrent_change_applied
    document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
    assert document["gridfs_id"] == old_gridfs_id
    assert document["history_object_id"] == concurrent_history_id
    assert database["history_object"].find_one({"_id": concurrent_history_id})
    assert database["fs.files"].count_documents({}) == 1
    assert database["gridfs_staging"].count_documents({}) == 0


def test_metadata_update_cannot_restore_gridfs_pointer_after_concurrent_replace(
    database, monkeypatch
):
    saved = save_original(database, make_timeseries)
    waveform_id = saved["_id"]
    old_gridfs_id = saved["gridfs_id"]
    metadata_writer = database.read_data(waveform_id, collection="wf_TimeSeries")
    metadata_writer["gridfs_id"] = old_gridfs_id
    metadata_writer["concurrent_metadata"] = "safe"
    sample_writer = database.read_data(waveform_id, collection="wf_TimeSeries")
    sample_writer.data = make_timeseries([5.0, 6.0, 7.0, 8.0]).data
    original_sync = Database._sync_metadata_before_update
    replacement_committed = False

    def replace_after_primary_read(target):
        nonlocal replacement_committed
        if not replacement_committed:
            replacement_committed = True
            database.update_data(
                sample_writer,
                mode="promiscuous",
                save_history=False,
            )
        return original_sync(target)

    monkeypatch.setattr(
        Database,
        "_sync_metadata_before_update",
        staticmethod(replace_after_primary_read),
    )

    database.update_metadata(metadata_writer, mode="promiscuous")

    assert replacement_committed
    document = database["wf_TimeSeries"].find_one({"_id": waveform_id})
    assert document["gridfs_id"] != old_gridfs_id
    assert document["concurrent_metadata"] == "safe"
    storage = gridfs.GridFS(database)
    assert not storage.exists(old_gridfs_id)
    assert storage.exists(document["gridfs_id"])


def test_distributed_gridfs_preflight_rejects_excluded_pointer(database):
    with pytest.raises(ValueError, match="gridfs_id is excluded"):
        write_distributed_data(
            None,
            database,
            storage_mode="gridfs",
            exclude_keys=["gridfs_id"],
        )

    assert database["wf_TimeSeries"].count_documents({}) == 0
    assert database["fs.files"].count_documents({}) == 0
    assert database["gridfs_staging"].count_documents({}) == 0


def test_distributed_gridfs_failure_leaves_reconcilable_stage(database, monkeypatch):
    datum = make_timeseries([1.0, 2.0, 3.0, 4.0])
    bag = dask.bag.from_sequence([datum], npartitions=1)
    failure = pymongo.errors.AutoReconnect("distributed waveform result unknown")
    original_insert_one = Collection.insert_one

    def fail_waveform_insert(self, document, *args, **kwargs):
        if self.name == "wf_TimeSeries":
            raise failure
        return original_insert_one(self, document, *args, **kwargs)

    monkeypatch.setattr(Collection, "insert_one", fail_waveform_insert)
    with dask.config.set(scheduler="synchronous"):
        with pytest.raises(MsPASSError, match="could not determine whether"):
            write_distributed_data(
                bag,
                database,
                storage_mode="gridfs",
                save_history=False,
            )

    stage = database["gridfs_staging"].find_one({})
    assert stage is not None
    assert gridfs.GridFS(database).exists(stage["new_gridfs_id"])
