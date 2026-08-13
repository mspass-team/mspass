import copy
import os
import pickle
import subprocess
import sys
import uuid
from importlib.metadata import distribution, version
from pathlib import Path

import gridfs
import pytest
from bson import ObjectId
from gridfs.errors import FileExists
from pymongo.errors import DuplicateKeyError

sys.path.append("python/tests")

from helper import (
    get_live_seismogram,
    get_live_seismogram_ensemble,
    get_live_timeseries,
    get_live_timeseries_ensemble,
)

import mspasspy.db.database as database_module
from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError, ProcessingHistory
from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database
from mspasspy.util import logging_helper


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


class InjectedWriteFailure(RuntimeError):
    pass


def _use_reserved_ids(monkeypatch, *reserved_ids):
    ids = iter(reserved_ids)

    def next_reserved_id(*args, **kwargs):
        if args or kwargs:
            return ObjectId(*args, **kwargs)
        return next(ids)

    monkeypatch.setattr(database_module, "ObjectId", next_reserved_id)


@pytest.fixture
def db():
    _assert_module_from_selected_build(database_module, Path("mspasspy/db/database.py"))
    client = DBClient("127.0.0.1")
    database_name = "issue_815_" + uuid.uuid4().hex
    database = Database(client, database_name)
    client.admin.command("ping")
    yield database
    client.drop_database(database_name)
    client.close()


def _document_snapshot(db):
    collection_names = (
        "wf_TimeSeries",
        "wf_Seismogram",
        "history_object",
        "elog",
        "cemetery",
        "abortions",
        "fs.files",
        "fs.chunks",
    )
    return {
        name: sorted(
            [copy.deepcopy(document) for document in db[name].find({})],
            key=lambda document: str(document["_id"]),
        )
        for name in collection_names
    }


def _reference_snapshot(datum):
    keys = (
        "_id",
        "storage_mode",
        "gridfs_id",
        "dir",
        "dfile",
        "foff",
        "nbytes",
        "format",
        "history_object_id",
        "elog_id",
    )
    return {key: datum[key] for key in keys if key in datum}


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


def _make_existing_datum(db, storage_mode, tmp_path=None, waveform_type="timeseries"):
    if waveform_type == "timeseries":
        datum = get_live_timeseries(ts_size=16)
        waveform_collection = "wf_TimeSeries"
    else:
        datum = get_live_seismogram(ts_size=16)
        waveform_collection = "wf_Seismogram"
    logging_helper.info(datum, "1", "processed")
    datum.elog.log_error("processed", "existing error", ErrorSeverity.Complaint)

    waveform_id = ObjectId()
    history_id = ObjectId()
    elog_id = ObjectId()
    waveform_document = {
        "_id": waveform_id,
        "history_object_id": history_id,
        "elog_id": elog_id,
        "storage_mode": storage_mode,
        "calib": datum["calib"],
    }
    db["history_object"].insert_one(
        {"_id": history_id, "marker": "pre-existing history"}
    )
    db["elog"].insert_one(
        {
            "_id": elog_id,
            waveform_collection + "_id": waveform_id,
            "logdata": [{"marker": "pre-existing elog"}],
        }
    )

    file_path = None
    if storage_mode == "gridfs":
        gridfs_id = gridfs.GridFS(db).put(b"pre-existing samples")
        datum["gridfs_id"] = gridfs_id
        waveform_document["gridfs_id"] = gridfs_id
    else:
        file_path = tmp_path / (waveform_type + "-samples.bin")
        file_path.write_bytes(b"pre-existing file samples")
        datum["dir"] = str(tmp_path)
        datum["dfile"] = file_path.name
        datum["foff"] = 0
        datum["nbytes"] = file_path.stat().st_size
        datum["format"] = "binary"
        waveform_document.update(
            {
                "dir": str(tmp_path),
                "dfile": file_path.name,
                "foff": 0,
                "nbytes": file_path.stat().st_size,
                "format": "binary",
            }
        )

    datum["_id"] = waveform_id
    datum["history_object_id"] = history_id
    datum["elog_id"] = elog_id
    datum["storage_mode"] = storage_mode
    db[waveform_collection].insert_one(waveform_document)
    datum.clear_modified()
    datum["calib"] = 2.0
    return datum, file_path


def _assert_failure_preserved_state(
    db,
    datum,
    documents_before,
    references_before,
    history_before,
    file_path=None,
    file_bytes_before=None,
):
    assert _document_snapshot(db) == documents_before
    assert _reference_snapshot(datum) == references_before
    assert _history_snapshot(datum) == history_before
    if file_path is not None:
        assert file_path.read_bytes() == file_bytes_before


def _raise_after_database_method(monkeypatch, method_name, failure):
    original = getattr(Database, method_name)

    def fail_after_call(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise failure

    monkeypatch.setattr(Database, method_name, fail_after_call)


def _raise_after_waveform_insert(monkeypatch, db, failure):
    original = Collection.insert_one

    def fail_after_insert(self, document, *args, **kwargs):
        result = original(self, document, *args, **kwargs)
        if self.database.name == db.name and self.name in (
            "wf_TimeSeries",
            "wf_Seismogram",
        ):
            raise failure
        return result

    monkeypatch.setattr(Collection, "insert_one", fail_after_insert)


def _raise_after_waveform_update(monkeypatch, db, failure):
    original = Collection.update_one

    def fail_after_update(self, *args, **kwargs):
        result = original(self, *args, **kwargs)
        if self.database.name == db.name and self.name in (
            "wf_TimeSeries",
            "wf_Seismogram",
        ):
            raise failure
        return result

    monkeypatch.setattr(Collection, "update_one", fail_after_update)


def _install_save_failure(monkeypatch, db, stage, failure):
    if stage == "metadata":
        monkeypatch.setattr(
            database_module,
            "md2doc",
            lambda *args, **kwargs: (_ for _ in ()).throw(failure),
        )
    elif stage == "gridfs":
        _raise_after_database_method(
            monkeypatch, "_save_sample_data_to_gridfs", failure
        )
    elif stage == "history":
        _raise_after_database_method(monkeypatch, "_save_history", failure)
    elif stage == "elog":
        _raise_after_database_method(monkeypatch, "_save_elog", failure)
    else:
        _raise_after_waveform_insert(monkeypatch, db, failure)


def _install_update_failure(monkeypatch, db, stage, failure):
    if stage == "metadata":
        _raise_after_database_method(monkeypatch, "update_metadata", failure)
    elif stage == "gridfs":
        _raise_after_database_method(
            monkeypatch, "_save_sample_data_to_gridfs", failure
        )
    elif stage == "history":
        _raise_after_database_method(monkeypatch, "_save_history", failure)
    elif stage == "elog":
        _raise_after_database_method(monkeypatch, "_save_elog", failure)
    else:
        _raise_after_waveform_update(monkeypatch, db, failure)


@pytest.mark.parametrize("stage", ["metadata", "gridfs", "history", "elog", "waveform"])
def test_save_gridfs_failures_are_compensated(db, monkeypatch, stage):
    datum, _ = _make_existing_datum(db, "gridfs")
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    failure = InjectedWriteFailure("save " + stage)
    _install_save_failure(monkeypatch, db, stage, failure)

    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            overwrite=True,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_save_file_sample_failure_restores_existing_file(db, monkeypatch, tmp_path):
    datum, file_path = _make_existing_datum(db, "file", tmp_path)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    file_bytes_before = file_path.read_bytes()
    failure = InjectedWriteFailure("file sample")
    original = database_module._fwrite_to_file

    def fail_after_file_write(*args, **kwargs):
        original(*args, **kwargs)
        raise failure

    monkeypatch.setattr(database_module, "_fwrite_to_file", fail_after_file_write)
    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="file",
            dir=str(tmp_path),
            dfile=file_path.name,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    _assert_failure_preserved_state(
        db,
        datum,
        documents_before,
        references_before,
        history_before,
        file_path,
        file_bytes_before,
    )


def test_save_file_mspass_error_is_rethrown_and_compensated(db, monkeypatch, tmp_path):
    datum, file_path = _make_existing_datum(db, "file", tmp_path)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    file_bytes_before = file_path.read_bytes()
    failure = MsPASSError("file sample failure", ErrorSeverity.Fatal)
    original = database_module._fwrite_to_file

    def fail_after_file_write(*args, **kwargs):
        original(*args, **kwargs)
        raise failure

    monkeypatch.setattr(database_module, "_fwrite_to_file", fail_after_file_write)
    with pytest.raises(MsPASSError) as caught:
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="file",
            dir=str(tmp_path),
            dfile=file_path.name,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    _assert_failure_preserved_state(
        db,
        datum,
        documents_before,
        references_before,
        history_before,
        file_path,
        file_bytes_before,
    )


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_file_rollback_preserves_concurrent_append(
    db, monkeypatch, tmp_path, waveform_type
):
    datum, file_path = _make_existing_datum(
        db, "file", tmp_path, waveform_type=waveform_type
    )
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    file_bytes_before = file_path.read_bytes()
    concurrent_bytes = b"concurrent append"
    original_write = database_module._fwrite_to_file

    def append_concurrently_then_write(*args, **kwargs):
        with open(file_path, "ab") as handle:
            handle.write(concurrent_bytes)
        return original_write(*args, **kwargs)

    failure = InjectedWriteFailure("waveform insert")
    monkeypatch.setattr(
        database_module, "_fwrite_to_file", append_concurrently_then_write
    )
    _raise_after_waveform_insert(monkeypatch, db, failure)
    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="file",
            dir=str(tmp_path),
            dfile=file_path.name,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )
    assert file_path.read_bytes() == file_bytes_before + concurrent_bytes


def test_save_removes_partial_gridfs_upload(db, monkeypatch):
    datum, _ = _make_existing_datum(db, "gridfs")
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    failure = InjectedWriteFailure("partial GridFS upload")

    def fail_after_partial_chunk(self, mspass_object, overwrite=False, **kwargs):
        new_gridfs_id = kwargs.get("new_gridfs_id", ObjectId())
        self["fs.chunks"].insert_one(
            {"files_id": new_gridfs_id, "n": 0, "data": b"partial"}
        )
        raise failure

    monkeypatch.setattr(
        Database, "_save_sample_data_to_gridfs", fail_after_partial_chunk
    )
    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            overwrite=True,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


@pytest.mark.parametrize("operation", ["save", "update"])
def test_gridfs_failure_after_old_delete_restores_old_file(db, monkeypatch, operation):
    datum, _ = _make_existing_datum(db, "gridfs")
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    failure = InjectedWriteFailure("GridFS put")

    def fail_put(*args, **kwargs):
        raise failure

    monkeypatch.setattr(gridfs.GridFS, "put", fail_put)
    with pytest.raises(InjectedWriteFailure) as caught:
        if operation == "save":
            db.save_data(
                datum,
                mode="promiscuous",
                storage_mode="gridfs",
                overwrite=True,
                save_history=True,
                return_data=True,
            )
        else:
            db.update_data(datum, mode="promiscuous")

    assert caught.value is failure
    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_save_waveform_id_collision_preserves_existing_document(db, monkeypatch):
    collision_id = ObjectId()
    sentinel = {"_id": collision_id, "marker": "pre-existing waveform"}
    db["wf_TimeSeries"].insert_one(sentinel)
    datum = get_live_timeseries(ts_size=8)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    _use_reserved_ids(monkeypatch, ObjectId(), collision_id)

    with pytest.raises(DuplicateKeyError):
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_save_history_id_collision_preserves_existing_document(db, monkeypatch):
    collision_id = ObjectId()
    sentinel = {"_id": collision_id, "marker": "pre-existing history"}
    db["history_object"].insert_one(sentinel)
    datum = get_live_timeseries(ts_size=8)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    _use_reserved_ids(monkeypatch, ObjectId(), ObjectId(), collision_id)

    with pytest.raises(DuplicateKeyError):
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=True,
            return_data=True,
        )

    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_save_elog_id_collision_preserves_existing_document(db, monkeypatch):
    collision_id = ObjectId()
    sentinel = {"_id": collision_id, "marker": "pre-existing elog"}
    db["elog"].insert_one(sentinel)
    datum = get_live_timeseries(ts_size=8)
    datum.elog.log_error("test", "new error", ErrorSeverity.Complaint)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    _use_reserved_ids(monkeypatch, ObjectId(), ObjectId(), collision_id)

    with pytest.raises(DuplicateKeyError):
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_save_gridfs_id_collision_preserves_empty_existing_file(db, monkeypatch):
    collision_id = ObjectId()
    gridfs.GridFS(db).put(b"", _id=collision_id)
    datum = get_live_timeseries(ts_size=8)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    _use_reserved_ids(monkeypatch, collision_id)

    with pytest.raises(FileExists):
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=False,
            return_data=True,
        )

    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )
    assert gridfs.GridFS(db).get(collision_id).read() == b""


@pytest.mark.parametrize("stage", ["metadata", "history", "elog", "waveform"])
def test_save_file_is_compensated_after_later_failures(
    db, monkeypatch, tmp_path, stage
):
    datum, file_path = _make_existing_datum(db, "file", tmp_path)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    file_bytes_before = file_path.read_bytes()
    failure = InjectedWriteFailure("file-backed save " + stage)
    _install_save_failure(monkeypatch, db, stage, failure)

    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            datum,
            mode="promiscuous",
            storage_mode="file",
            dir=str(tmp_path),
            dfile=file_path.name,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    _assert_failure_preserved_state(
        db,
        datum,
        documents_before,
        references_before,
        history_before,
        file_path,
        file_bytes_before,
    )


@pytest.mark.parametrize("stage", ["metadata", "gridfs", "history", "elog", "waveform"])
def test_update_gridfs_failures_are_compensated(db, monkeypatch, stage):
    datum, _ = _make_existing_datum(db, "gridfs")
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    failure = InjectedWriteFailure("update " + stage)
    _install_update_failure(monkeypatch, db, stage, failure)

    with pytest.raises(InjectedWriteFailure) as caught:
        db.update_data(datum, mode="promiscuous")

    assert caught.value is failure
    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_update_file_reference_survives_final_waveform_failure(
    db, monkeypatch, tmp_path
):
    datum, file_path = _make_existing_datum(db, "file", tmp_path)
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    file_bytes_before = file_path.read_bytes()
    failure = InjectedWriteFailure("update file-backed waveform")
    _raise_after_waveform_update(monkeypatch, db, failure)

    with pytest.raises(InjectedWriteFailure) as caught:
        db.update_data(datum, mode="promiscuous")

    assert caught.value is failure
    _assert_failure_preserved_state(
        db,
        datum,
        documents_before,
        references_before,
        history_before,
        file_path,
        file_bytes_before,
    )


def test_update_elog_id_collision_preserves_both_existing_documents(db, monkeypatch):
    datum, _ = _make_existing_datum(db, "gridfs")
    collision_id = ObjectId()
    db["elog"].insert_one(
        {"_id": collision_id, "marker": "unrelated pre-existing elog"}
    )
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    _use_reserved_ids(monkeypatch, ObjectId(), ObjectId(), collision_id)

    with pytest.raises(DuplicateKeyError):
        db.update_data(datum, mode="promiscuous")

    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_rollback_failure_does_not_replace_original_exception(db, monkeypatch):
    datum, _ = _make_existing_datum(db, "gridfs")
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    failure = InjectedWriteFailure("waveform update")
    cleanup_failure = InjectedWriteFailure("cleanup")
    _raise_after_waveform_update(monkeypatch, db, failure)
    original_rollback = Database._rollback_write_artifacts

    def fail_after_rollback(self, rollback_state):
        original_rollback(self, rollback_state)
        raise cleanup_failure

    monkeypatch.setattr(Database, "_rollback_write_artifacts", fail_after_rollback)
    with pytest.raises(InjectedWriteFailure) as caught:
        db.update_data(datum, mode="promiscuous")

    assert caught.value is failure
    _assert_failure_preserved_state(
        db, datum, documents_before, references_before, history_before
    )


def test_update_rollback_preserves_concurrent_unrelated_field(db, monkeypatch):
    datum, _ = _make_existing_datum(db, "gridfs")
    documents_before = _document_snapshot(db)
    expected_documents = copy.deepcopy(documents_before)
    expected_documents["wf_TimeSeries"][0]["concurrent_marker"] = "keep"
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    failure = InjectedWriteFailure("waveform update")
    original_update = Collection.update_one

    def fail_after_update_and_concurrent_write(self, *args, **kwargs):
        result = original_update(self, *args, **kwargs)
        if self.database.name == db.name and self.name == "wf_TimeSeries":
            original_update(
                self,
                {"_id": datum["_id"]},
                {"$set": {"concurrent_marker": "keep"}},
            )
            raise failure
        return result

    monkeypatch.setattr(
        Collection, "update_one", fail_after_update_and_concurrent_write
    )
    with pytest.raises(InjectedWriteFailure) as caught:
        db.update_data(datum, mode="promiscuous")

    assert caught.value is failure
    assert _document_snapshot(db) == expected_documents
    assert _reference_snapshot(datum) == references_before
    assert _history_snapshot(datum) == history_before


@pytest.mark.parametrize(
    ("operation", "storage_mode"),
    [
        ("save", "gridfs"),
        ("save", "file"),
        ("update", "gridfs"),
        ("update", "file"),
    ],
)
def test_seismogram_final_waveform_failure_is_atomic(
    db, monkeypatch, tmp_path, operation, storage_mode
):
    datum, file_path = _make_existing_datum(
        db, storage_mode, tmp_path, waveform_type="seismogram"
    )
    documents_before = _document_snapshot(db)
    references_before = _reference_snapshot(datum)
    history_before = _history_snapshot(datum)
    file_bytes_before = file_path.read_bytes() if file_path is not None else None
    failure = InjectedWriteFailure(operation + " Seismogram waveform")

    if operation == "save":
        _raise_after_waveform_insert(monkeypatch, db, failure)
        save_kwargs = {
            "mode": "promiscuous",
            "storage_mode": storage_mode,
            "save_history": True,
            "return_data": True,
        }
        if storage_mode == "gridfs":
            save_kwargs["overwrite"] = True
        else:
            save_kwargs["dir"] = str(tmp_path)
            save_kwargs["dfile"] = file_path.name
        operation_call = lambda: db.save_data(datum, **save_kwargs)
    else:
        _raise_after_waveform_update(monkeypatch, db, failure)
        operation_call = lambda: db.update_data(datum, mode="promiscuous")

    with pytest.raises(InjectedWriteFailure) as caught:
        operation_call()

    assert caught.value is failure
    _assert_failure_preserved_state(
        db,
        datum,
        documents_before,
        references_before,
        history_before,
        file_path,
        file_bytes_before,
    )


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_ensemble_later_member_failure_rolls_back_earlier_member(
    db, monkeypatch, waveform_type
):
    if waveform_type == "timeseries":
        ensemble = get_live_timeseries_ensemble(2)
        waveform_collection = "wf_TimeSeries"
    else:
        ensemble = get_live_seismogram_ensemble(2)
        waveform_collection = "wf_Seismogram"
    for member in ensemble.member:
        logging_helper.info(member, "1", "processed")
        member.elog.log_error("processed", "existing error", ErrorSeverity.Complaint)
    documents_before = _document_snapshot(db)
    references_before = [_reference_snapshot(member) for member in ensemble.member]
    histories_before = [_history_snapshot(member) for member in ensemble.member]
    failure = InjectedWriteFailure("second ensemble waveform")
    original_insert = Collection.insert_one
    waveform_insert_count = 0

    def fail_after_second_waveform(self, document, *args, **kwargs):
        nonlocal waveform_insert_count
        result = original_insert(self, document, *args, **kwargs)
        if self.database.name == db.name and self.name == waveform_collection:
            waveform_insert_count += 1
            if waveform_insert_count == 2:
                raise failure
        return result

    monkeypatch.setattr(Collection, "insert_one", fail_after_second_waveform)
    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            ensemble,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    assert waveform_insert_count == 2
    assert _document_snapshot(db) == documents_before
    assert [
        _reference_snapshot(member) for member in ensemble.member
    ] == references_before
    assert [_history_snapshot(member) for member in ensemble.member] == histories_before


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_file_ensemble_later_member_failure_removes_all_segments(
    db, monkeypatch, tmp_path, waveform_type
):
    if waveform_type == "timeseries":
        ensemble = get_live_timeseries_ensemble(2)
        waveform_collection = "wf_TimeSeries"
    else:
        ensemble = get_live_seismogram_ensemble(2)
        waveform_collection = "wf_Seismogram"
    for member in ensemble.member:
        logging_helper.info(member, "1", "processed")
    file_path = tmp_path / (waveform_type + "-ensemble.bin")
    file_bytes_before = b"pre-existing ensemble file"
    file_path.write_bytes(file_bytes_before)
    documents_before = _document_snapshot(db)
    references_before = [_reference_snapshot(member) for member in ensemble.member]
    histories_before = [_history_snapshot(member) for member in ensemble.member]
    failure = InjectedWriteFailure("second file ensemble waveform")
    original_insert = Collection.insert_one
    waveform_insert_count = 0

    def fail_after_second_waveform(self, document, *args, **kwargs):
        nonlocal waveform_insert_count
        result = original_insert(self, document, *args, **kwargs)
        if self.database.name == db.name and self.name == waveform_collection:
            waveform_insert_count += 1
            if waveform_insert_count == 2:
                raise failure
        return result

    monkeypatch.setattr(Collection, "insert_one", fail_after_second_waveform)
    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            ensemble,
            mode="promiscuous",
            storage_mode="file",
            dir=str(tmp_path),
            dfile=file_path.name,
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    assert waveform_insert_count == 2
    assert _document_snapshot(db) == documents_before
    assert file_path.read_bytes() == file_bytes_before
    assert [
        _reference_snapshot(member) for member in ensemble.member
    ] == references_before
    assert [_history_snapshot(member) for member in ensemble.member] == histories_before


def test_ensemble_live_failure_does_not_bury_dead_members(db, monkeypatch):
    ensemble = get_live_timeseries_ensemble(3)
    ensemble.member[0].elog.log_error(
        "test", "known dead member", ErrorSeverity.Invalid
    )
    ensemble.member[0].kill()
    documents_before = _document_snapshot(db)
    failure = InjectedWriteFailure("second live waveform")
    original_insert = Collection.insert_one
    waveform_insert_count = 0

    def fail_after_second_waveform(self, document, *args, **kwargs):
        nonlocal waveform_insert_count
        result = original_insert(self, document, *args, **kwargs)
        if self.database.name == db.name and self.name == "wf_TimeSeries":
            waveform_insert_count += 1
            if waveform_insert_count == 2:
                raise failure
        return result

    monkeypatch.setattr(Collection, "insert_one", fail_after_second_waveform)
    with pytest.raises(InjectedWriteFailure) as caught:
        db.save_data(
            ensemble,
            mode="promiscuous",
            storage_mode="gridfs",
            save_history=True,
            return_data=True,
        )

    assert caught.value is failure
    assert waveform_insert_count == 2
    assert _document_snapshot(db) == documents_before


def _observe_history_reset(monkeypatch, db, observations):
    original = Database._reset_processing_history

    def assert_durable_before_reset(datum, alg_name, alg_id, save_uuid):
        waveform_collection = (
            "wf_Seismogram" if isinstance(datum, Seismogram) else "wf_TimeSeries"
        )
        waveform_document = db[waveform_collection].find_one({"_id": datum["_id"]})
        history_document = db["history_object"].find_one(
            {"_id": waveform_document["history_object_id"]}
        )
        elog_document = db["elog"].find_one({"_id": waveform_document["elog_id"]})
        assert history_document[waveform_collection + "_id"] == datum["_id"]
        assert elog_document[waveform_collection + "_id"] == datum["_id"]
        if waveform_document["storage_mode"] == "gridfs":
            assert gridfs.GridFS(db).exists(waveform_document["gridfs_id"])
        else:
            sample_file = Path(waveform_document["dir"]) / waveform_document["dfile"]
            assert sample_file.exists()
            assert sample_file.stat().st_size >= (
                waveform_document["foff"] + waveform_document["nbytes"]
            )
        observations.append(copy.deepcopy(waveform_document))
        original(datum, alg_name, alg_id, save_uuid)

    monkeypatch.setattr(
        Database, "_reset_processing_history", staticmethod(assert_durable_before_reset)
    )


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_save_resets_history_only_after_all_references_are_durable(
    db, monkeypatch, waveform_type
):
    datum, _ = _make_existing_datum(db, "gridfs", waveform_type=waveform_type)
    observations = []
    _observe_history_reset(monkeypatch, db, observations)

    result = db.save_data(
        datum,
        mode="promiscuous",
        storage_mode="gridfs",
        overwrite=True,
        save_history=True,
        return_data=True,
    )

    assert result is datum
    assert len(observations) == 1
    assert datum.is_origin()
    assert observations[0]["history_object_id"] == datum["history_object_id"]
    assert observations[0]["elog_id"] == datum["elog_id"]
    assert observations[0]["gridfs_id"] == datum["gridfs_id"]


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_file_save_resets_history_only_after_references_are_durable(
    db, monkeypatch, tmp_path, waveform_type
):
    datum, file_path = _make_existing_datum(
        db, "file", tmp_path, waveform_type=waveform_type
    )
    observations = []
    _observe_history_reset(monkeypatch, db, observations)

    result = db.save_data(
        datum,
        mode="promiscuous",
        storage_mode="file",
        dir=str(tmp_path),
        dfile=file_path.name,
        save_history=True,
        return_data=True,
    )

    assert result is datum
    assert len(observations) == 1
    assert datum.is_origin()
    assert observations[0]["history_object_id"] == datum["history_object_id"]
    assert observations[0]["elog_id"] == datum["elog_id"]
    assert observations[0]["foff"] == datum["foff"]


@pytest.mark.parametrize("waveform_type", ["timeseries", "seismogram"])
def test_update_resets_history_only_after_all_references_are_durable(
    db, monkeypatch, waveform_type
):
    datum, _ = _make_existing_datum(db, "gridfs", waveform_type=waveform_type)
    observations = []
    _observe_history_reset(monkeypatch, db, observations)

    result = db.update_data(datum, mode="promiscuous")

    assert result is datum
    assert len(observations) == 1
    assert datum.is_origin()
    assert observations[0]["history_object_id"] == datum["history_object_id"]
    assert observations[0]["elog_id"] == datum["elog_id"]
    assert observations[0]["gridfs_id"] == datum["gridfs_id"]


def test_save_history_alone_does_not_reset_the_caller(db):
    datum = get_live_timeseries(ts_size=8)
    logging_helper.info(datum, "1", "processed")
    history_before = _history_snapshot(datum)

    history_id = db._save_history(datum)

    assert db["history_object"].find_one({"_id": history_id}) is not None
    assert _history_snapshot(datum) == history_before
