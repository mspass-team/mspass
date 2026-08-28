import copy
import os
import uuid
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import gridfs
import pytest
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database


@pytest.fixture
def database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://127.0.0.1:27017")
    probe = MongoClient(uri, serverSelectionTimeoutMS=2000)
    try:
        probe.admin.command("ping")
    except ServerSelectionTimeoutError as error:
        pytest.skip(f"MongoDB is unavailable at {uri}: {error}")
    finally:
        probe.close()
    client = DBClient(uri, serverSelectionTimeoutMS=2000)
    name = "test_delete_cleanup_order_" + uuid.uuid4().hex
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


OBJECT_CASES = [
    ("TimeSeries", "wf_TimeSeries"),
    ("Seismogram", "wf_Seismogram"),
]


def make_resource_graph(database, tmp_path, collection, storage_mode):
    parent_id = ObjectId()
    history_id = database["history_object"].insert_one({"target": True}).inserted_id
    waveform_id_key = collection + "_id"
    elog_id = (
        database["elog"]
        .insert_one({"logdata": [], waveform_id_key: parent_id, "explicit": True})
        .inserted_id
    )
    linked_elog_id = (
        database["elog"]
        .insert_one({"logdata": [], waveform_id_key: parent_id, "linked": True})
        .inserted_id
    )

    document = {
        "_id": parent_id,
        "storage_mode": storage_mode,
        "history_object_id": history_id,
        "elog_id": elog_id,
    }
    target_file = tmp_path / f"target-{parent_id}.bin"
    target_gridfs_id = None
    if storage_mode == "file":
        target_file.write_bytes(b"target samples")
        document.update({"dir": str(tmp_path), "dfile": target_file.name})
    else:
        target_gridfs_id = gridfs.GridFS(database).put(b"target samples")
        document["gridfs_id"] = target_gridfs_id
    database[collection].insert_one(document)

    unrelated_parent_id = (
        database[collection]
        .insert_one({"storage_mode": "none", "unrelated": True})
        .inserted_id
    )
    unrelated_history_id = (
        database["history_object"].insert_one({"unrelated": True}).inserted_id
    )
    unrelated_elog_id = (
        database["elog"]
        .insert_one({"logdata": [], waveform_id_key: unrelated_parent_id})
        .inserted_id
    )
    unrelated_gridfs_id = gridfs.GridFS(database).put(b"unrelated samples")
    unrelated_file = tmp_path / f"unrelated-{parent_id}.bin"
    unrelated_file.write_bytes(b"unrelated samples")

    return SimpleNamespace(
        collection=collection,
        parent_id=parent_id,
        parent_document=copy.deepcopy(document),
        history_id=history_id,
        elog_id=elog_id,
        linked_elog_id=linked_elog_id,
        target_file=target_file,
        target_gridfs_id=target_gridfs_id,
        unrelated_parent_id=unrelated_parent_id,
        unrelated_history_id=unrelated_history_id,
        unrelated_elog_id=unrelated_elog_id,
        unrelated_gridfs_id=unrelated_gridfs_id,
        unrelated_file=unrelated_file,
    )


def sample_exists(database, graph):
    if graph.target_gridfs_id is not None:
        return gridfs.GridFS(database).exists(graph.target_gridfs_id)
    return graph.target_file.exists()


def assert_unrelated_resources_exist(database, graph):
    assert database[graph.collection].find_one({"_id": graph.unrelated_parent_id})
    assert database["history_object"].find_one({"_id": graph.unrelated_history_id})
    assert database["elog"].find_one({"_id": graph.unrelated_elog_id})
    assert gridfs.GridFS(database).exists(graph.unrelated_gridfs_id)
    assert graph.unrelated_file.exists()


@pytest.mark.parametrize("object_type,collection", OBJECT_CASES)
@pytest.mark.parametrize(
    "failure_stage,storage_mode",
    [
        ("file", "file"),
        ("gridfs", "gridfs"),
        ("history", "gridfs"),
        ("elog", "gridfs"),
    ],
)
def test_child_cleanup_failure_keeps_parent_and_retry_finishes_deletion(
    database, tmp_path, object_type, collection, failure_stage, storage_mode
):
    graph = make_resource_graph(database, tmp_path, collection, storage_mode)
    failure = RuntimeError(f"injected {failure_stage} cleanup failure")
    original_delete_one = Collection.delete_one
    failed_collection = (
        "history_object" if failure_stage == "history" else failure_stage
    )

    def fail_selected_collection(self, *args, **kwargs):
        if self.name == failed_collection:
            raise failure
        return original_delete_one(self, *args, **kwargs)

    with ExitStack() as stack:
        if failure_stage == "file":
            stack.enter_context(patch("os.remove", side_effect=failure))
        elif failure_stage == "gridfs":
            stack.enter_context(
                patch.object(gridfs.GridFS, "delete", side_effect=failure)
            )
        else:
            stack.enter_context(
                patch.object(Collection, "delete_one", fail_selected_collection)
            )
        with pytest.raises(RuntimeError) as error:
            database.delete_data(
                graph.parent_id,
                object_type,
                remove_unreferenced_files=True,
            )

    assert error.value is failure
    retained_parent = database[collection].find_one({"_id": graph.parent_id})
    assert isinstance(retained_parent.pop("_mspass_delete_token"), ObjectId)
    assert retained_parent == graph.parent_document
    assert sample_exists(database, graph)
    assert bool(database["history_object"].find_one({"_id": graph.history_id})) is (
        failure_stage == "history"
    )
    assert bool(database["elog"].find_one({"_id": graph.elog_id})) is (
        failure_stage in {"history", "elog"}
    )
    assert bool(database["elog"].find_one({"_id": graph.linked_elog_id})) is (
        failure_stage in {"history", "elog"}
    )
    assert_unrelated_resources_exist(database, graph)

    database.delete_data(
        graph.parent_id,
        object_type,
        remove_unreferenced_files=True,
    )

    assert database[collection].find_one({"_id": graph.parent_id}) is None
    assert not sample_exists(database, graph)
    assert database["history_object"].find_one({"_id": graph.history_id}) is None
    assert database["elog"].find_one({"_id": graph.elog_id}) is None
    assert database["elog"].find_one({"_id": graph.linked_elog_id}) is None
    assert_unrelated_resources_exist(database, graph)


@pytest.mark.parametrize("object_type,collection", OBJECT_CASES)
@pytest.mark.parametrize("storage_mode", ["file", "gridfs"])
def test_already_missing_referenced_children_are_retry_safe(
    database, tmp_path, object_type, collection, storage_mode
):
    graph = make_resource_graph(database, tmp_path, collection, storage_mode)
    if graph.target_gridfs_id is not None:
        gridfs.GridFS(database).delete(graph.target_gridfs_id)
    else:
        graph.target_file.unlink()
    database["history_object"].delete_one({"_id": graph.history_id})
    database["elog"].delete_one({"_id": graph.elog_id})

    database.delete_data(
        graph.parent_id,
        object_type,
        remove_unreferenced_files=True,
    )

    assert database[collection].find_one({"_id": graph.parent_id}) is None
    assert database["elog"].find_one({"_id": graph.linked_elog_id}) is None
    assert_unrelated_resources_exist(database, graph)


@pytest.mark.parametrize(
    "object_type,collection,parent_fields",
    [
        ("TimeSeries", "wf_TimeSeries", {"storage_mode": "gridfs"}),
        (
            "Seismogram",
            "wf_Seismogram",
            {"storage_mode": "file", "dir": "unused"},
        ),
    ],
)
def test_missing_sample_reference_metadata_does_not_delete_unrelated_resources(
    database, tmp_path, object_type, collection, parent_fields
):
    parent_id = ObjectId()
    database[collection].insert_one({"_id": parent_id, **parent_fields})
    linked_elog_id = (
        database["elog"]
        .insert_one({collection + "_id": parent_id, "logdata": []})
        .inserted_id
    )
    unrelated_gridfs_id = gridfs.GridFS(database).put(b"unrelated")
    unrelated_file = tmp_path / "unrelated.bin"
    unrelated_file.write_bytes(b"unrelated")
    unrelated_history_id = (
        database["history_object"].insert_one({"unrelated": True}).inserted_id
    )
    unrelated_elog_id = (
        database["elog"].insert_one({"unrelated": True, "logdata": []}).inserted_id
    )

    database.delete_data(
        parent_id,
        object_type,
        remove_unreferenced_files=True,
    )

    assert database[collection].find_one({"_id": parent_id}) is None
    assert database["elog"].find_one({"_id": linked_elog_id}) is None
    assert gridfs.GridFS(database).exists(unrelated_gridfs_id)
    assert unrelated_file.exists()
    assert database["history_object"].find_one({"_id": unrelated_history_id})
    assert database["elog"].find_one({"_id": unrelated_elog_id})


@pytest.mark.parametrize(
    "clear_history,clear_elog",
    [(False, True), (True, False), (False, False)],
)
def test_optional_cleanup_flags_preserve_unrequested_children(
    database, clear_history, clear_elog
):
    parent_id = ObjectId()
    history_id = database["history_object"].insert_one({"target": True}).inserted_id
    explicit_elog_id = (
        database["elog"]
        .insert_one({"wf_TimeSeries_id": parent_id, "explicit": True})
        .inserted_id
    )
    linked_elog_id = (
        database["elog"]
        .insert_one({"wf_TimeSeries_id": parent_id, "linked": True})
        .inserted_id
    )
    database["wf_TimeSeries"].insert_one(
        {
            "_id": parent_id,
            "storage_mode": "file",
            "history_object_id": history_id,
            "elog_id": explicit_elog_id,
        }
    )

    database.delete_data(
        parent_id,
        "TimeSeries",
        clear_history=clear_history,
        clear_elog=clear_elog,
    )

    assert database["wf_TimeSeries"].find_one({"_id": parent_id}) is None
    assert bool(database["history_object"].find_one({"_id": history_id})) is (
        not clear_history
    )
    assert bool(database["elog"].find_one({"_id": explicit_elog_id})) is (
        not clear_elog
    )
    assert bool(database["elog"].find_one({"_id": linked_elog_id})) is (not clear_elog)


def test_shared_file_across_waveform_types_is_removed_only_after_the_last_parent(
    database, tmp_path
):
    shared_file = tmp_path / "shared.bin"
    shared_file.write_bytes(b"shared samples")
    first_id = (
        database["wf_TimeSeries"]
        .insert_one(
            {
                "storage_mode": "file",
                "dir": str(tmp_path),
                "dfile": shared_file.name,
            }
        )
        .inserted_id
    )
    second_id = (
        database["wf_Seismogram"]
        .insert_one(
            {
                "storage_mode": "file",
                "dir": str(tmp_path),
                "dfile": shared_file.name,
            }
        )
        .inserted_id
    )

    database.delete_data(first_id, "TimeSeries", remove_unreferenced_files=True)
    assert shared_file.exists()
    assert database["wf_Seismogram"].find_one({"_id": second_id})

    database.delete_data(second_id, "Seismogram", remove_unreferenced_files=True)
    assert not shared_file.exists()


def test_schema_defined_waveform_collection_preserves_a_shared_file(database, tmp_path):
    shared_file = tmp_path / "shared-with-archive.bin"
    shared_file.write_bytes(b"shared samples")
    parent_id = (
        database["wf_TimeSeries"]
        .insert_one(
            {
                "storage_mode": "file",
                "dir": str(tmp_path),
                "dfile": shared_file.name,
            }
        )
        .inserted_id
    )
    archive_collection = "wf_TimeSeries_archive"
    database.database_schema[archive_collection] = copy.deepcopy(
        database.database_schema["wf_TimeSeries"]
    )
    database[archive_collection].insert_one(
        {
            "storage_mode": "file",
            "dir": str(tmp_path),
            "dfile": shared_file.name,
        }
    )

    database.delete_data(parent_id, "TimeSeries", remove_unreferenced_files=True)

    assert shared_file.exists()
