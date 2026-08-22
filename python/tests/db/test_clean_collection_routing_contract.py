import copy
import os
import uuid

import gridfs
import pymongo
import pytest
from bson import ObjectId

from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database


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
    name = "test_clean_collection_routing_" + uuid.uuid4().hex
    try:
        database = Database(client, name)
        yield database
    finally:
        client.drop_database(name)
        client.close()


CASES = [
    ("TimeSeries", "wf_TimeSeries", "custom_TimeSeries"),
    ("Seismogram", "wf_Seismogram", "custom_Seismogram"),
]


def register_custom_collection(database, default_collection, custom_collection):
    database.database_schema[custom_collection] = copy.deepcopy(
        database.database_schema[default_collection]
    )


@pytest.mark.parametrize("object_type,default_collection,custom_collection", CASES)
@pytest.mark.parametrize("trigger", ["missing_required", "missing_xref"])
def test_clean_collection_deletes_only_the_explicit_collection(
    database, object_type, default_collection, custom_collection, trigger
):
    register_custom_collection(database, default_collection, custom_collection)
    assert (
        database.database_schema[custom_collection].data_type().__name__ == object_type
    )
    oid = ObjectId()
    fs = gridfs.GridFS(database)
    custom_gridfs_id = fs.put(b"custom waveform")
    default_gridfs_id = fs.put(b"default waveform")
    custom_document = {
        "_id": oid,
        "storage_mode": "gridfs",
        "gridfs_id": custom_gridfs_id,
        "marker": "custom",
    }
    default_document = {
        "_id": oid,
        "storage_mode": "gridfs",
        "gridfs_id": default_gridfs_id,
        "marker": "default",
    }
    database[custom_collection].insert_one(copy.deepcopy(custom_document))
    database[default_collection].insert_one(copy.deepcopy(default_document))
    custom_elog_id = (
        database["elog"]
        .insert_one({custom_collection + "_id": oid, "marker": "custom"})
        .inserted_id
    )
    default_elog_id = (
        database["elog"]
        .insert_one({default_collection + "_id": oid, "marker": "default"})
        .inserted_id
    )

    options = {"delete_missing_required": True}
    if trigger == "missing_xref":
        options = {
            "required_xref_list": ["site_id"],
            "delete_missing_xref": True,
        }
    result = database.clean_collection(custom_collection, **options)

    assert result == {}
    assert database[custom_collection].find_one({"_id": oid}) is None
    assert database[default_collection].find_one({"_id": oid}) == default_document
    assert not fs.exists(custom_gridfs_id)
    assert fs.exists(default_gridfs_id)
    assert database["elog"].find_one({"_id": custom_elog_id}) is None
    assert database["elog"].find_one({"_id": default_elog_id}) is not None


@pytest.mark.parametrize("object_type,default_collection,_custom_collection", CASES)
def test_delete_data_keeps_default_collection_behavior(
    database, object_type, default_collection, _custom_collection
):
    oid = ObjectId()
    fs = gridfs.GridFS(database)
    gridfs_id = fs.put(b"default waveform")
    database[default_collection].insert_one(
        {"_id": oid, "storage_mode": "gridfs", "gridfs_id": gridfs_id}
    )

    database.delete_data(oid, object_type)

    assert database[default_collection].find_one({"_id": oid}) is None
    assert not fs.exists(gridfs_id)


@pytest.mark.parametrize(
    "object_type,collection",
    [
        ("TimeSeries", "wf_Seismogram"),
        ("Seismogram", "wf_TimeSeries"),
        ("TimeSeries", "source"),
        ("TimeSeries", "not_a_schema_collection"),
    ],
)
def test_delete_data_rejects_an_explicit_nonmatching_waveform_collection(
    database, object_type, collection
):
    oid = ObjectId()

    with pytest.raises(MsPASSError) as error:
        database.delete_data(oid, object_type, collection=collection)

    assert error.value.severity == ErrorSeverity.Invalid


def test_clean_collection_canonicalizes_a_custom_collection_alias(database):
    default_collection = "wf_TimeSeries"
    custom_collection = "custom_TimeSeries"
    custom_alias = "custom_wf"
    register_custom_collection(database, default_collection, custom_collection)
    database.database_schema.set_default(custom_collection, custom_alias)
    oid = ObjectId()
    fs = gridfs.GridFS(database)
    custom_gridfs_id = fs.put(b"custom waveform")
    default_gridfs_id = fs.put(b"default waveform")
    database[custom_collection].insert_one(
        {"_id": oid, "storage_mode": "gridfs", "gridfs_id": custom_gridfs_id}
    )
    database[default_collection].insert_one(
        {"_id": oid, "storage_mode": "gridfs", "gridfs_id": default_gridfs_id}
    )

    database.clean_collection(custom_alias, delete_missing_required=True)

    assert database[custom_collection].find_one({"_id": oid}) is None
    assert database[default_collection].find_one({"_id": oid}) is not None
    assert not fs.exists(custom_gridfs_id)
    assert fs.exists(default_gridfs_id)


@pytest.mark.parametrize("object_type,default_collection,custom_collection", CASES)
def test_clean_collection_counts_file_references_in_the_explicit_collection(
    database, tmp_path, object_type, default_collection, custom_collection
):
    register_custom_collection(database, default_collection, custom_collection)
    oid = ObjectId()
    custom_file = tmp_path / "custom-waveforms"
    default_file = tmp_path / "default-waveforms"
    custom_file.write_bytes(b"shared custom waveform data")
    default_file.write_bytes(b"default waveform data")
    custom_document = {
        "_id": oid,
        "storage_mode": "file",
        "dir": str(tmp_path),
        "dfile": custom_file.name,
    }
    default_document = {
        "_id": oid,
        "storage_mode": "file",
        "dir": str(tmp_path),
        "dfile": default_file.name,
    }
    database[custom_collection].insert_one(copy.deepcopy(custom_document))
    database[custom_collection].insert_one(
        {
            "storage_mode": "file",
            "dir": str(tmp_path),
            "dfile": custom_file.name,
        }
    )
    database[default_collection].insert_one(copy.deepcopy(default_document))

    database.clean_collection(
        custom_collection,
        query={"_id": oid},
        delete_missing_required=True,
    )

    assert database[custom_collection].find_one({"_id": oid}) is None
    assert database[default_collection].find_one({"_id": oid}) == default_document
    assert custom_file.read_bytes() == b"shared custom waveform data"
    assert default_file.read_bytes() == b"default waveform data"
