import copy
import math
import os
import uuid
from unittest.mock import patch

import pytest

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
    name = "test_clean_field_updates_" + uuid.uuid4().hex
    database = Database(client, name)
    try:
        yield database
    finally:
        client.drop_database(name)
        client.close()


def test_clean_uses_exact_field_updates_and_preserves_concurrent_field(database):
    collection = database["wf_TimeSeries"]
    document_id = collection.insert_one(
        {
            "npts": "7",
            "calib": 1.0,
            "old_undefined": "rename me",
            "untouched": "keep me",
        }
    ).inserted_id
    update_documents = []
    original_update_one = Collection.update_one

    def interleaved_update(self, query, update, *args, **kwargs):
        if self.name == "wf_TimeSeries" and query == {"_id": document_id}:
            original_update_one(self, query, {"$set": {"calib": 9.5}}, *args, **kwargs)
            update_documents.append(copy.deepcopy(update))
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("clean must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", interleaved_update),
    ):
        fixed = database.clean(
            document_id,
            collection="wf_TimeSeries",
            rename_undefined={"old_undefined": "new_undefined"},
        )

    assert fixed == {"npts": 1}
    assert update_documents == [
        {
            "$set": {"npts": 7, "new_undefined": "rename me"},
            "$unset": {"old_undefined": ""},
        }
    ]
    result = collection.find_one({"_id": document_id})
    assert result["npts"] == 7
    assert result["new_undefined"] == "rename me"
    assert "old_undefined" not in result
    assert result["calib"] == 9.5
    assert result["untouched"] == "keep me"


def test_clean_delete_undefined_uses_only_unset(database):
    collection = database["wf_TimeSeries"]
    document_id = collection.insert_one(
        {"npts": 7, "calib": 1.0, "remove_me": "undefined"}
    ).inserted_id
    update_documents = []
    original_update_one = Collection.update_one

    def capture_update(self, query, update, *args, **kwargs):
        update_documents.append(copy.deepcopy(update))
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("clean must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", capture_update),
    ):
        database.clean(
            document_id,
            collection="wf_TimeSeries",
            delete_undefined=True,
        )

    assert update_documents == [{"$unset": {"remove_me": ""}}]
    result = collection.find_one({"_id": document_id})
    assert "remove_me" not in result
    assert result["npts"] == 7
    assert result["calib"] == 1.0


def test_rename_uses_exact_field_updates_and_preserves_concurrent_field(database):
    collection = database["wf_TimeSeries"]
    document_id = collection.insert_one(
        {
            "old_name": 1,
            "middle_name": 2,
            "calib": 1.0,
            "untouched": "keep me",
        }
    ).inserted_id
    update_documents = []
    original_update_one = Collection.update_one

    def interleaved_update(self, query, update, *args, **kwargs):
        if self.name == "wf_TimeSeries" and query == {"_id": document_id}:
            original_update_one(self, query, {"$set": {"calib": 8.5}}, *args, **kwargs)
            update_documents.append(copy.deepcopy(update))
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("rename must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", interleaved_update),
    ):
        counts = database._rename_attributes(
            "wf_TimeSeries",
            {"old_name": "middle_name", "middle_name": "final_name"},
            query={"_id": document_id},
        )

    assert counts == {"old_name": 1, "middle_name": 1}
    assert update_documents == [
        {
            "$set": {"final_name": 1},
            "$unset": {"old_name": "", "middle_name": ""},
        }
    ]
    result = collection.find_one({"_id": document_id})
    assert result["final_name"] == 1
    assert "old_name" not in result
    assert "middle_name" not in result
    assert result["calib"] == 8.5
    assert result["untouched"] == "keep me"


def test_clean_preserves_canonical_value_when_it_follows_an_alias(database):
    collection = database["wf_TimeSeries"]
    document_id = collection.insert_one({"dt": 2.0, "delta": 1.0}).inserted_id
    update_documents = []
    original_update_one = Collection.update_one

    def capture_update(self, query, update, *args, **kwargs):
        update_documents.append(copy.deepcopy(update))
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("clean must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", capture_update),
    ):
        fixed = database.clean(document_id, collection="wf_TimeSeries")

    assert fixed == {}
    assert update_documents == [{"$set": {"delta": 1.0}, "$unset": {"dt": ""}}]
    assert collection.find_one({"_id": document_id})["delta"] == 1.0


def test_clean_preserves_existing_rename_target_and_identity_is_noop(database):
    collection = database["wf_TimeSeries"]
    collision_id = collection.insert_one(
        {"old_name": "source", "target_name": "target"}
    ).inserted_id
    identity_id = collection.insert_one({"same_name": "same"}).inserted_id
    updates_by_id = {}
    original_update_one = Collection.update_one

    def capture_update(self, query, update, *args, **kwargs):
        updates_by_id[query["_id"]] = copy.deepcopy(update)
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("clean must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", capture_update),
    ):
        database.clean(
            collision_id,
            collection="wf_TimeSeries",
            rename_undefined={"old_name": "target_name"},
        )
        database.clean(
            identity_id,
            collection="wf_TimeSeries",
            rename_undefined={"same_name": "same_name"},
        )

    assert updates_by_id == {
        collision_id: {
            "$set": {"target_name": "target"},
            "$unset": {"old_name": ""},
        }
    }
    collision = collection.find_one({"_id": collision_id})
    assert collision["target_name"] == "target"
    assert "old_name" not in collision
    assert collection.find_one({"_id": identity_id})["same_name"] == "same"


def test_clean_failed_conversion_keeps_existing_unset_behavior(database):
    collection = database["wf_TimeSeries"]
    document_id = collection.insert_one({"npts": "not-an-integer"}).inserted_id
    update_documents = []
    original_update_one = Collection.update_one

    def capture_update(self, query, update, *args, **kwargs):
        update_documents.append(copy.deepcopy(update))
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("clean must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", capture_update),
    ):
        fixed = database.clean(document_id, collection="wf_TimeSeries")

    assert fixed == {}
    assert update_documents == [{"$unset": {"npts": ""}}]
    assert "npts" not in collection.find_one({"_id": document_id})


def test_rename_does_not_write_an_unselected_nan_field(database):
    collection = database["wf_TimeSeries"]
    document_id = collection.insert_one(
        {"old_name": 1, "unselected_nan": float("nan")}
    ).inserted_id
    update_documents = []
    original_update_one = Collection.update_one

    def capture_update(self, query, update, *args, **kwargs):
        update_documents.append(copy.deepcopy(update))
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("rename must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", capture_update),
    ):
        counts = database._rename_attributes(
            "wf_TimeSeries",
            {"old_name": "new_name"},
            query={"_id": document_id},
        )

    assert counts == {"old_name": 1}
    assert update_documents == [{"$set": {"new_name": 1}, "$unset": {"old_name": ""}}]
    result = collection.find_one({"_id": document_id})
    assert result["new_name"] == 1
    assert math.isnan(result["unselected_nan"])


def test_rename_target_collision_identity_and_id_source_behavior(database):
    collection = database["wf_TimeSeries"]
    collision_id = collection.insert_one({"old_name": 1, "target_name": 2}).inserted_id
    identity_id = collection.insert_one({"same_name": "same"}).inserted_id
    id_source_id = collection.insert_one({"value": "unchanged"}).inserted_id
    updates_by_id = {}
    original_update_one = Collection.update_one

    def capture_update(self, query, update, *args, **kwargs):
        updates_by_id[query["_id"]] = copy.deepcopy(update)
        return original_update_one(self, query, update, *args, **kwargs)

    with (
        patch.object(
            Collection,
            "replace_one",
            side_effect=AssertionError("rename must not replace the whole document"),
        ),
        patch.object(Collection, "update_one", capture_update),
    ):
        collision_counts = database._rename_attributes(
            "wf_TimeSeries",
            {"old_name": "target_name"},
            query={"_id": collision_id},
        )
        identity_counts = database._rename_attributes(
            "wf_TimeSeries",
            {"same_name": "same_name"},
            query={"_id": identity_id},
        )
        id_source_counts = database._rename_attributes(
            "wf_TimeSeries",
            {"_id": "moved_id"},
            query={"_id": id_source_id},
        )

    assert collision_counts == {"old_name": 1}
    assert identity_counts == {"same_name": 1}
    assert id_source_counts == {"_id": 0}
    assert updates_by_id == {
        collision_id: {
            "$set": {"target_name": 1},
            "$unset": {"old_name": ""},
        }
    }
    collision = collection.find_one({"_id": collision_id})
    assert collision["target_name"] == 1
    assert "old_name" not in collision
    assert collection.find_one({"_id": identity_id})["same_name"] == "same"
    assert collection.find_one({"_id": id_source_id}) == {
        "_id": id_source_id,
        "value": "unchanged",
    }
