import copy
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import distribution, version
from pathlib import Path
from threading import Barrier

import pymongo
import pytest
from bson import ObjectId

from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
import mspasspy.global_history.manager as manager_module
from mspasspy.global_history.manager import GlobalHistoryManager

HISTORY_COLLECTION = "history_global_algorithm_identity_contract"


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


def test_contract_suite_loads_manager_from_selected_build():
    _assert_module_from_selected_build(
        manager_module, Path("mspasspy/global_history/manager.py")
    )


@pytest.fixture(scope="module")
def mongo_database():
    client = pymongo.MongoClient(
        "mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=2_000
    )
    client.admin.command("ping")
    database_name = "mspass_global_algorithm_identity_" + str(ObjectId())
    database = client[database_name]
    try:
        yield database
    finally:
        client.drop_database(database_name)
        client.close()


def test_registry_has_normal_unique_compound_index_and_exact_identity(
    mongo_database,
):
    manager = GlobalHistoryManager(
        mongo_database, "identity-job", collection=HISTORY_COLLECTION
    )
    registry = mongo_database[manager.algorithm_collection]

    compound_indexes = [
        spec
        for spec in registry.index_information().values()
        if spec["key"]
        == [("alg_name", pymongo.ASCENDING), ("parameters", pymongo.ASCENDING)]
    ]
    assert len(compound_indexes) == 1
    assert compound_indexes[0]["unique"] is True
    assert all(
        direction != "text"
        for spec in registry.index_information().values()
        for _, direction in spec["key"]
    )

    compact = '{"value":1}'
    spaced = '{"value": 1}'
    first = manager.get_alg_id("filter", compact)
    assert manager.get_alg_id("filter", compact) == first
    assert manager.get_alg_id("filter", spaced) != first
    assert manager.get_alg_id("other-filter", compact) != first
    assert registry.count_documents({}) == 3
    identity_document = registry.find_one({"alg_id": first})
    assert identity_document == {
        "_id": identity_document["_id"],
        "alg_name": "filter",
        "parameters": compact,
        "alg_id": first,
    }


class RecordingCollection:
    def __init__(self):
        self.index_calls = []
        self.update_calls = []

    def create_index(self, keys, **kwargs):
        self.index_calls.append((keys, kwargs))

    def find_one_and_update(self, query, update, **kwargs):
        self.update_calls.append((copy.deepcopy(query), copy.deepcopy(update), kwargs))
        return copy.deepcopy(update["$setOnInsert"])

    def find_one(self, query, projection=None):
        return None


class RecordingDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, RecordingCollection())


def test_get_alg_id_uses_one_atomic_upsert_with_the_exact_string():
    database = RecordingDatabase()
    manager = GlobalHistoryManager(database, "spy-job", collection="spy_history")
    parameters = ' {"unchanged": true} '

    result = manager.get_alg_id("spy-algorithm", parameters)

    registry = database.collections[manager.algorithm_collection]
    assert len(registry.update_calls) == 1
    query, update, options = registry.update_calls[0]
    assert query == {"alg_name": "spy-algorithm", "parameters": parameters}
    assert update["$setOnInsert"]["alg_name"] == "spy-algorithm"
    assert update["$setOnInsert"]["parameters"] == parameters
    assert update["$setOnInsert"]["alg_id"] == result
    assert options == {
        "upsert": True,
        "return_document": pymongo.ReturnDocument.AFTER,
    }


def test_concurrent_first_use_from_multiple_clients_returns_one_id(mongo_database):
    database_name = mongo_database.name
    mongo_database[HISTORY_COLLECTION + "_algorithms"].drop()
    worker_count = 16
    barrier = Barrier(worker_count)

    def allocate():
        client = pymongo.MongoClient(
            "mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=2_000
        )
        try:
            manager = GlobalHistoryManager(
                client[database_name], "concurrent-job", collection=HISTORY_COLLECTION
            )
            barrier.wait(timeout=10)
            return manager.get_alg_id("concurrent-filter", '{"x":1}')
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        results = list(executor.map(lambda _: allocate(), range(worker_count)))

    assert len(set(results)) == 1
    registry = mongo_database[HISTORY_COLLECTION + "_algorithms"]
    assert registry.count_documents({}) == 1
    assert registry.find_one()["alg_id"] == results[0]


def test_invocations_are_separate_and_legacy_history_remains_unchanged(
    mongo_database,
):
    legacy_history = mongo_database[HISTORY_COLLECTION]
    legacy_history.delete_many({})
    legacy_text_index = legacy_history.create_index(
        [("alg_name", pymongo.TEXT), ("parameters", pymongo.TEXT)]
    )
    mongo_database[HISTORY_COLLECTION + "_algorithms"].delete_many({})
    legacy_job_id = ObjectId()
    legacy_alg_id = ObjectId()
    legacy_document = {
        "time": 1.25,
        "job_id": legacy_job_id,
        "job_name": "legacy-job",
        "alg_name": "legacy-filter",
        "alg_id": legacy_alg_id,
        "parameters": ' {"legacy":true} ',
    }
    inserted_id = (
        mongo_database[HISTORY_COLLECTION]
        .insert_one(copy.deepcopy(legacy_document))
        .inserted_id
    )
    legacy_snapshot = mongo_database[HISTORY_COLLECTION].find_one({"_id": inserted_id})

    manager = GlobalHistoryManager(
        mongo_database, "current-job", collection=HISTORY_COLLECTION
    )
    assert manager.get_alg_id("legacy-filter", ' {"legacy":true} ') == legacy_alg_id
    alg_id = manager.get_alg_id("current-filter", '{"x":1}')
    manager.logging(alg_id, "current-filter", '{"x":1}')
    manager.logging(alg_id, "current-filter", '{"x":1}')

    assert (
        mongo_database[HISTORY_COLLECTION].count_documents(
            {"job_name": "current-job", "alg_id": alg_id}
        )
        == 2
    )
    assert (
        mongo_database[manager.algorithm_collection].count_documents(
            {"alg_name": "current-filter", "parameters": '{"x":1}', "alg_id": alg_id}
        )
        == 1
    )
    assert manager.get_alg_list("legacy-job", legacy_job_id) == [legacy_snapshot]
    assert legacy_text_index in legacy_history.index_information()
    assert (
        mongo_database[HISTORY_COLLECTION].find_one({"_id": inserted_id})
        == legacy_snapshot
    )


def test_explicit_invocation_id_is_reused_by_exact_automatic_lookup(mongo_database):
    collection = HISTORY_COLLECTION + "_explicit"
    mongo_database[collection].delete_many({})
    mongo_database[collection + "_algorithms"].delete_many({})
    manager = GlobalHistoryManager(
        mongo_database, "explicit-job", collection=collection
    )
    explicit_id = "caller-supplied-id"
    parameters = ' {"preserve spacing": true} '

    manager.logging(explicit_id, "explicit-algorithm", parameters)

    assert manager.get_alg_id("explicit-algorithm", parameters) == explicit_id
    assert (
        mongo_database[manager.algorithm_collection].find_one(
            {"alg_name": "explicit-algorithm", "parameters": parameters}
        )["alg_id"]
        == explicit_id
    )


@pytest.mark.parametrize("legacy_alg_id", [ObjectId(), "explicit-algorithm-id"])
def test_set_legacy_or_explicit_id_keeps_registry_identity_consistent(
    mongo_database, legacy_alg_id
):
    collection = HISTORY_COLLECTION + "_set"
    mongo_database[collection].delete_many({})
    mongo_database[collection + "_algorithms"].delete_many({})
    manager = GlobalHistoryManager(mongo_database, "set-job", collection=collection)
    manager.logging(legacy_alg_id, "old-name", "old-parameters")

    manager.set_alg_name_and_parameters(
        legacy_alg_id, "renamed-algorithm", ' {"exact": true} '
    )

    identity_document = mongo_database[manager.algorithm_collection].find_one(
        {"alg_id": legacy_alg_id}
    )
    assert identity_document == {
        "_id": identity_document["_id"],
        "alg_name": "renamed-algorithm",
        "parameters": ' {"exact": true} ',
        "alg_id": legacy_alg_id,
    }
    assert manager.get_alg_id("renamed-algorithm", ' {"exact": true} ') == legacy_alg_id
    assert (
        mongo_database[collection].find_one({"alg_id": legacy_alg_id})["alg_name"]
        == "renamed-algorithm"
    )


def test_set_identity_errors_are_typed_and_do_not_partially_update_invocation(
    mongo_database,
):
    collection = HISTORY_COLLECTION + "_set_errors"
    mongo_database[collection].delete_many({})
    mongo_database[collection + "_algorithms"].delete_many({})
    manager = GlobalHistoryManager(mongo_database, "set-job", collection=collection)

    missing_id = ObjectId()
    with pytest.raises(MsPASSError, match=str(missing_id)) as excinfo:
        manager.set_alg_name_and_parameters(missing_id, "missing", "identity")
    assert excinfo.value.severity == ErrorSeverity.Fatal

    manager.get_alg_id("occupied", "identity")
    explicit_id = "collision-id"
    manager.logging(explicit_id, "before", "unchanged")
    invocation_snapshot = mongo_database[collection].find_one({"alg_id": explicit_id})
    with pytest.raises(pymongo.errors.DuplicateKeyError):
        manager.set_alg_name_and_parameters(explicit_id, "occupied", "identity")
    assert (
        mongo_database[collection].find_one({"alg_id": explicit_id})
        == invocation_snapshot
    )
