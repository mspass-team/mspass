import copy
import os
import subprocess
import uuid
from importlib.metadata import distribution, version
from pathlib import Path

import pymongo
import pytest

import mspasspy.history as history_module
from mspasspy.history import HistoryLogger


class MemoryHistoryCollection:
    def __init__(self):
        self.documents = []

    def insert_one(self, document):
        self.documents.append(copy.deepcopy(document))

    def find_one(self, query):
        return next(
            (
                copy.deepcopy(document)
                for document in self.documents
                if all(document.get(key) == value for key, value in query.items())
            ),
            None,
        )

    def replace_one(self, query, replacement):
        for index, document in enumerate(self.documents):
            if all(document.get(key) == value for key, value in query.items()):
                replacement = copy.deepcopy(replacement)
                if "_id" in document:
                    replacement["_id"] = document["_id"]
                self.documents[index] = replacement
                return
        raise AssertionError(f"no document matched {query}")


class MemoryDatabase:
    def __init__(self):
        self.history = MemoryHistoryCollection()


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


def test_history_module_is_loaded_from_selected_build():
    _assert_module_from_selected_build(history_module, "mspasspy/history.py")


def _step_values(logger):
    return [
        {
            "algorithm": step.algorithm,
            "param_type": step.param_type,
            "params": step.params,
        }
        for step in logger.history_chain
    ]


@pytest.fixture
def memory_database(monkeypatch):
    monkeypatch.setattr(history_module, "get_jobid", lambda database: 41)
    return MemoryDatabase()


@pytest.fixture
def mongo_database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://localhost:27017")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    database_name = None
    try:
        client.admin.command("ping")
        database_name = "mspass_test_history_steps_" + uuid.uuid4().hex
        yield client[database_name]
    finally:
        if database_name is not None:
            client.drop_database(database_name)
        client.close()


class TestHistoryLogger:
    def test_init(self, memory_database):
        logger = HistoryLogger(memory_database)

        assert logger.jobid == 41
        assert logger.history_collection is memory_database.history
        assert logger.history_chain == []

    def test_register(self, memory_database):
        logger = HistoryLogger(memory_database)

        logger.register("filter", "dict", {"frequency": 1.0})
        logger.register("filter", "dict", {"frequency": 2.0})

        assert _step_values(logger) == [
            {
                "algorithm": "filter",
                "param_type": "dict",
                "params": {"frequency": 1.0},
            },
            {
                "algorithm": "filter",
                "param_type": "dict",
                "params": {"frequency": 2.0},
            },
        ]

    def test_save_empty_history_uses_only_ordered_representation(self, memory_database):
        logger = HistoryLogger(memory_database)

        logger.save()

        assert memory_database.history.documents == [{"jobid": 41, "steps": []}]

    def test_repeated_steps_round_trip_in_exact_order(self, memory_database):
        logger = HistoryLogger(memory_database)
        expected_steps = [
            {
                "algorithm": "filter",
                "param_type": "dict",
                "params": {"frequency": 1.0},
            },
            {
                "algorithm": "stack",
                "param_type": "dict",
                "params": {"method": "median"},
            },
            {
                "algorithm": "filter",
                "param_type": "dict",
                "params": {"frequency": 2.0},
            },
        ]
        for step in expected_steps:
            logger.register(step["algorithm"], step["param_type"], step["params"])

        logger.save()

        assert memory_database.history.documents == [
            {"jobid": 41, "steps": expected_steps}
        ]
        loaded = HistoryLogger.load(memory_database, 41)
        assert loaded is not None
        assert _step_values(loaded) == expected_steps

    def test_loads_legacy_keyed_document_and_retires_legacy_writing(
        self, memory_database
    ):
        legacy_document = {
            "_id": "legacy-id",
            "jobid": 73,
            "filter": {
                "algorithm": "filter",
                "param_type": "dict",
                "params": {"frequency": 1.0},
            },
            "stack": {
                "algorithm": "stack",
                "param_type": "dict",
                "params": {"method": "mean"},
            },
        }
        memory_database.history.documents.append(legacy_document)

        loaded = HistoryLogger.load(memory_database, 73)

        assert loaded is not None
        assert _step_values(loaded) == [
            legacy_document["filter"],
            legacy_document["stack"],
        ]
        assert memory_database.history.documents == [legacy_document]

        loaded.save()
        assert len(memory_database.history.documents) == 1
        migrated = memory_database.history.documents[0]
        assert migrated == {
            "jobid": 73,
            "steps": [legacy_document["filter"], legacy_document["stack"]],
            "_id": "legacy-id",
        }
        assert set(migrated) == {"_id", "jobid", "steps"}
        assert (
            _step_values(HistoryLogger.load(memory_database, 73)) == migrated["steps"]
        )

    def test_legacy_algorithm_named_steps_is_not_mistaken_for_new_schema(
        self, memory_database
    ):
        legacy_step = {
            "algorithm": "steps",
            "param_type": "dict",
            "params": {"value": 1},
        }
        memory_database.history.documents.append({"jobid": 74, "steps": legacy_step})

        loaded = HistoryLogger.load(memory_database, 74)

        assert loaded is not None
        assert _step_values(loaded) == [legacy_step]

    def test_loads_empty_and_missing_histories(self, memory_database):
        memory_database.history.documents.append({"jobid": 75, "steps": []})

        loaded = HistoryLogger.load(memory_database, 75)

        assert loaded is not None
        assert loaded.history_chain == []
        assert HistoryLogger.load(memory_database, 999) is None


def test_real_mongo_repeated_steps_round_trip(mongo_database):
    logger = HistoryLogger(mongo_database)
    expected_steps = [
        {
            "algorithm": "filter",
            "param_type": "dict",
            "params": {"frequency": 1.0},
        },
        {
            "algorithm": "stack",
            "param_type": "dict",
            "params": {"method": "median"},
        },
        {
            "algorithm": "filter",
            "param_type": "dict",
            "params": {"frequency": 2.0},
        },
    ]
    for step in expected_steps:
        logger.register(step["algorithm"], step["param_type"], step["params"])

    logger.save()

    stored = mongo_database.history.find_one({"jobid": logger.jobid})
    assert stored["steps"] == expected_steps
    assert set(stored) == {"_id", "jobid", "steps"}
    loaded = HistoryLogger.load(mongo_database, logger.jobid)
    assert loaded is not None
    assert _step_values(loaded) == expected_steps


def test_real_mongo_legacy_read_then_save_replaces_with_ordered_schema(
    mongo_database,
):
    legacy_steps = [
        {
            "algorithm": "steps",
            "param_type": "dict",
            "params": {"value": 1},
        },
        {
            "algorithm": "stack",
            "param_type": "dict",
            "params": {"method": "mean"},
        },
    ]
    inserted = mongo_database.history.insert_one(
        {
            "jobid": 73,
            "steps": legacy_steps[0],
            "stack": legacy_steps[1],
        }
    )

    loaded = HistoryLogger.load(mongo_database, 73)
    assert loaded is not None
    assert _step_values(loaded) == legacy_steps
    loaded.save()

    assert mongo_database.history.count_documents({"jobid": 73}) == 1
    migrated = mongo_database.history.find_one({"jobid": 73})
    assert migrated == {
        "_id": inserted.inserted_id,
        "jobid": 73,
        "steps": legacy_steps,
    }
    reloaded = HistoryLogger.load(mongo_database, 73)
    assert reloaded is not None
    assert _step_values(reloaded) == legacy_steps
