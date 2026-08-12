import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pymongo
import pytest
from pymongo.errors import DuplicateKeyError, OperationFailure

import mspasspy.history as history_module
from mspasspy.history import HistoryLogger, get_jobid

COUNTER_COLLECTION = "history_counters"
COUNTER_NAME = "jobid"


def test_history_module_is_loaded_from_this_worktree():
    expected = Path(__file__).resolve().parents[1] / "mspasspy" / "history.py"
    assert Path(history_module.__file__).resolve() == expected


class AtomicCounterCollection:
    """Small controllable Mongo collection double for the allocation contract."""

    def __init__(self):
        self._lock = threading.Lock()
        self.value = 0
        self.exists = False
        self.index_calls = []
        self.update_calls = []

    def create_index(self, keys, **kwargs):
        self.index_calls.append((keys, kwargs))
        return "counter_name_1"

    def find_one_and_update(self, query, update, **kwargs):
        self.update_calls.append((query, update, kwargs))
        with self._lock:
            if isinstance(update, list):
                previous = (
                    {"counter_name": query["counter_name"], "value": self.value}
                    if self.exists
                    else None
                )
                condition = update[0]["$set"]["value"]["$cond"][0]
                requested_jobid = condition["$gt"][0]
                self.value = max(requested_jobid, self.value + 1)
                self.exists = True
                return previous
            self.value += update["$inc"]["value"]
            self.exists = True
            return {"counter_name": query["counter_name"], "value": self.value}


class CounterDatabase:
    def __init__(self, counters=None):
        self.counters = counters or AtomicCounterCollection()

    def __getitem__(self, collection):
        assert collection == COUNTER_COLLECTION
        return self.counters

    @property
    def history(self):
        raise AssertionError("job-id allocation must not scan history")


class UnscannableHistoryCollection:
    def count_documents(self, *args, **kwargs):
        raise AssertionError("job-id allocation must not count history documents")

    def find(self, *args, **kwargs):
        raise AssertionError("job-id allocation must not find history documents")


class HistoryLoggerDatabase(CounterDatabase):
    def __init__(self):
        super().__init__()
        self.legacy_history = UnscannableHistoryCollection()

    @property
    def history(self):
        return self.legacy_history


def _assert_counter_operation(call):
    query, update, kwargs = call
    assert query == {"counter_name": COUNTER_NAME}
    assert update == {"$inc": {"value": 1}}
    assert kwargs == {
        "upsert": True,
        "return_document": pymongo.ReturnDocument.AFTER,
    }


def _assert_requested_counter_operation(call, requested_jobid):
    query, update, kwargs = call
    next_jobid = {"$add": [{"$ifNull": ["$value", 0]}, 1]}
    assert query == {"counter_name": COUNTER_NAME}
    assert update == [
        {
            "$set": {
                "counter_name": COUNTER_NAME,
                "value": {
                    "$cond": [
                        {"$gt": [requested_jobid, next_jobid]},
                        requested_jobid,
                        next_jobid,
                    ]
                },
            }
        }
    ]
    assert kwargs == {
        "upsert": True,
        "return_document": pymongo.ReturnDocument.BEFORE,
    }


def _allocate_concurrently(database, worker_count=16, allocations_per_worker=8):
    start = threading.Barrier(worker_count)

    def allocate_batch():
        start.wait()
        return [get_jobid(database) for _ in range(allocations_per_worker)]

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        batches = [executor.submit(allocate_batch) for _ in range(worker_count)]
        return [jobid for batch in batches for jobid in batch.result()]


def test_get_jobid_uses_one_atomic_counter_operation():
    database = CounterDatabase()

    assert get_jobid(database) == 1
    assert database.counters.index_calls == [
        ([("counter_name", pymongo.ASCENDING)], {"unique": True})
    ]
    assert len(database.counters.update_calls) == 1
    _assert_counter_operation(database.counters.update_calls[0])


def test_history_logger_automatic_jobid_uses_counter_without_scanning_history():
    database = HistoryLoggerDatabase()

    logger = HistoryLogger(database)

    assert logger.jobid == 1
    assert logger.history_collection is database.legacy_history
    assert len(database.counters.update_calls) == 1
    _assert_counter_operation(database.counters.update_calls[0])


def test_history_logger_explicit_jobid_advances_the_counter(capsys):
    database = HistoryLoggerDatabase()

    requested = HistoryLogger(database, job=10)
    automatic = HistoryLogger(database)
    replaced = HistoryLogger(database, job=5)

    assert requested.jobid == 10
    assert automatic.jobid == 11
    assert replaced.jobid == 12
    _assert_requested_counter_operation(database.counters.update_calls[0], 10)
    _assert_counter_operation(database.counters.update_calls[1])
    _assert_requested_counter_operation(database.counters.update_calls[2], 5)
    assert capsys.readouterr().out == (
        "HistoryLogger(Warning):  input jobid= 5  was invalid.  Set jobid= 12\n"
    )


def test_get_jobid_controlled_concurrency_is_unique_and_contiguous():
    database = CounterDatabase()
    allocation_count = 128

    jobids = _allocate_concurrently(database)

    assert (
        database.counters.index_calls
        == [([("counter_name", pymongo.ASCENDING)], {"unique": True})]
        * allocation_count
    )
    assert len(database.counters.update_calls) == allocation_count
    assert all(
        call == database.counters.update_calls[0]
        for call in database.counters.update_calls
    )
    _assert_counter_operation(database.counters.update_calls[0])
    assert len(set(jobids)) == allocation_count
    assert sorted(jobids) == list(range(1, allocation_count + 1))


class FailingCounterCollection(AtomicCounterCollection):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def find_one_and_update(self, query, update, **kwargs):
        raise self.error


class FailingIndexCollection(AtomicCounterCollection):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def create_index(self, keys, **kwargs):
        raise self.error


@pytest.mark.parametrize("failure_stage", ("index", "counter"))
def test_get_jobid_propagates_mongo_errors_without_history_fallback(failure_stage):
    error = OperationFailure(f"{failure_stage} unavailable", code=91)
    counters = (
        FailingIndexCollection(error)
        if failure_stage == "index"
        else FailingCounterCollection(error)
    )
    database = CounterDatabase(counters)

    with pytest.raises(OperationFailure, match=failure_stage) as caught:
        get_jobid(database)
    assert caught.value is error


def test_explicit_jobid_propagates_counter_errors_without_history_fallback():
    database = HistoryLoggerDatabase()
    error = OperationFailure("counter unavailable", code=91)
    database.counters = FailingCounterCollection(error)

    with pytest.raises(OperationFailure, match="counter unavailable") as caught:
        HistoryLogger(database, job=10)
    assert caught.value is error


@pytest.fixture
def mongo_database():
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://localhost:27017")
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    database_name = None
    try:
        client.admin.command("ping")
        database_name = "mspass_test_history_jobid_" + uuid.uuid4().hex
        database = client[database_name]
        yield database
    finally:
        if database_name is not None:
            client.drop_database(database_name)
        client.close()


def test_get_jobid_real_mongo_initializes_counter_and_unique_index(mongo_database):
    assert get_jobid(mongo_database) == 1

    counters = mongo_database[COUNTER_COLLECTION]
    assert counters.find_one({"counter_name": COUNTER_NAME})["value"] == 1
    counter_index = next(
        info
        for info in counters.index_information().values()
        if info["key"] == [("counter_name", pymongo.ASCENDING)]
    )
    assert counter_index["unique"] is True
    with pytest.raises(DuplicateKeyError):
        counters.insert_one({"counter_name": COUNTER_NAME, "value": 999})


def test_get_jobid_real_mongo_concurrent_allocations(mongo_database):
    allocation_count = 128

    jobids = _allocate_concurrently(mongo_database)

    assert len(set(jobids)) == allocation_count
    assert sorted(jobids) == list(range(1, allocation_count + 1))
    assert (
        mongo_database[COUNTER_COLLECTION].find_one({"counter_name": COUNTER_NAME})[
            "value"
        ]
        == allocation_count
    )


def test_history_logger_real_mongo_explicit_jobid_reserves_high_water_mark(
    mongo_database, capsys
):
    requested = HistoryLogger(mongo_database, job=10)
    automatic = HistoryLogger(mongo_database)
    replaced = HistoryLogger(mongo_database, job=5)

    assert requested.jobid == 10
    assert automatic.jobid == 11
    assert replaced.jobid == 12
    assert (
        mongo_database[COUNTER_COLLECTION].find_one({"counter_name": COUNTER_NAME})[
            "value"
        ]
        == 12
    )
    assert capsys.readouterr().out == (
        "HistoryLogger(Warning):  input jobid= 5  was invalid.  Set jobid= 12\n"
    )
