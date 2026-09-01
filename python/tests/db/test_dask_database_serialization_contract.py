import functools
import os
import pickle
from types import SimpleNamespace

import dask.bag
import pandas as pd
import pytest
from distributed import Client, LocalCluster, get_worker
from distributed.protocol import pickle as dask_pickle

import mspasspy.util.db_utils as db_utils
from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection
from mspasspy.db.database import Database
from mspasspy.db.normalize import ObjectIdDBMatcher
import mspasspy.io.distributed as distributed_io
from mspasspy.util.db_utils import (
    MongoDBWorker,
    _WorkerDatabaseReference,
    fetch_dbhandle,
)
from mspasspy.workflow import sliding_window_pipeline

CONNECTION_URL = "mongodb://127.0.0.1:27017/?connect=false"
ERROR_GUIDANCE = ("MongoDBWorker", "fetch_dbhandle")


def _captured_closure(value):
    def task():
        return value

    return task


def _return_value(value):
    return value


def _captured_partial(value):
    return functools.partial(_return_value, value)


def _captured_bound_method(value):
    return value.read_data


class _CapturedCallable:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


def _must_not_run(value):
    raise AssertionError(f"unsafe task ran with {value!r}")


def _worker_client_identity(index, database_name):
    database = fetch_dbhandle(database_name)
    client = get_worker().data["dbclient"]
    return os.getpid(), id(client), id(database.client)


def _worker_database_sink(value, database_name):
    database = fetch_dbhandle(database_name)
    assert database.client is get_worker().data["dbclient"]


def _exception_text(error):
    messages = []
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        messages.append(str(error))
        error = error.__cause__ or error.__context__
    return "\n".join(messages)


@pytest.fixture
def database_handles():
    client = DBClient(CONNECTION_URL)
    database = client.get_database("serialization_contract")
    collection = database["wf_TimeSeries"]
    try:
        yield client, database, collection
    finally:
        client.close()


def test_ordinary_pickle_compatibility_is_preserved(database_handles):
    for handle in database_handles:
        restored = pickle.loads(pickle.dumps(handle))
        if isinstance(restored, DBClient):
            restored.close()
        elif isinstance(restored, Database):
            restored.client.close()
        elif isinstance(restored, Collection):
            restored.database.client.close()


@pytest.mark.parametrize("handle_index", range(3))
def test_direct_dask_serialization_fails_with_worker_lookup_guidance(
    database_handles, handle_index
):
    with pytest.raises(TypeError) as raised:
        dask_pickle.dumps(database_handles[handle_index])

    message = _exception_text(raised.value)
    assert all(text in message for text in ERROR_GUIDANCE)


@pytest.mark.parametrize(
    "capture",
    [
        _captured_closure,
        _captured_partial,
        _captured_bound_method,
        _CapturedCallable,
    ],
)
def test_indirect_dask_serialization_fails_with_worker_lookup_guidance(
    database_handles, capture
):
    payload = capture(database_handles[1])

    with pytest.raises(TypeError) as raised:
        dask_pickle.dumps(payload)

    message = _exception_text(raised.value)
    assert all(text in message for text in ERROR_GUIDANCE)


def test_worker_database_reference_never_pickles_its_local_handle(
    monkeypatch, database_handles
):
    database = database_handles[1]
    reference = _WorkerDatabaseReference(database)
    assert fetch_dbhandle(reference) is database

    restored = pickle.loads(pickle.dumps(reference))
    worker_database = object()

    class FakeClient:
        def get_database(self, name):
            assert name == database.name
            return worker_database

    monkeypatch.setattr(
        db_utils,
        "get_worker",
        lambda: SimpleNamespace(data={"dbclient": FakeClient()}),
    )
    assert fetch_dbhandle(restored) is worker_database


def test_database_matcher_rebinds_to_the_worker_owned_client(
    monkeypatch, database_handles
):
    database = database_handles[1]
    matcher = ObjectIdDBMatcher(database)
    restored = dask_pickle.loads(dask_pickle.dumps(matcher))
    worker_collection = object()

    class FakeDatabase:
        def __getitem__(self, collection):
            assert collection == "channel"
            return worker_collection

    class FakeClient:
        def get_database(self, name):
            assert name == database.name
            return FakeDatabase()

    monkeypatch.setattr(
        db_utils,
        "get_worker",
        lambda: SimpleNamespace(data={"dbclient": FakeClient()}),
    )
    assert restored._get_dbhandle() is worker_collection


def test_official_dask_read_graph_contains_only_worker_database_reference(
    database_handles,
):
    database = database_handles[1]
    matcher = ObjectIdDBMatcher(database)
    bag = distributed_io.read_distributed_data(
        pd.DataFrame([{"storage_mode": "file", "npts": 0}]),
        db=database,
        normalize=[matcher],
        scheduler="dask",
        npartitions=1,
    )

    dask_pickle.dumps(bag.__dask_graph__())


def test_official_dask_write_graph_contains_only_worker_database_reference(
    monkeypatch, database_handles
):
    captured = {}

    def capture_compute(self, scheduler=None):
        captured["graph"] = self.__dask_graph__()
        return []

    monkeypatch.setattr(dask.bag.Bag, "compute", capture_compute)
    input_bag = dask.bag.from_sequence([object()], npartitions=1)

    assert (
        distributed_io.write_distributed_data(
            input_bag,
            database_handles[1],
            scheduler="dask",
        )
        == []
    )
    dask_pickle.dumps(captured["graph"])


def test_plugin_owns_exactly_one_client_for_each_worker_lifecycle(monkeypatch):
    clients = []

    class FakeClient:
        def __init__(self, connection_url):
            self.connection_url = connection_url
            self.close_calls = 0
            clients.append(self)

        def close(self):
            self.close_calls += 1

    provider = SimpleNamespace(
        get_database_client=lambda: SimpleNamespace(_mspass_db_host=CONNECTION_URL)
    )
    plugin = MongoDBWorker(provider)
    monkeypatch.setattr(db_utils, "DBClient", FakeClient)

    for _ in range(2):
        worker = SimpleNamespace(data={})
        plugin.setup(worker)
        assert worker.data == {"dbclient": clients[-1]}
        plugin.teardown(worker)
        assert worker.data == {}
        assert clients[-1].close_calls == 1

    assert len(clients) == 2


def test_process_workers_reject_live_handles_and_reuse_plugin_client(
    database_handles,
):
    provider = SimpleNamespace(get_database_client=lambda: database_handles[0])
    plugin = MongoDBWorker(provider)
    with (
        LocalCluster(
            n_workers=1,
            threads_per_worker=1,
            processes=True,
            dashboard_address=None,
        ) as cluster,
        Client(cluster) as client,
    ):
        client.register_plugin(plugin, name="mongodb-worker-contract")
        before_restart = client.gather(
            [
                client.submit(
                    _worker_client_identity,
                    index,
                    database_handles[1].name,
                    pure=False,
                )
                for index in range(12)
            ]
        )
        assert len(set(before_restart)) == 1
        assert before_restart[0][1] == before_restart[0][2]

        assert (
            sliding_window_pipeline(
                range(12),
                _return_value,
                client,
                sliding_window_size=2,
                completion_function=_worker_database_sink,
                cfunc_args=[database_handles[1].name],
                completion_on_worker=True,
                retain_results=False,
            )
            is None
        )
        after_sink = client.submit(
            _worker_client_identity,
            0,
            database_handles[1].name,
            pure=False,
        ).result()
        assert after_sink == before_restart[0]

        for _ in range(12):
            with pytest.raises(Exception) as raised:
                future = client.submit(_must_not_run, database_handles[1], pure=False)
                future.result(timeout=5)
            message = _exception_text(raised.value)
            assert all(text in message for text in ERROR_GUIDANCE)

        old_pid = before_restart[0][0]
        worker_addresses = list(client.scheduler_info()["workers"])
        client.restart_workers(worker_addresses, timeout=20)
        after_restart = client.gather(
            [
                client.submit(
                    _worker_client_identity,
                    index,
                    database_handles[1].name,
                    pure=False,
                )
                for index in range(12)
            ]
        )
        assert len(set(after_restart)) == 1
        assert after_restart[0][1] == after_restart[0][2]
        assert after_restart[0][0] != old_pid
