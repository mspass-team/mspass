import os
import uuid
from types import SimpleNamespace

import pymongo
import pytest

import mspasspy.client as client_module
from mspasspy.ccore.utility import MsPASSError

ENDPOINT_CASES = [
    ("mongo", None, "mongo"),
    ("mongo", "27018", "mongo:27018"),
    ("mongo:27017", None, "mongo:27017"),
    ("mongo:27017", "27017", "mongo:27017"),
    ("mongo:27017", "27018", "mongo:27017"),
    ("mongodb://mongo", None, "mongodb://mongo"),
    ("mongodb://mongo", "27018", "mongodb://mongo:27018"),
    ("mongodb://mongo:27017", None, "mongodb://mongo:27017"),
    ("mongodb://mongo:27017", "27017", "mongodb://mongo:27017"),
    ("mongodb://mongo:27017", "27018", "mongodb://mongo:27017"),
    ("[2001:db8::1]", None, "[2001:db8::1]"),
    ("[2001:db8::1]", "27018", "[2001:db8::1]:27018"),
    ("[2001:db8::1]:27017", None, "[2001:db8::1]:27017"),
    ("[2001:db8::1]:27017", "27017", "[2001:db8::1]:27017"),
    ("[2001:db8::1]:27017", "27018", "[2001:db8::1]:27017"),
    ("mongodb://[2001:db8::1]", None, "mongodb://[2001:db8::1]"),
    ("mongodb://[2001:db8::1]", "27018", "mongodb://[2001:db8::1]:27018"),
    (
        "mongodb://[2001:db8::1]:27017",
        None,
        "mongodb://[2001:db8::1]:27017",
    ),
    (
        "mongodb://[2001:db8::1]:27017",
        "27017",
        "mongodb://[2001:db8::1]:27017",
    ),
    (
        "mongodb://[2001:db8::1]:27017",
        "27018",
        "mongodb://[2001:db8::1]:27017",
    ),
]


@pytest.fixture
def fake_client_dependencies(monkeypatch):
    control = SimpleNamespace(
        failure_stage=None,
        clients=[],
        databases=[],
        history_managers=[],
    )

    class FakeDBClient:
        def __init__(self, endpoint):
            self.endpoint = endpoint
            self.closed = False
            self.close_calls = 0
            control.clients.append(self)

        def server_info(self):
            if self.endpoint == "replacement" and control.failure_stage == "client":
                raise RuntimeError("client validation failed")
            return {"ok": 1}

        def close(self):
            self.close_calls += 1
            self.closed = True

    class FakeDatabase:
        def __init__(
            self,
            client,
            name,
            *args,
            schema=None,
            db_schema=None,
            md_schema=None,
            **kwargs,
        ):
            if client.endpoint == "replacement" and control.failure_stage == "database":
                raise RuntimeError("database construction failed")
            self.client = client
            self.name = name
            self.schema = schema
            if schema is not None:
                self.database_schema = ("database", schema)
                self.metadata_schema = ("metadata", schema)
            else:
                self.database_schema = db_schema or ("database", None)
                self.metadata_schema = md_schema or ("metadata", None)
            control.databases.append(self)

    class FakeGlobalHistoryManager:
        def __init__(self, history_db, job_name, collection=None):
            if (
                history_db.client.endpoint == "replacement"
                and control.failure_stage == "history"
            ):
                raise RuntimeError("history construction failed")
            self.history_db = history_db
            self.job_name = job_name
            self.collection = collection or "history_global"
            control.history_managers.append(self)

    monkeypatch.setattr(client_module, "DBClient", FakeDBClient)
    monkeypatch.setattr(client_module, "Database", FakeDatabase)
    monkeypatch.setattr(client_module, "GlobalHistoryManager", FakeGlobalHistoryManager)
    return control


def _new_fake_client(endpoint="old", **kwargs):
    return client_module.Client(
        database_host=endpoint,
        scheduler="none",
        database_name="default_db",
        schema="configured.yaml",
        job_name="job",
        collection="history_collection",
        **kwargs,
    )


@pytest.mark.parametrize("source", ["explicit", "environment"])
@pytest.mark.parametrize("endpoint,separate_port,expected", ENDPOINT_CASES)
def test_database_endpoint_matrix(
    monkeypatch,
    fake_client_dependencies,
    source,
    endpoint,
    separate_port,
    expected,
):
    monkeypatch.delenv("MSPASS_DB_ADDRESS", raising=False)
    monkeypatch.delenv("MONGODB_PORT", raising=False)
    if separate_port is not None:
        monkeypatch.setenv("MONGODB_PORT", separate_port)
    if source == "environment":
        monkeypatch.setenv("MSPASS_DB_ADDRESS", endpoint)
        database_host = None
    else:
        database_host = endpoint

    client = client_module.Client(database_host=database_host, scheduler="none")

    assert client.get_database_client().endpoint == expected


def test_get_database_propagates_schema_to_default_named_and_history(
    fake_client_dependencies,
):
    client = _new_fake_client()

    default_database = client.get_database()
    named_database = client.get_database("named_db")
    history_database = client.get_global_history_manager().history_db

    assert default_database.name == "default_db"
    assert named_database.name == "named_db"
    assert default_database.schema == "configured.yaml"
    assert named_database.schema == "configured.yaml"
    assert history_database.schema == "configured.yaml"
    assert default_database.client is client.get_database_client()
    assert named_database.client is client.get_database_client()
    assert history_database.client is client.get_database_client()


def test_successful_database_switch_commits_client_database_and_history_together(
    fake_client_dependencies,
):
    client = _new_fake_client()
    old_db_client = client.get_database_client()
    old_history_manager = client.get_global_history_manager()
    old_history_database = old_history_manager.history_db

    client.set_database_client("mongodb://new:27019", database_port="27020")

    new_db_client = client.get_database_client()
    new_history_manager = client.get_global_history_manager()
    new_history_database = new_history_manager.history_db
    assert new_db_client is not old_db_client
    assert new_db_client.endpoint == "mongodb://new:27019"
    assert new_history_manager is not old_history_manager
    assert new_history_database is not old_history_database
    assert new_history_database.client is new_db_client
    assert new_history_database.database_schema is old_history_database.database_schema
    assert new_history_database.metadata_schema is old_history_database.metadata_schema
    assert new_history_manager.job_name == old_history_manager.job_name
    assert new_history_manager.collection == old_history_manager.collection
    assert client.get_database().client is new_db_client
    assert client.get_database().schema == "configured.yaml"
    assert old_db_client.closed is False
    assert old_db_client.close_calls == 0
    assert new_db_client.closed is False
    assert new_db_client.close_calls == 0


@pytest.mark.parametrize("failure_stage", ["client", "database", "history"])
def test_failed_database_switch_preserves_all_old_state(
    fake_client_dependencies, failure_stage
):
    control = fake_client_dependencies
    client = _new_fake_client()
    old_db_client = client.get_database_client()
    old_history_manager = client.get_global_history_manager()
    old_history_database = old_history_manager.history_db
    control.failure_stage = failure_stage

    with pytest.raises(
        MsPASSError,
        match="Runntime error: cannot create a database client with: replacement",
    ):
        client.set_database_client("replacement")

    assert client.get_database_client() is old_db_client
    assert client.get_global_history_manager() is old_history_manager
    assert client.get_global_history_manager().history_db is old_history_database
    assert client.get_database().client is old_db_client
    assert old_history_database.client is old_db_client
    assert old_db_client.closed is False
    assert old_db_client.close_calls == 0
    assert old_db_client.server_info() == {"ok": 1}
    assert control.clients[-1] is not old_db_client
    assert control.clients[-1].closed is True
    assert control.clients[-1].close_calls == 1


@pytest.fixture
def real_mongo(monkeypatch):
    uri = os.environ.get("MSPASS_TEST_MONGODB_URI", "mongodb://localhost:27017")
    monkeypatch.delenv("MONGODB_PORT", raising=False)
    admin_client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    admin_client.admin.command("ping")
    default_name = "mspass_test_client_default_" + uuid.uuid4().hex
    named_name = "mspass_test_client_named_" + uuid.uuid4().hex
    try:
        yield uri, default_name, named_name
    finally:
        admin_client.drop_database(default_name)
        admin_client.drop_database(named_name)
        admin_client.close()


def test_real_mongo_schema_and_successful_same_server_switch(real_mongo):
    uri, default_name, named_name = real_mongo
    client = client_module.Client(
        database_host=uri,
        scheduler="none",
        database_name=default_name,
        schema="mspass_lite.yaml",
        job_name="real_job",
    )
    old_db_client = client.get_database_client()
    try:
        default_database = client.get_database()
        named_database = client.get_database(named_name)
        history_database = client.get_global_history_manager().history_db
        for database in (default_database, named_database, history_database):
            with pytest.raises(KeyError, match="site"):
                database.database_schema._attr_dict["site"]
            assert database.client is old_db_client

        client.set_database_client(uri, database_port="1")

        new_db_client = client.get_database_client()
        assert new_db_client is not old_db_client
        assert new_db_client._mspass_db_host == uri
        assert client.get_database().client is new_db_client
        assert client.get_database(named_name).client is new_db_client
        assert client.get_global_history_manager().history_db.client is new_db_client
        assert client.get_global_history_manager().job_name == "real_job"
        with pytest.raises(KeyError, match="site"):
            client.get_database().database_schema._attr_dict["site"]
    finally:
        old_db_client.close()
        client.get_database_client().close()
