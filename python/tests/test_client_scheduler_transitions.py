import os
import subprocess
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

import mspasspy.client as client_module
import mspasspy.ccore.utility as utility_module
from mspasspy.client import Client
from mspasspy.ccore.utility import MsPASSError


class FakeDaskClient:
    instances = []
    fail_construction = False
    fail_registration = False

    def __init__(self, address=None):
        if self.fail_construction:
            raise RuntimeError("dask construction failed")
        self.address = address
        self.close_calls = 0
        self.register_calls = 0
        self.on_close = None
        type(self).instances.append(self)

    def register_plugin(self, plugin, name=None):
        self.register_calls += 1
        if self.fail_registration:
            raise RuntimeError("dask validation failed")

    def close(self):
        self.close_calls += 1
        if self.on_close is not None:
            self.on_close()


class FakeSparkContext:
    def __init__(self, master):
        self.master = master
        self.stop_calls = 0
        self.on_stop = None

    def stop(self):
        self.stop_calls += 1
        if self.on_stop is not None:
            self.on_stop()


class FakeSparkBuilder:
    def __init__(self, returned_master=None, failure=None, existing_context=None):
        self.returned_master = returned_master
        self.failure = failure
        self.existing_context = existing_context
        self.requested_master = None
        self.get_or_create_calls = 0
        self.context = None

    def appName(self, name):
        return self

    def master(self, master):
        self.requested_master = master
        return self

    def getOrCreate(self):
        self.get_or_create_calls += 1
        if self.failure is not None:
            raise self.failure
        if self.existing_context is not None:
            self.context = self.existing_context
        else:
            master = self.returned_master or self.requested_master
            self.context = FakeSparkContext(master)
        return SimpleNamespace(sparkContext=self.context)


@pytest.fixture
def fake_schedulers(monkeypatch):
    FakeDaskClient.instances = []
    FakeDaskClient.fail_construction = False
    FakeDaskClient.fail_registration = False
    monkeypatch.setattr(client_module, "DaskClient", FakeDaskClient)
    monkeypatch.setattr(client_module, "_mspasspy_has_dask_distributed", True)
    monkeypatch.setattr(
        client_module, "MongoDBWorker", lambda *args, **kwargs: object()
    )
    monkeypatch.setattr(client_module, "_mspasspy_has_pyspark", True)
    monkeypatch.setattr(
        client_module,
        "SparkContext",
        SimpleNamespace(_active_spark_context=None),
    )


def _new_client_without_scheduler():
    client = Client.__new__(Client)
    client._scheduler = None
    client._scheduler_disabled = True
    return client


def _new_dask_client_state(owned):
    client = Client.__new__(Client)
    old_dask = FakeDaskClient("tcp://old-dask:8786")
    client._scheduler = "dask"
    client._scheduler_disabled = False
    client._dask_client = old_dask
    client._dask_client_address = "tcp://old-dask:8786"
    client._dask_client_owned = owned
    return client, old_dask


def _new_spark_client_state(owned=True):
    client = Client.__new__(Client)
    old_spark = FakeSparkContext("spark://old-spark:7077")
    client._scheduler = "spark"
    client._scheduler_disabled = False
    client._spark_context = old_spark
    client._spark_master_url = "spark://old-spark:7077"
    client._spark_context_owned = owned
    return client, old_spark


def _patch_client_startup(monkeypatch):
    monkeypatch.setenv("MSPASS_HOME", str(Path(__file__).resolve().parents[2]))
    monkeypatch.setattr(client_module.DBClient, "server_info", lambda self: {})
    monkeypatch.setattr(
        client_module.GlobalHistoryManager,
        "__init__",
        lambda self, *args, **kwargs: None,
    )


@pytest.mark.parametrize(
    "address,port,default_port,default_scheme,expected",
    [
        ("worker", None, "8786", None, "worker:8786"),
        ("tcp://worker", None, "8786", None, "tcp://worker:8786"),
        ("worker:9000", "9000", "8786", None, "worker:9000"),
        ("worker:9000", "9999", "8786", None, "worker:9000"),
        ("tcp://worker:9000", "9999", "8786", None, "tcp://worker:9000"),
        ("[2001:db8::1]", None, "8786", None, "[2001:db8::1]:8786"),
        ("[2001:db8::1]:9000", "9999", "8786", None, "[2001:db8::1]:9000"),
        (
            "tcp://[2001:db8::1]",
            9000,
            "8786",
            None,
            "tcp://[2001:db8::1]:9000",
        ),
        (
            "tcp://[2001:db8::1]:9000",
            "9999",
            "8786",
            None,
            "tcp://[2001:db8::1]:9000",
        ),
        ("master", "7077", None, "spark", "spark://master:7077"),
        (
            "spark://[2001:db8::2]",
            "7077",
            None,
            "spark",
            "spark://[2001:db8::2]:7077",
        ),
        (
            "spark://master:7077",
            "9999",
            None,
            "spark",
            "spark://master:7077",
        ),
        ("local", "9999", None, "spark", "local"),
        ("local[*]", "9999", None, "spark", "local[*]"),
    ],
)
def test_scheduler_endpoint_builder(
    address, port, default_port, default_scheme, expected
):
    assert (
        client_module._build_scheduler_endpoint(
            address,
            scheduler_port=port,
            default_port=default_port,
            default_scheme=default_scheme,
        )
        == expected
    )


def test_constructor_records_caller_and_client_dask_ownership(
    monkeypatch, fake_schedulers
):
    _patch_client_startup(monkeypatch)
    provided_dask = FakeDaskClient("caller-managed")

    caller_owned_client = Client(scheduler="dask", dask_client=provided_dask)
    client_owned_client = Client(
        scheduler="dask",
        scheduler_host="tcp://[2001:db8::4]:9000",
    )

    assert caller_owned_client._dask_client is provided_dask
    assert caller_owned_client._dask_client_address is None
    assert caller_owned_client._dask_client_owned is False
    assert client_owned_client._dask_client.address == "tcp://[2001:db8::4]:9000"
    assert client_owned_client._dask_client_address == "tcp://[2001:db8::4]:9000"
    assert client_owned_client._dask_client_owned is True


def test_constructor_records_active_spark_master(monkeypatch, fake_schedulers):
    _patch_client_startup(monkeypatch)
    monkeypatch.setenv("SPARK_MASTER_PORT", "9999")
    builder = FakeSparkBuilder()
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))

    client = Client(
        scheduler="spark",
        scheduler_host="spark://[2001:db8::5]:7077",
    )

    assert builder.requested_master == "spark://[2001:db8::5]:7077"
    assert client._spark_master_url == "spark://[2001:db8::5]:7077"
    assert client._spark_context is builder.context
    assert client._spark_context_owned is True


def test_constructor_reuses_existing_implicit_local_spark(monkeypatch, fake_schedulers):
    _patch_client_startup(monkeypatch)
    existing_context = FakeSparkContext("local[*]")
    builder = FakeSparkBuilder(existing_context=existing_context)
    client_module.SparkContext._active_spark_context = existing_context
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))

    client = Client(scheduler="spark")

    assert builder.requested_master == "local"
    assert client._spark_context is existing_context
    assert client._spark_master_url == "local[*]"
    assert client._spark_context_owned is False


def test_constructor_rejects_nonlocal_existing_context(monkeypatch, fake_schedulers):
    _patch_client_startup(monkeypatch)
    existing_context = FakeSparkContext("spark://existing:7077")
    builder = FakeSparkBuilder(existing_context=existing_context)
    client_module.SparkContext._active_spark_context = existing_context
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))

    with pytest.raises(MsPASSError, match="cannot create a spark configuration"):
        Client(scheduler="spark")

    assert existing_context.stop_calls == 0


def test_explicit_local_master_remains_strict_and_atomic(monkeypatch, fake_schedulers):
    client, old_dask = _new_dask_client_state(owned=True)
    builder = FakeSparkBuilder(returned_master="local[*]")
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))
    before = vars(client).copy()

    with pytest.raises(MsPASSError, match="cannot create a spark configuration"):
        client.set_scheduler("spark", "local")

    assert vars(client) == before
    assert client._dask_client is old_dask
    assert old_dask.close_calls == 0
    assert builder.context.stop_calls == 1


@pytest.mark.parametrize(
    "scheduler,address,port,expected",
    [
        (
            "dask",
            "tcp://[2001:db8::6]:9000",
            "9999",
            "tcp://[2001:db8::6]:9000",
        ),
        (
            "spark",
            "spark://[2001:db8::7]:7077",
            "9999",
            "spark://[2001:db8::7]:7077",
        ),
    ],
)
def test_constructor_environment_endpoint_embedded_port_is_authoritative(
    monkeypatch, fake_schedulers, scheduler, address, port, expected
):
    _patch_client_startup(monkeypatch)
    monkeypatch.setenv("MSPASS_SCHEDULER", scheduler)
    monkeypatch.setenv("MSPASS_SCHEDULER_ADDRESS", address)
    if scheduler == "dask":
        monkeypatch.setenv("DASK_SCHEDULER_PORT", port)
    else:
        monkeypatch.setenv("SPARK_MASTER_PORT", port)
        builder = FakeSparkBuilder()
        monkeypatch.setattr(
            client_module, "SparkSession", SimpleNamespace(builder=builder)
        )

    client = Client()

    if scheduler == "dask":
        assert client._dask_client.address == expected
        assert client._dask_client_address == expected
    else:
        assert builder.requested_master == expected
        assert client._spark_master_url == expected


@pytest.mark.parametrize(
    "provided,expected_close_calls",
    [(False, 1), (True, 0)],
    ids=["client-owned", "caller-owned"],
)
def test_constructor_dask_validation_failure_respects_ownership(
    monkeypatch, fake_schedulers, provided, expected_close_calls
):
    _patch_client_startup(monkeypatch)
    dask_client = FakeDaskClient("provided") if provided else None
    FakeDaskClient.fail_registration = True

    with pytest.raises(MsPASSError, match="cannot (create|configure).+dask client"):
        Client(scheduler="dask", dask_client=dask_client)

    failed_client = dask_client or FakeDaskClient.instances[-1]
    assert failed_client.register_calls == 1
    assert failed_client.close_calls == expected_close_calls


@pytest.mark.parametrize("old_owned,expected_close_calls", [(True, 1), (False, 0)])
def test_dask_to_dask_commits_before_owned_cleanup(
    monkeypatch, fake_schedulers, old_owned, expected_close_calls
):
    client, old_dask = _new_dask_client_state(old_owned)
    state_seen_during_close = []
    old_dask.on_close = lambda: state_seen_during_close.append(
        (client._scheduler, client._dask_client)
    )

    client.set_scheduler("dask", "tcp://new-dask:9000", scheduler_port="9999")

    new_dask = client._dask_client
    assert new_dask is not old_dask
    assert new_dask.address == "tcp://new-dask:9000"
    assert new_dask.register_calls == 1
    assert client._dask_client_address == "tcp://new-dask:9000"
    assert client._dask_client_owned is True
    assert old_dask.close_calls == expected_close_calls
    if old_owned:
        assert state_seen_during_close == [("dask", new_dask)]
    else:
        assert state_seen_during_close == []


@pytest.mark.parametrize("old_owned,expected_close_calls", [(True, 1), (False, 0)])
def test_dask_to_spark_commits_before_owned_cleanup(
    monkeypatch, fake_schedulers, old_owned, expected_close_calls
):
    client, old_dask = _new_dask_client_state(old_owned)
    builder = FakeSparkBuilder()
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))
    state_seen_during_close = []
    old_dask.on_close = lambda: state_seen_during_close.append(
        (client._scheduler, client._spark_context)
    )

    client.set_scheduler("spark", "spark://new-spark:7077", scheduler_port="9999")

    assert client._scheduler == "spark"
    assert client._spark_context is builder.context
    assert client._spark_master_url == "spark://new-spark:7077"
    assert client._spark_context_owned is True
    assert not hasattr(client, "_dask_client")
    assert old_dask.close_calls == expected_close_calls
    if old_owned:
        assert state_seen_during_close == [("spark", builder.context)]
    else:
        assert state_seen_during_close == []


@pytest.mark.parametrize("old_owned,expected_stop_calls", [(True, 1), (False, 0)])
def test_spark_to_dask_commits_before_owned_cleanup(
    fake_schedulers, old_owned, expected_stop_calls
):
    client, old_spark = _new_spark_client_state(old_owned)
    state_seen_during_stop = []
    old_spark.on_stop = lambda: state_seen_during_stop.append(
        (client._scheduler, client._dask_client)
    )

    client.set_scheduler("dask", "[2001:db8::3]", scheduler_port=8787)

    new_dask = client._dask_client
    assert client._scheduler == "dask"
    assert new_dask.address == "[2001:db8::3]:8787"
    assert client._dask_client_address == "[2001:db8::3]:8787"
    assert client._dask_client_owned is True
    assert not hasattr(client, "_spark_context")
    assert old_spark.stop_calls == expected_stop_calls
    if old_owned:
        assert state_seen_during_stop == [("dask", new_dask)]
    else:
        assert state_seen_during_stop == []


def test_dask_cleanup_failure_propagates_after_replacement_is_committed(
    fake_schedulers,
):
    client, old_dask = _new_dask_client_state(owned=True)
    cleanup_error = RuntimeError("old dask cleanup failed")

    def fail_close():
        raise cleanup_error

    old_dask.on_close = fail_close

    with pytest.raises(RuntimeError) as caught:
        client.set_scheduler("dask", "tcp://new-dask:9000")

    assert caught.value is cleanup_error
    assert client._scheduler == "dask"
    assert client._dask_client is not old_dask
    assert client._dask_client.address == "tcp://new-dask:9000"
    assert client._dask_client_address == "tcp://new-dask:9000"
    assert client._dask_client_owned is True
    assert old_dask.close_calls == 1


def test_spark_cleanup_failure_propagates_after_replacement_is_committed(
    fake_schedulers,
):
    client, old_spark = _new_spark_client_state(owned=True)
    cleanup_error = RuntimeError("old spark cleanup failed")

    def fail_stop():
        raise cleanup_error

    old_spark.on_stop = fail_stop

    with pytest.raises(RuntimeError) as caught:
        client.set_scheduler("dask", "tcp://new-dask:9000")

    assert caught.value is cleanup_error
    assert client._scheduler == "dask"
    assert client._dask_client.address == "tcp://new-dask:9000"
    assert client._dask_client_address == "tcp://new-dask:9000"
    assert client._dask_client_owned is True
    assert not hasattr(client, "_spark_context")
    assert old_spark.stop_calls == 1


@pytest.mark.parametrize("scheduler", ["dask", "spark"])
@pytest.mark.parametrize("owned", [True, False], ids=["owned", "caller-owned"])
def test_close_scheduler_is_idempotent_and_respects_ownership(scheduler, owned):
    if scheduler == "dask":
        client, resource = _new_dask_client_state(owned)
    else:
        client, resource = _new_spark_client_state(owned)

    assert client.close_scheduler() is None
    assert client.get_scheduler() is None
    assert client._scheduler is None
    assert client._scheduler_disabled is True
    assert not hasattr(client, "_dask_client")
    assert not hasattr(client, "_spark_context")
    call_count = resource.close_calls if scheduler == "dask" else resource.stop_calls
    assert call_count == int(owned)

    assert client.close_scheduler() is None
    call_count = resource.close_calls if scheduler == "dask" else resource.stop_calls
    assert call_count == int(owned)


@pytest.mark.parametrize("scheduler", ["dask", "spark"])
def test_close_scheduler_propagates_cleanup_failure_after_detaching(scheduler):
    if scheduler == "dask":
        client, resource = _new_dask_client_state(owned=True)
        cleanup_error = RuntimeError("dask shutdown failed")

        def fail_cleanup():
            raise cleanup_error

        resource.on_close = fail_cleanup
    else:
        client, resource = _new_spark_client_state(owned=True)
        cleanup_error = RuntimeError("spark shutdown failed")

        def fail_cleanup():
            raise cleanup_error

        resource.on_stop = fail_cleanup

    with pytest.raises(RuntimeError) as caught:
        client.close_scheduler()

    assert caught.value is cleanup_error
    assert client.get_scheduler() is None
    assert client._scheduler is None
    assert client._scheduler_disabled is True
    assert not hasattr(client, "_dask_client")
    assert not hasattr(client, "_spark_context")

    assert client.close_scheduler() is None
    call_count = resource.close_calls if scheduler == "dask" else resource.stop_calls
    assert call_count == 1


def test_same_spark_master_is_no_op(monkeypatch, fake_schedulers):
    client, old_spark = _new_spark_client_state()
    builder = FakeSparkBuilder(failure=AssertionError("builder must not be called"))
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))
    before = vars(client).copy()

    result = client.set_scheduler(
        "spark", "spark://old-spark:7077", scheduler_port="9999"
    )

    assert result is None
    assert vars(client) == before
    assert client._spark_context is old_spark
    assert old_spark.stop_calls == 0
    assert builder.get_or_create_calls == 0


@pytest.mark.parametrize("master", ["local", "local[*]"])
def test_same_local_spark_master_is_no_op(monkeypatch, fake_schedulers, master):
    client, old_spark = _new_spark_client_state()
    client._spark_master_url = master
    old_spark.master = master
    builder = FakeSparkBuilder(failure=AssertionError("builder must not be called"))
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))
    before = vars(client).copy()

    result = client.set_scheduler("spark", master, scheduler_port="9999")

    assert result is None
    assert vars(client) == before
    assert client._spark_context is old_spark
    assert old_spark.stop_calls == 0
    assert builder.get_or_create_calls == 0


def test_different_spark_master_is_rejected_atomically(monkeypatch, fake_schedulers):
    client, old_spark = _new_spark_client_state()
    builder = FakeSparkBuilder(failure=AssertionError("builder must not be called"))
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))
    before = vars(client).copy()

    with pytest.raises(MsPASSError, match="refusing to change active Spark master"):
        client.set_scheduler("spark", "new-spark", scheduler_port="7077")

    assert vars(client) == before
    assert client._spark_context is old_spark
    assert old_spark.stop_calls == 0
    assert builder.get_or_create_calls == 0


def test_none_to_dask_construction_failure_is_atomic(fake_schedulers):
    client = _new_client_without_scheduler()
    before = vars(client).copy()
    FakeDaskClient.fail_construction = True

    with pytest.raises(MsPASSError, match="cannot create a dask client"):
        client.set_scheduler("dask", "new-dask", scheduler_port="8786")

    assert vars(client) == before


@pytest.mark.parametrize("old_owned", [True, False], ids=["owned", "caller-owned"])
def test_dask_validation_failure_is_atomic_and_closes_only_new_client(
    fake_schedulers, old_owned
):
    client, old_dask = _new_dask_client_state(owned=old_owned)
    before = vars(client).copy()
    FakeDaskClient.fail_registration = True

    with pytest.raises(MsPASSError, match="cannot create a dask client"):
        client.set_scheduler("dask", "new-dask", scheduler_port="8786")

    new_dask = FakeDaskClient.instances[-1]
    assert new_dask is not old_dask
    assert new_dask.close_calls == 1
    assert old_dask.close_calls == 0
    assert vars(client) == before


@pytest.mark.parametrize(
    "old_owned,builder",
    [
        (
            True,
            FakeSparkBuilder(failure=RuntimeError("spark construction failed")),
        ),
        (
            False,
            FakeSparkBuilder(failure=RuntimeError("spark construction failed")),
        ),
        (True, FakeSparkBuilder(returned_master="spark://unexpected:7077")),
        (False, FakeSparkBuilder(returned_master="spark://unexpected:7077")),
    ],
    ids=[
        "owned-construction",
        "caller-owned-construction",
        "owned-master-validation",
        "caller-owned-master-validation",
    ],
)
def test_spark_construction_and_validation_failures_are_atomic(
    monkeypatch, fake_schedulers, old_owned, builder
):
    client, old_dask = _new_dask_client_state(owned=old_owned)
    before = vars(client).copy()
    monkeypatch.setattr(client_module, "SparkSession", SimpleNamespace(builder=builder))

    with pytest.raises(MsPASSError, match="cannot create a spark configuration"):
        client.set_scheduler("spark", "new-spark", scheduler_port="7077")

    assert vars(client) == before
    assert client._dask_client is old_dask
    assert old_dask.close_calls == 0


def test_spark_to_dask_construction_failure_is_atomic(fake_schedulers):
    client, old_spark = _new_spark_client_state()
    before = vars(client).copy()
    FakeDaskClient.fail_construction = True

    with pytest.raises(MsPASSError, match="cannot create a dask client"):
        client.set_scheduler("dask", "new-dask", scheduler_port="8786")

    assert vars(client) == before
    assert client._spark_context is old_spark
    assert old_spark.stop_calls == 0


def test_missing_optional_dask_is_explicit_and_atomic(monkeypatch):
    client, old_spark = _new_spark_client_state()
    before = vars(client).copy()
    monkeypatch.setattr(client_module, "DaskClient", None)
    monkeypatch.setattr(client_module, "_mspasspy_has_dask_distributed", False)
    monkeypatch.setattr(
        client_module,
        "_mspasspy_dask_import_error",
        ImportError("dask is unavailable"),
    )

    with pytest.raises(MsPASSError, match="dask.distributed could not be imported"):
        client.set_scheduler("dask", "new-dask", scheduler_port="8786")

    assert vars(client) == before
    assert client._spark_context is old_spark
    assert old_spark.stop_calls == 0


def test_client_import_and_request_with_missing_optional_dask(tmp_path):
    dask_package = tmp_path / "dask"
    dask_package.mkdir()
    (dask_package / "__init__.py").write_text(
        'raise ImportError("dask is unavailable")\n', encoding="utf-8"
    )
    env = os.environ.copy()
    env["MSPASS_CLIENT_MODULE"] = str(Path(client_module.__file__).resolve())
    runtime_python = Path(utility_module.__file__).resolve().parents[2]
    env["PYTHONPATH"] = os.pathsep.join(
        [str(tmp_path), str(runtime_python), env.get("PYTHONPATH", "")]
    )

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.util
import os
import sys

client_source = os.environ["MSPASS_CLIENT_MODULE"]
spec = importlib.util.spec_from_file_location("mspasspy.client", client_source)
client_module = importlib.util.module_from_spec(spec)
sys.modules["mspasspy.client"] = client_module
spec.loader.exec_module(client_module)

from mspasspy.client import Client
from mspasspy.ccore.utility import MsPASSError

assert client_module.__file__ == client_source

client = Client.__new__(Client)
client._scheduler = None
client._scheduler_disabled = True
try:
    client.set_scheduler("dask", "localhost")
except MsPASSError as err:
    assert "dask.distributed could not be imported" in str(err), str(err)
else:
    raise AssertionError("missing dask should raise MsPASSError")

try:
    Client(scheduler="dask")
except MsPASSError as err:
    assert "dask.distributed could not be imported" in str(err), str(err)
else:
    raise AssertionError("Client(scheduler='dask') should reject missing dask")
""",
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
