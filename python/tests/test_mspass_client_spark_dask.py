import copy
import os

import dask.bag as daskbag
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from dask.distributed import Client as DaskClient
from mspasspy.db.client import DBClient

import gridfs
import numpy as np
import obspy
import sys
import re

import pymongo
import pytest

from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")

from mspasspy.util import logging_helper
from mspasspy.client import Client
from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)


def mock_excpt(*args, **kwargs):
    raise Exception("mocked exception")


def _patch_client_startup_for_scheduler_address(monkeypatch):
    monkeypatch.setattr(DBClient, "server_info", lambda self: {})
    monkeypatch.setattr(
        GlobalHistoryManager, "__init__", lambda self, *args, **kwargs: None
    )


def _dask_client_error_match(address):
    return re.escape("cannot create a dask client with: " + address)


def _new_uninitialized_dask_client():
    return DaskClient.__new__(DaskClient)


def test_dask_scheduler_full_uri_constructor_address(monkeypatch):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("tcp://127.0.0.1:41585"),
    ):
        Client(scheduler="dask", scheduler_host="tcp://127.0.0.1:41585")


def test_dask_scheduler_full_uri_env_address(monkeypatch):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    monkeypatch.setenv("MSPASS_SCHEDULER", "dask")
    monkeypatch.setenv("MSPASS_SCHEDULER_ADDRESS", "tcp://127.0.0.1:41585")
    monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("tcp://127.0.0.1:41585"),
    ):
        Client()


def test_dask_scheduler_bare_host_uses_env_port(monkeypatch):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    monkeypatch.setenv("DASK_SCHEDULER_PORT", "12345")
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("168.0.0.1:12345"),
    ):
        Client(scheduler="dask", scheduler_host="168.0.0.1")


def test_dask_scheduler_bare_host_uses_default_port(monkeypatch):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("168.0.0.1:8786"),
    ):
        Client(scheduler="dask", scheduler_host="168.0.0.1")


def test_set_scheduler_dask_uses_full_uri_address(monkeypatch):
    client = Client.__new__(Client)
    client._scheduler = "spark"
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("tcp://127.0.0.1:41585"),
    ):
        client.set_scheduler("dask", "tcp://127.0.0.1:41585")
    assert client._scheduler == "spark"


def test_set_scheduler_dask_coerces_non_string_port(monkeypatch):
    client = Client.__new__(Client)
    client._scheduler = "spark"
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("168.0.0.1:12345"),
    ):
        client.set_scheduler("dask", "168.0.0.1", scheduler_port=12345)
    assert client._scheduler == "spark"


def test_set_scheduler_dask_empty_port_uses_default(monkeypatch):
    client = Client.__new__(Client)
    client._scheduler = "spark"
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    with pytest.raises(
        MsPASSError,
        match=_dask_client_error_match("168.0.0.1:8786"),
    ):
        client.set_scheduler("dask", "168.0.0.1", scheduler_port="")
    assert client._scheduler == "spark"


def test_scheduler_none_constructor_does_not_create_scheduler(monkeypatch, capsys):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    client = Client(scheduler="none")
    assert client._scheduler is None
    assert client.get_scheduler() is None
    assert not hasattr(client, "_dask_client")
    assert not hasattr(client, "_spark_context")
    assert capsys.readouterr().out == ""


def test_scheduler_none_from_environment_does_not_create_scheduler(monkeypatch, capsys):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    monkeypatch.setenv("MSPASS_SCHEDULER", "none")
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    client = Client()
    assert client._scheduler is None
    assert client.get_scheduler() is None
    assert not hasattr(client, "_dask_client")
    assert capsys.readouterr().out == ""


def test_fourth_positional_argument_still_sets_job_name(monkeypatch):
    captured = {}

    monkeypatch.setattr(DBClient, "server_info", lambda self: {})

    def mock_global_history_manager(self, history_db, job_name, collection=None):
        captured["job_name"] = job_name

    monkeypatch.setattr(GlobalHistoryManager, "__init__", mock_global_history_manager)
    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)

    client = Client("127.0.0.1", "none", None, "my_job")

    assert captured["job_name"] == "my_job"
    assert client.get_scheduler() is None
    assert not hasattr(client, "_dask_client")


def test_dask_scheduler_uses_provided_dask_client(monkeypatch):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    provided_client = _new_uninitialized_dask_client()
    registered_plugins = []

    def mock_register_plugin(self, plugin, name=None):
        registered_plugins.append((self, plugin, name))

    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    monkeypatch.setattr(DaskClient, "register_plugin", mock_register_plugin)

    client = Client(scheduler="dask", dask_client=provided_client)

    assert client.get_scheduler() is provided_client
    assert registered_plugins
    assert registered_plugins[0][0] is provided_client
    assert registered_plugins[0][2] == "mongodb_worker"


def test_dask_client_takes_precedence_over_scheduler_host(monkeypatch):
    _patch_client_startup_for_scheduler_address(monkeypatch)
    provided_client = _new_uninitialized_dask_client()
    registered_plugins = []

    monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
    monkeypatch.setattr(
        DaskClient,
        "register_plugin",
        lambda self, plugin, name=None: registered_plugins.append(name),
    )

    client = Client(
        scheduler="dask",
        scheduler_host="tcp://127.0.0.1:41585",
        dask_client=provided_client,
    )

    assert client.get_scheduler() is provided_client
    assert registered_plugins == ["mongodb_worker"]


def test_dask_client_with_scheduler_none_raises(monkeypatch):
    provided_client = _new_uninitialized_dask_client()
    with pytest.raises(
        MsPASSError,
        match="dask_client cannot be used when scheduler is none.",
    ):
        Client(scheduler="none", dask_client=provided_client)


def test_dask_client_with_scheduler_spark_raises(monkeypatch):
    provided_client = _new_uninitialized_dask_client()
    with pytest.raises(
        MsPASSError,
        match="dask_client can only be used with the dask scheduler.",
    ):
        Client(scheduler="spark", dask_client=provided_client)


def test_dask_client_requires_distributed_client(monkeypatch):
    with pytest.raises(
        MsPASSError,
        match="dask_client should be a dask.distributed.Client",
    ):
        Client(scheduler="dask", dask_client=object())


class TestMsPASSClient:
    @staticmethod
    def _close_dask_scheduler(client):
        dask_client = getattr(client, "_dask_client", None)
        if dask_client is not None:
            dask_client.close()

    def setup_class(self):
        self.client = Client()

    def teardown_class(self):
        self._close_dask_scheduler(self.client)

    def test_init(self):
        with pytest.raises(
            MsPASSError,
            match="database_host should be a string but <class 'list'> is found.",
        ):
            Client(database_host=[])

        with pytest.raises(
            MsPASSError,
            match="scheduler should be dask, spark, or none but xxx is found.",
        ):
            Client(scheduler="xxx")

        with pytest.raises(
            MsPASSError,
            match="scheduler_host should be a string but <class 'list'> is found.",
        ):
            Client(scheduler_host=[])

        with pytest.raises(
            MsPASSError,
            match="job_name should be a string but <class 'list'> is found.",
        ):
            Client(job_name=[])

        with pytest.raises(
            MsPASSError,
            match="database_name should be a string but <class 'list'> is found.",
        ):
            Client(database_name=[])

        with pytest.raises(
            MsPASSError,
            match="collection should be a string but <class 'list'> is found.",
        ):
            Client(collection=[])

    def test_default(self):
        # test db_client
        host, port = self.client._db_client.address
        assert host == "127.0.0.1"
        assert port == 27017

        # test database_name
        assert self.client._default_database_name == "mspass"
        assert not self.client._default_schema
        assert not self.client._default_collection

        # test global_history_manager
        assert isinstance(self.client._global_history_manager, GlobalHistoryManager)
        assert self.client._global_history_manager.job_name == "mspass"
        assert self.client._global_history_manager.collection == "history_global"
        assert self.client._global_history_manager.history_db.name == "mspass"

        # test dask scheduler
        assert isinstance(self.client._dask_client, DaskClient)
        assert not hasattr(self.client, "_spark_context")

    def test_db_client(self, monkeypatch):
        monkeypatch.setenv("MONGODB_PORT", "12345")
        monkeypatch.setattr(DBClient, "server_info", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a database client with: 168.0.0.1:12345",
        ):
            client = Client(database_host="168.0.0.1")
        monkeypatch.undo()

        # test with env
        monkeypatch.setenv("MONGODB_PORT", "12345")
        monkeypatch.setenv("MSPASS_DB_ADDRESS", "168.0.0.1")
        monkeypatch.setattr(DBClient, "server_info", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a database client with: 168.0.0.1:12345",
        ):
            client = Client()
        monkeypatch.undo()

        monkeypatch.setenv("MONGODB_PORT", "12345")
        monkeypatch.setenv("MSPASS_DB_ADDRESS", "168.0.0.1")
        self._close_dask_scheduler(self.client)
        client = Client(database_host="localhost:27017")
        try:
            host, port = client._db_client.address
            assert host == "localhost"
            assert port == 27017
        finally:
            self._close_dask_scheduler(client)
            monkeypatch.undo()
            self.client = Client()

    def test_dask_scheduler(self, monkeypatch):
        monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a dask client with: 168.0.0.1:8786",
        ):
            client = Client(scheduler="dask", scheduler_host="168.0.0.1")
        monkeypatch.undo()

        monkeypatch.setenv("MSPASS_SCHEDULER", "dask")
        monkeypatch.setenv("MSPASS_SCHEDULER_ADDRESS", "168.0.0.1")
        monkeypatch.setenv("DASK_SCHEDULER_PORT", "12345")
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a dask client with: 168.0.0.1:12345",
        ):
            client = Client()
        monkeypatch.undo()

        monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match=_dask_client_error_match("tcp://127.0.0.1:41585"),
        ):
            client = Client(scheduler="dask", scheduler_host="tcp://127.0.0.1:41585")
        monkeypatch.undo()

        monkeypatch.setenv("MSPASS_SCHEDULER", "dask")
        monkeypatch.setenv("MSPASS_SCHEDULER_ADDRESS", "tcp://127.0.0.1:41585")
        monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match=_dask_client_error_match("tcp://127.0.0.1:41585"),
        ):
            client = Client()
        monkeypatch.undo()

        monkeypatch.setenv("DASK_SCHEDULER_PORT", "12345")
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match=_dask_client_error_match("168.0.0.1:12345"),
        ):
            client = Client(scheduler="dask", scheduler_host="168.0.0.1")
        monkeypatch.undo()

        monkeypatch.delenv("DASK_SCHEDULER_PORT", raising=False)
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match=_dask_client_error_match("168.0.0.1:8786"),
        ):
            client = Client(scheduler="dask", scheduler_host="168.0.0.1")
        monkeypatch.undo()

    def test_spark_scheduler(self, monkeypatch):
        monkeypatch.setattr(SparkSession, "builder", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a spark configuration with: spark://168.0.0.1",
        ):
            client = Client(scheduler="spark", scheduler_host="168.0.0.1")
        monkeypatch.undo()

        monkeypatch.setenv("MSPASS_SCHEDULER", "spark")
        monkeypatch.setenv("MSPASS_SCHEDULER_ADDRESS", "168.0.0.1")
        monkeypatch.setenv("SPARK_MASTER_PORT", "12345")
        monkeypatch.setattr(SparkSession, "builder", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a spark configuration with: spark://168.0.0.1:12345",
        ):
            client = Client()
        monkeypatch.undo()

    def test_get_database_client(self):
        db_client = self.client.get_database_client()
        assert isinstance(db_client, DBClient)
        host, port = db_client.address
        assert host == "127.0.0.1"
        assert port == 27017

    def test_get_database(self):
        db1 = self.client.get_database()
        assert db1.name == "mspass"
        db2 = self.client.get_database(database_name="test")
        assert db2.name == "test"

    def test_get_global_history_manager(self):
        manager = self.client.get_global_history_manager()
        assert isinstance(manager, GlobalHistoryManager)

    def test_get_scheduler(self):
        assert isinstance(self.client.get_scheduler(), DaskClient)
        client = Client(scheduler="spark")
        assert isinstance(client.get_scheduler(), SparkContext)

    def test_set_database_client(self, monkeypatch):
        self.client.set_database_client("localhost", database_port="27017")
        host, port = self.client._db_client.address
        assert host == "localhost"
        assert port == 27017

        monkeypatch.setattr(DBClient, "server_info", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a database client with: 168.0.0.1:12345",
        ):
            self.client.set_database_client("168.0.0.1", database_port="12345")
        monkeypatch.undo()
        # test restore
        host, port = self.client._db_client.address
        assert host == "localhost"
        assert port == 27017

    def test_set_global_history_manager(self):
        with pytest.raises(
            TypeError,
            match="history_db should be a mspasspy.db.Database but <class 'list'> is found.",
        ):
            self.client.set_global_history_manager([], "test")
        with pytest.raises(
            TypeError, match="job_name should be a string but <class 'list'> is found."
        ):
            self.client.set_global_history_manager(self.client.get_database("test"), [])
        with pytest.raises(
            TypeError,
            match="collection should be a string but <class 'list'> is found.",
        ):
            self.client.set_global_history_manager(
                self.client.get_database("test"), "test", collection=[]
            )

        self.client.set_global_history_manager(
            self.client.get_database("test"), "test_job", collection="test_history"
        )
        assert isinstance(self.client._global_history_manager, GlobalHistoryManager)
        assert self.client._global_history_manager.job_name == "test_job"
        assert self.client._global_history_manager.collection == "test_history"
        assert self.client._global_history_manager.history_db.name == "test"

    def test_set_scheduler(self, monkeypatch):
        # test invalid parameters
        with pytest.raises(
            MsPASSError,
            match="scheduler should be either dask or spark but test is found.",
        ):
            self.client.set_scheduler("test", "test")

        # test set dask, previous is dask
        temp_dask_client = self.client._dask_client
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a dask client with: localhost:8786",
        ):
            self.client.set_scheduler("dask", "localhost", scheduler_port="8786")
        monkeypatch.undo()
        assert self.client._scheduler == "dask"
        assert isinstance(self.client._dask_client, DaskClient)
        assert self.client._dask_client == temp_dask_client

        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match=_dask_client_error_match("tcp://127.0.0.1:41585"),
        ):
            self.client.set_scheduler("dask", "tcp://127.0.0.1:41585")
        monkeypatch.undo()
        assert self.client._scheduler == "dask"
        assert isinstance(self.client._dask_client, DaskClient)
        assert self.client._dask_client == temp_dask_client

        # test set spark, previous is dask
        monkeypatch.setattr(SparkSession, "builder", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a spark configuration with: spark://168.1.2.3:7077",
        ):
            self.client.set_scheduler("spark", "168.1.2.3", scheduler_port="7077")
        monkeypatch.undo()
        # restore back
        assert self.client._scheduler == "dask"
        assert not hasattr(self.client, "_spark_context")
        assert self.client._dask_client == temp_dask_client

        # test set spark, previous is spark
        test_client_2 = Client(scheduler="spark")
        monkeypatch.setattr(SparkSession, "builder", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a spark configuration with: spark://123.4.5.6:7077",
        ):
            test_client_2.set_scheduler("spark", "123.4.5.6", scheduler_port="7077")
        monkeypatch.undo()
        # restore back
        assert test_client_2._scheduler == "spark"

        # test set dask, previous is spark
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(
            MsPASSError,
            match="Runntime error: cannot create a dask client with: localhost:8786",
        ):
            test_client_2.set_scheduler("dask", "localhost", scheduler_port="8786")
        monkeypatch.undo()
        assert test_client_2._scheduler == "spark"
        assert not hasattr(test_client_2, "_dask_client")
