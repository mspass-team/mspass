from unittest import mock
import mspasspy
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.ccore.utility import MsPASSError
from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.util import logging_helper
import mspasspy.util.db_utils as _db_utils  # preload before masking optional Dask
from mspasspy.db.client import DBClient

import gridfs
import numpy as np
import obspy
import os
from pathlib import Path
import subprocess
import sys
import re

import pymongo
import pytest

from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")


def mock_excpt(*args, **kwargs):
    raise Exception("mocked exception")


_client_attribute_was_present = hasattr(mspasspy, "client")
_original_client_attribute = getattr(mspasspy, "client", None)
with mock.patch.dict(
    sys.modules, {"pyspark": None, "dask.distributed": None, "dask": None}
):
    # Re-evaluate optional scheduler availability inside the isolated module map.
    sys.modules.pop("mspasspy.client", None)
    from mspasspy.client import Client
    import mspasspy.client as client_module

    class TestMsPASSClient:
        def setup_class(self):
            self.client = Client(scheduler="none")

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
            client = Client(database_host="localhost:27017", scheduler="none")
            host, port = client._db_client.address
            assert host == "localhost"
            assert port == 27017
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
            assert self.client.get_scheduler() is None
            assert not hasattr(self.client, "_dask_client")
            env = os.environ.copy()
            env["MSPASS_CLIENT_MODULE"] = str(Path(client_module.__file__).resolve())
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import importlib.util
import os
import sys

for name in ("pyspark", "dask", "dask.distributed"):
    sys.modules[name] = None

client_source = os.environ["MSPASS_CLIENT_MODULE"]
spec = importlib.util.spec_from_file_location("mspasspy.client", client_source)
client_module = importlib.util.module_from_spec(spec)
sys.modules["mspasspy.client"] = client_module
spec.loader.exec_module(client_module)
assert client_module.__file__ == client_source

client_module.DBClient.server_info = lambda self: {}
client_module.GlobalHistoryManager.__init__ = lambda self, *args, **kwargs: None
client = client_module.Client()
assert client._scheduler is None
assert client.get_scheduler() is None
""",
                ],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            assert result.returncode == 0, result.stderr

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
                TypeError,
                match="job_name should be a string but <class 'list'> is found.",
            ):
                self.client.set_global_history_manager(
                    self.client.get_database("test"), []
                )
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


if _client_attribute_was_present:
    mspasspy.client = _original_client_attribute
else:
    delattr(mspasspy, "client")
