import copy
import os

import dask.bag as daskbag
from pyspark import SparkConf, SparkContext
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
from mspasspy.db.database import Database
from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, DoubleVector


def mock_excpt():
    raise Exception('mocked exception')

class TestMsPASSClient():

    def setup_class(self):
        self.client = Client()
    
    def test_init(self):
        with pytest.raises(MsPASSError, match="database_host should be a string but <class 'list'> is found."):
            Client(database_host=[])

        with pytest.raises(MsPASSError, match="scheduler should be either dask or spark but xxx is found."):
            Client(scheduler='xxx')

        with pytest.raises(MsPASSError, match="scheduler_host should be a string but <class 'list'> is found."):
            Client(scheduler_host=[])

        with pytest.raises(MsPASSError, match="job_name should be a string but <class 'list'> is found."):
            Client(job_name=[])
        
        with pytest.raises(MsPASSError, match="database_name should be a string but <class 'list'> is found."):
            Client(database_name=[])

        with pytest.raises(MsPASSError, match="collection should be a string but <class 'list'> is found."):
            Client(collection=[])
        
    def test_default(self):
        # test db_client
        host, port = self.client._db_client.address
        assert host == 'localhost'
        assert port == 27017

        # test database_name
        assert self.client._default_database_name == 'mspass'
        assert not self.client._default_schema
        assert not self.client._default_collection

        # test global_history_manager
        assert isinstance(self.client._global_history_manager, GlobalHistoryManager)
        assert self.client._global_history_manager.job_name == 'mspass'
        assert self.client._global_history_manager.collection == 'history_global'
        assert self.client._global_history_manager.history_db.name == 'mspass'

        # test dask scheduler
        assert isinstance(self.client._dask_client, DaskClient)
        assert not hasattr(self.client._dask_client, '_spark_context')

    def test_db_client(self, monkeypatch):
        monkeypatch.setenv('MONGODB_PORT', '12345')
        monkeypatch.setattr(DBClient, "server_info", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a database client with: 168.0.0.1:12345'):
            client = Client(database_host='168.0.0.1')
        monkeypatch.undo()
        
        # test with env
        monkeypatch.setenv('MONGODB_PORT', '12345')
        monkeypatch.setenv('MSPASS_DB_ADDRESS', '168.0.0.1')
        monkeypatch.setattr(DBClient, "server_info", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a database client with: 168.0.0.1:12345'):
            client = Client()
        monkeypatch.undo()

        monkeypatch.setenv('MONGODB_PORT', '12345')
        monkeypatch.setenv('MSPASS_DB_ADDRESS', '168.0.0.1')
        client = Client(database_host='localhost:27017')
        host, port = client._db_client.address
        assert host == 'localhost'
        assert port == 27017
        monkeypatch.undo()

    def test_dask_scheduler(self, monkeypatch):
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a dask client with: 168.0.0.1:8786'):
            client = Client(scheduler='dask', scheduler_host='168.0.0.1')
        monkeypatch.undo()

        monkeypatch.setenv('MSPASS_SCHEDULER', 'dask')
        monkeypatch.setenv('MSPASS_SCHEDULER_ADDRESS', '168.0.0.1')
        monkeypatch.setenv('DASK_SCHEDULER_PORT', '12345')
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a dask client with: 168.0.0.1:12345'):
            client = Client()
        monkeypatch.undo()

    def test_spark_scheduler(self, monkeypatch):
        monkeypatch.setattr(SparkConf, "__init__", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a spark configuration with: spark://168.0.0.1'):
            client = Client(scheduler='spark', scheduler_host='168.0.0.1')
        monkeypatch.undo()

        monkeypatch.setenv('MSPASS_SCHEDULER', 'spark')
        monkeypatch.setenv('MSPASS_SCHEDULER_ADDRESS', '168.0.0.1')
        monkeypatch.setenv('SPARK_MASTER_PORT', '12345')
        monkeypatch.setattr(SparkConf, "__init__", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a spark configuration with: spark://168.0.0.1:12345'):
            client = Client()
        monkeypatch.undo()

    def test_get_database_client(self):
        db_client = self.client.get_database_client()
        assert isinstance(db_client, DBClient)
        host, port = db_client.address
        assert host == 'localhost'
        assert port == 27017

    def test_get_database(self):
        db1 = self.client.get_database()
        assert db1.name == 'mspass'
        db2 = self.client.get_database(database_name='test')
        assert db2.name == 'test'

    def test_get_global_history_manager(self):
        manager = self.client.get_global_history_manager()
        assert isinstance(manager, GlobalHistoryManager)

    def test_get_scheduler(self):
        assert isinstance(self.client.get_scheduler(), DaskClient)
        client = Client(scheduler='spark')
        assert isinstance(client.get_scheduler(), SparkContext)

    def test_set_database_client(self, monkeypatch):
        self.client.set_database_client('localhost', database_port='27017')
        host, port = self.client._db_client.address
        assert host == 'localhost'
        assert port == 27017

        client = Client()
        monkeypatch.setattr(DBClient, "server_info", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a database client with: 168.0.0.1:12345'):
            client.set_database_client('168.0.0.1', database_port='12345')
        monkeypatch.undo()
        # test restore
        host, port = client._db_client.address
        assert host == 'localhost'
        assert port == 27017

    def test_set_global_history_manager(self):
        with pytest.raises(TypeError, match="history_db should be a mspasspy.db.Database but <class 'list'> is found."):
            self.client.set_global_history_manager([], 'test')
        with pytest.raises(TypeError, match="job_name should be a string but <class 'list'> is found."):
            self.client.set_global_history_manager(self.client.get_database('test'), [])
        with pytest.raises(TypeError, match="collection should be a string but <class 'list'> is found."):
            self.client.set_global_history_manager(self.client.get_database('test'), 'test', collection=[])

        self.client.set_global_history_manager(self.client.get_database('test'), 'test_job', collection='test_history')
        assert isinstance(self.client._global_history_manager, GlobalHistoryManager)
        assert self.client._global_history_manager.job_name == 'test_job'
        assert self.client._global_history_manager.collection == 'test_history'
        assert self.client._global_history_manager.history_db.name == 'test'

    def test_set_scheduler(self, monkeypatch):
        with pytest.raises(MsPASSError, match='scheduler should be either dask or spark but test is found.'):
            self.client.set_scheduler('test', 'test')

        temp_dask_client = self.client._dask_client
        monkeypatch.setattr(DaskClient, "__init__", mock_excpt)
        with pytest.raises(MsPASSError, match='Runntime error: cannot create a dask client with: localhost:8786'):
            self.client.set_scheduler('dask', 'localhost', scheduler_port='8786')
        monkeypatch.undo()
        assert isinstance(self.client._dask_client, DaskClient)
        assert self.client._dask_client == temp_dask_client

        self.client.set_scheduler('spark', '127.0.0.3', scheduler_port='7077')
        assert self.client._scheduler == 'spark'
        assert self.client._spark_master_url == 'spark://127.0.0.3:7077'
        assert isinstance(self.client._spark_context, SparkContext)
