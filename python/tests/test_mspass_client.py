import copy
import os

import dask.bag as daskbag
from pyspark import SparkConf, SparkContext
from dask.distributed import Client as DaskClient

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


class TestMsPASSClient():

    def setup_class(self):
        self.client = Client(database_host='localhost', scheduler='dask', scheduler_host='localhost', job_name='mspass', database_name='mspass', schema=None, collection=None)
    
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
        assert self.client.default_database_name == 'mspass'

        # test global_history_manager
        assert isinstance(self.client._global_history_manager, GlobalHistoryManager)
        assert self.client._global_history_manager.job_name == 'mspass'
        assert self.client._global_history_manager.collection == 'history'
        assert self.client._global_history_manager.history_db.name == 'mspass'

        # test dask scheduler
        assert isinstance(self.client._dask_client, DaskClient)
        assert not hasattr(self.client._dask_client, '_spark_conf')
