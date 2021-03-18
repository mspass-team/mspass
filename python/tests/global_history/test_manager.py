import copy
import os

import dask.bag as daskbag
from pyspark import SparkConf, SparkContext

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
from mspasspy.db.client import Client
from mspasspy.db.database import Database
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)
from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.ccore.utility import MsPASSError
import mspasspy.algorithms.signals as signals
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, DoubleVector
from mspasspy.reduce import stack

def spark_map(input, manager):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    data = sc.parallelize(input)
    res = data.mspass_map(lambda ts: signals.filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0'),  global_history=manager)
    return res.collect()

def dask_map(input, manager):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0', global_history=manager)
    return res.compute()

def dask_reduce(input, manager):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_reduce(lambda a, b: stack(a, b, preserve_history=True, instance='3'), global_history=manager)
    return res.compute()

def spark_reduce(input, sc, manager):
    data = sc.parallelize(input)
    # zero = get_live_timeseries()
    # zero.data = DoubleVector(np.zeros(255))
    res = data.mspass_reduce(lambda a, b: stack(a, b, preserve_history=True, instance='3'), global_history=manager)
    return res

class TestManager():

    def setup_class(self):
        self.client = Client('localhost')
        self.db = Database(self.client, 'test_database')

        self.manager = GlobalHistoryManager(self.client, 'test_manager', 'test_job', collection='history')

    def test_init(self):
        with pytest.raises(MsPASSError, match="job_name should be a string but <class 'list'> is found."):
            GlobalHistoryManager(self.client, 'test_manager', [], collection='history')
        
        with pytest.raises(MsPASSError, match="database_name should be a string but <class 'list'> is found."):
            GlobalHistoryManager(self.client, [], 'test_job', collection='history')

        with pytest.raises(MsPASSError, match="collection should be a string but <class 'list'> is found."):
            GlobalHistoryManager(self.client, 'test_manager', 'test_job', collection=[])

        assert self.manager.job_name == 'test_job'
        assert self.manager.client == self.client
        assert self.manager.database_name == 'test_manager'
        assert not self.manager.schema
        assert self.manager.collection == 'history'

    def test_logging(self):
        alg_id = ObjectId()
        manager_db = Database(self.client, 'test_manager')
        manager_db['history'].delete_many({})
        self.manager.logging('test_alg_name', alg_id, 'test_parameter')
        res = manager_db['history'].find_one({'job_name': self.manager.job_name})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'test_alg_name'
        assert res['alg_id'] == alg_id
        assert res['parameters'] == 'test_parameter'
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 1
        # clean up
        manager_db['history'].delete_many({})

    def test_mspass_map(self):
        l = [get_live_timeseries() for i in range(5)]
        # test mspass_map for spark
        spark_res = spark_map(l, self.manager)

        manager_db = Database(self.client, 'test_manager')
        res = manager_db['history'].find_one({'job_name': self.manager.job_name})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'signals.filter'
        assert res['parameters'] == 'ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance=\'0\''

        # test mspass_map for dask
        manager_db['history'].delete_many({})
        spark_res = dask_map(l, self.manager)

        manager_db = Database(self.client, 'test_manager')
        res = manager_db['history'].find_one({'job_name': self.manager.job_name})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'filter'
        assert res['parameters'] == "bandpass {'freqmin': 1, 'freqmax': 5, 'preserve_history': True, 'instance': '0'}"

        # test mspass_reduce for spark
        