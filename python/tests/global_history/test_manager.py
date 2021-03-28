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
import mspasspy.algorithms.signals as signals
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, DoubleVector
from mspasspy.reduce import stack

def spark_map(input, manager, alg_name=None, parameters=None):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    data = sc.parallelize(input)
    res = data.mspass_map(lambda ts: signals.filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0'), global_history=manager, alg_name=alg_name, parameters=parameters)
    return res.collect()

def dask_map(input, manager, alg_name=None, parameters=None):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0', global_history=manager, alg_name=alg_name, parameters=parameters)
    return res.compute()

def dask_reduce(input, manager, alg_name=None, parameters=None):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_reduce(lambda a, b: stack(a, b, preserve_history=True, instance='3'), global_history=manager, alg_name=alg_name, parameters=parameters)
    return res.compute()

def spark_reduce(input, manager, alg_name=None, parameters=None):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    data = sc.parallelize(input)
    res = data.mspass_reduce(lambda a, b: stack(a, b, preserve_history=True, instance='2'), global_history=manager, alg_name=alg_name, parameters=parameters)
    return res

class TestManager():

    def setup_class(self):
        self.client = Client('localhost')
        db = Database(self.client, 'test_manager')
        db['history'].drop_indexes()

        self.manager = GlobalHistoryManager(db, 'test_job', collection='history')

    def test_init(self):
        assert self.manager.job_name == 'test_job'
        assert self.manager.collection == 'history'
        assert self.manager.history_db.name == 'test_manager'

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
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 1
        res = manager_db['history'].find_one({'job_name': self.manager.job_name})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'signals.filter'
        assert res['parameters'] == 'ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance=\'0\''
        spark_alg_id = res['alg_id']

        # test mspass_map for dask
        dask_res = dask_map(l, self.manager)

        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 2
        assert manager_db['history'].count_documents({'alg_name': 'filter'}) == 1
        res = manager_db['history'].find_one({'alg_name': 'filter'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'filter'
        assert res['parameters'] == "bandpass,freqmin=1,freqmax=5,preserve_history=True,instance=0"
        # different alg -> different alg_id
        assert not res['alg_id'] == spark_alg_id
        dask_alg_id = res['alg_id']

        # same alg + parameters combination -> same alg_id
        dask_res = dask_map(l, self.manager)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 3
        assert manager_db['history'].count_documents({'alg_id': dask_alg_id}) == 2
        docs = manager_db['history'].find({'alg_id': dask_alg_id})
        doc1 = docs[0]
        doc2 = docs[1]
        assert not doc1['time'] == doc2['time']
        assert doc1['job_id'] == doc2['job_id']
        assert doc1['job_name'] == doc2['job_name']
        assert doc1['alg_name'] == doc2['alg_name']
        assert doc1['parameters'] == doc2['parameters']

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = 'signals.filter'
        spark_alg_parameters = 'ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance=\'0\''
        spark_res = spark_map(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 4
        assert manager_db['history'].count_documents({'alg_name': 'signals.filter'}) == 2
        dcos = manager_db['history'].find({'alg_name': 'signals.filter'})
        doc1 = docs[0]
        doc2 = docs[1]
        assert not doc1['time'] == doc2['time']
        assert doc1['job_id'] == doc2['job_id']
        assert doc1['job_name'] == doc2['job_name']
        assert doc1['alg_name'] == doc2['alg_name']
        assert doc1['alg_id'] == doc2['alg_id']
        assert doc1['parameters'] == doc2['parameters']

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = 'new_signals.filter'
        spark_alg_parameters = 'ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance=\'0\''
        spark_res = spark_map(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 5
        assert manager_db['history'].count_documents({'alg_name': 'new_signals.filter'}) == 1
        res = manager_db['history'].find_one({'alg_name': 'new_signals.filter'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'new_signals.filter'
        assert res['parameters'] == 'ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance=\'0\''
        new_spark_alg_id = res['alg_id']
        assert manager_db['history'].count_documents({'alg_id': new_spark_alg_id}) == 1

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = 'filter'
        dask_alg_parameters = 'bandpass,freqmin=1,freqmax=5,preserve_history=True,instance=0'
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 6
        assert manager_db['history'].count_documents({'alg_id': dask_alg_id}) == 3
        docs = manager_db['history'].find({'alg_id': dask_alg_id})
        doc1 = docs[0]
        doc2 = docs[1]
        doc3 = docs[2]
        assert not doc1['time'] == doc2['time']
        assert not doc1['time'] == doc3['time']
        assert not doc2['time'] == doc3['time']
        assert doc1['job_id'] == doc2['job_id'] == doc3['job_id']
        assert doc1['job_name'] == doc2['job_name'] == doc3['job_name']
        assert doc1['alg_name'] == doc2['alg_name'] == doc3['alg_name']
        assert doc1['parameters'] == doc2['parameters'] == doc3['parameters']

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = 'new_filter'
        dask_alg_parameters = 'bandpass,freqmin=1,freqmax=5,preserve_history=True,instance=0'
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 7
        assert manager_db['history'].count_documents({'alg_name': 'new_filter'}) == 1
        res = manager_db['history'].find_one({'alg_name': 'new_filter'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'new_filter'
        assert res['parameters'] == 'bandpass,freqmin=1,freqmax=5,preserve_history=True,instance=0'
        new_dask_alg_id = res['alg_id']
        assert manager_db['history'].count_documents({'alg_id': new_dask_alg_id}) == 1

    def test_mspass_reduce(self):
        manager_db = Database(self.client, 'test_manager')
        manager_db['history'].delete_many({})
        
        l = [get_live_timeseries() for i in range(5)]
        # test mspass_reduce for spark
        spark_res = spark_reduce(l, self.manager)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 1
        assert manager_db['history'].count_documents({'alg_name': 'stack'}) == 1
        res = manager_db['history'].find_one({'alg_name': 'stack'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'stack'
        assert res['parameters'] == 'a, b, preserve_history=True, instance=\'2\''
        spark_alg_id = res['alg_id']

        # test mspass_reduce for dask
        dask_res = dask_reduce(l, self.manager)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 2
        assert manager_db['history'].count_documents({'alg_name': 'stack'}) == 2
        assert manager_db['history'].count_documents({'alg_id': spark_alg_id}) == 1
        
        docs = manager_db['history'].find({'alg_name': 'stack'})
        for doc in docs:
            if doc['alg_id'] == spark_alg_id:
                continue
            res = doc
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'stack'
        assert res['parameters'] == 'a, b, preserve_history=True, instance=\'3\''
        # different alg -> different alg_id
        assert not res['alg_id'] == spark_alg_id
        dask_alg_id = res['alg_id']

        # same alg + parameters combination -> same alg_id
        dask_res = dask_reduce(l, self.manager)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 3
        assert manager_db['history'].count_documents({'alg_name': 'stack'}) == 3
        assert manager_db['history'].count_documents({'alg_id': dask_alg_id}) == 2
        docs = manager_db['history'].find({'alg_id': dask_alg_id})
        doc1 = docs[0]
        doc2 = docs[1]
        assert not doc1['time'] == doc2['time']
        assert doc1['job_id'] == doc2['job_id']
        assert doc1['job_name'] == doc2['job_name']
        assert doc1['alg_name'] == doc2['alg_name']
        assert doc1['parameters'] == doc2['parameters']

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = 'stack'
        spark_alg_parameters = 'a, b, preserve_history=True, instance=\'2\''
        spark_res = spark_reduce(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 4
        assert manager_db['history'].count_documents({'alg_name': 'stack'}) == 4
        assert manager_db['history'].count_documents({'alg_name': 'stack', 'parameters':'a, b, preserve_history=True, instance=\'2\''}) == 2

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = 'new_stack'
        spark_alg_parameters = 'a, b, preserve_history=True, instance=\'2\''
        spark_res = spark_reduce(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 5
        assert manager_db['history'].count_documents({'alg_name': 'new_stack'}) == 1

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = 'stack'
        dask_alg_parameters = 'a, b, preserve_history=True, instance=\'3\''
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 6
        assert manager_db['history'].count_documents({'alg_name': 'stack'}) == 5
        assert manager_db['history'].count_documents({'alg_name': 'stack', 'parameters':'a, b, preserve_history=True, instance=\'3\''}) == 3

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = 'new_stack'
        dask_alg_parameters = 'a, b, preserve_history=True, instance=\'3\''
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['history'].count_documents({'job_name': self.manager.job_name}) == 7
        assert manager_db['history'].count_documents({'alg_name': 'new_stack', 'parameters':'a, b, preserve_history=True, instance=\'3\''}) == 1

    def test_get_alg_id(self):
        manager_db = Database(self.client, 'test_manager')
        assert not self.manager.get_alg_id('aaa','bbb')
        res = manager_db['history'].find_one({'alg_name': 'stack', 'parameters':'a, b, preserve_history=True, instance=\'2\''})
        assert self.manager.get_alg_id('stack', 'a, b, preserve_history=True, instance=\'2\'') == res['alg_id']

    def test_get_alg_list(self):
        assert len(self.manager.get_alg_list(self.manager.job_name, job_id=self.manager.job_id)) == 7

    def test_set_alg_name_and_parameters(self):
        manager_db = Database(self.client, 'test_manager')
        assert manager_db['history'].count_documents({'alg_name': 'stack', 'parameters':'a, b, preserve_history=True, instance=\'3\''}) == 3
        res = manager_db['history'].find_one({'alg_name': 'stack', 'parameters':'a, b, preserve_history=True, instance=\'3\''})
        alg_id = res['alg_id']
        self.manager.set_alg_name_and_parameters(alg_id, 'test_alg_name', 'test_parameters')
        assert manager_db['history'].count_documents({'alg_name': 'stack', 'parameters':'a, b, preserve_history=True, instance=\'3\''}) == 0
        assert manager_db['history'].count_documents({'alg_name': 'test_alg_name', 'parameters':'test_parameters'}) == 3
        res = manager_db['history'].find_one({'alg_name': 'test_alg_name', 'parameters':'test_parameters'})
        assert res['alg_id'] == alg_id