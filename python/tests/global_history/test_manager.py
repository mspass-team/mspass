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
from mspasspy.ccore.utility import Metadata

def spark_map(input, manager, alg_name=None, parameters=None):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    data = sc.parallelize(input)
    res = data.mspass_map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, global_history=manager, alg_name=alg_name, parameters=parameters)
    return res.collect()

def dask_map(input, manager, alg_name=None, parameters=None):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, global_history=manager, alg_name=alg_name, parameters=parameters)
    return res.compute()

def dask_reduce(input, manager, alg_name=None, parameters=None):
    ddb = daskbag.from_sequence(input)
    res = ddb.mspass_reduce(stack, preserve_history=True, alg_id='3', global_history=manager, alg_name=alg_name, parameters=parameters)
    return res.compute()

def spark_reduce(input, manager, alg_name=None, parameters=None):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf)
    data = sc.parallelize(input)
    res = data.mspass_reduce(stack, preserve_history=True, alg_id='2', global_history=manager, alg_name=alg_name, parameters=parameters)
    return res


class TestManager():

    def setup_class(self):
        self.client = Client('localhost')
        db = Database(self.client, 'test_manager')
        db['global_history'].drop_indexes()

        self.manager = GlobalHistoryManager(db, 'test_job', collection='global_history')

    def test_init(self):
        assert self.manager.job_name == 'test_job'
        assert self.manager.collection == 'global_history'
        assert self.manager.history_db.name == 'test_manager'

    def test_logging(self):
        alg_id = ObjectId()
        manager_db = Database(self.client, 'test_manager')
        manager_db['global_history'].delete_many({})
        self.manager.logging('test_alg_name', alg_id, 'test_parameter')
        res = manager_db['global_history'].find_one({'job_name': self.manager.job_name})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'test_alg_name'
        assert res['alg_id'] == alg_id
        assert res['parameters'] == 'test_parameter'
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 1
        # clean up
        manager_db['global_history'].delete_many({})

    def test_mspass_map(self):
        l = [get_live_timeseries() for i in range(5)]
        # add net, sta, chan, loc to avoid metadata serialization problem
        for i in range(5):
            l[i]['chan'] = 'HHZ'
            l[i]['loc'] = 'test_loc'
            l[i]['net'] = 'test_net'
            l[i]['sta'] = 'test_sta'
        # test mspass_map for spark
        spark_res = spark_map(l, self.manager)

        manager_db = Database(self.client, 'test_manager')
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 1
        res = manager_db['global_history'].find_one({'job_name': self.manager.job_name})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'filter'
        assert res['parameters'] == 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        spark_alg_id = res['alg_id']

        # test mspass_map for dask
        dask_res = dask_map(l, self.manager)

        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 2
        assert manager_db['global_history'].count_documents({'alg_id': spark_alg_id}) == 2
        docs = manager_db['global_history'].find({'alg_id': spark_alg_id})
        assert docs[0]['job_id'] == docs[1]['job_id'] == self.manager.job_id
        assert docs[0]['job_name'] == docs[1]['job_name'] == self.manager.job_name
        assert docs[0]['alg_name'] == docs[1]['alg_name'] == 'filter'
        assert docs[0]['parameters'] == docs[1]['parameters'] == "bandpass,freqmin=1,freqmax=5,preserve_history=True"
        assert not docs[0]['time'] == docs[1]['time']

        # same alg + parameters combination -> same alg_id
        dask_res = dask_map(l, self.manager)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 3
        assert manager_db['global_history'].count_documents({'alg_id': spark_alg_id}) == 3

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = 'filter'
        spark_alg_parameters = 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        spark_res = spark_map(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 4
        assert manager_db['global_history'].count_documents({'alg_id': spark_alg_id}) == 4

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = 'new_filter'
        spark_alg_parameters = 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        spark_res = spark_map(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 5
        assert manager_db['global_history'].count_documents({'alg_name': 'new_filter'}) == 1
        res = manager_db['global_history'].find_one({'alg_name': 'new_filter'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'new_filter'
        assert res['parameters'] == 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        new_spark_alg_id = res['alg_id']
        assert manager_db['global_history'].count_documents({'alg_id': new_spark_alg_id}) == 1

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = 'filter'
        dask_alg_parameters = 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 6
        assert manager_db['global_history'].count_documents({'alg_id': spark_alg_id}) == 5

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = 'new_filter_2'
        dask_alg_parameters = 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 7
        assert manager_db['global_history'].count_documents({'alg_name': 'new_filter_2'}) == 1
        res = manager_db['global_history'].find_one({'alg_name': 'new_filter_2'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'new_filter_2'
        assert res['parameters'] == 'bandpass,freqmin=1,freqmax=5,preserve_history=True'
        new_dask_alg_id = res['alg_id']
        assert manager_db['global_history'].count_documents({'alg_id': new_dask_alg_id}) == 1

    def test_mspass_reduce(self):
        manager_db = Database(self.client, 'test_manager')
        manager_db['global_history'].delete_many({})
        
        l = [get_live_timeseries() for i in range(5)]
        # test mspass_reduce for spark
        spark_res = spark_reduce(l, self.manager)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 1
        assert manager_db['global_history'].count_documents({'alg_name': 'stack'}) == 1
        res = manager_db['global_history'].find_one({'alg_name': 'stack'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'stack'
        assert res['parameters'] == 'preserve_history=True,alg_id=2'
        spark_alg_id = res['alg_id']

        # test mspass_reduce for dask
        dask_res = dask_reduce(l, self.manager)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 2
        assert manager_db['global_history'].count_documents({'alg_name': 'stack'}) == 2
        assert manager_db['global_history'].count_documents({'alg_id': spark_alg_id}) == 1
        
        docs = manager_db['global_history'].find({'alg_name': 'stack'})
        for doc in docs:
            if doc['alg_id'] == spark_alg_id:
                continue
            res = doc
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'stack'
        assert res['parameters'] == 'preserve_history=True,alg_id=3'
        # different alg -> different alg_id
        assert not res['alg_id'] == spark_alg_id
        dask_alg_id = res['alg_id']

        # same alg + parameters combination -> same alg_id
        dask_res = dask_reduce(l, self.manager)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 3
        assert manager_db['global_history'].count_documents({'alg_name': 'stack'}) == 3
        assert manager_db['global_history'].count_documents({'alg_id': dask_alg_id}) == 2
        docs = manager_db['global_history'].find({'alg_id': dask_alg_id})
        doc1 = docs[0]
        doc2 = docs[1]
        assert not doc1['time'] == doc2['time']
        assert doc1['job_id'] == doc2['job_id']
        assert doc1['job_name'] == doc2['job_name']
        assert doc1['alg_name'] == doc2['alg_name']
        assert doc1['parameters'] == doc2['parameters']

        # SPARK test user provided alg_name and parameter(exist)
        spark_alg_name = 'stack'
        spark_alg_parameters = 'preserve_history=True,alg_id=2'
        spark_res = spark_reduce(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 4
        assert manager_db['global_history'].count_documents({'alg_name': 'stack'}) == 4
        assert manager_db['global_history'].count_documents({'alg_name': 'stack', 'parameters':'preserve_history=True,alg_id=3'}) == 2

        # SPARK test user provided alg_name and parameter(new)
        spark_alg_name = 'new_stack'
        spark_alg_parameters = 'preserve_history=True,alg_id=2'
        spark_res = spark_reduce(l, self.manager, alg_name=spark_alg_name, parameters=spark_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 5
        assert manager_db['global_history'].count_documents({'alg_name': 'new_stack'}) == 1
        res = manager_db['global_history'].find_one({'alg_name': 'new_stack'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'new_stack'
        assert res['parameters'] == 'preserve_history=True,alg_id=2'

        # DASK test user provided alg_name and parameter(exist)
        dask_alg_name = 'stack'
        dask_alg_parameters = 'preserve_history=True,alg_id=3'
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 6
        assert manager_db['global_history'].count_documents({'alg_name': 'stack'}) == 5
        assert manager_db['global_history'].count_documents({'alg_name': 'stack', 'parameters':'preserve_history=True,alg_id=3'}) == 3

        # DASK test user provided alg_name and parameter(new)
        dask_alg_name = 'new_stack'
        dask_alg_parameters = 'preserve_history=True,alg_id=3'
        dask_res = dask_map(l, self.manager, alg_name=dask_alg_name, parameters=dask_alg_parameters)
        assert manager_db['global_history'].count_documents({'job_name': self.manager.job_name}) == 7
        assert manager_db['global_history'].count_documents({'alg_name': 'new_stack', 'parameters':'preserve_history=True,alg_id=3'}) == 1
        res = manager_db['global_history'].find_one({'alg_name': 'new_stack', 'parameters':'preserve_history=True,alg_id=3'})
        assert res['job_id'] == self.manager.job_id
        assert res['job_name'] == self.manager.job_name
        assert res['alg_name'] == 'new_stack'
        assert res['parameters'] == 'preserve_history=True,alg_id=3'

    def test_get_alg_id(self):
        manager_db = Database(self.client, 'test_manager')
        assert not self.manager.get_alg_id('aaa','bbb')
        res = manager_db['global_history'].find_one({'alg_name': 'new_stack', 'parameters':'preserve_history=True,alg_id=3'})
        assert self.manager.get_alg_id('new_stack', 'preserve_history=True,alg_id=3') == res['alg_id']

    def test_get_alg_list(self):
        assert len(self.manager.get_alg_list(self.manager.job_name, job_id=self.manager.job_id)) == 7

    def test_set_alg_name_and_parameters(self):
        manager_db = Database(self.client, 'test_manager')
        assert manager_db['global_history'].count_documents({'alg_name': 'stack', 'parameters':'preserve_history=True,alg_id=3'}) == 3
        res = manager_db['global_history'].find_one({'alg_name': 'stack', 'parameters':'preserve_history=True,alg_id=3'})
        alg_id = res['alg_id']
        self.manager.set_alg_name_and_parameters(alg_id, 'test_alg_name', 'test_parameters')
        assert manager_db['global_history'].count_documents({'alg_name': 'stack', 'parameters':'preserve_history=True,alg_id=3'}) == 0
        assert manager_db['global_history'].count_documents({'alg_name': 'test_alg_name', 'parameters':'test_parameters'}) == 3
        res = manager_db['global_history'].find_one({'alg_name': 'test_alg_name', 'parameters':'test_parameters'})
        assert res['alg_id'] == alg_id

    def test_object_history(self):
        manager_db = Database(self.client, 'test_manager')
        manager_db['global_history'].delete_many({})
        manager_db['history_object'].delete_many({})
        l = [get_live_timeseries() for i in range(2)]
        # add net, sta, chan, loc to avoid metadata serialization problem
        for i in range(2):
            l[i]['chan'] = 'HHZ'
            l[i]['loc'] = 'test_loc'
            l[i]['net'] = 'test_net'
            l[i]['sta'] = 'test_sta'
        spark_res = spark_map(l, self.manager)
        assert manager_db['global_history'].count_documents({'alg_name': 'filter'}) == 1
        res = manager_db['global_history'].find_one({'alg_name': 'filter'})
        alg_id = res['alg_id']
        save_res_code = manager_db.save_data(spark_res[0])
        # hardcode net, sta, net, loc to avoid serialization problem here, they are readonly metadata keys -> non fatal keys = 4
        assert save_res_code == 4
        assert manager_db['history_object'].count_documents({'alg_name': 'filter'}) == 1
        doc = manager_db['history_object'].find_one({'alg_name': 'filter'})
        assert doc
        assert doc['_id'] == spark_res[0].current_nodedata().uuid
        assert doc['wf_TimeSeries_id'] == spark_res[0]['_id']
        assert doc['alg_id'] == str(alg_id)
        assert doc['alg_name'] == 'filter'