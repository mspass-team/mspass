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

def dask_map(input):
    ddb = daskbag.from_sequence(input)
    res = ddb.map(signals.filter, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0')
    return res.compute()


def spark_map(input):
    appName = 'mspass-test'
    master = 'local'
    conf = SparkConf().setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    data = sc.parallelize(input)
    res = data.map(lambda ts: signals.filter(ts, "bandpass", freqmin=1, freqmax=5, preserve_history=True, instance='0'))
    return res.collect()

class TestManager():

    def setup_class(self):
        self.client = Client('localhost')
        self.db = Database(self.client, 'test_database')

        self.test_ts = get_live_timeseries()
        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        self.db['site'].insert_one({'_id': site_id, 'net': 'net', 'sta': 'sta', 'loc': 'loc', 'lat': 1.0, 'lon': 1.0,
                            'elev': 2.0, 'starttime': datetime.utcnow().timestamp(),
                            'endtime': datetime.utcnow().timestamp()})
        self.db['channel'].insert_one({'_id': channel_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc1', 'chan': 'chan',
                                'lat': 1.1, 'lon': 1.1, 'elev': 2.1, 'starttime': datetime.utcnow().timestamp(),
                                'endtime': datetime.utcnow().timestamp(), 'edepth': 3.0, 'vang': 1.0,
                                'hang': 1.0})
        self.db['source'].insert_one({'_id': source_id, 'lat': 1.2, 'lon': 1.2, 'time': datetime.utcnow().timestamp(),
                                'depth': 3.1, 'magnitude': 1.0})
        self.test_ts['site_id'] = site_id
        self.test_ts['source_id'] = source_id
        self.test_ts['channel_id'] = channel_id

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
        pass