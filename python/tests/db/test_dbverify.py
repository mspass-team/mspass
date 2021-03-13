import copy
import os

import dask.bag
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
from mspasspy.db.script import dbverify
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)

class TestDBVerify():

    def setup_class(self):
        client = Client('localhost')
        self.db = Database(client, 'test_dbverify')

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

    def test_main(self, capfd):
        self.db['wf_TimeSeries'].delete_many({})
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, 'deepcopy', '1')
        logging_helper.info(ts2, 'deepcopy', '1')
        logging_helper.info(ts3, 'deepcopy', '1')

        # fix types
        ts1['npts'] = '123'
        ts2['delta'] = '3'
        ts3['npts'] = 'xyz'
        # wrong normalized key
        ts1['site_id'] = ObjectId()
        ts2.erase('source_id')
        # required attributes
        ts3.erase('starttime')

        save_res_code = self.db.save_data(ts1, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        save_res_code = self.db.save_data(ts2, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])
        save_res_code = self.db.save_data(ts3, mode='promiscuous', storage_mode='gridfs', exclude_keys=['extra2'])

        # no error pop up when runing dbverify, only printing messages
        dbverify.main(['test_dbverify', '-t', 'normalization'])
        dbverify.main(['test_dbverify', '-t', 'required'])
        dbverify.main(['test_dbverify', '-t', 'schema_check'])