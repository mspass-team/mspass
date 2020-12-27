import copy
import mongomock
import numpy as np
import pymongo
import pytest
import sys
from bson.codec_options import CodecOptions
from bson.raw_bson import RawBSONDocument
from unittest.mock import patch, Mock

from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")
from mspasspy.db.database import Database, read_distributed_data
from helper import (get_live_seismogram,
                    get_live_timeseries)

@patch('mspasspy.db.database.Database.read_data')
def test_read_distributed_data(mock_method, spark_context):
    # client = pymongo.MongoClient('localhost')
    # db = Database(client, 'mspasspy_test_db')

    test_ts = get_live_timeseries()

    # site_id = ObjectId()
    # channel_id = ObjectId()
    # source_id = ObjectId()
    # db['site'].insert_one({'_id': site_id, 'net': 'net', 'sta': 'sta', 'loc': 'loc', 'lat': 1.0, 'lon': 1.0,
    #                        'elev': 2.0, 'starttime': datetime.utcnow().timestamp(),
    #                        'endtime': datetime.utcnow().timestamp()})
    # db['channel'].insert_one({'_id': channel_id, 'net': 'net1', 'sta': 'sta1', 'loc': 'loc1', 'chan': 'chan',
    #                           'lat': 1.1, 'lon': 1.1, 'elev': 2.1, 'starttime': datetime.utcnow().timestamp(),
    #                           'endtime': datetime.utcnow().timestamp(), 'edepth': 3.0, 'vang': 1.0,
    #                           'hang': 1.0})
    # db['source'].insert_one({'_id': source_id, 'lat': 1.2, 'lon': 1.2, 'time': datetime.utcnow().timestamp(),
    #                          'depth': 3.1, 'magnitude': 1.0})
    # test_ts['site_id'] = site_id
    # test_ts['source_id'] = source_id
    # test_ts['channel_id'] = channel_id
    #
    # ts1 = copy.deepcopy(test_ts)
    # ts2 = copy.deepcopy(test_ts)
    # ts3 = copy.deepcopy(test_ts)
    #
    # db.save_data(ts1, 'gridfs')
    # db.save_data(ts2, 'gridfs')
    # db.save_data(ts3, 'gridfs')
    # cursors = db['wf_TimeSeries'].find({})
    print(mock_method)
    mock_method.return_value = test_ts
    client = pymongo.MongoClient('localhost')
    db = Database(client, 'mspasspy_test_db')
    assert db.read_data(ObjectId(), 'wf_TimeSeries') == test_ts
    spark_list = read_distributed_data('localhost', 'mspasspy_test_db', [{'_id': ObjectId} for _ in range(3)],
                                       'wf_TimeSeries', spark_context=spark_context)
    list = spark_list.collect()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()


    # client = pymongo.MongoClient('localhost')
    # client.drop_database('mspasspy_test_db')