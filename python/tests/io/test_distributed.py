import copy
import io
import os
import pickle

import dask.bag as daskbag
import gridfs
import numpy as np
import obspy
import obspy.clients.fdsn.client
import pytest
import sys
import re
import collections

import boto3
from moto import mock_s3
import botocore.session
from botocore.stub import Stubber
from unittest.mock import patch, Mock
import json
import base64


from mspasspy.util.converter import (
    TimeSeries2Trace,
    Pf2AttributeNameTbl,
    Textfile2Dataframe,
)
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.ccore.utility import (
    dmatrix,
    ErrorSeverity,
    Metadata,
    MsPASSError,
    ProcessingHistory,
    AtomicType,
    AntelopePf,
)

from mspasspy.db.schema import DatabaseSchema, MetadataSchema
from mspasspy.util import logging_helper
from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")

from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)

from mspasspy.io.distributed import (
    read_distributed_data_new,
    read_to_dataframe,
    read_files,
    write_distributed_data,
    write_to_db,
    write_files,
)


def test_read_distributed_data(spark_context):
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")

    test_ts = get_live_timeseries()

    client = DBClient("localhost")
    db = Database(client, "mspasspy_test_db")

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db["site"].insert_one(
        {
            "_id": site_id,
            "net": "net",
            "sta": "sta",
            "loc": "loc",
            "lat": 1.0,
            "lon": 1.0,
            "elev": 2.0,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
        }
    )
    db["channel"].insert_one(
        {
            "_id": channel_id,
            "net": "net1",
            "sta": "sta1",
            "loc": "loc1",
            "chan": "chan",
            "lat": 1.1,
            "lon": 1.1,
            "elev": 2.1,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
            "edepth": 3.0,
            "vang": 1.0,
            "hang": 1.0,
        }
    )
    db["source"].insert_one(
        {
            "_id": source_id,
            "lat": 1.2,
            "lon": 1.2,
            "time": datetime.utcnow().timestamp(),
            "depth": 3.1,
            "magnitude": 1.0,
        }
    )
    test_ts["site_id"] = site_id
    test_ts["source_id"] = source_id
    test_ts["channel_id"] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)
    logging_helper.info(ts1, "1", "deepcopy")
    logging_helper.info(ts2, "1", "deepcopy")
    logging_helper.info(ts3, "1", "deepcopy")

    ts_list = [ts1, ts2, ts3]
    dir = "data/"
    if not os.path.exists(dir):
        os.makedirs(dir)
    dfile = "test_db_output"
    for ts in ts_list:
        db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="file",
            dir="./data/",
            dfile="test_db_output",
        )
    cursors = db["wf_TimeSeries"].find({})

    spark_list = read_distributed_data_new(
        db,
        cursors,
        mode="cautious",
        normalize=["source", "site", "channel"],
        format="spark",
        spark_context=spark_context,
    )
    list = spark_list.collect()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")


def test_read_distributed_data_df(spark_context):
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")

    test_ts = get_live_timeseries()

    client = DBClient("localhost")
    db = Database(client, "mspasspy_test_db")

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db["site"].insert_one(
        {
            "_id": site_id,
            "net": "net",
            "sta": "sta",
            "loc": "loc",
            "lat": 1.0,
            "lon": 1.0,
            "elev": 2.0,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
        }
    )
    db["channel"].insert_one(
        {
            "_id": channel_id,
            "net": "net1",
            "sta": "sta1",
            "loc": "loc1",
            "chan": "chan",
            "lat": 1.1,
            "lon": 1.1,
            "elev": 2.1,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
            "edepth": 3.0,
            "vang": 1.0,
            "hang": 1.0,
        }
    )
    db["source"].insert_one(
        {
            "_id": source_id,
            "lat": 1.2,
            "lon": 1.2,
            "time": datetime.utcnow().timestamp(),
            "depth": 3.1,
            "magnitude": 1.0,
        }
    )
    test_ts["site_id"] = site_id
    test_ts["source_id"] = source_id
    test_ts["channel_id"] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)
    logging_helper.info(ts1, "1", "deepcopy")
    logging_helper.info(ts2, "1", "deepcopy")
    logging_helper.info(ts3, "1", "deepcopy")

    ts_list = [ts1, ts2, ts3]
    dir = "data/"
    if not os.path.exists(dir):
        os.makedirs(dir)
    dfile = "test_db_output"
    for ts in ts_list:
        db.save_data(
            ts,
            mode="pedantic",
            storage_mode="file",
            dir="./data/",
            dfile="test_db_output",
        )
    cursors = db["wf_TimeSeries"].find({})

    df = read_to_dataframe(
        db,
        cursors,
        mode="cautious",
        normalize=["source", "site", "channel"],
        load_history=True,
    )

    list2 = read_distributed_data_new(
        df,
        cursor=None,
        format="spark",
        mode="promiscuous",
        spark_context=spark_context,
    ).collect()
    assert len(list2) == 3
    for l in list2:
        assert l
        assert np.isclose(l.data, test_ts.data).all()
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")


def test_read_distributed_data_dask():
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")

    test_ts = get_live_timeseries()

    client = DBClient("localhost")
    db = Database(client, "mspasspy_test_db")

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db["site"].insert_one(
        {
            "_id": site_id,
            "net": "net",
            "sta": "sta",
            "loc": "loc",
            "lat": 1.0,
            "lon": 1.0,
            "elev": 2.0,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
        }
    )
    db["channel"].insert_one(
        {
            "_id": channel_id,
            "net": "net1",
            "sta": "sta1",
            "loc": "loc1",
            "chan": "chan",
            "lat": 1.1,
            "lon": 1.1,
            "elev": 2.1,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
            "edepth": 3.0,
            "vang": 1.0,
            "hang": 1.0,
        }
    )
    db["source"].insert_one(
        {
            "_id": source_id,
            "lat": 1.2,
            "lon": 1.2,
            "time": datetime.utcnow().timestamp(),
            "depth": 3.1,
            "magnitude": 1.0,
        }
    )
    test_ts["site_id"] = site_id
    test_ts["source_id"] = source_id
    test_ts["channel_id"] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)
    logging_helper.info(ts1, "1", "deepcopy")
    logging_helper.info(ts2, "1", "deepcopy")
    logging_helper.info(ts3, "1", "deepcopy")

    ts_list = [ts1, ts2, ts3]
    dir = "data/"
    if not os.path.exists(dir):
        os.makedirs(dir)
    dfile = "test_db_output"
    for ts in ts_list:
        db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="file",
            dir="./data/",
            dfile="test_db_output",
        )
    cursors = db["wf_TimeSeries"].find({})

    dask_list = read_distributed_data_new(
        db,
        cursors,
        mode="pedantic",
        normalize=["source", "site", "channel"],
        format="dask",
    )
    list = dask_list.compute()
    assert len(list) == 3
    for l in list:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")


def test_read_distributed_data_dask_df():
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")

    test_ts = get_live_timeseries()

    client = DBClient("localhost")
    db = Database(client, "mspasspy_test_db")

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db["site"].insert_one(
        {
            "_id": site_id,
            "net": "net",
            "sta": "sta",
            "loc": "loc",
            "lat": 1.0,
            "lon": 1.0,
            "elev": 2.0,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
        }
    )
    db["channel"].insert_one(
        {
            "_id": channel_id,
            "net": "net1",
            "sta": "sta1",
            "loc": "loc1",
            "chan": "chan",
            "lat": 1.1,
            "lon": 1.1,
            "elev": 2.1,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
            "edepth": 3.0,
            "vang": 1.0,
            "hang": 1.0,
        }
    )
    db["source"].insert_one(
        {
            "_id": source_id,
            "lat": 1.2,
            "lon": 1.2,
            "time": datetime.utcnow().timestamp(),
            "depth": 3.1,
            "magnitude": 1.0,
        }
    )
    test_ts["site_id"] = site_id
    test_ts["source_id"] = source_id
    test_ts["channel_id"] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)
    logging_helper.info(ts1, "1", "deepcopy")
    logging_helper.info(ts2, "1", "deepcopy")
    logging_helper.info(ts3, "1", "deepcopy")

    ts_list = [ts1, ts2, ts3]
    dir = "data/"
    if not os.path.exists(dir):
        os.makedirs(dir)
    dfile = "test_db_output"
    for ts in ts_list:
        db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="file",
            dir="./data/",
            dfile="test_db_output",
        )
    cursors = db["wf_TimeSeries"].find({})

    df = read_to_dataframe(
        db,
        cursors,
        mode="cautious",
        normalize=["source", "site", "channel"],
        load_history=True,
    )

    list2 = read_distributed_data_new(df, cursor=None, format="dask").compute()
    assert len(list2) == 3
    for l in list2:
        assert l
        assert np.isclose(l.data, test_ts.data).all()

    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")


def test_write_distributed_data(spark_context):
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")

    test_ts = get_live_timeseries()

    client = DBClient("localhost")
    db = Database(client, "mspasspy_test_db")

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db["site"].insert_one(
        {
            "_id": site_id,
            "net": "net",
            "sta": "sta",
            "loc": "loc",
            "lat": 1.0,
            "lon": 1.0,
            "elev": 2.0,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
        }
    )
    db["channel"].insert_one(
        {
            "_id": channel_id,
            "net": "net1",
            "sta": "sta1",
            "loc": "loc1",
            "chan": "chan",
            "lat": 1.1,
            "lon": 1.1,
            "elev": 2.1,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
            "edepth": 3.0,
            "vang": 1.0,
            "hang": 1.0,
        }
    )
    db["source"].insert_one(
        {
            "_id": source_id,
            "lat": 1.2,
            "lon": 1.2,
            "time": datetime.utcnow().timestamp(),
            "depth": 3.1,
            "magnitude": 1.0,
        }
    )
    test_ts["site_id"] = site_id
    test_ts["source_id"] = source_id
    test_ts["channel_id"] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)
    dir = os.path.abspath("./data/")
    ts1["dir"] = dir
    ts2["dir"] = dir
    ts3["dir"] = dir
    ts1["dfile"] = "f1"
    ts2["dfile"] = "f2"
    ts3["dfile"] = "f3"
    logging_helper.info(ts1, "1", "deepcopy")
    logging_helper.info(ts2, "1", "deepcopy")
    logging_helper.info(ts3, "1", "deepcopy")

    ts_list = [ts1, ts2, ts3]
    list_ = spark_context.parallelize(ts_list)
    df = write_distributed_data(
        list_,
        db,
        mode="pedantic",
        storage_mode="file",
        format="spark",
    )
    obj_list = read_distributed_data_new(
        df, format="spark", spark_context=spark_context
    ).collect()
    for idx, l in enumerate(obj_list):
        assert l
        assert np.isclose(l.data, ts_list[idx].data).all()


def test_write_distributed_data_dask():
    client = DBClient("localhost")
    client.drop_database("mspasspy_test_db")

    test_ts = get_live_timeseries()

    client = DBClient("localhost")
    db = Database(client, "mspasspy_test_db")

    site_id = ObjectId()
    channel_id = ObjectId()
    source_id = ObjectId()
    db["site"].insert_one(
        {
            "_id": site_id,
            "net": "net",
            "sta": "sta",
            "loc": "loc",
            "lat": 1.0,
            "lon": 1.0,
            "elev": 2.0,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
        }
    )
    db["channel"].insert_one(
        {
            "_id": channel_id,
            "net": "net1",
            "sta": "sta1",
            "loc": "loc1",
            "chan": "chan",
            "lat": 1.1,
            "lon": 1.1,
            "elev": 2.1,
            "starttime": datetime.utcnow().timestamp(),
            "endtime": datetime.utcnow().timestamp(),
            "edepth": 3.0,
            "vang": 1.0,
            "hang": 1.0,
        }
    )
    db["source"].insert_one(
        {
            "_id": source_id,
            "lat": 1.2,
            "lon": 1.2,
            "time": datetime.utcnow().timestamp(),
            "depth": 3.1,
            "magnitude": 1.0,
        }
    )
    test_ts["site_id"] = site_id
    test_ts["source_id"] = source_id
    test_ts["channel_id"] = channel_id

    ts1 = copy.deepcopy(test_ts)
    ts2 = copy.deepcopy(test_ts)
    ts3 = copy.deepcopy(test_ts)
    dir = os.path.abspath("./data/")
    ts1["dir"] = dir
    ts2["dir"] = dir
    ts3["dir"] = dir
    ts1["dfile"] = "f1"
    ts2["dfile"] = "f2"
    ts3["dfile"] = "f3"
    logging_helper.info(ts1, "1", "deepcopy")
    logging_helper.info(ts2, "1", "deepcopy")
    logging_helper.info(ts3, "1", "deepcopy")

    ts_list = [ts1, ts2, ts3]
    list_ = daskbag.from_sequence(ts_list)
    df = write_distributed_data(
        list_,
        db,
        mode="cautious",
        storage_mode="file",
        format="dask",
    )
    obj_list = read_distributed_data_new(df, format="dask").compute()
    for idx, l in enumerate(obj_list):
        assert l
        assert np.isclose(l.data, ts_list[idx].data).all()
