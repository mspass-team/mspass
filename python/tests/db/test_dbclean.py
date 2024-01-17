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
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.db.script import dbclean
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)


class TestDBClean:
    def setup_class(self):
        client = DBClient("localhost")
        self.db = Database(client, "test_dbclean")

        self.test_ts = get_live_timeseries()
        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        self.db["site"].insert_one(
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
        self.db["channel"].insert_one(
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
        self.db["source"].insert_one(
            {
                "_id": source_id,
                "lat": 1.2,
                "lon": 1.2,
                "time": datetime.utcnow().timestamp(),
                "depth": 3.1,
                "magnitude": 1.0,
            }
        )
        self.test_ts["site_id"] = site_id
        self.test_ts["source_id"] = source_id
        self.test_ts["channel_id"] = channel_id

    def test_rename_list_to_dict(self):
        rlist = ["a:1", "b:2", "c:3"]
        result = dbclean.rename_list_to_dict(rlist)
        assert len(result) == 3
        assert result == {"a": "1", "b": "2", "c": "3"}

        rlist = ["a:1:2"]
        with pytest.raises(SystemExit) as e:
            dbclean.rename_list_to_dict(rlist)
        assert e.type == SystemExit
        assert e.value.code == -1

    def test_main(self):
        self.db["wf_TimeSeries"].delete_many({})
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        logging_helper.info(ts3, "1", "deepcopy")

        # fix types
        ts1["npts"] = "123"
        ts2["delta"] = "3"
        ts3["npts"] = "xyz"

        save_res_code = self.db.save_data(
            ts1,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        save_res_code = self.db.save_data(
            ts2,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        save_res_code = self.db.save_data(
            ts3,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )

        # exit
        with pytest.raises(SystemExit) as e:
            dbclean.main(["test_dbclean", "wf_TimeSeries"])
        assert e.type == SystemExit
        assert e.value.code == -1

        # delete starttime attribute
        # rename calib to rename_calib
        dbclean.main(
            [
                "test_dbclean",
                "wf_TimeSeries",
                "-ft",
                "-d",
                "starttime",
                "-r",
                "calib:rename_calib",
            ]
        )

        res1 = self.db["wf_TimeSeries"].find_one({"_id": ts1["_id"]})
        res2 = self.db["wf_TimeSeries"].find_one({"_id": ts2["_id"]})
        res3 = self.db["wf_TimeSeries"].find_one({"_id": ts3["_id"]})

        assert res1["npts"] == 123
        assert "starttime" not in res1
        assert "calib" not in res1
        assert "rename_calib" in res1

        assert res2["delta"] == 3.0
        assert "starttime" not in res2
        assert "calib" not in res2
        assert "rename_calib" in res2

        # can't be fixed
        assert res3["npts"] == "xyz"
        assert "starttime" not in res3
        assert "calib" not in res3
        assert "rename_calib" in res3

        self.db["wf_TimeSeries"].delete_many({})
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        logging_helper.info(ts3, "1", "deepcopy")

        # fix types
        ts1["npts"] = "123"
        ts2["delta"] = "3"
        ts3["npts"] = "xyz"

        save_res_code = self.db.save_data(
            ts1,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        save_res_code = self.db.save_data(
            ts2,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        save_res_code = self.db.save_data(
            ts3,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )

        # only fix types
        dbclean.main(["test_dbclean", "wf_TimeSeries", "-ft"])
        assert res1["npts"] == 123
        assert res2["delta"] == 3.0
        # can't be fixed
        assert res3["npts"] == "xyz"
