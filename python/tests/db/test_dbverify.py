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

from bson import json_util
from bson.objectid import ObjectId
from datetime import datetime

sys.path.append("python/tests")

from mspasspy.util import logging_helper
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.db.script import dbverify
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)


class TestDBVerify:
    def setup_class(self):
        client = DBClient("localhost")
        self.db = Database(client, "test_dbverify")

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

    def test_main(self, capfd):
        self.db["wf_TimeSeries"].delete_many({})
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        logging_helper.info(ts3, "1", "deepcopy")

        # fix types
        ts1["npts"] = "123"
        ts1["extra1"] = "extra1"
        ts2["delta"] = "3"
        ts2["extra2"] = "extra2"
        ts3["npts"] = "xyz"
        ts3["extra2"] = "extra2"
        # wrong normalized key
        ts1["site_id"] = ObjectId()
        ts2.erase("source_id")

        save_res_code = self.db.save_data(
            ts1, mode="promiscuous", storage_mode="gridfs"
        )
        save_res_code = self.db.save_data(
            ts2, mode="promiscuous", storage_mode="gridfs"
        )
        # erase required attributes
        save_res_code = self.db.save_data(
            ts3, mode="promiscuous", storage_mode="gridfs", exclude_keys=["starttime"]
        )
        doc1 = self.db["wf_TimeSeries"].find_one({"_id": ts1["_id"]})
        doc2 = self.db["wf_TimeSeries"].find_one({"_id": ts2["_id"]})
        doc3 = self.db["wf_TimeSeries"].find_one({"_id": ts3["_id"]})
        doc1_str = json_util.dumps(doc1, indent=2)
        doc2_str = json_util.dumps(doc2, indent=2)
        doc3_str = json_util.dumps(doc3, indent=2)

        # default normalization test
        dbverify.main(["test_dbverify", "-t", "normalization"])
        out, err = capfd.readouterr()
        assert (
            out
            == "normalization test on normalized key= site_id  found problems\nFound broken links in  1 documents checked\nNote error count limit= 1000\nIf the count is the same it means all data probably contain missing cross referencing ids\nRun in verbose mode to find out more information you will need to fix the problem\ncheck_links found no broken links with normalized key= channel_id\ncheck_links found no broken links with normalized key= source_id\n"
        )

        # more than 1 collection to test
        dbverify.main(
            ["test_dbverify", "-t", "normalization", "-c", "wf_TimeSeries", "site"]
        )
        out, err = capfd.readouterr()
        assert (
            out
            == "WARNING:  normalization test can only be run on one collection at a time\nParsed a list with the following contents:   ['wf_TimeSeries', 'site']\nRunning test on the first item in that list\nnormalization test on normalized key= site_id  found problems\nFound broken links in  1 documents checked\nNote error count limit= 1000\nIf the count is the same it means all data probably contain missing cross referencing ids\nRun in verbose mode to find out more information you will need to fix the problem\ncheck_links found no broken links with normalized key= channel_id\ncheck_links found no broken links with normalized key= source_id\n"
        )

        # verbose mode
        dbverify.main(["test_dbverify", "-t", "normalization", "-v"])
        out, err = capfd.readouterr()
        assert (
            out
            == "check_link found the following docs in  wf_TimeSeries  with broken links to  site_id\n////////////////Doc number  1  with error///////////////\n"
            + doc1_str
            + "\n////////////////////////////////////////////////////////\ncheck_links found no undefined linking key to normalized key= site_id\ncheck_links found no broken links with normalized key= channel_id\ncheck_links found no undefined linking key to normalized key= channel_id\ncheck_links found no broken links with normalized key= source_id\ncheck_link found the following docs in  wf_TimeSeries  with undefined link keys to  source_id\n////////////////Doc number  1  with error///////////////\n"
            + doc2_str
            + "\n////////////////////////////////////////////////////////\n"
        )

        # default required test
        dbverify.main(["test_dbverify", "-t", "required"])
        out, err = capfd.readouterr()
        mmkeys = {"npts": 2, "delta": 1}
        mm_keys_str = json_util.dumps(mmkeys, indent=2)
        undef_keys = {"starttime": 1}
        undef_keys_str = json_util.dumps(undef_keys, indent=2)
        assert (
            out
            == "////Results from run_check_required on collection= wf_TimeSeries\nCollection found  3  documents with type inconsistencies\nOffending keys and number found follow:\n"
            + mm_keys_str
            + "\nCollection found  1  documents with required keys that were not defined\nOffending keys and number found follow:\n"
            + undef_keys_str
            + "\n"
        )

        # default schema_check test
        dbverify.main(["test_dbverify", "-t", "schema_check"])
        out, err = capfd.readouterr()
        mmkeys = {"npts": 2, "delta": 1}
        mm_keys_str = json_util.dumps(mmkeys, indent=2)
        undef_keys = {"extra1": 1, "extra2": 2}
        undef_keys_str = json_util.dumps(undef_keys, indent=2)
        assert (
            out
            == "check_attribute_types result for collection= wf_TimeSeries\nCollection found  3  documents with type inconsistencies\nOffending keys and number found follow:\n"
            + mm_keys_str
            + "\nCollection found  3  documents with keys not defined in the schema\nOffending keys and number found follow:\n"
            + undef_keys_str
            + "\n"
        )
