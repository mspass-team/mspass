from mspasspy.db.normalize import (
    ID_matcher,
    mseed_channel_matcher,
    mseed_site_matcher,
    origin_time_source_matcher,
    normalize_mseed,
)

from mspasspy.db.database import Database, read_distributed_data
from mspasspy.db.client import DBClient

import os
import bson.json_util
import numpy
import copy
from bson.objectid import ObjectId


def backup_db(db, backup_db_dir):
    """
    A helper function to dump a collection.
    This function will not be used in the test. It is put here just for reference.
    """
    collections = db.collection_names()
    for i, collection_name in enumerate(collections):
        col = getattr(db, collections[i])
        collection = col.find()
        jsonpath = collection_name + ".json"
        jsonpath = os.path.join(backup_db_dir, jsonpath)
        with open(jsonpath, "wb") as jsonfile:
            jsonfile.write(bson.json_util.dumps(collection).encode())


def restore_db(db, backup_db_dir):
    """
    A helper function to load the dump of a collection
    There is no equivalant to mongodump in pymongo, so we use two helper functions
    to dump/load database manually
    """
    json_files = os.listdir(backup_db_dir)
    for file in json_files:
        jsonpath = os.path.join(backup_db_dir, file)
        collection_name = file.split(".")[0]
        with open(jsonpath, "rb") as jsonfile:
            rawdata = jsonfile.read()
            jsondata = bson.json_util.loads(rawdata)
            db[collection_name].insert_many(jsondata)


def Metadata_cmp(a, b):
    """
    A helper function to compare two metadata/dict objects,
    return true if they are equal, false if they are not.
    """
    for key in a:
        if key not in b:
            return False
        if a[key] != b[key]:
            if type(a[key]) is float and numpy.isclose(a[key], b[key]):
                continue
            return False

    for key in b:
        if key not in a:
            return False
        if a[key] != b[key]:
            if type(a[key]) is float and numpy.isclose(a[key], b[key]):
                continue
            return False

    return True


class TestNormalize:
    def setup_class(self):
        client = DBClient("localhost")
        client.drop_database("nmftest")
        self.db = Database(
            client, "nmftest", db_schema="mspass.yaml", md_schema="mspass.yaml"
        )
        self.dump_path = "./python/tests/data/db_dump"
        restore_db(self.db, self.dump_path)

    def teardown_class(self):
        client = DBClient("localhost")
        client.drop_database("nmftest")

    def setup_method(self):
        self.doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("627fc20559a116ff99f38243")}
        )
        self.ts = self.db.read_data(self.doc, collection="wf_miniseed")

    def test_ID_matcher_get_document(self):
        cached_matcher = ID_matcher(self.db)
        uncached_matcher = ID_matcher(self.db, cache_normalization_data=False)

        norm_key_undefine_msg = (
            "Normalizing ID with key=channel_id is not defined in this object"
        )
        cache_id_undefine_msg = "] not defined in cache"
        uncache_id_undefine_msg = "] not defined in normalization collection"

        #   Test get_document
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.get_document(ts)
        uncached_retdoc = uncached_matcher.get_document(ts)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #       Normalizing key undefined
        ts = copy.deepcopy(self.ts)
        del ts["channel_id"]
        retdoc = cached_matcher.get_document(ts)
        assert (
            (retdoc is None)
            and (norm_key_undefine_msg in str(ts.elog.get_error_log()))
            and ts.dead()
        )
        ts = copy.deepcopy(self.ts)
        del ts["channel_id"]
        retdoc = uncached_matcher.get_document(ts)
        assert (
            (retdoc is None)
            and (norm_key_undefine_msg in str(ts.elog.get_error_log()))
            and ts.dead()
        )

        #       Id can't be found
        ts = copy.deepcopy(self.ts)
        ts["channel_id"] = "something_random"
        cached_retdoc = cached_matcher.get_document(ts)
        assert (
            (cached_retdoc is None)
            and (cache_id_undefine_msg in str(ts.elog.get_error_log()))
            and ts.dead()
        )
        retdoc = uncached_matcher.get_document(ts)
        assert (
            (retdoc is None)
            and (uncache_id_undefine_msg in str(ts.elog.get_error_log()))
            and ts.dead()
        )

    def test_ID_matcher_normalize(self):
        cached_matcher = ID_matcher(self.db)
        uncached_matcher = ID_matcher(self.db, cache_normalization_data=False)

        cache_id_undefine_msg = "] not defined in cache"

        #   Test normalize
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        uncached_retdoc = uncached_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "channel_lat" in cached_retdoc

        #       Test prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1, prepend_collection_name=False)
        uncached_retdoc = uncached_matcher(ts_2, prepend_collection_name=False)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "lat" in cached_retdoc

        #       Test Kill_on_failure and Verbose
        cached_matcher = ID_matcher(self.db, kill_on_failure=False)
        ts = copy.deepcopy(self.ts)
        ts["channel_id"] = "something_random"
        cached_retdoc = cached_matcher.get_document(ts)
        assert (
            (cached_retdoc is None)
            and (cache_id_undefine_msg in str(ts.elog.get_error_log()))
            and not ts.dead()
        )

        #       Test Normalize dead
        cached_matcher = ID_matcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        cached_retdoc = cached_matcher(ts)
        assert Metadata_cmp(self.ts, cached_retdoc)

    def test_mseed_channel_matcher_get_document(self):
        cached_matcher = mseed_channel_matcher(self.db)
        uncached_matcher = mseed_channel_matcher(
            self.db, cache_normalization_data=False
        )

        #   get document for dict
        doc = copy.deepcopy(self.doc)
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #   get document for TimeSeries
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.get_document(ts)
        uncached_retdoc = uncached_matcher.get_document(ts)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #   Test get document with time arguments:
        #       test with a time, multi doc found
        #       1: 'starttime': 1262908800.0, 'endtime': 1321549500.0
        #       2. 'starttime': 1400000000.0, 'endtime': 1500000000.0
        doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("627fc20659a116ff99f38356")}
        )
        ts = self.db.read_data(doc, collection="wf_miniseed")
        cached_retdoc = cached_matcher.get_document(ts, time=1272908800.0)
        uncached_retdoc = uncached_matcher.get_document(ts, time=1272908800.0)
        assert cached_retdoc is not None
        assert uncached_retdoc is not None

        #   test with a time, multi doc can't found
        ts_1 = copy.deepcopy(ts)
        ts_2 = copy.deepcopy(ts)
        cached_retdoc = cached_matcher.get_document(ts_1, time=1972908800.0)
        uncached_retdoc = uncached_matcher.get_document(ts_2, time=1972908800.0)
        assert cached_retdoc is None
        assert uncached_retdoc is None
        assert ts_1.dead()
        assert ts_2.dead()
        assert "No match for net_sta_chan_loc =TA_034A_BHE and time=" in str(
            ts_1.elog.get_error_log()
        )
        assert "No match for query = " in str(ts_2.elog.get_error_log())

        #   test without a time, or a startime, take first doc, warning
        del doc["starttime"]
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert cached_retdoc is not None
        assert uncached_retdoc is not None

        #   test with a doc with incorrect id
        doc = copy.deepcopy(self.doc)
        del doc["net"]
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert cached_retdoc is None
        assert uncached_retdoc is None

    def test_mseed_channel_matcher_normalize(self):
        cached_matcher = mseed_channel_matcher(self.db)
        uncached_matcher = mseed_channel_matcher(
            self.db, cache_normalization_data=False
        )

        cache_id_undefine_msg = "No entries are present in channel collection for net_sta_chan_loc = something_random"

        #   Test normalize
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        uncached_retdoc = uncached_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "channel_lat" in cached_retdoc

        #       Test prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = mseed_channel_matcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = mseed_channel_matcher(
            self.db, prepend_collection_name=False, cache_normalization_data=False
        )
        uncached_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "lat" in cached_retdoc and "channel_lat" not in cached_retdoc

        #       Test Kill_on_failure and Verbose
        cached_matcher = mseed_channel_matcher(self.db, kill_on_failure=False)
        ts = copy.deepcopy(self.ts)
        ts["net"] = "something_random"
        cached_retdoc = cached_matcher.get_document(ts)
        assert (
            (cached_retdoc is None)
            and (cache_id_undefine_msg in str(ts.elog.get_error_log()))
            and not ts.dead()
        )

        #       Test Normalize dead
        cached_matcher = mseed_channel_matcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        cached_retdoc = cached_matcher(ts)
        assert Metadata_cmp(self.ts, cached_retdoc)

    def test_mseed_site_matcher_get_document(self):
        cached_matcher = mseed_site_matcher(self.db)
        uncached_matcher = mseed_site_matcher(self.db, cache_normalization_data=False)

        #   get document for dict
        doc = copy.deepcopy(self.doc)
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #   get document for TimeSeries
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.get_document(ts)
        uncached_retdoc = uncached_matcher.get_document(ts)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #   Test get document with time arguments:
        #       test with a time, multi doc found
        #       1: 'starttime': 1262908800.0, 'endtime': 1321574399.0
        #       2. 'starttime': 1400000000.0, 'endtime': 1500000000.0
        doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("627fc20659a116ff99f38356")}
        )
        ts = self.db.read_data(doc, collection="wf_miniseed")
        cached_retdoc = cached_matcher.get_document(ts, time=1272908800.0)
        uncached_retdoc = uncached_matcher.get_document(ts, time=1472908800.0)
        assert cached_retdoc is not None
        assert uncached_retdoc is not None

        #   test with a time, multi doc can't found
        ts_1 = copy.deepcopy(ts)
        ts_2 = copy.deepcopy(ts)
        cached_retdoc = cached_matcher.get_document(ts_1, time=1972908800.0)
        uncached_retdoc = uncached_matcher.get_document(ts_2, time=1972908800.0)
        assert cached_retdoc is None
        assert uncached_retdoc is None
        assert ts_1.dead()
        assert ts_2.dead()
        assert "No match for net_sta_chan_loc =TA_034A and time=" in str(
            ts_1.elog.get_error_log()
        )
        assert "No match for query = " in str(ts_2.elog.get_error_log())

        #   test without a time, or a startime, take first doc, warning
        del doc["starttime"]
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert cached_retdoc is not None
        assert uncached_retdoc is not None

        #   test with a doc with incorrect id
        doc = copy.deepcopy(self.doc)
        del doc["net"]
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert cached_retdoc is None
        assert uncached_retdoc is None

    def test_mseed_site_matcher_normalize(self):
        cached_matcher = mseed_site_matcher(self.db)
        uncached_matcher = mseed_site_matcher(self.db, cache_normalization_data=False)

        cache_id_undefine_msg = "No entries are present in channel collection for net_sta_chan_loc = something_random"

        #   Test normalize
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        uncached_retdoc = uncached_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "site_lat" in cached_retdoc

        #       Test prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = mseed_site_matcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = mseed_site_matcher(
            self.db, prepend_collection_name=False, cache_normalization_data=False
        )
        uncached_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "lat" in cached_retdoc and "site_lat" not in cached_retdoc

        #       Test Kill_on_failure and Verbose
        cached_matcher = mseed_site_matcher(self.db, kill_on_failure=False)
        ts = copy.deepcopy(self.ts)
        ts["net"] = "something_random"
        cached_retdoc = cached_matcher.get_document(ts)
        assert (
            (cached_retdoc is None)
            and (cache_id_undefine_msg in str(ts.elog.get_error_log()))
            and not ts.dead()
        )

        #       Test Normalize dead
        cached_matcher = mseed_site_matcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        cached_retdoc = cached_matcher(ts)
        assert Metadata_cmp(self.ts, cached_retdoc)

    def test_origin_time_source_matcher_get_document(self):
        cached_matcher = origin_time_source_matcher(self.db)
        uncached_matcher = origin_time_source_matcher(
            self.db, cache_normalization_data=False
        )

        orig_doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("62812b08178bf05fe5787d82")}
        )
        orig_ts = self.db.read_data(orig_doc, collection="wf_miniseed")

        #   get document for dict
        doc = copy.deepcopy(orig_doc)
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #   get document for TimeSeries
        ts = copy.deepcopy(orig_ts)
        cached_retdoc = cached_matcher.get_document(ts)
        uncached_retdoc = uncached_matcher.get_document(ts)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)

        #   test with a undefined start time
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.get_document(ts_1)
        uncached_retdoc = uncached_matcher.get_document(ts_2)
        assert cached_retdoc is None
        assert uncached_retdoc is None
        assert ts_1.dead()
        assert ts_2.dead()
        assert "No match for time between" in str(ts_1.elog.get_error_log())
        assert "No match for query = {'time':" in str(ts_2.elog.get_error_log())

        #   test without a time, or a startime, take first doc, warning
        cached_matcher = origin_time_source_matcher(self.db, tolerance=1000000000.0)
        uncached_matcher = origin_time_source_matcher(
            self.db, tolerance=1000000000.0, cache_normalization_data=False
        )
        cached_retdoc = cached_matcher.get_document(doc)
        uncached_retdoc = uncached_matcher.get_document(doc)
        assert cached_retdoc is not None
        assert uncached_retdoc is not None

    def test_origin_time_source_matcher_normalize(self):
        cached_matcher = origin_time_source_matcher(self.db)
        uncached_matcher = origin_time_source_matcher(
            self.db, cache_normalization_data=False
        )

        orig_doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("62812b08178bf05fe5787d82")}
        )
        orig_ts = self.db.read_data(orig_doc, collection="wf_miniseed")

        cache_id_undefine_msg = "No match for time between"

        #   Test normalize
        ts_1 = copy.deepcopy(orig_ts)
        ts_2 = copy.deepcopy(orig_ts)
        cached_retdoc = cached_matcher(ts_1)
        uncached_retdoc = uncached_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "source_time" in cached_retdoc

        #       Test prepend_collection_name
        ts_1 = copy.deepcopy(orig_ts)
        ts_2 = copy.deepcopy(orig_ts)
        matcher = origin_time_source_matcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = origin_time_source_matcher(
            self.db, prepend_collection_name=False, cache_normalization_data=False
        )
        uncached_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc, uncached_retdoc)
        assert "time" in cached_retdoc and "source_time" not in cached_retdoc

        #       Test Kill_on_failure and Verbose
        cached_matcher = origin_time_source_matcher(self.db, kill_on_failure=False)
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.get_document(ts)
        assert (
            (cached_retdoc is None)
            and (cache_id_undefine_msg in str(ts.elog.get_error_log()))
            and not ts.dead()
        )

        #       Test Normalize dead
        cached_matcher = mseed_site_matcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        cached_retdoc = cached_matcher(ts)
        assert Metadata_cmp(self.ts, cached_retdoc)

    def test_css30_arrival_interval_matcher(self):
        pass
