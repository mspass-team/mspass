# TODO based on Aug 15, 2022 group meeting
"""
1.  Add decorator for mspass history to normalize function - DONE
2.  Change api to have find return a None on failure instead of an empty list
3.  Biggest change is to consider how to do a pandas implementation.  That will
best be done in a discussio page on github before commiting the revisions
"""
from mspasspy.db.normalize import (
    normalize,
    EqualityDBMatcher,
    EqualityMatcher,
    ObjectIdMatcher,
    ObjectIdDBMatcher,
    MiniseedMatcher,
    MiniseedDBMatcher,
    OriginTimeDBMatcher,
    OriginTimeMatcher,
    normalize_mseed,
    bulk_normalize,
)

from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
from mspasspy.ccore.seismic import TimeReferenceType

import os
import bson.json_util
import numpy
import copy
from bson.objectid import ObjectId
import pandas as pd
import numpy as np


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
            if type(a[key]) in (float, np.float64) and numpy.isclose(a[key], b[key]):
                continue
            if numpy.isnan(a[key]) & numpy.isnan(b[key]):
                continue
            return False

    for key in b:
        if key not in a:
            return False
        if a[key] != b[key]:
            if type(a[key]) in (float, np.float64) and numpy.isclose(a[key], b[key]):
                continue
            if numpy.isnan(a[key]) & numpy.isnan(b[key]):
                continue
            return False

    return True


class TestNormalize:
    def setup_class(self):
        client = DBClient("localhost")
        client.drop_database("matchertest")
        self.db = Database(
            client, "matchertest", db_schema="mspass.yaml", md_schema="mspass.yaml"
        )
        self.dump_path = "./python/tests/data/db_dump"
        restore_db(self.db, self.dump_path)

    def teardown_class(self):
        client = DBClient("localhost")
        client.drop_database("matchertest")

    def setup_method(self):
        self.doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("627fc20559a116ff99f38243")}
        )
        self.ts = self.db.read_data(self.doc, collection="wf_miniseed")
        self.ts.tref = (
            TimeReferenceType.UTC
        )  #   Change the reference type to avoid issues in tests


class TestDataFrameCacheMatcherUtil(TestNormalize):
    def setup_method(self):
        super().setup_method()
        self.df = pd.DataFrame(list(self.db["channel"].find()))

    def test_custom_null_values(self):
        null_row = {
            "lat": 0,
            "lon": -100.00000000000000000000000001,
            "depth": -99,
            "time": np.nan,
            "magnitude": np.nan,
        }

        self.df = self.df.append(null_row, ignore_index=True)
        null_value_dict = {
            "lat": 0.0,  #   Matching a float using a float
            "lon": -100,  #   Matching a float using an int
            "depth": 0,  #   This rule won't match row
        }

        matched_dict = {
            "lat": np.nan,
            "lon": np.nan,
            "depth": np.float64(
                -99.0
            ),  #   The dataframe would convert the int to float
            "time": np.nan,
            "magnitude": np.nan,
        }
        originTimeMatcher = OriginTimeMatcher(
            self.df, source_time_key="time", custom_null_values=null_value_dict
        )
        assert Metadata_cmp(
            originTimeMatcher.cache.to_dict(orient="records")[-1], matched_dict
        )


class TestObjectIdMatcher(TestNormalize):
    def setup_method(self):
        super().setup_method()
        self.df = pd.DataFrame(list(self.db["channel"].find()))

    def test_ID_matcher_find_one(self):
        cached_matcher = ObjectIdMatcher(self.db)
        db_matcher = ObjectIdDBMatcher(self.db)

        cached_matcher_key_undefined_msg = (
            "cache_id method found no match for this datum"
        )
        db_matcher_key_undefined_msg = "query_generator method failed to generate a valid query - required attributes are probably missing"
        cache_id_undefine_msg = "cache_id method found no match for this datum"
        db_matcher_find_none_msg = "yielded no documents"

        #       Test find_one
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.find_one(ts)
        db_retdoc = db_matcher.find_one(ts)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert cached_retdoc[1] is None
        assert db_retdoc[1] is None

        #       Match key undefined
        ts = copy.deepcopy(self.ts)
        del ts["channel_id"]
        retdoc = cached_matcher.find_one(ts)
        assert (retdoc[0] is None) and (
            cached_matcher_key_undefined_msg in str(retdoc[1].get_error_log())
        )
        ts = copy.deepcopy(self.ts)
        del ts["channel_id"]
        retdoc = db_matcher.find_one(ts)
        assert (retdoc[0] is None) and (
            db_matcher_key_undefined_msg in str(retdoc[1].get_error_log())
        )

        #       Id can't be found
        # This test is a relic of the old qpi.  It tests am error
        # condition of the DatabaseMatcher and CachedMatcher find method
        # For clarity it should probably be done in a different part of
        # this class.  For now this is just a documenation of that fact
        ts = copy.deepcopy(self.ts)
        ts["channel_id"] = bson.objectid.ObjectId()
        cached_retdoc = cached_matcher.find_one(ts)
        assert (cached_retdoc[0] is None) and (
            cache_id_undefine_msg in str(cached_retdoc[1].get_error_log())
        )
        retdoc = db_matcher.find_one(ts)
        assert (retdoc[0] is None) and (
            db_matcher_find_none_msg in str(retdoc[1].get_error_log())
        )

    def test_ID_matcher_find_one_df(self):
        #   TODO: There should be a more elegant way to do this instead of copying the whole test
        cached_matcher = ObjectIdMatcher(self.df)
        db_matcher = ObjectIdDBMatcher(self.db)

        cached_matcher_key_undefined_msg = (
            "cache_id method found no match for this datum"
        )
        db_matcher_key_undefined_msg = "query_generator method failed to generate a valid query - required attributes are probably missing"
        cache_id_undefine_msg = "cache_id method found no match for this datum"
        db_matcher_find_none_msg = "yielded no documents"

        #       Test find_one
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.find_one(ts)
        db_retdoc = db_matcher.find_one(ts)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert cached_retdoc[1] is None
        assert db_retdoc[1] is None

        #       Match key undefined
        ts = copy.deepcopy(self.ts)
        del ts["channel_id"]
        retdoc = cached_matcher.find_one(ts)
        assert (retdoc[0] is None) and (
            cached_matcher_key_undefined_msg in str(retdoc[1].get_error_log())
        )
        ts = copy.deepcopy(self.ts)
        del ts["channel_id"]
        retdoc = db_matcher.find_one(ts)
        assert (retdoc[0] is None) and (
            db_matcher_key_undefined_msg in str(retdoc[1].get_error_log())
        )

        #       Id can't be found
        # This test is a relic of the old qpi.  It tests am error
        # condition of the DatabaseMatcher and CachedMatcher find method
        # For clarity it should probably be done in a different part of
        # this class.  For now this is just a documenation of that fact
        ts = copy.deepcopy(self.ts)
        ts["channel_id"] = bson.objectid.ObjectId()
        cached_retdoc = cached_matcher.find_one(ts)
        assert (cached_retdoc[0] is None) and (
            cache_id_undefine_msg in str(cached_retdoc[1].get_error_log())
        )
        retdoc = db_matcher.find_one(ts)
        assert (retdoc[0] is None) and (
            db_matcher_find_none_msg in str(retdoc[1].get_error_log())
        )

    def test_ID_matcher_normalize(self):
        cached_matcher = ObjectIdMatcher(self.db)
        db_matcher = ObjectIdDBMatcher(self.db)

        #       Tests call method
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        db_retdoc = db_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "channel_lat" in cached_retdoc[0]

        #       Test prepend_collection_name turned off
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = ObjectIdMatcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = ObjectIdDBMatcher(self.db, prepend_collection_name=False)
        db_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "lat" in cached_retdoc[0]

        #       Test handling of dead data
        matcher = ObjectIdMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None
        matcher = ObjectIdDBMatcher(self.db)
        retdoc = matcher(ts)
        assert retdoc[0] is None

    def test_ID_matcher_normalize_df(self):
        cached_matcher = ObjectIdMatcher(self.df)
        db_matcher = ObjectIdDBMatcher(self.db)

        #       Tests call method
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        db_retdoc = db_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "channel_lat" in cached_retdoc[0]

        #       Test prepend_collection_name turned off
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = ObjectIdMatcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = ObjectIdDBMatcher(self.db, prepend_collection_name=False)
        db_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "lat" in cached_retdoc[0]

        #       Test handling of dead data
        matcher = ObjectIdMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None
        matcher = ObjectIdDBMatcher(self.db)
        retdoc = matcher(ts)
        assert retdoc[0] is None


class TestMiniseedMatcher(TestNormalize):
    def setup_method(self):
        super().setup_method()
        self.df = pd.DataFrame(list(self.db["channel"].find()))

    def test_MiniseedMatcher_find_one(self):
        cached_matcher = MiniseedMatcher(self.db)
        db_matcher = MiniseedDBMatcher(self.db)

        #   find_one for TimeSeries
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.find_one(ts)
        db_retdoc = db_matcher.find_one(ts)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])

        #   Test get document with time arguments:
        #       test with a time, multi doc found
        #       1: 'starttime': 1262908800.0, 'endtime': 1321549500.0
        #       2. 'starttime': 1400000000.0, 'endtime': 1500000000.0
        doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("627fc20659a116ff99f38356")}
        )
        ts = self.db.read_data(doc, collection="wf_miniseed")
        ts_1 = copy.deepcopy(ts)
        ts_2 = copy.deepcopy(ts)

        #   test with a doc with incorrect id
        doc = copy.deepcopy(self.doc)
        del doc["net"]
        broken_ts = self.db.read_data(doc, collection="wf_miniseed")
        cached_retdoc = cached_matcher.find_one(broken_ts)
        db_retdoc = db_matcher.find_one(broken_ts)
        assert cached_retdoc[0] is None
        assert db_retdoc[0] is None

        #   test with a time, multi doc can't found
        ts_1 = copy.deepcopy(ts)
        ts_2 = copy.deepcopy(ts)
        ts_1.set_t0(1972908800.0)
        ts_2.set_t0(1972908800.0)
        cached_retdoc = cached_matcher.find_one(ts_1)
        db_retdoc = db_matcher.find_one(ts_2)
        assert cached_retdoc[0] is None
        assert db_retdoc[0] is None
        # this is a minimal test - just existence.   maybe should test
        # contents of elog returned
        # probably should have a test for the content of the elog return here
        assert cached_retdoc[1] is not None
        assert db_retdoc[1] is not None

    def test_MiniseedMatcher_find_one_df(self):
        cached_matcher = MiniseedMatcher(self.df)
        db_matcher = MiniseedDBMatcher(self.db)

        #   find_one for TimeSeries
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.find_one(ts)
        db_retdoc = db_matcher.find_one(ts)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])

        #   Test get document with time arguments:
        #       test with a time, multi doc found
        #       1: 'starttime': 1262908800.0, 'endtime': 1321549500.0
        #       2. 'starttime': 1400000000.0, 'endtime': 1500000000.0
        doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("627fc20659a116ff99f38356")}
        )
        ts = self.db.read_data(doc, collection="wf_miniseed")
        ts_1 = copy.deepcopy(ts)
        ts_2 = copy.deepcopy(ts)

        #   test with a doc with incorrect id
        doc = copy.deepcopy(self.doc)
        del doc["net"]
        broken_ts = self.db.read_data(doc, collection="wf_miniseed")
        cached_retdoc = cached_matcher.find_one(broken_ts)
        db_retdoc = db_matcher.find_one(broken_ts)
        assert cached_retdoc[0] is None
        assert db_retdoc[0] is None

        #   test with a time, multi doc can't found
        ts_1 = copy.deepcopy(ts)
        ts_2 = copy.deepcopy(ts)
        ts_1.set_t0(1972908800.0)
        ts_2.set_t0(1972908800.0)
        cached_retdoc = cached_matcher.find_one(ts_1)
        db_retdoc = db_matcher.find_one(ts_2)
        assert cached_retdoc[0] is None
        assert db_retdoc[0] is None
        # this is a minimal test - just existence.   maybe should test
        # contents of elog returned
        # probably should have a test for the content of the elog return here
        assert cached_retdoc[1] is not None
        assert db_retdoc[1] is not None

    def test_MiniseedMatcher_normalize(self):
        cached_matcher = MiniseedMatcher(self.db)
        db_matcher = MiniseedDBMatcher(self.db)

        #   Test call method
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        db_retdoc = db_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "channel_lat" in cached_retdoc[0]

        #       Test prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = MiniseedMatcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = MiniseedDBMatcher(self.db, prepend_collection_name=False)
        db_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "lat" in cached_retdoc[0] and "channel_lat" not in cached_retdoc[0]

        #       Test handling of dead data
        matcher = MiniseedMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None
        matcher = MiniseedDBMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None

    def test_MiniseedMatcher_normalize_df(self):
        cached_matcher = MiniseedMatcher(self.df)
        db_matcher = MiniseedDBMatcher(self.db)

        #   Test call method
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        db_retdoc = db_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "channel_lat" in cached_retdoc[0]

        #       Test prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = MiniseedMatcher(self.db, prepend_collection_name=False)
        cached_retdoc = matcher(ts_1)
        matcher = MiniseedDBMatcher(self.db, prepend_collection_name=False)
        db_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "lat" in cached_retdoc[0] and "channel_lat" not in cached_retdoc[0]

        #       Test handling of dead data
        matcher = MiniseedMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None
        matcher = MiniseedDBMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None


class TestEqualityMatcher(TestNormalize):
    def setup_method(self):
        super().setup_method()
        self.df = pd.DataFrame(list(self.db["site"].find()))
        self.cached_matcher_multi_match_msg = (
            "You should use find instead of find_one if the match is not unique"
        )
        self.db_matcher_multi_match_msg = "Using first one in list"

    def test_EqualityMatcher_find_one(self):
        cached_matcher = EqualityMatcher(
            self.db,
            "site",
            {"net": "net"},
            ["net", "coords"],
            require_unique_match=False,
        )
        db_matcher = EqualityDBMatcher(
            self.db, "site", {"net": "net"}, ["net", "coords"]
        )

        #   Test find_one
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.find_one(ts)
        db_retdoc = db_matcher.find_one(ts)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert self.cached_matcher_multi_match_msg in str(
            cached_retdoc[1].get_error_log()
        )
        assert self.db_matcher_multi_match_msg in str(db_retdoc[1].get_error_log())

    def test_EqualityMatcher_find_one_df(self):
        cached_matcher = EqualityMatcher(
            self.df,
            "site",
            {"net": "net"},
            ["net", "coords"],
            require_unique_match=False,
        )

        db_matcher = EqualityDBMatcher(
            self.db, "site", {"net": "net"}, ["net", "coords"]
        )

        #   Test find_one
        ts = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher.find_one(ts)
        db_retdoc = db_matcher.find_one(ts)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert self.cached_matcher_multi_match_msg in str(
            cached_retdoc[1].get_error_log()
        )
        assert self.db_matcher_multi_match_msg in str(db_retdoc[1].get_error_log())

    def test_EqualityMatcher_normalize(self):
        cached_matcher = EqualityMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=True,
        )
        db_matcher = EqualityDBMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=True,
        )

        #   Test __call__ method is find_one
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        db_retdoc = db_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "site_coords" in cached_retdoc[0]

        # repeat run through normalize function
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = normalize(ts_1, cached_matcher)
        db_retdoc = normalize(ts_2, db_matcher)
        assert Metadata_cmp(cached_retdoc, db_retdoc)
        assert "site_coords" in cached_retdoc

        #   Test turning off prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = EqualityMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=False,
        )
        cached_retdoc = matcher(ts_1)
        matcher = EqualityDBMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=False,
        )
        db_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "coords" in cached_retdoc[0]

    def test_EqualityMatcher_normalize_df(self):
        cached_matcher = EqualityMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=True,
        )
        db_matcher = EqualityDBMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=True,
        )

        #   Test __call__ method is find_one
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = cached_matcher(ts_1)
        db_retdoc = db_matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "site_coords" in cached_retdoc[0]

        # repeat run through normalize function
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        cached_retdoc = normalize(ts_1, cached_matcher)
        db_retdoc = normalize(ts_2, db_matcher)
        assert Metadata_cmp(cached_retdoc, db_retdoc)
        assert "site_coords" in cached_retdoc

        #   Test turning off prepend_collection_name
        ts_1 = copy.deepcopy(self.ts)
        ts_2 = copy.deepcopy(self.ts)
        matcher = EqualityMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=False,
        )
        cached_retdoc = matcher(ts_1)
        matcher = EqualityDBMatcher(
            self.db,
            "site",
            {"net": "net"},
            attributes_to_load=["net", "coords"],
            require_unique_match=False,
            prepend_collection_name=False,
        )
        db_retdoc = matcher(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])
        assert "coords" in cached_retdoc[0]


class TestOriginTimeMatcher(TestNormalize):
    def setup_method(self):
        super().setup_method()
        self.df = pd.DataFrame(list(self.db["source"].find()))
        self.cached_matcher_multi_match_msg = (
            "You should use find instead of find_one if the match is not unique"
        )
        self.db_matcher_multi_match_msg = "Using first one in list"

    def test_OriginTimeMatcher_find_one(self):
        cached_matcher = OriginTimeMatcher(self.db, source_time_key="time")
        db_matcher = OriginTimeDBMatcher(self.db, source_time_key="time")

        orig_doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("62812b08178bf05fe5787d82")}
        )
        orig_ts = self.db.read_data(orig_doc, collection="wf_miniseed")

        #   get document for TimeSeries
        ts_1 = copy.deepcopy(orig_ts)
        ts_2 = copy.deepcopy(orig_ts)
        cached_retdoc = cached_matcher.find_one(ts_1)
        db_retdoc = db_matcher.find_one(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])

        #   test using time key from Metadata
        cached_matcher = OriginTimeMatcher(
            self.db, data_time_key="testtime", source_time_key="time"
        )
        db_matcher = OriginTimeDBMatcher(
            self.db, data_time_key="testtime", source_time_key="time"
        )

        ts = copy.deepcopy(orig_ts)
        ts["testtime"] = ts.t0
        db_retdoc = db_matcher.find_one(ts)
        assert db_retdoc[0] is not None

        # validate handling of mismatched time (no match)
        db_matcher = OriginTimeDBMatcher(
            self.db, data_time_key="testtime", source_time_key="time"
        )
        ts = copy.deepcopy(orig_ts)
        ts["testtime"] = 9999.99
        db_retdoc = db_matcher.find_one(ts)
        assert db_retdoc[0] is None

    def test_OriginTimeMatcher_normalize(self):
        db_matcher = OriginTimeDBMatcher(self.db)

        orig_doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("62812b08178bf05fe5787d82")}
        )
        orig_ts = self.db.read_data(orig_doc, collection="wf_miniseed")

        #   Test call
        ts_2 = copy.deepcopy(orig_ts)
        db_retdoc = db_matcher(ts_2)
        assert "source_time" in db_retdoc[0]

        #       Test prepend_collection_name turned off

        ts_2 = copy.deepcopy(orig_ts)
        matcher = OriginTimeDBMatcher(self.db, prepend_collection_name=False)
        db_retdoc = matcher(ts_2)
        assert "time" in db_retdoc[0] and "source_time" not in db_retdoc[0]

        #       Test handling of dead data

        matcher = OriginTimeDBMatcher(self.db)
        ts = copy.deepcopy(self.ts)
        ts.kill()
        retdoc = matcher(ts)
        assert retdoc[0] is None

    def test_OriginTimeMatcher_find_one_df(self):
        cached_matcher = OriginTimeMatcher(self.df, source_time_key="time")
        db_matcher = OriginTimeDBMatcher(self.db, source_time_key="time")

        orig_doc = self.db.wf_miniseed.find_one(
            {"_id": ObjectId("62812b08178bf05fe5787d82")}
        )
        orig_ts = self.db.read_data(orig_doc, collection="wf_miniseed")

        #   get document for TimeSeries
        ts_1 = copy.deepcopy(orig_ts)
        ts_2 = copy.deepcopy(orig_ts)
        cached_retdoc = cached_matcher.find_one(ts_1)
        db_retdoc = db_matcher.find_one(ts_2)
        assert Metadata_cmp(cached_retdoc[0], db_retdoc[0])

        #   test using time key from Metadata
        cached_matcher = OriginTimeMatcher(
            self.df, data_time_key="testtime", source_time_key="time"
        )
        db_matcher = OriginTimeDBMatcher(
            self.db, data_time_key="testtime", source_time_key="time"
        )

        ts = copy.deepcopy(orig_ts)
        ts["testtime"] = ts.t0
        db_retdoc = db_matcher.find_one(ts)
        assert db_retdoc[0] is not None

        # validate handling of mismatched time (no match)
        db_matcher = OriginTimeDBMatcher(
            self.db, data_time_key="testtime", source_time_key="time"
        )
        ts = copy.deepcopy(orig_ts)
        ts["testtime"] = 9999.99
        db_retdoc = db_matcher.find_one(ts)
        assert db_retdoc[0] is None


class TestMatcherHelperFunctions(TestNormalize):
    def test_normalize_mseed(self):
        #   First delete all the existing references:
        self.db.wf_miniseed.update_many(
            {}, {"$unset": {"channel_id": None, "site_id": None}}
        )
        ret = normalize_mseed(self.db, normalize_channel=False, normalize_site=True)
        assert ret == [3934, 0, 3934]
        rand_doc = self.db.wf_miniseed.find_one()
        assert "channel_id" not in rand_doc
        assert "site_id" in rand_doc

        self.db.wf_miniseed.update_many(
            {}, {"$unset": {"channel_id": None, "site_id": None}}
        )
        ret = normalize_mseed(self.db, normalize_channel=True, normalize_site=False)
        assert ret == [3934, 3934, 0]
        rand_doc = self.db.wf_miniseed.find_one()
        assert "channel_id" in rand_doc
        assert "site_id" not in rand_doc

        self.db.wf_miniseed.update_many(
            {}, {"$unset": {"channel_id": None, "site_id": None}}
        )
        ret = normalize_mseed(self.db, normalize_channel=True, normalize_site=True)
        assert ret == [3934, 3934, 3934]
        rand_doc = self.db.wf_miniseed.find_one()
        assert "channel_id" in rand_doc
        assert "site_id" in rand_doc

    def test_bulk_normalize(self):
        matcher_function_list = []
        matcher = MiniseedMatcher(
            self.db,
            attributes_to_load=["_id", "net", "sta", "chan", "starttime", "endtime"],
        )
        matcher_function_list.append(matcher)

        sitematcher = MiniseedMatcher(
            self.db,
            collection="site",
            attributes_to_load=["_id", "net", "sta", "starttime", "endtime"],
        )
        matcher_function_list.append(sitematcher)

        ret = bulk_normalize(
            self.db,
            wfquery={},
            wf_col="wf_miniseed",
            matcher_list=matcher_function_list,
        )
        assert ret == [3934, 3934, 3934]
