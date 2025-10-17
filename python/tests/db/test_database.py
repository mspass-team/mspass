import copy
import io
import os
import pickle

import gridfs
import numpy as np
import obspy
import obspy.clients.fdsn.client
import pytest
import sys
import re

import boto3
from moto import mock_aws
import botocore.session
from unittest.mock import patch, Mock
import json
import base64
from unittest import mock


sys.path.append("python/tests")

from helper import (
    get_live_seismogram,
    get_live_timeseries,
)

from mspasspy.util.converter import (
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
    ErrorSeverity,
    Metadata,
    MsPASSError,
    AntelopePf,
)

from mspasspy.db.schema import DatabaseSchema, MetadataSchema
from mspasspy.util import logging_helper
from bson.objectid import ObjectId
from datetime import datetime
from mspasspy.db.database import Database, geoJSON_doc
from mspasspy.db.client import DBClient
from mspasspy.db.collection import Collection


class TestDatabase:
    def setup_class(self):
        client = DBClient("localhost")
        self.db = Database(client, "dbtest")
        self.db2 = Database(client, "dbtest")
        self.metadata_def = MetadataSchema()
        # clean up the database locally
        for col_name in self.db.list_collection_names():
            self.db[col_name].delete_many({})

        self.test_ts = get_live_timeseries()
        # this is may not be necessary but is useful to be sure
        # state is clear
        self.test_ts.clear_modified()
        # this is used to test automatic dropping of any value
        # that is all all spaces
        self.test_ts["test"] = " "
        self.test_ts["extra1"] = "extra1"
        self.test_ts["extra2"] = "extra2"  # exclude
        self.test_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        self.test_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        # this is used to test aliases
        self.test_ts.erase("starttime")
        self.test_ts["t0"] = datetime.utcnow().timestamp()

        self.test_seis = get_live_seismogram()
        self.test_seis["tmatrix"] = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]
        # this is may not be necessary but is useful to be sure
        # state is clear
        self.test_ts.clear_modified()
        # this is used to test automatic dropping of any value
        # that is all all spaces
        self.test_seis["test"] = " "
        self.test_seis["extra1"] = "extra1"
        self.test_seis["extra2"] = "extra2"  # exclude
        self.test_seis.elog.log_error(
            "alg", str("message"), ErrorSeverity.Informational
        )
        self.test_seis.elog.log_error(
            "alg", str("message"), ErrorSeverity.Informational
        )

        self.test_seis.erase("starttime")
        self.test_seis["t0"] = datetime.utcnow().timestamp()

        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        self.db["site"].insert_one(
            {
                "_id": site_id,
                "net": "net1",
                "sta": "sta1",
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

        self.test_seis["site_id"] = site_id
        self.test_seis["source_id"] = source_id
        self.test_ts["site_id"] = site_id
        self.test_ts["source_id"] = source_id
        self.test_ts["channel_id"] = channel_id

        # save inventory to db
        inv = obspy.read_inventory("python/tests/data/TA.035A.xml")
        self.db.save_inventory(inv)

    def test_init(self):
        db_schema = DatabaseSchema("mspass_lite.yaml")
        md_schema = MetadataSchema("mspass_lite.yaml")
        client = DBClient("localhost")
        db = Database(client, "dbtest", db_schema=db_schema, md_schema=md_schema)
        with pytest.raises(AttributeError, match="no attribute"):
            dummy = db.database_schema.site
        with pytest.raises(MsPASSError, match="not defined"):
            dummy = db.database_schema["source"]

    def test_save_elogs(self):
        tmp_ts = get_live_timeseries()
        tmp_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        tmp_ts.elog.log_error(
            "alg2", str("message2"), ErrorSeverity.Informational
        )  # multi elogs fix me
        errs = tmp_ts.elog.get_error_log()
        elog_id = self.db._save_elog(tmp_ts)
        assert isinstance(elog_id, ObjectId)
        elog_doc = self.db["elog"].find_one({"_id": elog_id})
        for err, res in zip(errs, elog_doc["logdata"]):
            assert res["algorithm"] == err.algorithm
            assert res["error_message"] == err.message
            assert "wf_TImeSeries_id" not in res

        # empty logs
        tmp_ts = get_live_timeseries()
        errs = tmp_ts.elog.get_error_log()
        elog_id = self.db._save_elog(tmp_ts)
        assert elog_id is None

        # save with a dead object
        tmp_ts = get_live_timeseries()
        tmp_ts.elog.log_error("alg", str("message"), ErrorSeverity.Informational)
        tmp_ts.live = False
        elog_id = self.db._save_elog(tmp_ts)
        elog_doc = self.db["elog"].find_one({"_id": elog_id})
        assert elog_doc["tombstone"] == dict(tmp_ts)

    def test_save_and_read_data(self):
        # first test read and write in native binary format
        tmp_seis = get_live_seismogram()
        dir = "python/tests/data/"
        dfile = "test_db_output"
        seis_return = self.db._save_sample_data_to_file(tmp_seis, dir, dfile)
        assert seis_return.is_defined("foff")
        assert seis_return.is_defined("dir")
        assert seis_return.is_defined("dfile")
        assert seis_return.is_defined("format")
        assert seis_return.is_defined("storage_mode")
        foff = seis_return["foff"]
        dir = seis_return["dir"]
        dfile = seis_return["dfile"]
        assert seis_return["format"] == "binary"
        assert seis_return["storage_mode"] == "file"
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        self.db._read_data_from_dfile(tmp_seis_2, dir, dfile, foff)
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

        tmp_ts = get_live_timeseries()
        ts_return = self.db._save_sample_data_to_file(tmp_ts, dir, dfile)
        assert ts_return.is_defined("foff")
        assert ts_return.is_defined("dir")
        assert ts_return.is_defined("dfile")
        assert ts_return.is_defined("format")
        assert ts_return.is_defined("storage_mode")
        foff = ts_return["foff"]
        dir = ts_return["dir"]
        dfile = ts_return["dfile"]
        assert ts_return["format"] == "binary"
        assert ts_return["storage_mode"] == "file"
        tmp_ts_2 = TimeSeries()
        tmp_ts_2.npts = 255
        self.db._read_data_from_dfile(tmp_ts_2, dir, dfile, foff)
        assert all(a == b for a, b in zip(tmp_ts.data, tmp_ts_2.data))

        # verify error handler thrown when file is not found
        with pytest.raises(MsPASSError, match="_fread_from_file failed"):
            self.db._read_data_from_dfile(tmp_ts_2, dir, dfile + "dummy", foff)

        # test read and write  miniseed format
        tmp_seis = get_live_seismogram()
        dir = "python/tests/data/"
        dfile = "test_mseed_output"
        seis_return = self.db._save_sample_data_to_file(
            tmp_seis, dir, dfile, format="mseed"
        )
        assert seis_return.is_defined("foff")
        assert seis_return.is_defined("dir")
        assert seis_return.is_defined("dfile")
        assert seis_return.is_defined("nbytes")
        assert seis_return.is_defined("format")
        assert seis_return.is_defined("storage_mode")
        foff = seis_return["foff"]
        dir = seis_return["dir"]
        dfile = seis_return["dfile"]
        nbytes = seis_return["nbytes"]
        assert seis_return["format"] == "mseed"
        assert seis_return["storage_mode"] == "file"

        tmp_seis_2 = Seismogram()
        # this method is a bit weird in acting like a subroutine
        # result is returned in tmp_seis_2
        self.db._read_data_from_dfile(
            tmp_seis_2, dir, dfile, foff, nbytes, format="mseed"
        )
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

        tmp_ts = get_live_timeseries()
        ts_return = self.db._save_sample_data_to_file(
            tmp_ts, dir, dfile, format="mseed"
        )
        assert ts_return.is_defined("foff")
        assert ts_return.is_defined("dir")
        assert ts_return.is_defined("dfile")
        assert ts_return.is_defined("nbytes")
        assert ts_return.is_defined("format")
        assert ts_return.is_defined("storage_mode")
        foff = ts_return["foff"]
        dir = ts_return["dir"]
        dfile = ts_return["dfile"]
        nbytes = ts_return["nbytes"]
        assert ts_return["format"] == "mseed"
        assert ts_return["storage_mode"] == "file"

        tmp_ts_2 = TimeSeries()
        self.db._read_data_from_dfile(
            tmp_ts_2, dir, dfile, foff, nbytes, format="mseed"
        )
        assert all(a == b for a, b in zip(tmp_ts.data, tmp_ts_2.data))

    def test_save_and_read_gridfs(self):
        tmp_seis = get_live_seismogram()
        seis_return = self.db._save_sample_data_to_gridfs(tmp_seis)
        assert seis_return.is_defined("storage_mode")
        assert seis_return.is_defined("gridfs_id")
        assert seis_return["storage_mode"] == "gridfs"
        gridfs_id = seis_return["gridfs_id"]
        tmp_seis_2 = Seismogram()
        tmp_seis_2.npts = 255
        self.db._read_data_from_gridfs(tmp_seis_2, gridfs_id)
        assert all(a.any() == b.any() for a, b in zip(tmp_seis.data, tmp_seis_2.data))

        # test handling of problems with npts - two error conditions
        # note the next two tests in older version threw exceptions.
        tmp_seis_2.erase("npts")
        self.db._read_data_from_gridfs(tmp_seis_2, gridfs_id)
        assert tmp_seis_2.dead()
        assert tmp_seis_2.elog.size() == 1

        tmp_seis_2.npts = 256
        tmp_seis_2.set_live()
        self.db._read_data_from_gridfs(tmp_seis_2, gridfs_id)
        assert tmp_seis_2.dead()
        # because we recycle tmp_seis the above adds another elog message
        assert tmp_seis_2.elog.size() == 2

        tmp_ts = get_live_timeseries()
        ts_return = self.db._save_sample_data_to_gridfs(tmp_ts)
        assert ts_return.is_defined("storage_mode")
        assert ts_return.is_defined("gridfs_id")
        assert ts_return["storage_mode"] == "gridfs"
        gridfs_id = ts_return["gridfs_id"]
        tmp_ts_2 = TimeSeries()
        tmp_ts_2.npts = 255
        self.db._read_data_from_gridfs(tmp_ts_2, gridfs_id)
        assert np.isclose(tmp_ts.data, tmp_ts_2.data).all()

        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(gridfs_id)
        # test overwrite mode
        self.db._save_sample_data_to_gridfs(tmp_ts, True)
        assert not gfsh.exists(gridfs_id)

    def mock_urlopen(*args):
        response = Mock()
        with open("python/tests/data/read_data_from_url.pickle", "rb") as handle:
            response.read.side_effect = [pickle.load(handle)]
        return response

    def test_read_data_from_url(self):
        with patch("urllib.request.urlopen", new=self.mock_urlopen):
            url = "http://service.iris.edu/fdsnws/dataselect/1/query?net=IU&sta=ANMO&loc=00&cha=BH?&start=2010-02-27T06:30:00.000&end=2010-02-27T06:35:00.000"
            tmp_ts = TimeSeries()
            self.db._read_data_from_url(tmp_ts, url)
            tmp_seis = Seismogram()
            self.db._read_data_from_url(tmp_seis, url)
            assert all(a == b for a, b in zip(tmp_ts.data, tmp_seis.data[0][:]))

        # test invalid url
        bad_url = "http://service.iris.edu/fdsnws/dataselect/1/query?net=IU&sta=ANMO&loc=00&cha=DUMMY&start=2010-02-27T06:30:00.000&end=2010-02-27T06:35:00.000"
        with pytest.raises(MsPASSError, match="Error while downloading"):
            self.db._read_data_from_url(tmp_ts, bad_url)

    def test_mspass_type_helper(self):
        schema = self.metadata_def.Seismogram
        assert type([1.0, 1.2]) == schema.type("tmatrix")
        assert type(1) == schema.type("npts")
        assert type(1.1) == schema.type("delta")

    def test_save_load_history(self):
        ts = get_live_timeseries()
        ts["_id"] = "test_id"
        logging_helper.info(ts, "1", "dummy_func")
        logging_helper.info(ts, "2", "dummy_func_2")
        ts0 = TimeSeries(ts)
        assert ts.number_of_stages() == 2
        history_object_id = self.db._save_history(ts)
        res = self.db["history_object"].find_one({"_id": history_object_id})
        assert res
        assert "save_uuid" in res
        assert res["save_stage"] == 2
        # when  ot given an alg_name and alg_id _save_history extracts it
        # from the end of the history chain.  This tests that is done
        assert res["alg_name"] == "dummy_func_2"
        assert res["alg_id"] == "2"
        assert "processing_history" in res

        #  We tested _test_history above in isolation. Now
        #  use save_data and verify we get the same history
        # chain saved but with a wf id cross reference in the
        # saved document
        ts = TimeSeries(ts0)
        ts_saved = self.db.save_data(
            ts, save_history=True, alg_id="testid", return_data=True
        )
        assert ts_saved.live
        assert "history_object_id" in ts_saved
        hid = ts_saved["history_object_id"]
        doc = self.db.history_object.find_one({"_id": hid})
        assert doc
        assert "save_uuid" in doc
        # save_data adds it's stamp to history with tag "save_data"
        # so this differs from direct call to _save_history
        assert doc["save_stage"] == 3
        assert doc["alg_name"] == "save_data"
        assert doc["alg_id"] == "testid"
        assert "processing_history" in res
        # only difference from direct _save_history call are these
        assert "wf_TimeSeries_id" in doc
        assert ts_saved.is_origin()

        # Now simulate taking the output of save and passing it to
        # a differnt algorithm and saving it.
        # save_data clears the history chain after saving and sets
        # ts_saved to define the result as an "origin" (assert immediatley above)
        # after simulting the algorithm we save and verify that result
        logging_helper.info(ts_saved, "3", "dummy_func_3")
        nodes = ts_saved.get_nodes()
        assert ts_saved.number_of_stages() == 1
        # changing alg_id in this context would not be normal. Done
        # here to assure test below is unique for the saved document
        ts_saved = self.db.save_data(
            ts_saved, save_history=True, alg_id="testid2", return_data=True
        )
        assert ts_saved.live
        assert "history_object_id" in ts_saved
        hid = ts_saved["history_object_id"]
        doc = self.db.history_object.find_one({"_id": hid})
        assert doc
        assert "save_uuid" in doc
        # save_data adds it's stamp to history with tag "save_data"
        # so this differs from direct call to _save_history
        assert doc["save_stage"] == 2
        assert doc["alg_name"] == "save_data"
        # see note above
        assert doc["alg_id"] == "testid2"
        assert "processing_history" in res
        assert "wf_TimeSeries_id" in doc
        assert ts_saved.is_origin()

        # Next test the private _load_history method by itself
        # We retrieve the document last saved into a
        # TimeSeries container.   Content tests match those
        # immediately above, but we use the ProcessingHistory
        # api instead of the document retrieved with find_one
        # ts is copied as a convenience to get a live datum.
        # note use of ts_saved is a dependence from above that is
        # critical for this test
        ts_2 = TimeSeries(ts)
        history_object_id = ts_saved["history_object_id"]
        self.db._load_history(
            ts_2,
            history_object_id,
            alg_name="test_load_history",
            alg_id="tlh_0",
        )
        assert ts_2.number_of_stages() == 3
        assert ts_2.current_nodedata().algorithm == "test_load_history"
        assert ts_2.current_nodedata().algid == "tlh_0"

        # repeat with null id field
        # In that case result should be set as origin
        # and there should be no elog entry as that state is
        # treated as a signal to define this as an origin
        ts_3 = TimeSeries(ts)
        self.db._load_history(
            ts_3,
            None,
            alg_name="test_alg_name",
            alg_id="test_alg_id",
        )
        assert ts_3.is_origin()
        nd = ts_3.current_nodedata()
        assert nd.algorithm == "test_alg_name"
        assert nd.algid == "test_alg_id"
        assert ts_3.elog.size() == 0

        # Repeat above with an invalid ObjectId.  Result
        # shouldd be similar to that with None but there
        # should be an elog message we verify here
        bad_hid = ObjectId()
        ts_3 = TimeSeries(ts)
        self.db._load_history(
            ts_3,
            bad_hid,
            alg_name="test_alg_name",
            alg_id="test_alg_id",
        )
        assert ts_3.is_origin()
        nd = ts_3.current_nodedata()
        assert nd.algorithm == "test_alg_name"
        assert nd.algid == "test_alg_id"
        assert ts_3.elog.size() == 1

        # test with read_data using and defining load_history True
        ts_4 = self.db.read_data(
            ts_3["_id"], load_history=True, alg_id="testreader_fakeid"
        )
        assert ts_4.live
        assert ts_4.is_origin()
        assert ts_4.number_of_stages() == 3
        nd = ts_4.current_nodedata()
        assert nd.algorithm == "read_data"
        assert nd.algid == "testreader_fakeid"

    def test_update_metadata(self):
        ts = TimeSeries(self.test_ts)
        exclude = ["extra2"]
        # insert document into wf_TimeSeries - use this id for later updates
        wfid = (
            self.db["wf_TimeSeries"]
            .insert_one(
                {
                    "npts": 1,
                    "delta": 0.1,
                    "sampling_rate": 20.0,
                    "starttime_shift": 1.0,
                    "calib": 0.1,
                    "format": "SAC",
                }
            )
            .inserted_id
        )
        # test objects that are not TimeSeries or Seismogram
        with pytest.raises(
            TypeError,
            match="Database.update_metadata:  only TimeSeries and Seismogram are supported\nReceived data of type="
            + str(type(123)),
        ):
            res_ts = self.db.update_metadata(123)

        # test dead object
        ts.kill()
        res_ts = self.db.update_metadata(ts)
        assert not res_ts

        # could use set_live but safer to make a fresh copy to avoid side effects of kill
        ts = TimeSeries(self.test_ts)
        # test mode that not in promiscuous, cautious and pedantic
        with pytest.raises(
            MsPASSError,
            match="Database.update_metadata: only promiscuous, cautious and pedantic are supported, but 123 was requested.",
        ):
            res_ts = self.db.update_metadata(ts, mode="123")

        # test _id not in mspass_object
        with pytest.raises(
            MsPASSError,
            match=re.escape(
                "Database.update_metadata: input data object is missing required waveform object id value (_id) - update is not possible without it"
            ),
        ):
            res_ts = self.db.update_metadata(ts)

        ts["_id"] = wfid
        # test promiscuous
        ts["extra1"] = "extra1+"
        # compound keys implying normalization should always be deleted
        # even in promiscuous mode
        ts["site_net"] = "Asia"
        ts["npts"] = 255
        logging_helper.info(ts, "2", "update_metadata")
        res_ts = self.db.update_metadata(
            ts, mode="promiscuous", exclude_keys=exclude, force_keys=["extra3"]
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # test that normalized attribute defined key was deleted
        assert "site_net" not in doc
        # same - update_metadata should not add an error
        # weirdness in this test is that the parent (self.test_ts) has two informational
        # elog entries so the size of elog to begin is 2.    Hence we test for 2 not 0
        assert ts.elog.size() == 2
        # test updates are as expected
        assert doc["extra1"] == "extra1+"
        assert "source_id" in doc
        assert "site_id" in doc
        assert "channel_id" in doc
        assert doc["npts"] == 255
        # test exclude keys
        assert "extra2" not in doc
        # test clear alias
        assert "t0" not in doc
        assert "starttime" in doc
        # test empty keys
        assert "test" not in doc
        # test sync_metadata_before_update
        assert "utc_convertible" in doc
        assert "time_standard" in doc
        # test force_keys(but extra3 is not in metadata)
        assert "extra3" not in doc

        # test cautious(required key) -> fail
        ts = TimeSeries(self.test_ts)
        old_npts = ts["npts"]
        ts.put_string("npts", "xyz")
        logging_helper.info(ts, "2", "update_metadata")
        # adding id needed since we are testing update
        ts["_id"] = wfid
        res_ts = self.db.update_metadata(
            ts, exclude_keys=["extra1", "extra2", "utc_convertible"]
        )

        assert res_ts.dead()
        # attain parent had elog size of 2.  The above should add an entry so
        # expect 3
        assert len(res_ts.elog.get_error_log()) == 3

        # test cautious(required key) -> success
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        ts.put_string("npts", "100")
        res_ts = self.db.update_metadata(
            ts, mode="cautious", exclude_keys=["extra1", "extra2", "utc_convertible"]
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # attr value remains the same
        npts = doc["npts"]
        assert isinstance(npts, int)
        assert doc["npts"] == 100
        # leaves datum live but should add one elog entry
        assert res_ts.elog.size() == 3

        # test cautious(normal key) -> fail
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        old_sampling_rate = ts["sampling_rate"]
        ts.put_string("sampling_rate", "xyz")
        res_ts = self.db.update_metadata(
            ts, mode="cautious", exclude_keys=["extra1", "extra2", "utc_convertible"]
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # attr value remains the same
        assert doc["sampling_rate"] == old_sampling_rate
        # add one elog entry
        assert res_ts.elog.size() == 3

        # test cautious(normal key) -> success
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        ts.put_string("sampling_rate", "1.0")
        res_ts = self.db.update_metadata(
            ts, mode="cautious", exclude_keys=["extra1", "extra2", "utc_convertible"]
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # attr value remains the same
        assert doc["sampling_rate"] == 1.0
        # adds one elog entry
        assert res_ts.elog.size() == 3

        # test cautious(schema undefined key)
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        ts["extra3"] = "123"
        res_ts = self.db.update_metadata(
            ts, mode="cautious", exclude_keys=["extra1", "extra2", "utc_convertible"]
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # Verify new attribute was properly added
        assert doc["extra3"] == "123"
        # adds 1 more log error to the elog
        assert res_ts.elog.size() == 3

        # test pedantic(required key) -> fail
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        # earlier edits of a common record alter npts so we have to
        # fetch that value not the one from ts
        doc = self.db["wf_TimeSeries"].find_one({"_id": wfid})
        old_npts = doc["npts"]
        ts.put_string("npts", "xyz")
        res_ts = self.db.update_metadata(
            ts,
            mode="pedantic",
            exclude_keys=["extra1", "extra2", "utc_convertible"],
        )
        assert res_ts.dead()
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # attr value remains the same
        assert doc["npts"] == old_npts
        # That update should add two elog entries
        assert res_ts.elog.size() == 4

        # test pedantic(required key) -> does auto repair and returns live
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        ts.put_string("npts", "123")
        res_ts = self.db.update_metadata(
            ts,
            mode="pedantic",
            exclude_keys=["extra1", "extra2", "utc_convertible"],
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # attr value remains the same
        assert doc["npts"] == 123
        # This update problem adds 2 elog entries
        assert res_ts.elog.size() == 4

        # test pedantic(normal key) that fails and causes a kill
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        # earlier updates may change sampling rate stored for the
        # document we've been altering so we pull it from there
        # not ts
        doc = self.db.wf_TimeSeries.find_one({"_id": wfid})
        old_sampling_rate = doc["sampling_rate"]
        ts.put_string("sampling_rate", "xyz")
        res_ts = self.db.update_metadata(
            ts,
            mode="pedantic",
            exclude_keys=["extra1", "extra2", "utc_convertible"],
        )
        assert res_ts.dead()
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # verify value did not change in database document even if
        # the update failed
        assert doc["sampling_rate"] == old_sampling_rate
        # add two more log error to the elog
        assert res_ts.elog.size() == 4

        # test pedantic(normal key) where the auto repairs work
        # will leave datum returned live but adds elog entries
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        ts.live = True
        ts.put_string("sampling_rate", "5.0")
        res_ts = self.db.update_metadata(
            ts,
            mode="pedantic",
            exclude_keys=["extra1", "extra2", "utc_convertible"],
        )
        # this test probably should be testing if ts is dead
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # attr value remains the same
        assert doc["sampling_rate"] == 5.0
        # add two more log error to the elog
        assert res_ts.elog.size() == 4

        # test pedantic mode with a key not defined in the schema
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = wfid
        ts["extra4"] = "123"
        res_ts = self.db.update_metadata(
            ts,
            mode="pedantic",
            exclude_keys=["extra1", "extra2", "utc_convertible"],
        )
        assert res_ts.live
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        # in pedantic mode update will be refused so the test
        # attribute should not be present
        assert "extra4" not in doc
        # This error adds only one elog entry
        assert res_ts.elog.size() == 3

        # test trying to do an update with an id not in the db
        # should add a new document with the new objectid
        ts = TimeSeries(self.test_ts)
        # adding id needed since we are testing update
        ts["_id"] = ObjectId()
        res_ts = self.db.update_metadata(ts)
        # should insert a document into wf collection
        doc = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res_ts.live
        assert doc is not None

        # test tmatrix attribute when update seismogram
        test_seis0 = get_live_seismogram()
        # work with a copy to avoid side effects
        test_seis = Seismogram(test_seis0)
        # insert template for updates into the database
        wfid = (
            self.db["wf_Seismogram"]
            .insert_one(
                {
                    "npts": 1,
                    "delta": 0.1,
                    "sampling_rate": 20.0,
                    "starttime_shift": 1.0,
                    "calib": 0.1,
                    "format": "SAC",
                }
            )
            .inserted_id
        )
        test_seis["_id"] = wfid
        res_seis = self.db.update_metadata(test_seis, mode="promiscuous")
        res = self.db["wf_Seismogram"].find_one({"_id": test_seis["_id"]})
        assert res
        assert res_seis.live
        assert res["site_id"] == test_seis["site_id"]
        assert "cardinal" in res and res["cardinal"]
        assert "orthogonal" in res and res["orthogonal"]
        assert res["tmatrix"] == [1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0]
        # change tmatrix
        # refresh with a copy
        test_seis = Seismogram(test_seis0)
        test_seis["_id"] = wfid
        test_seis.tmatrix = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        res_seis = self.db.update_metadata(test_seis, mode="promiscuous")
        res = self.db["wf_Seismogram"].find_one({"_id": test_seis["_id"]})
        assert res_seis.live
        assert res
        assert res["tmatrix"] == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]

    def test_update_data(self):
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        # insert ts into the database
        res_ts = self.db.save_data(
            ts, mode="cautious", storage_mode="gridfs", return_data=True
        )
        assert ts.live
        # This test was reversed in api change mid 2023.  Now always set storage_mode so reverse test
        # assert not "storage_mode" in ts
        assert "storage_mode" in ts
        # change read only attribute to create a elog entry
        ts["net"] = "test_net"
        # add one more history entry into the chain
        logging_helper.info(ts, "2", "Database.update_data")
        # reserve old values
        old_gridfs_id = ts["gridfs_id"]
        old_history_object_id = ts["history_object_id"]
        old_elog_id = ts["elog_id"]
        old_elog_size = len(ts.elog.get_error_log())

        # default behavior
        res_ts = self.db.update_data(ts, mode="promiscuous")
        assert ts.live
        assert "storage_mode" in ts and ts["storage_mode"] == "gridfs"
        assert not ts["gridfs_id"] == old_gridfs_id
        assert not ts["history_object_id"] == old_history_object_id
        assert not ts["elog_id"] == old_elog_id
        # Changes for V2 modified the output of this test due to
        # an implementation detail.  V1 had the following:
        # should add 3 more elog entries(one in update_metadata, two in update_data)
        # assert len(ts.elog.get_error_log()) == old_elog_size + 3
        # Revision for V2 update_data does not add an elog entry so this
        # assertion was dropped.  Retained commented out in the event
        # this gets changed later.
        # assert en(ts.elog.get_error_log()) == old_elog_size + 2
        # check history_object collection and elog_id collection
        wf_res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        elog_res = self.db["elog"].find_one({"_id": ts["elog_id"]})
        history_object_res = self.db["history_object"].find_one(
            {"_id": ts["history_object_id"]}
        )
        assert ts["gridfs_id"] == wf_res["gridfs_id"]
        assert ts["history_object_id"] == wf_res["history_object_id"]
        assert ts["elog_id"] == wf_res["elog_id"]
        assert elog_res and elog_res["wf_TimeSeries_id"] == ts["_id"]
        assert history_object_res
        assert history_object_res["alg_id"] == "0"
        assert history_object_res["alg_name"] == "Database.update_data"

        # incorrect storage mode
        ts.erase("net")
        ts["storage_mode"] = "file"
        logging_helper.info(ts, "2", "Database.update_data")
        old_elog_size = len(ts.elog.get_error_log())
        res_ts = self.db.update_data(ts, mode="promiscuous")
        assert ts["storage_mode"] == "gridfs"
        assert len(ts.elog.get_error_log()) == old_elog_size + 1

        # test dead object
        old_elog_size = len(ts.elog.get_error_log())
        logging_helper.info(ts, "2", "Database.update_data")
        ts.live = False
        res_ts = self.db.update_data(ts, mode="promiscuous")
        assert len(ts.elog.get_error_log()) == old_elog_size
        assert not ts.live

    def test_save_read_data_timeseries(self):
        """
        This method tests various permutations of read and write
        of TimeSeries data objects.  It probably should be broken into
        about a dozen different functions but for now think of it
        as a series of tests of read_data and write_data and
        associated baggage.   Instead we have comments that enumerate
        blocks testing specific features.  It has a companion
        below called "tes_save_read_data_seismogram" that is
        nearly identical but for Seismogram objects.
        The main differences is noremalization with channel instead of
        site.
        """
        #  Test 0 - nothing to read with a random object id
        # returns a default constructed dead datum with an elog entry
        fail_ts = self.db.read_data(
            ObjectId(), mode="cautious", normalize=["channel", "source"]
        )
        assert fail_ts.dead()
        assert fail_ts.elog.size() > 0

        # first create valid simulation data for tests
        promiscuous_ts0 = copy.deepcopy(self.test_ts)
        cautious_ts0 = copy.deepcopy(self.test_ts)
        pedantic_ts0 = copy.deepcopy(self.test_ts)
        logging_helper.info(promiscuous_ts0, "1", "deepcopy")
        logging_helper.info(cautious_ts0, "1", "deepcopy")
        logging_helper.info(pedantic_ts0, "1", "deepcopy")
        # We use the C++ copy constructor here to build working
        # copies.  It doesn't alter history so they are pure clones
        # of 0 version with stage 1 of history "deepcopy"
        promiscuous_ts = TimeSeries(promiscuous_ts0)
        cautious_ts = TimeSeries(cautious_ts0)
        pedantic_ts = TimeSeries(pedantic_ts0)

        # Test 1
        # initial test of a basic save in promiscuous mode and all
        # else defaulted - promiscuous is actually the default but set
        # here for emphasis.  Well, actually return_data is needed
        # to validate but default is false
        res_ts = self.db.save_data(
            promiscuous_ts,
            mode="promiscuous",
            return_data=True,
        )
        assert res_ts.live
        # read it back in and compare the metadata entries.
        # they will not match completely because t0 is an alias
        # that is converted in all modes to starttime.  The original
        # promiscuous_ts saved has a "test" key that is empty and
        # is also automatically dropped in all modes - all spaces
        # is dogmatically treated as null
        res_ts_read = self.db.read_data(res_ts["_id"], collection="wf_TimeSeries")
        skipkeys = ["t0", "test", "is_abortion"]
        for k in res_ts:
            if k not in skipkeys:
                assert res_ts_read[k] == res_ts[k]
        # this verifies the alias
        assert res_ts_read["starttime"] == res_ts["t0"]
        # verify "test" was dropped
        assert "test" not in res_ts_read
        # the writer should add this
        assert "is_abortion" in res_ts_read
        assert not res_ts_read["is_abortion"]
        # We uses this to retrieve this record for later test
        basic_save_ts_id = res_ts["_id"]

        #
        # Test 2
        # same as above but add exclude_keys to verify that works
        # use a fresh copy of initial datum
        promiscuous_ts = TimeSeries(promiscuous_ts0)
        res_ts = self.db.save_data(
            promiscuous_ts,
            mode="promiscuous",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert res_ts.live
        # check extra2 was excluded
        res_ts_read = self.db.read_data(res_ts["_id"], collection="wf_TimeSeries")
        skipkeys = ["t0", "test", "is_abortion"]
        for k in res_ts:
            if k not in skipkeys:
                assert res_ts_read[k] == res_ts[k]
        # this verifies the alias
        assert res_ts_read["starttime"] == res_ts["t0"]
        # verify "test" was dropped
        assert "test" not in res_ts_read
        # the writer should add this
        assert "is_abortion" in res_ts_read
        assert not res_ts_read["is_abortion"]
        # We uses this to retrieve this record for later test
        basic_save_ts_id = res_ts["_id"]

        # test 3
        # here we validate the history data was saved correctly
        # this actualy somewhat duplicates tests in test_save_load_history
        # but differs a bit because it uses TimeSeries so was retained  (Aug. 2023)
        assert res_ts.live
        assert promiscuous_ts.live
        assert not res_ts.is_defined("extra2")
        # check it is the origin in the processing history after save
        wf_doc = self.db["wf_TimeSeries"].find_one({"_id": promiscuous_ts["_id"]})

        history_object_doc = self.db["history_object"].find_one(
            {"_id": wf_doc["history_object_id"]}
        )
        assert wf_doc
        assert history_object_doc
        assert history_object_doc["wf_TimeSeries_id"] == promiscuous_ts["_id"]
        assert history_object_doc["alg_name"] == "save_data"
        assert history_object_doc["alg_id"] == "0"
        assert "processing_history" in history_object_doc
        assert history_object_doc["save_stage"] == 2
        assert "save_uuid" in history_object_doc

        # test 4
        # Verify setting a required attribute to an invalid value
        # leads to a failed save and dead datum return
        cautious_ts = TimeSeries(cautious_ts0)
        cautious_ts.put_string("npts", "xyz")
        res_ts = self.db.save_data(
            cautious_ts, mode="cautious", storage_mode="gridfs", return_data=True
        )
        assert res_ts.dead()
        # save kills both original and return because data is passed by reference
        # following need to be aware cautious_ts is dead
        assert cautious_ts.dead()

        # test 5
        # for invalid required args cautious and pedantic act the same
        # here we use sampling_rate wrong instead of npts
        # i.e. this is a comparable test to previous
        pedantic_ts = TimeSeries(pedantic_ts0)
        pedantic_ts.put_string("sampling_rate", "xyz")
        res_ts = self.db.save_data(
            pedantic_ts, mode="pedantic", storage_mode="gridfs", return_data=True
        )
        assert res_ts.dead()
        assert pedantic_ts.dead()
        # the cemetery should contain two documents from each of the
        # above saves at this point
        n_dead = self.db.cemetery.count_documents({})
        assert n_dead == 2

        # test 6
        # Valitdate normalziation works correctly
        # the following uses the id of the basic save done near the
        # top of this test function

        # in this mode the retrieved
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        promiscuous_ts2 = self.db.read_data(
            basic_save_ts_id,
            mode="promiscuous",
            normalize=["channel", "source"],
            collection="wf_TimeSeries",
        )
        assert "_id" in promiscuous_ts2
        # 2 should have the same metadata as original plus _id
        # hence loop is driven by datum prior to being saved
        # 2 should also contain the normalizatoin data which we test after this loop
        skipkeys = ["t0", "test", "is_abortion"]
        for k in promiscuous_ts:
            if k not in skipkeys:
                assert promiscuous_ts[k] == promiscuous_ts2[k]
        assert not promiscuous_ts2["is_abortion"]
        # normalize should set these
        channellist = [
            "channel_lat",
            "channel_lon",
            "channel_elev",
            "channel_starttime",
            "channel_endtime",
        ]
        sourcelist = ["source_lat", "source_lon", "source_depth", "source_time"]
        for k in channellist:
            assert k in promiscuous_ts2
        for k in sourcelist:
            assert k in promiscuous_ts2

        # test 7
        # test exclude_keys feature of read_data - above tested save_data
        # so they are easily confused
        exclude_list = ["_id", "endtime", "channel_id", "extra1"]
        exclude_promiscuous_ts2 = self.db.read_data(
            basic_save_ts_id,
            mode="promiscuous",
            normalize=["channel", "source"],
            exclude_keys=exclude_list,
        )
        for k in exclude_list:
            assert k not in exclude_promiscuous_ts2

        # test 8
        # This test saves a datum with an invalid value in promiscuous
        # mode.   It is then read back cautious and we validate
        # the expected errors are present in elog
        # Note we restore cautious_ts to its original state and
        # add the error as the above changes it.  That makes this
        # test more stable and less confusing than the previous version
        cautious_ts = TimeSeries(cautious_ts0)
        cautious_ts["npts"] = "foobar"
        res_ts = self.db.save_data(
            cautious_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        # promicuous save ignores the error and leaves these live
        assert res_ts.live
        assert cautious_ts.live
        # this will fail with an invalid type error logged because
        # previous save has invalid npts which makes it impossible to
        # reconstruct the datum
        cautious_ts2 = self.db.read_data(
            res_ts["_id"],
            mode="cautious",
            normalize=["channel", "source"],
            collection="wf_TimeSeries",
        )
        # This next test could change if the schema changes.
        # currently it issues a complaint for keys history_id, elog_id, and extra1
        # The second message is the npts botch error set above
        cautious_ts2_elog = cautious_ts2.elog.get_error_log()
        assert len(cautious_ts2.elog) == 2
        assert cautious_ts2_elog[0].badness == ErrorSeverity.Complaint
        assert cautious_ts2_elog[1].badness == ErrorSeverity.Invalid

        assert "data" not in cautious_ts2
        assert cautious_ts2["is_abortion"]

        # test 9
        #  Now do the same thing but set npts to a string that can
        # be converted to an int - note 255 is magic from the seis generator
        cautious_ts = TimeSeries(cautious_ts0)
        cautious_ts.put_string("npts", "255")
        res_ts = self.db.save_data(
            cautious_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            return_data=True,
            collection="wf_TimeSeries",
        )
        assert res_ts.live
        assert cautious_ts.live
        cautious_ts2 = self.db.read_data(
            res_ts["_id"],
            mode="cautious",
        )
        assert cautious_ts2.live
        assert cautious_ts2["npts"] == 255
        assert not cautious_ts2["is_abortion"]
        # in pedantic mode the same read should cause the datum
        # to be killed
        pedantic_ts2 = self.db.read_data(
            res_ts["_id"], mode="pedantic", collection="wf_TimeSeries"
        )
        assert pedantic_ts2.dead()
        assert pedantic_ts2["is_abortion"]

        # test 10
        # test save with non exist id under cautious mode
        cautious_ts = TimeSeries(cautious_ts0)
        non_exist_id = ObjectId()
        cautious_ts["_id"] = non_exist_id
        logging_helper.info(cautious_ts, "3", "save_data")
        res_ts = self.db.save_data(cautious_ts, mode="cautious", return_data=True)
        assert res_ts.live
        assert cautious_ts.live
        assert "_id" in cautious_ts
        assert not cautious_ts["_id"] == non_exist_id

        # Test 11
        # Test new normalize method using list of matchers
        # use both cached and db version to test both
        from mspasspy.db.normalize import ObjectIdMatcher, ObjectIdDBMatcher

        chankeylist = [
            "_id",
            "lat",
            "lon",
            "elev",
            "starttime",
            "endtime",
            "hang",
            "vang",
        ]
        channel_matcher = ObjectIdMatcher(
            self.db,
            collection="channel",
            attributes_to_load=chankeylist,
            load_if_defined=["net", "sta", "loc"],
        )
        source_matcher = ObjectIdDBMatcher(
            self.db,
            collection="source",
            attributes_to_load=["_id", "lat", "lon", "depth", "time"],
            load_if_defined=["magnitude"],
        )
        # basic_save_ts_id is defined earlier in this function
        # is the stock datum saved with defaults in promiscuous mode
        promiscuous_ts2 = self.db.read_data(
            basic_save_ts_id,
            mode="promiscuous",
            normalize=[channel_matcher, source_matcher],
            collection="wf_TimeSeries",
        )
        # this is a looser list than what we used earlier that
        # was carried forward in major revision aug 2023.
        # minor issue - the major point of this is after we
        # verify these match

        wf_keys = [
            "npts",
            "delta",
            "sampling_rate",
            "calib",
            "starttime",
            "dtype",
            "channel_id",
            "source_id",
            "storage_mode",
            "dir",
            "dfile",
            "foff",
            "gridfs_id",
            "url",
            "elog_id",
            "history_object_id",
            "time_standard",
            "tmatrix",
        ]
        for key in wf_keys:
            if key in promiscuous_ts:
                assert promiscuous_ts[key] == promiscuous_ts2[key]
        assert "test" not in promiscuous_ts2
        assert "extra2" not in promiscuous_ts2
        # now verify normalization worked correctly

        res = self.db["channel"].find_one({"_id": promiscuous_ts["channel_id"]})
        for k in chankeylist:
            if k == "_id":
                k2 = "channel_id"
            else:
                k2 = "channel_{}".format(k)
            assert promiscuous_ts2[k2] == res[k]
        # TODO:  should handle load_if_defined properly

        res = self.db["source"].find_one({"_id": promiscuous_ts["source_id"]})
        assert promiscuous_ts2["source_lat"] == res["lat"]
        assert promiscuous_ts2["source_lon"] == res["lon"]
        assert promiscuous_ts2["source_depth"] == res["depth"]
        assert promiscuous_ts2["source_time"] == res["time"]
        assert promiscuous_ts2["source_magnitude"] == res["magnitude"]
        # Necessary to avoid state problems with other tests
        self.db.drop_collection("cemetery")
        self.db.drop_collection("abortions")

    def test_save_read_data_seismogram(self):
        """
        This method tests various permutations of read and write
        of Seismogram data objects.  It probably should be broken into
        about a dozen different functions but for now think of it
        as a series of tests of read_data and write_data and
        associated baggage.   Instead we have comments that enumerate
        blocks testing specific features.
        """
        #  Test 0 - nothing to read with a random object id
        # returns a default constructed dead datum with an elog entry
        fail_seis = self.db.read_data(
            ObjectId(), mode="cautious", normalize=["site", "source"]
        )
        assert fail_seis.dead()
        assert fail_seis.elog.size() > 0

        # tests for Seismogram
        # first create valid simulation data for tests
        promiscuous_seis0 = copy.deepcopy(self.test_seis)
        cautious_seis0 = copy.deepcopy(self.test_seis)
        pedantic_seis0 = copy.deepcopy(self.test_seis)
        logging_helper.info(promiscuous_seis0, "1", "deepcopy")
        logging_helper.info(cautious_seis0, "1", "deepcopy")
        logging_helper.info(pedantic_seis0, "1", "deepcopy")
        # We use the C++ copy constructor here to build working
        # copies.  It doesn't alter history so they are pure clones
        # of 0 version with stage 1 of history "deepcopy"
        promiscuous_seis = Seismogram(promiscuous_seis0)
        cautious_seis = Seismogram(cautious_seis0)
        pedantic_seis = Seismogram(pedantic_seis0)

        # Test 1
        # initial test of a basic save in promiscuous mode and all
        # else defaulted - promiscuous is actually the default but set
        # here for emphasis.  Well, actually return_data is needed
        # to validate but default is false
        res_seis = self.db.save_data(
            promiscuous_seis,
            mode="promiscuous",
            return_data=True,
        )
        assert res_seis.live
        # read it back in and compare the metadata entries.
        # they will not match completely because t0 is an alias
        # that is converted in all modes to starttime.  The original
        # promiscuous_seis saved has a "test" key that is empty and
        # is also automatically dropped in all modes - all spaces
        # is dogmatically treated as null
        res_seis_read = self.db.read_data(res_seis["_id"], collection="wf_Seismogram")
        skipkeys = ["t0", "test", "is_abortion"]
        for k in res_seis:
            if k not in skipkeys:
                assert res_seis_read[k] == res_seis[k]
        # this verifies the alias
        assert res_seis_read["starttime"] == res_seis["t0"]
        # verify "test" was dropped
        assert "test" not in res_seis_read
        # the writer should add this
        assert "is_abortion" in res_seis_read
        assert not res_seis_read["is_abortion"]
        # We uses this to retrieve this record for later test
        basic_save_seis_id = res_seis["_id"]

        #
        # Test 2
        # same as above but add exclude_keys to verify that works
        # use a fresh copy of initial datum
        promiscuous_seis = Seismogram(promiscuous_seis0)
        res_seis = self.db.save_data(
            promiscuous_seis,
            mode="promiscuous",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert res_seis.live
        # check extra2 was excluded
        res_seis_read = self.db.read_data(res_seis["_id"], collection="wf_Seismogram")
        skipkeys = ["t0", "test", "is_abortion"]
        for k in res_seis:
            if k not in skipkeys:
                assert res_seis_read[k] == res_seis[k]
        # this verifies the alias
        assert res_seis_read["starttime"] == res_seis["t0"]
        # verify "test" was dropped
        assert "test" not in res_seis_read
        # the writer should add this
        assert "is_abortion" in res_seis_read
        assert not res_seis_read["is_abortion"]
        # We uses this to retrieve this record for later test
        basic_save_seis_id = res_seis["_id"]

        # test 3
        # here we validate the history data was saved correctly
        # this actualy somewhat duplicates tests in test_save_load_history
        # but differs a bit because it uses Seismogram so was retained  (Aug. 2023)
        assert res_seis.live
        assert promiscuous_seis.live
        assert not res_seis.is_defined("extra2")
        # check it is the origin in the processing history after save
        wf_doc = self.db["wf_Seismogram"].find_one({"_id": promiscuous_seis["_id"]})

        history_object_doc = self.db["history_object"].find_one(
            {"_id": wf_doc["history_object_id"]}
        )
        assert wf_doc
        assert history_object_doc
        assert history_object_doc["wf_Seismogram_id"] == promiscuous_seis["_id"]
        assert history_object_doc["alg_name"] == "save_data"
        assert history_object_doc["alg_id"] == "0"
        assert "processing_history" in history_object_doc
        assert history_object_doc["save_stage"] == 2
        assert "save_uuid" in history_object_doc

        # test 4
        # Verify setting a required attribute to an invalid value
        # leads to a failed save and dead datum return
        cautious_seis = Seismogram(cautious_seis0)
        cautious_seis.put_string("npts", "xyz")
        res_seis = self.db.save_data(
            cautious_seis, mode="cautious", storage_mode="gridfs", return_data=True
        )
        assert res_seis.dead()
        # save kills both original and return because data is passed by reference
        # following need to be aware cautious_seis is dead
        assert cautious_seis.dead()

        # test 5
        # for invalid required args cautious and pedantic act the same
        # here we use sampling_rate wrong instead of npts
        # i.e. this is a comparable test to previous
        pedantic_seis = Seismogram(pedantic_seis0)
        pedantic_seis.put_string("sampling_rate", "xyz")
        res_seis = self.db.save_data(
            pedantic_seis, mode="pedantic", storage_mode="gridfs", return_data=True
        )
        assert res_seis.dead()
        assert pedantic_seis.dead()
        # the cemetery should contain two documents from each of the
        # above saves at this point
        n_dead = self.db.cemetery.count_documents({})
        assert n_dead == 2

        # test 6
        # Valitdate normalziation works correctly
        # the following uses the id of the basic save done near the
        # top of this test function

        # in this mode the retrieved
        self.db.database_schema.set_default("wf_Seismogram", "wf")
        promiscuous_seis2 = self.db.read_data(
            basic_save_seis_id,
            mode="promiscuous",
            normalize=["site", "source"],
            collection="wf_Seismogram",
        )
        assert "_id" in promiscuous_seis2
        # 2 should have the same metadata as original plus _id
        # hence loop is driven by datum prior to being saved
        # 2 should also contain the normalizatoin data which we test after this loop
        skipkeys = ["t0", "test", "is_abortion"]
        for k in promiscuous_seis:
            if k not in skipkeys:
                assert promiscuous_seis[k] == promiscuous_seis2[k]
        assert not promiscuous_seis2["is_abortion"]
        # normalize should set these
        sitelist = [
            "site_lat",
            "site_lon",
            "site_elev",
            "site_starttime",
            "site_endtime",
        ]
        sourcelist = ["source_lat", "source_lon", "source_depth", "source_time"]
        for k in sitelist:
            assert k in promiscuous_seis2
        for k in sourcelist:
            assert k in promiscuous_seis2

        # test 7
        # test exclude_keys feature of read_data - above tested save_data
        # so they are easily confused
        exclude_list = ["_id", "endtime", "channel_id", "extra1"]
        exclude_promiscuous_seis2 = self.db.read_data(
            basic_save_seis_id,
            mode="promiscuous",
            normalize=["site", "source"],
            exclude_keys=exclude_list,
        )
        for k in exclude_list:
            assert k not in exclude_promiscuous_seis2

        # test 8
        # This test saves a datum with an invalid value in promiscuous
        # mode.   It is then read back cautious and we validate
        # the expected errors are present in elog
        # Note we restore cautious_seis to its original state and
        # add the error as the above changes it.  That makes this
        # test more stable and less confusing than the previous version
        cautious_seis = Seismogram(cautious_seis0)
        cautious_seis["npts"] = "foobar"
        res_seis = self.db.save_data(
            cautious_seis,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        # promicuous save ignores the error and leaves these live
        assert res_seis.live
        assert cautious_seis.live
        # this will fail with an invalid type error logged because
        # previous save has invalid npts which makes it impossible to
        # reconstruct the datum
        cautious_seis2 = self.db.read_data(
            res_seis["_id"],
            mode="cautious",
            normalize=["site", "source"],
            collection="wf_Seismogram",
        )
        # This next test could change if the schema changes.
        # currently it issues a complaint for keys history_id, elog_id, and extra1
        # The second message is the npts botch error set above
        cautious_seis2_elog = cautious_seis2.elog.get_error_log()
        assert len(cautious_seis2.elog) == 2
        assert cautious_seis2_elog[0].badness == ErrorSeverity.Complaint
        assert cautious_seis2_elog[1].badness == ErrorSeverity.Invalid

        assert "data" not in cautious_seis2
        assert cautious_seis2["is_abortion"]

        # test 9
        #  Now do the same thing but set npts to a string that can
        # be converted to an int - note 255 is magic from the seis generator
        cautious_seis = Seismogram(cautious_seis0)
        cautious_seis.put_string("npts", "255")
        res_seis = self.db.save_data(
            cautious_seis,
            mode="promiscuous",
            storage_mode="gridfs",
            return_data=True,
            collection="wf_Seismogram",
        )
        assert res_seis.live
        assert cautious_seis.live
        cautious_seis2 = self.db.read_data(
            res_seis["_id"],
            mode="cautious",
        )
        assert cautious_seis2.live
        assert cautious_seis2["npts"] == 255
        assert not cautious_seis2["is_abortion"]
        # in pedantic mode the same read should cause the datum
        # to be killed
        pedantic_seis2 = self.db.read_data(
            res_seis["_id"], mode="pedantic", collection="wf_Seismogram"
        )
        assert pedantic_seis2.dead()
        assert pedantic_seis2["is_abortion"]

        # test 10
        # test save with non exist id under cautious mode
        cautious_seis = Seismogram(cautious_seis0)
        non_exist_id = ObjectId()
        cautious_seis["_id"] = non_exist_id
        logging_helper.info(cautious_seis, "3", "save_data")
        res_seis = self.db.save_data(cautious_seis, mode="cautious", return_data=True)
        assert res_seis.live
        assert cautious_seis.live
        assert "_id" in cautious_seis
        assert not cautious_seis["_id"] == non_exist_id

        # Test 11
        # Test new normalize method using list of matchers
        # use both cached and db version to test both
        from mspasspy.db.normalize import ObjectIdMatcher, ObjectIdDBMatcher

        site_matcher = ObjectIdMatcher(
            self.db,
            collection="site",
            attributes_to_load=["_id", "lat", "lon", "elev", "starttime", "endtime"],
            load_if_defined=["net", "sta", "loc"],
        )
        source_matcher = ObjectIdDBMatcher(
            self.db,
            collection="source",
            attributes_to_load=["_id", "lat", "lon", "depth", "time"],
            load_if_defined=["magnitude"],
        )
        # basic_save_seis_id is defined earlier in this function
        # is the stock datum saved with defaults in promiscuous mode
        promiscuous_seis2 = self.db.read_data(
            basic_save_seis_id,
            mode="promiscuous",
            normalize=[site_matcher, source_matcher],
            collection="wf_Seismogram",
        )
        # this is a looser list than what we used earlier that
        # was carried forward in major revision aug 2023.
        # minor issue - the major point of this is after we
        # verify these match

        wf_keys = [
            "npts",
            "delta",
            "sampling_rate",
            "calib",
            "starttime",
            "dtype",
            "site_id",
            "channel_id",
            "source_id",
            "storage_mode",
            "dir",
            "dfile",
            "foff",
            "gridfs_id",
            "url",
            "elog_id",
            "history_object_id",
            "time_standard",
            "tmatrix",
        ]
        for key in wf_keys:
            if key in promiscuous_seis:
                assert promiscuous_seis[key] == promiscuous_seis2[key]
        assert "test" not in promiscuous_seis2
        assert "extra2" not in promiscuous_seis2
        # now verify normalization worked correctly

        res = self.db["site"].find_one({"_id": promiscuous_seis["site_id"]})
        assert promiscuous_seis2["site_lat"] == res["lat"]
        assert promiscuous_seis2["site_lon"] == res["lon"]
        assert promiscuous_seis2["site_elev"] == res["elev"]
        assert promiscuous_seis2["site_starttime"] == res["starttime"]
        assert promiscuous_seis2["site_endtime"] == res["endtime"]
        assert promiscuous_seis2["site_net"] == res["net"]
        assert promiscuous_seis2["site_sta"] == res["sta"]
        assert promiscuous_seis2["site_loc"] == res["loc"]

        res = self.db["source"].find_one({"_id": promiscuous_seis["source_id"]})
        assert promiscuous_seis2["source_lat"] == res["lat"]
        assert promiscuous_seis2["source_lon"] == res["lon"]
        assert promiscuous_seis2["source_depth"] == res["depth"]
        assert promiscuous_seis2["source_time"] == res["time"]
        assert promiscuous_seis2["source_magnitude"] == res["magnitude"]
        # Necessary to avoid state problems with other tests
        self.db.drop_collection("cemetery")
        self.db.drop_collection("abortions")

    def test_index_mseed_file(self):
        dir = "python/tests/data/"
        dfile = "3channels.mseed"
        fname = os.path.join(dir, dfile)
        self.db.index_mseed_file(fname, collection="wf_miniseed")
        assert self.db["wf_miniseed"].count_documents({}) == 3

        for doc in self.db["wf_miniseed"].find():
            ts = self.db.read_data(doc, collection="wf_miniseed")
            assert ts.npts == len(ts.data)

    def test_delete_wf(self):
        # clear all the wf collection documents
        # self.db['wf_TimeSeries'].delete_many({})

        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert save_res.live
        assert ts.live

        nonexistent_id = ObjectId()
        # only delete waveform data
        with pytest.raises(
            TypeError, match="only TimeSeries and Seismogram are supported"
        ):
            self.db.delete_data(nonexistent_id, "site")
        # can not find the document with given _id
        with pytest.raises(
            MsPASSError,
            match="Could not find document in wf collection by _id: {}.".format(
                nonexistent_id
            ),
        ):
            self.db.delete_data(nonexistent_id, "TimeSeries")

        # check gridfs exist
        gfsh = gridfs.GridFS(self.db)
        assert gfsh.exists(res["gridfs_id"])
        # insert a dummy elog document with wf_TimeSeries_id equals to ts['_id']
        self.db["elog"].insert_one({"_id": ObjectId(), "wf_TimeSeries_id": ts["_id"]})

        # grid_fs delete(clear_history, clear_elog)
        self.db.delete_data(ts["_id"], "TimeSeries")
        assert not self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert not gfsh.exists(res["gridfs_id"])
        assert not self.db["history_object"].find_one({"_id": res["history_object_id"]})
        assert not self.db["elog"].find_one({"_id": res["elog_id"]})
        assert self.db["elog"].count_documents({"wf_TimeSeries_id": ts["_id"]}) == 0

        # file delete(not remove_unreferenced_files, clear_history, clear_elog)
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="file",
            dir="./python/tests/data/",
            dfile="test_db_output_1",
            exclude_keys=["extra2"],
            return_data=True,
        )
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})

        assert save_res.live
        self.db.delete_data(ts["_id"], "TimeSeries")
        assert not self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert not self.db["history_object"].find_one({"_id": res["history_object_id"]})
        assert not self.db["elog"].find_one({"_id": res["elog_id"]})
        assert self.db["elog"].count_documents({"wf_TimeSeries_id": ts["_id"]}) == 0
        # file still exists
        fname = os.path.join(res["dir"], res["dfile"])
        assert os.path.exists(fname)

        # file delete(remove_unreferenced_files, clear_history, clear_elog), with 2 wf doc using same file dir/dfile
        ts = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="file",
            dir="./python/tests/data/",
            dfile="test_db_output_1",
            exclude_keys=["extra2"],
            return_data=True,
        )
        save_res2 = self.db.save_data(
            ts2,
            mode="promiscuous",
            storage_mode="file",
            dir="./python/tests/data/",
            dfile="test_db_output_1",
            exclude_keys=["extra2"],
            return_data=True,
        )

        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert save_res.live
        self.db.delete_data(ts["_id"], "TimeSeries", remove_unreferenced_files=True)
        assert not self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert not self.db["history_object"].find_one({"_id": res["history_object_id"]})
        assert not self.db["elog"].find_one({"_id": res["elog_id"]})
        assert self.db["elog"].count_documents({"wf_TimeSeries_id": ts["_id"]}) == 0
        # file still exists, because another wf doc is using it
        fname = os.path.join(res["dir"], res["dfile"])
        assert os.path.exists(fname)

        res2 = self.db["wf_TimeSeries"].find_one({"_id": ts2["_id"]})
        assert save_res2.live
        self.db.delete_data(ts2["_id"], "TimeSeries", remove_unreferenced_files=True)
        assert not self.db["wf_TimeSeries"].find_one({"_id": ts2["_id"]})
        assert not self.db["history_object"].find_one(
            {"_id": res2["history_object_id"]}
        )
        assert not self.db["elog"].find_one({"_id": res2["elog_id"]})
        assert self.db["elog"].count_documents({"wf_TimeSeries_id": ts2["_id"]}) == 0
        # file not exists
        fname = os.path.join(res2["dir"], res2["dfile"])
        assert not os.path.exists(fname)

    def test_clean_collection(self):
        # clear all the wf collection documents
        self.db["wf_TimeSeries"].delete_many({})

        # test non exist document in the database
        fixed_cnt = self.db.clean_collection("wf_TimeSeries", query={"_id": ObjectId()})
        assert not fixed_cnt

        # test fixed_out
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        ts1 = copy.deepcopy(self.test_ts)
        ts1["starttime_shift"] = 1.0
        ts2 = copy.deepcopy(self.test_ts)
        ts2["starttime_shift"] = 1.0
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        ts1["npts"] = "123"
        ts2["delta"] = "12"
        ts2["starttime"] = "123"

        save_res = self.db.save_data(
            ts1,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            ts2,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live

        fixed_cnt = self.db.clean_collection("wf_TimeSeries")
        assert fixed_cnt == {"npts": 1, "delta": 1}

    def test_clean(self, capfd):
        """
        Collection of several tests of clean method.
        """
        # make certain wf_TimeSeries is empty to avoid state problems
        # from previous tests
        self.db.drop_collection("wf_TimeSeries")

        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")

        # Test 1:  invalid parameters to function
        with pytest.raises(
            MsPASSError,
            match="verbose_keys should be a list , but <class 'str'> is requested.",
        ):
            self.db.clean(ObjectId(), verbose_keys="123")
        with pytest.raises(
            MsPASSError,
            match="rename_undefined should be a dict , but <class 'str'> is requested.",
        ):
            self.db.clean(ObjectId(), rename_undefined="123")
        with pytest.raises(
            MsPASSError,
            match="required_xref_list should be a list , but <class 'str'> is requested.",
        ):
            self.db.clean(ObjectId(), required_xref_list="123")

        # Test 2:
        # erase a required field in TimeSeries.  In promiscuous mode that
        # will work but produce a wf cocument that is invalid
        # later tests are to handle that problem datum
        ts.erase("npts")
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        assert ts.live
        # save this for later test
        bad_datum_id = save_res["_id"]

        # test nonexist document
        nonexist_id = ObjectId()
        fixes_cnt = self.db.clean(nonexist_id, verbose=True)
        assert not fixes_cnt
        out, err = capfd.readouterr()
        assert (
            out
            == "collection wf_TimeSeries document _id: {}, is not found\n".format(
                nonexist_id
            )
        )

        # test verbose_keys and delete required fields missing document if delete_missing_required is True
        fixes_cnt = self.db.clean(
            bad_datum_id,
            verbose_keys=["delta"],
            verbose=True,
            delete_missing_required=True,
        )
        assert len(fixes_cnt) == 0
        # test if it is deleted
        assert not self.db["wf_TimeSeries"].find_one({"_id": bad_datum_id})
        assert not self.db["history_object"].find_one(
            {"wf_TimeSeries_id": bad_datum_id}
        )
        assert not self.db["elog"].find_one({"wf_TimeSeries_id": ts["_id"]})
        out, err = capfd.readouterr()
        assert (
            out
            == "collection wf_TimeSeries document _id: {}, delta: {}, required attribute: npts are missing. the document is deleted.\n".format(
                ts["_id"], ts["delta"]
            )
        )

        # Test 3:
        # test check_xref and delete required xref_keys missing document
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["starttime_shift"] = 1.0
        ts.erase("site_id")
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        # The document with this id has the missing site_id value
        bad_xref_wfid = save_res["_id"]
        fixes_cnt = self.db.clean(
            bad_xref_wfid,
            verbose=True,
            required_xref_list=["site_id"],
            delete_missing_xref=True,
        )
        assert len(fixes_cnt) == 0
        assert not self.db["wf_TimeSeries"].find_one({"_id": bad_xref_wfid})
        assert not self.db["history_object"].find_one(
            {"wf_TimeSeries_id": bad_xref_wfid}
        )
        assert not self.db["elog"].find_one({"wf_TimeSeries_id": bad_xref_wfid})
        out, err = capfd.readouterr()
        assert (
            out
            == "collection wf_TimeSeries document _id: {}, required xref keys: site_id are missing. the document is deleted.\n".format(
                ts["_id"]
            )
        )

        # Test 4:  test successful type conversions - not a function option
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        # npts has type str, should convert to int
        ts["npts"] = "123"
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        fixes_cnt = self.db.clean(ts["_id"], verbose=True)
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert res["_id"] == ts["_id"]
        assert "npts" in res and res["npts"] == 123
        assert len(fixes_cnt) == 1
        assert fixes_cnt == {"npts": 1}
        out, err = capfd.readouterr()
        assert (
            out
            == "collection wf_TimeSeries document _id: {}, attribute npts conversion from 123 to <class 'int'> is done.\n".format(
                ts["_id"]
            )
        )

        # Test 5:  test impossible type conversion
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        # npts has type str, but unable to convert to int
        ts["npts"] = "xyz"
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        fixes_cnt = self.db.clean(ts["_id"], verbose=True)
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert "npts" not in res
        # can not be fixed
        assert len(fixes_cnt) == 0
        out, err = capfd.readouterr()
        assert (
            out
            == "collection wf_TimeSeries document _id: {}, attribute npts conversion from xyz to <class 'int'> cannot be done.\n".format(
                ts["_id"]
            )
        )

        # test removing aliases
        test_source_id = ObjectId()
        self.db["source"].insert_one(
            {
                "_id": test_source_id,
                "EVLA": 1.2,
                "lon": 1.2,
                "time": datetime.utcnow().timestamp(),
                "depth": 3.1,
                "MAG": 1.0,
            }
        )
        fixes_cnt = self.db.clean(test_source_id, collection="source", verbose=True)
        res = self.db["source"].find_one({"_id": test_source_id})
        assert res
        assert "EVLA" not in res
        assert "MAG" not in res
        assert "lat" in res
        assert "magnitude" in res
        assert len(fixes_cnt) == 0
        self.db["source"].delete_one({"_id": test_source_id})
        out, err = capfd.readouterr()
        assert not out

        # test undefined key-value pair and delete_undefined
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert "extra1" in res
        fixes_cnt = self.db.clean(ts["_id"], verbose=True, delete_undefined=True)
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert "extra1" not in res
        assert len(fixes_cnt) == 0
        out, err = capfd.readouterr()
        assert not out

        # test rename attributes
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert "extra1" in res
        val = res["extra1"]
        fixes_cnt = self.db.clean(
            ts["_id"], verbose=True, rename_undefined={"extra1": "rename_extra"}
        )
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert "extra1" not in res
        assert "rename_extra" in res
        assert res["rename_extra"] == val
        assert len(fixes_cnt) == 0
        out, err = capfd.readouterr()
        assert not out

        # test not verbose
        fixes_cnt = self.db.clean(
            ts["_id"], rename_undefined={"rename_extra": "rename_extra_2"}
        )
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert res
        assert "rename_extra" not in res
        assert "rename_extra_2" in res
        assert res["rename_extra_2"] == val
        assert len(fixes_cnt) == 0

    def test_verify(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["starttime_shift"] = 1.0
        # xref_key doc not found
        ts["site_id"] = 123
        # undefined required key
        ts.erase("npts")
        # mismatch type
        ts["delta"] = "123"
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert not "npts" in res
        assert res["delta"] == "123"

        # test doc that does not exist
        non_exist_id = ObjectId()
        with pytest.raises(
            MsPASSError,
            match=" has no matching document",
        ):
            problematic_keys = self.db.verify(
                non_exist_id, "wf_TimeSeries", tests=["xref", "type", "undefined"]
            )

        # test xref, undefined and type
        problematic_keys = self.db.verify(
            ts["_id"], "wf_TimeSeries", tests=["xref", "type", "undefined"]
        )
        assert len(problematic_keys) == 3
        assert (
            "site_id" in problematic_keys
            and len(problematic_keys["site_id"]) == 2
            and "xref" in problematic_keys["site_id"]
            and "type" in problematic_keys["site_id"]
        )
        assert "npts" in problematic_keys and problematic_keys["npts"] == ["undefined"]
        assert "delta" in problematic_keys and problematic_keys["delta"] == ["type"]

    def test_check_xref_key(self):
        bad_xref_key_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_xref_key_ts, "1", "deepcopy")
        bad_xref_key_ts["site_id"] = ObjectId()
        bad_wf_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_wf_ts, "1", "deepcopy")

        save_res = self.db.save_data(
            bad_xref_key_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            bad_wf_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2", "site_id"],
            return_data=True,
        )
        assert save_res.live

        bad_xref_key_doc = self.db["wf_TimeSeries"].find_one(
            {"_id": bad_xref_key_ts["_id"]}
        )
        bad_wf_doc = self.db["wf_TimeSeries"].find_one({"_id": bad_wf_ts["_id"]})

        # if xref_key is not defind -> not checking
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(
            bad_xref_key_doc, "wf_TimeSeries", "xxx_id"
        )
        assert not is_bad_xref_key
        assert not is_bad_wf

        # if xref_key is not a xref_key -> not checking
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(
            bad_xref_key_doc, "wf_TimeSeries", "npts"
        )
        assert not is_bad_xref_key
        assert not is_bad_wf
        # aliases
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(
            bad_xref_key_doc, "wf_TimeSeries", "dt"
        )
        assert not is_bad_xref_key
        assert not is_bad_wf

        # can't find normalized document
        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(
            bad_xref_key_doc, "wf_TimeSeries", "site_id"
        )
        assert is_bad_xref_key
        assert not is_bad_wf

        is_bad_xref_key, is_bad_wf = self.db._check_xref_key(
            bad_wf_doc, "wf_TimeSeries", "site_id"
        )
        assert not is_bad_xref_key
        assert is_bad_wf

    def test_check_undefined_keys(self):
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts.erase("npts")

        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2", "calib"],
            return_data=True,
        )
        assert save_res.live
        self.db["wf_TimeSeries"].update_one({"_id": ts["_id"]}, {"$set": {"t0": 1.0}})
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert "calib" not in res
        assert "npts" not in res
        assert "t0" in res

        undefined_keys = self.db._check_undefined_keys(res, "wf_TimeSeries")
        assert len(undefined_keys) == 2
        assert "npts" in undefined_keys
        assert "starttime_shift" in undefined_keys

    def test_check_mismatch_key(self):
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["npts"] = "xyz"

        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2", "starttime"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})

        assert self.db._check_mismatch_key(res, "wf_TimeSeries", "npts")
        assert self.db._check_mismatch_key(res, "wf_TimeSeries", "nsamp")
        assert not self.db._check_mismatch_key(res, "wf_TimeSeries", "xxx")
        assert not self.db._check_mismatch_key(res, "wf_TimeSeries", "delta")

    def test_delete_attributes(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert "delta" in res and "sampling_rate" in res and "starttime" in res
        counts = self.db._delete_attributes(
            "wf_TimeSeries", ["delta", "sampling_rate", "starttime"]
        )
        assert len(counts) == 3 and counts == {
            "delta": 1,
            "sampling_rate": 1,
            "starttime": 1,
        }
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert (
            not "delta" in res and not "sampling_rate" in res and not "starttime" in res
        )

    def test_rename_attributes(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["starttime_shift"] = 1.0
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert "delta" in res and "sampling_rate" in res and "starttime" in res
        delta_val = res["delta"]
        sampling_rate_val = res["sampling_rate"]
        starttime_val = res["starttime"]
        counts = self.db._rename_attributes(
            "wf_TimeSeries",
            {"delta": "dt", "sampling_rate": "sr", "starttime": "st"},
        )
        assert len(counts) == 3 and counts == {
            "delta": 1,
            "sampling_rate": 1,
            "starttime": 1,
        }
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert (
            not "delta" in res and not "sampling_rate" in res and not "starttime" in res
        )
        assert "dt" in res and "sr" in res and "st" in res
        assert (
            res["dt"] == delta_val
            and res["sr"] == sampling_rate_val
            and res["st"] == starttime_val
        )

    def test_fix_attribute_types(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        ts = copy.deepcopy(self.test_ts)
        logging_helper.info(ts, "1", "deepcopy")
        ts["npts"] = "xyz"
        ts["delta"] = "123"
        ts["sampling_rate"] = "123"
        save_res = self.db.save_data(
            ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert (
            res["npts"] == "xyz"
            and res["delta"] == "123"
            and res["sampling_rate"] == "123"
        )
        counts = self.db._fix_attribute_types("wf_TimeSeries")
        assert len(counts) == 2 and counts == {"delta": 1, "sampling_rate": 1}
        res = self.db["wf_TimeSeries"].find_one({"_id": ts["_id"]})
        assert (
            res["npts"] == "xyz"
            and res["delta"] == 123.0
            and res["sampling_rate"] == 123.0
        )

    def test_check_links(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        missing_site_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(missing_site_id_ts, "1", "deepcopy")
        missing_site_id_ts.erase("site_id")

        bad_site_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_site_id_ts, "1", "deepcopy")
        bad_site_id_ts["site_id"] = ObjectId()

        bad_source_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_source_id_ts, "1", "deepcopy")
        bad_source_id_ts["source_id"] = ObjectId()

        bad_channel_id_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_channel_id_ts, "1", "deepcopy")
        bad_channel_id_ts["channel_id"] = ObjectId()

        save_res = self.db.save_data(
            missing_site_id_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            bad_site_id_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            bad_source_id_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            bad_channel_id_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live

        # undefined collection name
        with pytest.raises(
            MsPASSError,
            match="check_links:  collection xxx is not defined in database schema",
        ):
            self.db._check_links(collection="xxx")

        # undefined xref keys
        with pytest.raises(
            MsPASSError, match="check_links:  illegal value for normalize arg=npts"
        ):
            self.db._check_links(xref_key="npts", collection="wf")

        # no documents found
        wfquery = {"_id": ObjectId()}
        with pytest.raises(
            MsPASSError,
            match=re.escape(
                "checklinks:  wf_TimeSeries collection has no data matching query={}".format(
                    str(wfquery)
                )
            ),
        ):
            self.db._check_links(collection="wf", wfquery=wfquery)

        # check with default, all xref keys
        (bad_id_list, missing_id_list) = self.db._check_links(collection="wf")
        assert len(bad_id_list) == 3
        assert set(bad_id_list) == set(
            [
                bad_site_id_ts["_id"],
                bad_source_id_ts["_id"],
                bad_channel_id_ts["_id"],
            ]
        )
        assert len(missing_id_list) == 1
        assert missing_id_list == [missing_site_id_ts["_id"]]

        # check with a single xref key
        (bad_id_list, missing_id_list) = self.db._check_links(
            xref_key="site_id", collection="wf"
        )
        assert len(bad_id_list) == 1
        assert bad_id_list == [bad_site_id_ts["_id"]]
        assert len(missing_id_list) == 1
        assert missing_id_list == [missing_site_id_ts["_id"]]

        # check with a user specified xref keys
        (bad_id_list, missing_id_list) = self.db._check_links(
            xref_key=["site_id", "source_id"], collection="wf"
        )
        assert len(bad_id_list) == 2
        assert set(bad_id_list) == set([bad_site_id_ts["_id"], bad_source_id_ts["_id"]])
        assert len(missing_id_list) == 1
        assert missing_id_list == [missing_site_id_ts["_id"]]

    def test_check_attribute_types(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        bad_type_docs_ts = copy.deepcopy(self.test_ts)
        undefined_key_docs_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(bad_type_docs_ts, "1", "deepcopy")
        logging_helper.info(undefined_key_docs_ts, "1", "deepcopy")
        bad_type_docs_ts["npts"] = "xyz"

        save_res = self.db.save_data(
            bad_type_docs_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            undefined_key_docs_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live

        # test empty matched documents
        query_dict = {"_id": ObjectId()}
        with pytest.raises(
            MsPASSError,
            match=re.escape(
                "check_attribute_types:  query={} yields zero matching documents".format(
                    str(query_dict)
                )
            ),
        ):
            (bad_type_docs, undefined_key_docs) = self.db._check_attribute_types(
                collection="wf_TimeSeries", query=query_dict
            )

        # test bad_type_docs and undefined_key_docs
        (bad_type_docs, undefined_key_docs) = self.db._check_attribute_types(
            collection="wf_TimeSeries"
        )
        assert len(bad_type_docs) == 1
        assert bad_type_docs == {bad_type_docs_ts["_id"]: {"npts": "xyz"}}
        assert len(undefined_key_docs) == 2
        assert undefined_key_docs == {
            bad_type_docs_ts["_id"]: {"extra1": "extra1"},
            undefined_key_docs_ts["_id"]: {"extra1": "extra1"},
        }

    def test_check_required(self):
        # clear all documents
        self.db["wf_TimeSeries"].delete_many({})
        wrong_types_ts = copy.deepcopy(self.test_ts)
        undef_ts = copy.deepcopy(self.test_ts)
        logging_helper.info(wrong_types_ts, "1", "deepcopy")
        logging_helper.info(undef_ts, "1", "deepcopy")
        wrong_types_ts["npts"] = "xyz"

        save_res = self.db.save_data(
            wrong_types_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live
        save_res = self.db.save_data(
            undef_ts,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_keys=["extra2"],
            return_data=True,
        )
        assert save_res.live

        # test empty matched documents
        query_dict = {"_id": ObjectId()}
        with pytest.raises(
            MsPASSError,
            match=re.escape(
                "check_required:  query={} yields zero matching documents".format(
                    str(query_dict)
                )
            ),
        ):
            (wrong_types, undef) = self.db._check_required(
                collection="wf_TimeSeries",
                keys=["npts", "delta", "starttime"],
                query=query_dict,
            )

        # test undefined keys
        with pytest.raises(
            MsPASSError,
            match="check_required:  schema has no definition for key=undefined_key",
        ):
            (wrong_types, undef) = self.db._check_required(
                collection="wf_TimeSeries", keys=["undefined_key"]
            )

        # test bad_type_docs and undefined_key_docs
        (wrong_types, undef) = self.db._check_required(
            collection="wf_TimeSeries", keys=["npts", "delta", "starttime_shift"]
        )
        assert len(wrong_types) == 1
        assert wrong_types == {wrong_types_ts["_id"]: {"npts": "xyz"}}
        assert len(undef) == 2
        assert undef == {
            wrong_types_ts["_id"]: ["starttime_shift"],
            undef_ts["_id"]: ["starttime_shift"],
        }

    def test_update_ensemble_metadata(self):
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        logging_helper.info(ts3, "1", "deepcopy")
        self.db.save_data(ts1, storage_mode="gridfs", return_data=True)
        self.db.save_data(ts2, storage_mode="gridfs", return_data=True)
        self.db.save_data(ts3, storage_mode="gridfs", return_data=True)

        time = datetime.utcnow().timestamp()
        ts1.t0 = time
        ts1["tst"] = time
        ts2.t0 = time
        ts3.t0 = time + 5.0
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        ts_ensemble.set_live()

        logging_helper.info(ts_ensemble.member[0], "2", "update_data")
        logging_helper.info(ts_ensemble.member[1], "2", "update_data")
        logging_helper.info(ts_ensemble.member[2], "2", "update_data")
        # Test this section is a temporary to see if save_ensemble_data
        # resolves the _id problem:
        res = self.db.save_data(ts_ensemble, return_data=True)
        # This test needs to be moved and/or changed.   It is failing with
        # and error that says it needs the _id to do an update.
        # I'm commenting out the next 3 asserts because they will fail until
        # that is resolved
        self.db.update_ensemble_metadata(ts_ensemble, mode="promiscuous")
        doc = self.db["wf_TimeSeries"].find_one({"_id": res.member[0]["_id"]})
        assert doc["starttime"] == time
        doc = self.db["wf_TimeSeries"].find_one({"_id": res.member[1]["_id"]})
        assert doc["starttime"] == time
        doc = self.db["wf_TimeSeries"].find_one({"_id": res.member[2]["_id"]})
        assert doc["starttime"] != time

        time_new = datetime.utcnow().timestamp()
        ts_ensemble.member[0]["tst"] = time + 1
        ts_ensemble.member[0].t0 = time_new

        # this section also fails because of the disconnect with _id so
        # I'm also temporarily disabling it
        logging_helper.info(ts_ensemble.member[0], "2", "update_data")
        logging_helper.info(ts_ensemble.member[1], "2", "update_data")
        logging_helper.info(ts_ensemble.member[2], "2", "update_data")
        # self.db.update_ensemble_metadata(ts_ensemble, mode='promiscuous', exclude_keys=['tst'])
        # res = self.db['wf_TimeSeries'].find_one({'_id': ts1['_id']})
        # assert res['tst'] == time
        # assert res['starttime'] == time_new

        # make sure the elog entry do not have duplicates from the two updates
        res1 = self.db["wf_TimeSeries"].find_one({"_id": ts1["_id"]})
        res2 = self.db["wf_TimeSeries"].find_one({"_id": ts2["_id"]})
        res3 = self.db["wf_TimeSeries"].find_one({"_id": ts3["_id"]})
        # disabling for now - the above finds are failing as in this test
        # script in spyder the database is empty - some disconnect I dont understand
        # assert len(self.db['elog'].find_one({'_id': res1['elog_id']})['logdata']) == 2
        # assert len(self.db['elog'].find_one({'_id': res2['elog_id']})['logdata']) == 2
        # assert len(self.db['elog'].find_one({'_id': res3['elog_id']})['logdata']) == 2

        # using seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        logging_helper.info(seis1, "1", "deepcopy")
        logging_helper.info(seis2, "1", "deepcopy")
        logging_helper.info(seis3, "1", "deepcopy")
        self.db.save_data(seis1, storage_mode="gridfs", return_data=True)
        self.db.save_data(seis2, storage_mode="gridfs", return_data=True)
        self.db.save_data(seis3, storage_mode="gridfs", return_data=True)
        time = datetime.utcnow().timestamp()
        seis1.t0 = time
        seis1["tst"] = time
        seis2.t0 = time
        seis3.t0 = time
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)

        logging_helper.info(seis_ensemble.member[0], "2", "update_data")
        logging_helper.info(seis_ensemble.member[1], "2", "update_data")
        logging_helper.info(seis_ensemble.member[2], "2", "update_data")
        # Disabling this parallel problem with TimeSeries version of the same test
        # self.db.update_ensemble_metadata(seis_ensemble, mode='promiscuous', exclude_objects=[2])
        res = self.db["wf_Seismogram"].find_one({"_id": seis1["_id"]})
        # assert res['starttime'] == time
        res = self.db["wf_Seismogram"].find_one({"_id": seis2["_id"]})
        # assert res['starttime'] == time
        res = self.db["wf_Seismogram"].find_one({"_id": seis3["_id"]})
        # assert res['starttime'] != time

        time_new = datetime.utcnow().timestamp()
        seis_ensemble.member[0]["tst"] = time + 1
        seis_ensemble.member[0].t0 = time_new
        logging_helper.info(seis_ensemble.member[0], "2", "update_data")
        logging_helper.info(seis_ensemble.member[1], "2", "update_data")
        logging_helper.info(seis_ensemble.member[2], "2", "update_data")
        # Disabling this one again for the same reason as TimeSeries version
        # self.db.update_ensemble_metadata(seis_ensemble, mode='promiscuous', exclude_keys=['tst'])
        res = self.db["wf_Seismogram"].find_one({"_id": seis1["_id"]})
        # assert res['tst'] == time
        # assert res['starttime'] == time_new

    def test_save_ensemble_data(self):
        """
        Tests writer for ensembles.   In v1 this was done with
        different methods.  From v2 forward the recommended use is
        to run with save_data.  These tests were modified to make
        that change Sept 2023.  Also cleaned up some internal
        state dependencies in earlier version that carried data forward
        inside this function in confusing ways that caused maintenance
        issues.

        Note retained compatibility tests for old api.  If and when
        those are completely deprecated those sections will need to be
        changed.
        """
        self.db.drop_collection("wf_TimeSeries")
        self.db.drop_collection("wf_Seismogram")
        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        logging_helper.info(ts3, "1", "deepcopy")
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        ts_ensemble.set_live()
        # We use this copy below to restore the state of the
        # data being tested - avoids confusing dependencies that
        # were present in earlier versions of this test script
        ts_ensemble0 = copy.deepcopy(ts_ensemble)
        # First test save in backward compatibility with
        # save_ensemble_data using directory list and dfile list.
        # Note this function may be deprecated in future releases
        # An anomaly the revision is that exclude_objects edits
        # the return deleting the excluded members.  Older version
        # just left it unaltered and didn't save it.
        # I (glp) judged that the wrong behavior
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        dfile_list = ["test_db_output", "test_db_output"]
        dir_list = ["python/tests/data/", "python/tests/data/"]
        ts_ensemble = self.db.save_ensemble_data(
            ts_ensemble,
            mode="promiscuous",
            storage_mode="file",
            dfile_list=dfile_list,
            dir_list=dir_list,
            exclude_objects=[1],
        )
        assert len(ts_ensemble.member) == 2
        # Test atomic read of members just written
        # Note exclude_objects now deletes data excluded from member vector
        # so we don't get here without passing the assert immediately above

        for i in range(len(ts_ensemble.member)):
            res = self.db.read_data(
                ts_ensemble[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source", "channel"],
            )
            assert res.live
            assert np.isclose(ts_ensemble.member[i].data, res.data).all()

        # Repeat same test as immediately above using gridfs storage mode
        # and adding a second history node
        ts_ensemble = copy.deepcopy(ts_ensemble0)
        logging_helper.info(ts_ensemble.member[0], "2", "save_data")
        logging_helper.info(ts_ensemble.member[1], "2", "save_data")
        logging_helper.info(ts_ensemble.member[2], "2", "save_data")
        ts_ensemble = self.db.save_ensemble_data(
            ts_ensemble,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_objects=[1],
        )
        # Test atomic read of members just written
        # Note exclude_objects use above is why we add the conditional
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        for i in range(len(ts_ensemble.member)):
            res = self.db.read_data(
                ts_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source", "channel"],
            )
            assert res.live
            assert np.isclose(ts_ensemble.member[i].data, res.data).all()

        # Now repeat both of the above using the new shorter named
        # function save_data (previously only supported atomic data)
        # we don't use exclude as that is is expected to be deprecated
        ts_ensemble = copy.deepcopy(ts_ensemble0)
        ts_ensemble = self.db.save_data(
            ts_ensemble,
            mode="promiscuous",
            storage_mode="file",
            dir="python/tests/data/",
            dfile="test_db_output",
            return_data=True,
        )
        assert ts_ensemble.live
        assert len(ts_ensemble.member) == 3
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        # using atomic reader for now - ensemble reader is tested
        # in test_read_ensemble_data
        for i in range(len(ts_ensemble.member)):
            res = self.db.read_data(
                ts_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source", "channel"],
            )
            assert res.live
            assert np.isclose(ts_ensemble.member[i].data, res.data).all()

        # Repeat same test as immediately above using gridfs storage mode
        # and adding a second history node
        ts_ensemble = copy.deepcopy(ts_ensemble0)
        logging_helper.info(ts_ensemble.member[0], "2", "save_data")
        logging_helper.info(ts_ensemble.member[1], "2", "save_data")
        logging_helper.info(ts_ensemble.member[2], "2", "save_data")
        ts_ensemble = self.db.save_data(
            ts_ensemble,
            mode="promiscuous",
            storage_mode="gridfs",
            return_data=True,
        )
        assert len(ts_ensemble.member) == 3
        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        # using atomic reader for now - ensemble reader is tested
        # in test_read_ensemble_data
        for i in range(len(ts_ensemble.member)):
            res = self.db.read_data(
                ts_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source", "channel"],
            )
            assert res.live
            assert np.isclose(ts_ensemble.member[i].data, res.data).all()

        # Near copy of above for Seismogram
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        logging_helper.info(seis1, "1", "deepcopy")
        logging_helper.info(seis2, "1", "deepcopy")
        logging_helper.info(seis3, "1", "deepcopy")
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)
        seis_ensemble.set_live()
        # We use this copy below to restore the state of the
        # data being tested - avoids confusing dependencies that
        # were present in earlier versions of this test script
        seis_ensemble0 = copy.deepcopy(seis_ensemble)
        # First test save in backward compatibility with
        # save_ensemble_data using directory list and dfile list.
        # Note this function may be deprecated in future releases
        # An anomaly the revision is that exclude_objects edits
        # the return deleting the excluded members.  Older version
        # just left it unaltered and didn't save it.
        # I (glp) judged that the wrong behavior
        self.db.database_schema.set_default("wf_Seismogram", "wf")
        dfile_list = ["test_db_output_seis", "test_db_output_seis"]
        dir_list = ["python/tests/data/", "python/tests/data/"]
        seis_ensemble = self.db.save_ensemble_data(
            seis_ensemble,
            mode="promiscuous",
            storage_mode="file",
            dfile_list=dfile_list,
            dir_list=dir_list,
            exclude_objects=[1],
        )
        assert len(seis_ensemble.member) == 2
        # Test atomic read of members just written
        # Note exclude_objects now deletes exlcuded from member vector
        # so we don't get here without passing the assert immediately above
        for i in range(len(seis_ensemble.member)):
            res = self.db.read_data(
                seis_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source"],
                collection="wf_Seismogram",
            )
            assert res.live
            assert np.isclose(seis_ensemble.member[i].data, res.data).all()

        # Repeat same test as immediately above using gridfs storage mode
        # and adding a second history node
        seis_ensemble = copy.deepcopy(seis_ensemble0)
        logging_helper.info(seis_ensemble.member[0], "2", "save_data")
        logging_helper.info(seis_ensemble.member[1], "2", "save_data")
        logging_helper.info(seis_ensemble.member[2], "2", "save_data")
        seis_ensemble = self.db.save_ensemble_data(
            seis_ensemble,
            mode="promiscuous",
            storage_mode="gridfs",
            exclude_objects=[1],
        )
        # Test atomic read of members just written
        # Note exclude_objects use above is why we add the conditional
        self.db.database_schema.set_default("wf_Seismogram", "wf")
        for i in range(len(seis_ensemble.member)):
            res = self.db.read_data(
                seis_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source"],
            )
        assert res.live
        assert np.isclose(seis_ensemble.member[i].data, res.data).all()

        # Now repeat both of the above using the new shorter named
        # function save_data (previously only supported atomic data)
        # we don't use exclude as that is is expected to be deprecated
        seis_ensemble = copy.deepcopy(seis_ensemble0)
        seis_ensemble = self.db.save_data(
            seis_ensemble,
            mode="promiscuous",
            storage_mode="file",
            dir="python/tests/data/",
            dfile="test_db_output",
            return_data=True,
        )
        assert seis_ensemble.live
        assert len(seis_ensemble.member) == 3
        self.db.database_schema.set_default("wf_Seismogram", "wf")
        # using atomic reader for now - ensemble reader is tested
        # in test_read_ensemble_data
        for i in range(len(seis_ensemble.member)):
            res = self.db.read_data(
                seis_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source"],
            )
        assert res.live
        assert np.isclose(seis_ensemble.member[i].data, res.data).all()

        # Repeat same test as immediately above using gridfs storage mode
        # and adding a second history node
        seis_ensemble = copy.deepcopy(seis_ensemble0)
        logging_helper.info(seis_ensemble.member[0], "2", "save_data")
        logging_helper.info(seis_ensemble.member[1], "2", "save_data")
        logging_helper.info(seis_ensemble.member[2], "2", "save_data")
        seis_ensemble = self.db.save_data(
            seis_ensemble,
            mode="promiscuous",
            storage_mode="gridfs",
            return_data=True,
        )
        assert len(seis_ensemble.member) == 3
        self.db.database_schema.set_default("wf_Seismogram", "wf")
        # using atomic reader for now - ensemble reader is tested
        # in test_read_ensemble_data
        for i in range(len(seis_ensemble.member)):
            res = self.db.read_data(
                seis_ensemble.member[i]["_id"],
                mode="promiscuous",
                normalize=["site", "source"],
            )
            assert res.live
            assert np.isclose(seis_ensemble.member[i].data, res.data).all()

    def test_read_ensemble_data(self):
        """
        Test function for reading ensembles.  This file has multiple
        tests for different features.  It currently contains
        legacy tests of methods that may be depcrecated in the
        future when only read_data and read_distributed_data
        will be the mspass readers.   Be warned this set of tests
        will need to be altered when and if the old ensmeble
        functions are removed.
        """
        self.db.drop_collection("wf_TimeSeries")
        self.db.drop_collection("wf_Seismogram")

        ts1 = copy.deepcopy(self.test_ts)
        ts2 = copy.deepcopy(self.test_ts)
        ts3 = copy.deepcopy(self.test_ts)
        logging_helper.info(ts1, "1", "deepcopy")
        logging_helper.info(ts2, "1", "deepcopy")
        logging_helper.info(ts3, "1", "deepcopy")
        ts_ensemble = TimeSeriesEnsemble()
        ts_ensemble.member.append(ts1)
        ts_ensemble.member.append(ts2)
        ts_ensemble.member.append(ts3)
        ts_ensemble.set_live()

        self.db.database_schema.set_default("wf_TimeSeries", "wf")
        # as a legacy function wrapper for this method sets optional
        # return_data True.
        ensemble_saved = self.db.save_ensemble_data(
            ts_ensemble, mode="promiscuous", storage_mode="gridfs"
        )
        # Test legacy reader with cursor
        # this works because there is only one previous write to
        # wf_TimeSeries
        cursor = self.db.wf_TimeSeries.find({})
        res = self.db.read_ensemble_data(
            cursor,
            ensemble_metadata={"key1": "value1", "key2": "value2"},
            mode="cautious",
            normalize=["source", "site", "channel"],
        )
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, ts_ensemble.member[i].data).all()
        # test ensemble_metadata
        ts_ensemble_metadata = Metadata(res)
        assert (
            "key1" in ts_ensemble_metadata and ts_ensemble_metadata["key1"] == "value1"
        )
        assert (
            "key2" in ts_ensemble_metadata and ts_ensemble_metadata["key2"] == "value2"
        )
        # test legacy use of list of ObjectIds.
        # That should now generate a TypeError exception we test here
        with pytest.raises(TypeError, match="for arg0"):
            res = self.db.read_ensemble_data(
                [
                    ensemble_saved.member[0]["_id"],
                    ensemble_saved.member[1]["_id"],
                    ensemble_saved.member[2]["_id"],
                ],
                ensemble_metadata={"key1": "value1", "key2": "value2"},
                mode="cautious",
                normalize=["source", "site", "channel"],
            )

        # test read_data method with cursor which is the read_data signal for ensemble
        # hence we use read_data not the legacy function
        # With current implementation this is identical to call
        # above with read_ensmble_data
        cursor = self.db["wf_TimeSeries"].find({})
        res = self.db.read_data(
            cursor,
            ensemble_metadata={"key1": "value1", "key2": "value2"},
            mode="cautious",
            normalize=["source", "site", "channel"],
        )

        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, ts_ensemble.member[i].data).all()
        # test ensemble_metadata
        ts_ensemble_metadata = Metadata(res)
        assert (
            "key1" in ts_ensemble_metadata and ts_ensemble_metadata["key1"] == "value1"
        )
        assert (
            "key2" in ts_ensemble_metadata and ts_ensemble_metadata["key2"] == "value2"
        )

        # repeat for Seismogram - these tests are a near copy
        # of above for TimeSeries
        seis1 = copy.deepcopy(self.test_seis)
        seis2 = copy.deepcopy(self.test_seis)
        seis3 = copy.deepcopy(self.test_seis)
        logging_helper.info(seis1, "1", "deepcopy")
        logging_helper.info(seis2, "1", "deepcopy")
        logging_helper.info(seis3, "1", "deepcopy")
        seis_ensemble = SeismogramEnsemble()
        seis_ensemble.member.append(seis1)
        seis_ensemble.member.append(seis2)
        seis_ensemble.member.append(seis3)
        seis_ensemble.set_live()
        self.db.database_schema.set_default("wf_Seismogram", "wf")
        # as a legacy function wrapper for this method sets optional
        # return_data True.
        ensemble_saved = self.db.save_ensemble_data(
            seis_ensemble, mode="promiscuous", storage_mode="gridfs"
        )
        # Test legacy reader with cursor
        # this works because there is only one previous write to
        # wf_TimeSeries
        cursor = self.db.wf_Seismogram.find({})
        res = self.db.read_ensemble_data(
            cursor,
            ensemble_metadata={"key1": "value1", "key2": "value2"},
            mode="cautious",
            normalize=["source", "site"],
        )
        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, seis_ensemble.member[i].data).all()
        # test ensemble_metadata
        seis_ensemble_metadata = Metadata(res)
        assert (
            "key1" in seis_ensemble_metadata
            and seis_ensemble_metadata["key1"] == "value1"
        )
        assert (
            "key2" in seis_ensemble_metadata
            and seis_ensemble_metadata["key2"] == "value2"
        )
        # test legacy use of list of ObjectIds.
        # That should now generate a TypeError exception we test here
        with pytest.raises(TypeError, match="for arg0"):
            res = self.db.read_ensemble_data(
                [
                    ensemble_saved.member[0]["_id"],
                    ensemble_saved.member[1]["_id"],
                    ensemble_saved.member[2]["_id"],
                ],
                ensemble_metadata={"key1": "value1", "key2": "value2"},
                mode="cautious",
                normalize=["source", "site"],
            )

        # test read_data method with cursor which is the read_data signal for ensemble
        # hence we use read_data not the legacy function
        # With current implementation this is identical to call
        # above with read_ensmble_data
        cursor = self.db["wf_Seismogram"].find({})
        res = self.db.read_data(
            cursor,
            ensemble_metadata={"key1": "value1", "key2": "value2"},
            mode="cautious",
            normalize=["source", "site"],
        )

        assert len(res.member) == 3
        for i in range(3):
            assert np.isclose(res.member[i].data, seis_ensemble.member[i].data).all()
        # test ensemble_metadata
        seis_ensemble_metadata = Metadata(res)
        assert (
            "key1" in seis_ensemble_metadata
            and seis_ensemble_metadata["key1"] == "value1"
        )
        assert (
            "key2" in seis_ensemble_metadata
            and seis_ensemble_metadata["key2"] == "value2"
        )

    def test_get_response(self):
        inv = obspy.read_inventory("python/tests/data/TA.035A.xml")
        net = "TA"
        sta = "035A"
        loc = ""
        time = 1263254400.0 + 100.0
        for chan in ["BHE", "BHN", "BHZ"]:
            r = self.db.get_response(net, sta, chan, loc, time)
            r0 = inv.get_response("TA.035A..BHE", time)
            assert r == r0
        with pytest.raises(MsPASSError, match="missing one of required arguments"):
            self.db.get_response()
        assert self.db.get_response(net="TA", sta="036A", chan="BHE", time=time) is None

    def teardown_class(self):
        import glob

        try:
            os.remove("python/tests/data/test_db_output")
            os.remove("python/tests/data/test_mseed_output")
        except OSError:
            pass
        client = DBClient("localhost")
        client.drop_database("dbtest")
        # version 2 tests have random file name creation we handle this
        # way with glob
        filelist = glob.glob("./*-binary")  # binary format writes to undefine dfile
        filelist2 = glob.glob("./*.ms")  # mseed writes to undefined to here
        try:
            for f in filelist:
                os.remove(f)
            for f in filelist2:
                os.remove(f)
        except OSError:
            pass

    def test_load_source_site_channel_metadata(self):
        ts = copy.deepcopy(self.test_ts)

        # insert arbitrary site/channel/source data in db
        site_id = ObjectId()
        channel_id = ObjectId()
        source_id = ObjectId()
        ts["site_id"] = site_id
        ts["channel_id"] = channel_id
        ts["source_id"] = source_id
        self.db["site"].insert_one(
            {
                "_id": site_id,
                "net": "net10",
                "sta": "sta10",
                "loc": "loc10",
                "lat": 5.0,
                "lon": 5.0,
                "elev": 5.0,
                "starttime": 1.0,
                "endtime": 1.0,
            }
        )
        self.db["channel"].insert_one(
            {
                "_id": channel_id,
                "net": "net10",
                "sta": "sta10",
                "loc": "loc10",
                "chan": "chan10",
                "lat": 5.0,
                "lon": 5.0,
                "elev": 5.0,
                "starttime": 1.0,
                "endtime": 1.0,
                "edepth": 5.0,
                "vang": 5.0,
                "hang": 5.0,
            }
        )
        self.db["source"].insert_one(
            {
                "_id": source_id,
                "lat": 5.0,
                "lon": 5.0,
                "time": 1.0,
                "depth": 5.0,
                "magnitude": 5.0,
            }
        )

        # load site/channel/source data into the mspass object
        self.db.load_site_metadata(
            ts, exclude_keys=["site_elev"], include_undefined=False
        )
        assert ts["net"] == "net10"
        assert ts["sta"] == "sta10"
        assert ts["site_lat"] == 5.0
        assert ts["site_lon"] == 5.0
        assert "site_elev" not in ts
        assert ts["site_starttime"] == 1.0
        assert ts["site_endtime"] == 1.0

        self.db.load_source_metadata(
            ts, exclude_keys=["source_magnitude"], include_undefined=False
        )
        assert ts["source_lat"] == 5.0
        assert ts["source_lon"] == 5.0
        assert ts["source_depth"] == 5.0
        assert ts["source_time"] == 1.0
        assert "source_magnitude" not in ts

        self.db.load_channel_metadata(
            ts, exclude_keys=["channel_edepth"], include_undefined=False
        )
        assert ts["chan"] == "chan10"
        assert ts["loc"] == "loc10"
        assert ts["channel_hang"] == 5.0
        assert ts["channel_vang"] == 5.0
        assert ts["channel_lat"] == 5.0
        assert ts["channel_lon"] == 5.0
        assert ts["channel_elev"] == 5.0
        assert "channel_edepth" not in ts
        assert ts["channel_starttime"] == 1.0
        assert ts["channel_endtime"] == 1.0

    def _setup_aws_mock(self):
        mock_inst_s3 = mock_aws()
        # mock_inst_lambda = mock_lambda()

    @mock_aws
    def test_index_and_read_s3_continuous(self):
        #   Test _read_data_from_s3_continuous
        #   First upload a miniseed object to the mock server.

        # Monkey patch boto3.client to ensure all S3 clients disable checksum validation
        original_client = boto3.client

        def patched_client(*args, **kwargs):
            client = original_client(*args, **kwargs)
            if args and args[0] == "s3":
                # Patch the get_object method for this S3 client
                original_get_object = client.get_object

                def patched_get_object(**get_kwargs):
                    # Force disable checksum validation for moto compatibility
                    get_kwargs["ChecksumMode"] = "DISABLED"
                    return original_get_object(**get_kwargs)

                client.get_object = patched_get_object
            return client

        # Apply the monkey patch
        boto3.client = patched_client

        try:
            s3_client = boto3.client(
                "s3",
                region_name="us-east-1",
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )
            src_bucket = "scedc-pds"
            s3 = boto3.resource("s3", region_name="us-east-1")

            s3_client.create_bucket(Bucket=src_bucket)
            mseed_path = "python/tests/data/CICAC__HNZ___2017005.ms"
            mseed_name = "CICAC__HNZ___2017005.ms"
            mseed_st = obspy.read(mseed_path)
            mseed_st.merge()
            stats = mseed_st[0].stats
            src_mseed_doc = dict()
            src_mseed_doc["year"] = "2017"
            src_mseed_doc["day_of_year"] = "005"
            src_mseed_doc["sta"] = stats["station"]
            src_mseed_doc["net"] = stats["network"]
            src_mseed_doc["chan"] = stats["channel"]
            if "location" in stats and stats["location"]:
                src_mseed_doc["loc"] = stats["location"]
            src_mseed_doc["sampling_rate"] = stats["sampling_rate"]
            src_mseed_doc["delta"] = 1.0 / stats["sampling_rate"]
            src_mseed_doc["starttime"] = stats["starttime"].timestamp
            if "npts" in stats and stats["npts"]:
                src_mseed_doc["npts"] = stats["npts"]
            src_mseed_doc["storage_mode"] = "s3_continuous"
            src_mseed_doc["format"] = "mseed"

            mseed_upload_key = (
                "continuous_waveforms/2017/2017_005/CICAC__HNZ___2017005.ms"
            )
            s3_client.upload_file(
                Filename=mseed_path, Bucket=src_bucket, Key=mseed_upload_key
            )
            self.db.index_mseed_s3_continuous(
                s3_client,
                2017,
                5,
                network="CI",
                station="CAC",
                channel="HNZ",
                collection="test_s3_db",
            )
            assert self.db["test_s3_db"].count_documents({}) == 1
            ms_doc = self.db.test_s3_db.find_one()
            shared_items = {
                k: ms_doc[k]
                for k in src_mseed_doc
                if k in ms_doc and src_mseed_doc[k] == ms_doc[k]
            }
            assert len(shared_items) == 11

            del ms_doc["_id"]
            ts = TimeSeries(ms_doc, np.ndarray([0], dtype=np.float64))
            ts.npts = ms_doc["npts"]
            self.db._read_data_from_s3_continuous(
                mspass_object=ts,
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )
            assert ts.data is not None
            assert ts.data == DoubleVector(mseed_st[0].data.astype("float64"))
        finally:
            # Restore the original boto3.client
            boto3.client = original_client

    @mock_aws
    def test_index_and_read_s3_event(self):
        #   First upload a miniseed object to the mock server.
        s3_client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        src_bucket = "scedc-pds"
        s3 = boto3.resource("s3", region_name="us-east-1")

        #   Index the miniseed file
        s3_client.create_bucket(Bucket=src_bucket)
        mseed_path = "python/tests/data/37780584.ms"
        mseed_name = "37780584.ms"
        mseed_st = obspy.read(mseed_path)
        mseed_st.merge()
        stats = mseed_st[0].stats
        src_mseed_doc = dict()
        src_mseed_doc["year"] = "2017"
        src_mseed_doc["day_of_year"] = "005"
        src_mseed_doc["storage_mode"] = "s3_event"
        src_mseed_doc["format"] = "mseed"

        #   Read the data
        mseed_upload_key = "event_waveforms/2017/2017_005/37780584.ms"
        s3_client.upload_file(
            Filename=mseed_path, Bucket=src_bucket, Key=mseed_upload_key
        )
        self.db.index_mseed_s3_event(
            s3_client,
            2017,
            5,
            37780584,
            "37780584.ms",
            dir=None,
            collection="test_s3_db",
        )
        query = {"dfile": "37780584.ms"}
        assert self.db["test_s3_db"].count_documents(query) == 344
        ms_doc = self.db.test_s3_db.find_one(query)

        ts = TimeSeries(ms_doc, np.ndarray([0], dtype=np.float64))
        self.db._read_data_from_s3_event(
            mspass_object=ts,
            dir=ms_doc["dir"],
            dfile=ms_doc["dfile"],
            foff=ms_doc["foff"],
            nbytes=ms_doc["nbytes"],
            format="mseed",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        assert ts.data is not None
        assert ts.data == DoubleVector(mseed_st[0].data.astype("float64"))

    def test_save_and_read_lambda(self):
        mseed_path = "python/tests/data/CICAC__HNZ___2017005.ms"
        mseed_name = "CICAC__HNZ___2017005.ms"

        #   Mock the lambda invoking process
        #   There is no proper tools to do it, so here we implement it in an awkward way
        def mock_make_api_call(self, operation_name, kwarg):
            if operation_name == "Invoke":
                # mseed_st = obspy.read(mseed_path)
                mseed_rawbytes = open(mseed_path, "rb").read()
                mseed_strbytes = base64.b64encode(mseed_rawbytes).decode("utf-8")
                payload = json.dumps(
                    {"ret_type": "content", "ret_value": mseed_strbytes}
                )
                mock_response = {
                    "Code": "200",
                    "Payload": (io.StringIO)(payload),
                }  # for convenience, here we don't window the data.
                return mock_response
            return botocore.client.BaseClient._make_api_call(
                self, operation_name, kwarg
            )

        with patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            ts = self.db._download_windowed_mseed_file(
                "fake_access_key",
                "fake_secret_key",
                2017,
                5,
                network="CI",
                station="CAC",
                channel="HNZ",
            )
            assert ts == obspy.read(mseed_path)

        mseed_st = obspy.read(mseed_path)
        mseed_st.merge()
        stats = mseed_st[0].stats
        src_mseed_doc = dict()
        src_mseed_doc["year"] = "2017"
        src_mseed_doc["day_of_year"] = "005"
        src_mseed_doc["sta"] = stats["station"]
        src_mseed_doc["net"] = stats["network"]
        src_mseed_doc["chan"] = stats["channel"]
        if "location" in stats and stats["location"]:
            src_mseed_doc["loc"] = stats["location"]
        src_mseed_doc["sampling_rate"] = stats["sampling_rate"]
        src_mseed_doc["delta"] = 1.0 / stats["sampling_rate"]
        src_mseed_doc["starttime"] = stats["starttime"].timestamp
        if "npts" in stats and stats["npts"]:
            src_mseed_doc["npts"] = stats["npts"]
        src_mseed_doc["storage_mode"] = "s3_continuous"
        src_mseed_doc["format"] = "mseed"

        ts = TimeSeries(src_mseed_doc, np.ndarray([0], dtype=np.float64))
        ts.npts = src_mseed_doc["npts"]
        with patch("botocore.client.BaseClient._make_api_call", new=mock_make_api_call):
            self.db._read_data_from_s3_lambda(
                mspass_object=ts,
                aws_access_key_id="fake_access_key",
                aws_secret_access_key="fake_secret_key",
            )
        assert ts.data is not None
        assert ts.data == DoubleVector(mseed_st[0].data.astype("float64"))

    def mock_fdsn_get_waveform(*args, **kwargs):
        with open("python/tests/data/index_and_read_fdsn.pickle", "rb") as handle:
            return pickle.load(handle)

    def test_index_and_read_fdsn(self):
        with patch(
            "obspy.clients.fdsn.client.Client.get_waveforms",
            new=self.mock_fdsn_get_waveform,
        ):
            self.db.index_mseed_FDSN(
                "IRIS",
                2010,
                58,
                "IU",
                "ANMO",
                "00",
                "BHZ",
                collection="test_s3_fdsn",
            )
            assert self.db["test_s3_fdsn"].count_documents({}) == 1
            fdsn_doc = self.db.test_s3_fdsn.find_one()
            assert fdsn_doc["provider"] == "IRIS"
            assert fdsn_doc["year"] == "2010"
            assert fdsn_doc["day_of_year"] == "058"

            del fdsn_doc["_id"]
            tmp_ts = TimeSeries(fdsn_doc, np.ndarray([0], dtype=np.float64))
            self.db._read_data_from_fdsn(tmp_ts)
            tmp_st = self.mock_fdsn_get_waveform()
            assert all(a == b for a, b in zip(tmp_ts.data, tmp_st[0].data))

    def test_save_dataframe(self):
        dir = "python/tests/data/"
        pffile = "test_import.pf"
        textfile = "testdb.wfdisc"
        #   To test null value and rename feature

        field_list = [
            "sta",
            "chan",
            "starttime",
            "endtime",
            "dir",
            "dfile",
            "foff",
            "datatype",
            "nsamp",
            "samprate",
            "calper",
            "chanid",
        ]
        rename_dict = {"datatype": "dtype", "starttime": "t0"}
        pf = AntelopePf(os.path.join(dir, pffile))
        attributes = Pf2AttributeNameTbl(pf, tag="wfdisc")
        df = Textfile2Dataframe(
            os.path.join(dir, textfile),
            attribute_names=attributes[0],
            attributes_to_use=field_list,
            rename_attributes=rename_dict,
            parallel=True,
            one_to_one=True,
        )

        save_num = self.db.save_dataframe(
            df,
            "testdataframe",
            parallel=False,
            one_to_one=True,
            null_values=attributes[2],
        )
        assert save_num == 1953
        assert self.db["testdataframe"].count_documents({}) == 1953

        query = {"sta": "112A"}
        cursor = self.db.testdataframe.find(query)
        assert self.db.testdataframe.count_documents(query) == 3
        for doc in cursor:
            assert "calper" not in doc
            assert "chanid" not in doc

    def test_save_textfile(self):
        dir = "python/tests/data/"
        pffile = "test_import.pf"
        textfile = "testdb.wfprocess"

        pf = AntelopePf(os.path.join(dir, pffile))
        attributes = Pf2AttributeNameTbl(pf, tag="wfprocess")
        save_num = self.db.save_textfile(
            os.path.join(dir, textfile),
            collection="testtextfile",
            attribute_names=attributes[0],
            parallel=False,
            one_to_one=False,
        )

        assert save_num == 651
        assert self.db["testtextfile"].count_documents({}) == 651

        query = {"pwfid": 3102}
        assert 1 == self.db.testtextfile.count_documents(query)

        query = {"pwfid": 3752}
        assert 1 == self.db.testtextfile.count_documents(query)

    def test_set_schema(self):
        assert self.db.database_schema._attr_dict["site"]
        self.db.set_schema("mspass_lite.yaml")
        with pytest.raises(KeyError, match="site"):
            self.db.database_schema._attr_dict["site"]

    def test_geoJSON_doc(self):
        """
        Tests only the function geoJSON_doc added Jan 2024 to properly
        create geoJSON records that allow geospatial queries in
        site, channel, and source.  That function is now used in
        Database.save_inventory and Database.save_catalog to always
        save a geoJSOn format location data.   We assume the bulk of the
        code is already tested when those two functions are tested in
        other test functions.  The big thing here is testing the
        bad data input handlers.
        """
        # test null doc input
        doc = geoJSON_doc(22.0, 44.0, key="testpoint")
        assert "testpoint" in doc
        val = doc["testpoint"]
        assert val["type"] == "Point"
        coords = val["coordinates"]
        # note coordinates pair is lon,lat
        assert coords[0] == 44.0
        assert coords[1] == 22.0

        doc = geoJSON_doc(33.0, 55.0, doc=doc, key="testpoint2")
        #  doc should contain both testpoint and testpoint2
        #  This tests update feature of this function
        assert "testpoint" in doc
        assert "testpoint2" in doc
        val = doc["testpoint2"]
        assert val["type"] == "Point"
        coords = val["coordinates"]
        # note coordinates pair is lon,lat
        assert coords[0] == 55.0
        assert coords[1] == 33.0

        # recoverable value test
        doc = geoJSON_doc(20, 270.0, key="recoverable")
        assert "recoverable" in doc
        val = doc["recoverable"]
        coords = val["coordinates"]
        # use is_close because -90 is computed but normal floating point math
        # for that simple calculation would allow an == to also work
        assert np.isclose(coords[0], -90.0)
        assert coords[1] == 20.0

    def test_database_serialization_cycle(self):
        """
        Test complete serialization/deserialization cycle for Database objects.
        This tests the core functionality needed for distributed computing.
        """
        # Create a database instance
        client = DBClient("localhost")
        db = Database(client, "test_serialization")

        # Test __getstate__ - should return serializable state
        state = db.__getstate__()
        assert isinstance(state, dict)
        assert "_mspass_db_host" in state
        assert "_BaseObject__codec_options" in state
        assert "_Database__client" not in state  # Should be excluded
        assert "stedronsky" not in state  # Should be excluded

        # Test __setstate__ - should recreate the object
        new_db = Database.__new__(Database)
        new_db.__setstate__(state)

        # Verify the deserialized object has correct attributes
        assert new_db.name == db.name
        assert hasattr(new_db, "_Database__client")
        assert hasattr(new_db, "_BaseObject__codec_options")
        assert hasattr(new_db, "stedronsky")
        assert new_db._BaseObject__codec_options is not None

    def test_client_serialization_cycle(self):
        """
        Test complete serialization/deserialization cycle for DBClient objects.
        This tests the core functionality needed for distributed computing.
        """
        # Create a client instance
        client = DBClient("localhost")

        # Test __getstate__ - should return serializable state
        state = client.__getstate__()
        assert isinstance(state, dict)
        assert "host" in state
        assert "args" in state
        assert "kwargs" in state
        assert "default_database_name" in state

        # Test __setstate__ - should recreate the object
        new_client = DBClient.__new__(DBClient)
        new_client.__setstate__(state)

        # Verify the deserialized object has correct attributes
        assert new_client._mspass_db_host == client._mspass_db_host
        assert hasattr(new_client, "_mspass_connection_args")
        assert hasattr(new_client, "_mspass_connection_kwargs")

    def test_collection_serialization_cycle(self):
        """
        Test complete serialization/deserialization cycle for Collection objects.
        This tests the core functionality needed for distributed computing.
        """
        # Create a collection instance
        client = DBClient("localhost")
        db = Database(client, "test_serialization")
        collection = Collection(db, "test_collection")

        # Test __getstate__ - should return serializable state
        state = collection.__getstate__()
        assert isinstance(state, dict)
        assert "_BaseObject__codec_options" in state
        assert "_mspass_db_name" in state
        assert "_mspass_db_host" in state
        assert "_Collection__database" not in state  # Should be excluded

        # Test __setstate__ - should recreate the object
        new_collection = Collection.__new__(Collection)
        new_collection.__setstate__(state)

        # Verify the deserialized object has correct attributes
        assert hasattr(new_collection, "_Collection__database")
        assert hasattr(new_collection, "_BaseObject__codec_options")
        assert hasattr(new_collection, "_codec_options")

        with pytest.raises(ValueError, match="Illegal geographic input"):
            doc = geoJSON_doc(20, 400)
