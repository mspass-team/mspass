#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This a pytest script to tesst the class called ApplyCalibEngine.

Created on Thu Jan 23 07:50:25 2025

@author: pavlis
"""
from mspasspy.algorithms.calib import ApplyCalibEngine
import sys

sys.path.append("python/tests")
from helper import get_live_timeseries

from mspasspy.db.client import DBClient
from mspasspy.ccore.seismic import TimeSeriesEnsemble, TimeSeries, DoubleVector
from mspasspy.ccore.utility import MsPASSError

import numpy as np

from obspy import read_inventory, UTCDateTime

import pytest


class TestApplyCalibEngine:
    def setup_class(self):
        client = DBClient()
        client.drop_database("test_calib")
        self.db = client.get_database("test_calib")
        xmlfile = "./python/tests/data/calib_teststa.xml"
        inv = read_inventory(xmlfile, format="STATIONXML")
        self.db.save_inventory(inv)
        self.ts = get_live_timeseries()

    def teardown_class(self):
        client = DBClient()
        client.drop_database("test_calib")

    def test_constructor(self):
        """
        This method does a partial test of the constructor.  Some
        error handlers are difficult to simulate that handle bad
        response data.  This tests the most common known issues
        that are hadled.
        """
        # this should be clean for the test data
        engine = ApplyCalibEngine(self.db)
        # the actual test data have more entries than this
        # many channels are dropped because they are ot velocity channels
        # the test file creates 324 entries of which 192 are acceptible
        # to this application
        assert len(engine.calib) == 192  # number of entries in test file

        # These will all raise an exception from no data surviving
        # the first somewhat duplicates above but is worh doing
        # to tess excepion handling
        with pytest.raises(MsPASSError, match="Database has no valid response data"):
            engine = ApplyCalibEngine(self.db, ounits=["foobar"], verbose=False)
        with pytest.raises(MsPASSError, match="Database has no valid response data"):
            engine = ApplyCalibEngine(self.db, ounits=["foobar"], verbose=True)

        # I don't tink the test data test this condition so we repeat
        # similar to above
        with pytest.raises(MsPASSError, match="Database has no valid response data"):
            engine = ApplyCalibEngine(
                self.db, response_data_key="foobar", verbose=False
            )
        with pytest.raises(MsPASSError, match="Database has no valid response data"):
            engine = ApplyCalibEngine(self.db, response_data_key="foobar", verbose=True)

        # TODO:  could add a bad response file to test file to test handling
        # of block where the response data is invalid.

    def test_apply(self):
        # create coopies of test timeseries that will match channel docs
        # note testing this TimeSeriesEnsemble also tests the atomic
        # section of apply as apply calls itself in a loop for ensembles
        e = TimeSeriesEnsemble()
        # build an ensemle of BHZ::00 channels - all should work
        # the weird endtime query is needed becaue the ANMO data is flawed
        cursor = self.db.channel.find(
            {
                "chan": "BHZ",
                "loc": "00",
                "endtime": {"$gt": UTCDateTime("2002-01-01").timestamp},
            },
        )

        for doc in cursor:
            # print_metadata(doc)
            print(
                doc["sta"], UTCDateTime(doc["starttime"]), UTCDateTime(doc["endtime"])
            )
            ts = TimeSeries(self.ts)
            # all the test data have loc set - watch out if you change it
            for k in ["net", "sta", "chan", "loc"]:
                ts[k] = doc[k]
            ts.erase("calib")
            ts["channel_id"] = doc["_id"]
            # ts has random numbers - replace with ones
            ts.data = DoubleVector(np.ones(ts.npts))
            e.member.append(ts)
        # copy to allow refreshing e below
        e0 = TimeSeriesEnsemble(e)

        engine = ApplyCalibEngine(self.db)
        # These were extracted from test file
        # if the test data changes these willl need to change
        calib_expected = {
            "ADK": float(0.9347279474309003),
            "AFI": float(1.1048783694646975),
            "ANMO": float(1.1564985387640963),
        }
        e = engine.apply_calib(e)
        self._validate_BHZ(e)

        # test handling of dead data
        e = TimeSeriesEnsemble(e0)
        e.member[1].kill()
        e = engine.apply_calib(e)
        self._validate_BHZ(e)

        # test error handlers
        # first when calib is already present it is altered but
        # sample data should only be multiplied by match used
        # b apply_calib
        e = TimeSeriesEnsemble(e0)
        for i in range(len(e.member)):
            e.member[i]["calib"] = 2.0
        e = engine.apply_calib(e)
        self._validate_BHZ(e)
        for d in e.member:
            sta = d["sta"]
            calib = d["calib"]
            assert np.isclose(calib, calib_expected[sta] * 2.0)
            # each entry should have an elog entry
            assert d.elog.size() > 0

        ts0 = TimeSeries(e0.member[0])
        # test behavior when channel_id is not defined
        ts = TimeSeries(ts0)
        ts.erase("channel_id")
        ts = engine.apply_calib(ts)
        assert ts.dead()
        assert ts.elog.size() > 0

        ts = TimeSeries(ts0)
        ts.erase("channel_id")
        ts = engine.apply_calib(ts, kill_if_undefined=False)
        assert ts.live
        assert ts.elog.size() > 0

        # test illegal type for arg0
        with pytest.raises(ValueError, match="Illegal input data type"):
            engine.apply_calib("foo")

    def _validate_BHZ(self, e):
        """
        Validates ensemble procssed by apply_calib with
        hard wired constants for response files used in this
        test.  Multiplier just be 1.0 for default behavior.
        apply_calib multiplies any existing calib value so we use
        that in one test to check that behavior.
        """
        calib_expected = {
            "ADK": float(0.9347279474309003),
            "AFI": float(1.1048783694646975),
            "ANMO": float(1.1564985387640963),
        }
        for d in e.member:
            if d.live:
                ce = calib_expected[d["sta"]]
                assert np.isclose(d.data, ce).all()
            else:
                # dead data should be ignored
                assert np.isclose(d.data, 1.0).all()
