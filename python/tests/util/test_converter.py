import numpy as np
import obspy
import bson.objectid
import sys

sys.path.append("python/tests")
from helper import get_live_timeseries_ensemble, get_live_seismogram_ensemble

from mspasspy.ccore.utility import dmatrix, Metadata, AntelopePf
from mspasspy.ccore.seismic import DoubleVector, Seismogram, TimeSeries
from mspasspy.util.converter import (
    dict2Metadata,
    Metadata2dict,
    TimeSeries2Trace,
    Seismogram2Stream,
    Trace2TimeSeries,
    Stream2Seismogram,
    Pf2AttributeNameTbl,
    Textfile2Dataframe,
)


def setup_function(function):
    ts_size = 255
    sampling_rate = 20.0

    function.dict1 = {
        "network": "IU",
        "station": "ANMO",
        "starttime": obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
        "npts": ts_size,
        "sampling_rate": sampling_rate,
        "channel": "BHE",
        "live": True,
        "_id": bson.objectid.ObjectId(),
        "jdate": obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
        "date_str": obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
        "not_defined_date": obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
    }
    function.dict2 = {
        "network": "IU",
        "station": "ANMO",
        "starttime": obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
        "npts": ts_size,
        "sampling_rate": sampling_rate,
        "channel": "BHN",
    }
    function.dict3 = {
        "network": "IU",
        "station": "ANMO",
        "starttime": obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
        "npts": ts_size,
        "sampling_rate": sampling_rate,
        "channel": "BHZ",
    }
    function.tr1 = obspy.Trace(
        data=np.random.randint(0, 1000, ts_size), header=function.dict1
    )
    function.tr2 = obspy.Trace(
        data=np.random.randint(0, 1000, ts_size), header=function.dict2
    )
    function.tr3 = obspy.Trace(
        data=np.random.randint(0, 1000, ts_size), header=function.dict3
    )
    function.stream = obspy.Stream(traces=[function.tr1, function.tr2, function.tr3])

    function.md1 = Metadata()
    function.md1.put("network", "IU")
    function.md1.put("npts", ts_size)
    function.md1.put("sampling_rate", sampling_rate)
    function.md1.put("live", True)

    function.ts1 = TimeSeries()
    function.ts1.data = DoubleVector(np.random.rand(ts_size))
    function.ts1.live = True
    function.ts1.dt = 1 / sampling_rate
    function.ts1.t0 = 0
    function.ts1.npts = ts_size
    # TODO: need to bind the constructor that can do TimeSeries(md1)
    function.ts1.put("net", "IU")
    function.ts1.put("npts", ts_size)
    function.ts1.put("sampling_rate", sampling_rate)

    function.seismogram = Seismogram()
    # TODO: the default of seismogram.tref is UTC which is inconsistent with the default
    # for TimeSeries()
    # TODO: It would be nice to have dmatrix support numpy.ndarray as input
    function.seismogram.data = dmatrix(3, ts_size)
    for i in range(3):
        for j in range(ts_size):
            function.seismogram.data[i, j] = np.random.rand()

    function.seismogram.live = True
    function.seismogram.dt = 1 / sampling_rate
    function.seismogram.t0 = 0
    function.seismogram.npts = ts_size
    # FIXME: if the following key is network, the Seismogram2Stream will error out
    # when calling TimeSeries2Trace internally due to the issue when mdef.is_defined(k)
    # returns True but k is an alias, the mdef.type(k) will error out.
    function.seismogram.put("net", "IU")
    function.seismogram.put("npts", ts_size)
    function.seismogram.put("sampling_rate", sampling_rate)

    # TODO: Ideally, these two variable should not be required. Default behavior
    # needed such that mdef will be constructed with the default yaml file, and
    # the elog can be converted to string stored in the dictionary
    # function.mdef = MetadataDefinitions()
    # function.elog = ErrorLogger()
    # function.mdef.add('date_str', 'string date for testing', MDtype.String)


def test_dict2Metadata():
    md = dict2Metadata(test_dict2Metadata.dict1)
    assert md.get("network") == test_dict2Metadata.dict1["network"]
    assert md.get("station") == test_dict2Metadata.dict1["station"]
    assert md.get("npts") == test_dict2Metadata.dict1["npts"]
    assert md.get("sampling_rate") == test_dict2Metadata.dict1["sampling_rate"]
    assert md.get("channel") == test_dict2Metadata.dict1["channel"]

    assert md.get("_id") == test_dict2Metadata.dict1["_id"]

    assert md.get("starttime") == test_dict2Metadata.dict1["starttime"]
    assert md.get("jdate") == test_dict2Metadata.dict1["jdate"]
    assert md.get("date_str") == test_dict2Metadata.dict1["date_str"]


def test_Metadata2dict():
    d = Metadata2dict(test_Metadata2dict.md1)
    assert test_Metadata2dict.md1.get("network") == d["network"]
    assert test_Metadata2dict.md1.get("live") == d["live"]
    assert test_Metadata2dict.md1.get("npts") == d["npts"]
    assert test_Metadata2dict.md1.get("sampling_rate") == d["sampling_rate"]


def test_TimeSeries2Trace():
    tr = TimeSeries2Trace(test_TimeSeries2Trace.ts1)
    assert tr.stats["delta"] == test_TimeSeries2Trace.ts1.dt
    assert tr.stats["sampling_rate"] == 1.0 / test_TimeSeries2Trace.ts1.dt
    assert tr.stats["npts"] == test_TimeSeries2Trace.ts1.npts
    assert tr.stats["starttime"] == obspy.core.UTCDateTime(test_TimeSeries2Trace.ts1.t0)

    assert tr.stats["net"] == test_TimeSeries2Trace.ts1.get("net")
    assert tr.stats["npts"] == test_TimeSeries2Trace.ts1.get("npts")
    assert tr.stats["sampling_rate"] == test_TimeSeries2Trace.ts1.get("sampling_rate")


def test_Trace2TimeSeries():
    # TODO: aliases handling is not tested. Not clear what the
    #  expected behavior should be.
    ts = Trace2TimeSeries(test_Trace2TimeSeries.tr1)
    assert np.isclose(ts.data, test_Trace2TimeSeries.tr1.data).all()
    assert ts.live == True
    assert ts.dt == test_Trace2TimeSeries.tr1.stats.delta
    assert ts.t0 == test_Trace2TimeSeries.tr1.stats.starttime.timestamp
    assert ts.npts == test_Trace2TimeSeries.tr1.stats.npts

    assert ts.get("net") == test_Trace2TimeSeries.tr1.stats["network"]
    assert ts.get("sta") == test_Trace2TimeSeries.tr1.stats["station"]
    assert ts.get("chan") == test_Trace2TimeSeries.tr1.stats["channel"]
    assert ts.get("calib") == test_Trace2TimeSeries.tr1.stats["calib"]


def test_Seismogram2Stream():
    strm = Seismogram2Stream(test_Seismogram2Stream.seismogram)
    assert strm[0].stats["delta"] == test_Seismogram2Stream.seismogram.dt
    # FIXME: The sampling_rate defined in Metadata will overwrite
    # seismogram.dt after the conversion, even if the two are inconsistent.
    assert strm[1].stats["sampling_rate"] == 1.0 / test_Seismogram2Stream.seismogram.dt
    assert strm[2].stats["npts"] == test_Seismogram2Stream.seismogram.npts
    assert strm[0].stats["starttime"] == obspy.core.UTCDateTime(
        test_Seismogram2Stream.seismogram.t0
    )

    assert strm[1].stats["network"] == test_Seismogram2Stream.seismogram.get("net")
    assert strm[2].stats["npts"] == test_Seismogram2Stream.seismogram.get("npts")


def test_Stream2Seismogram():
    # TODO: need to refine the test as well as the behavior of the function.
    # Right now when cardinal is false, azimuth and dip needs to be defined.
    seis = Stream2Seismogram(test_Stream2Seismogram.stream, cardinal=True)
    assert np.isclose(
        np.array(seis.data)[0], test_Stream2Seismogram.stream[0].data
    ).all()
    assert np.isclose(
        np.array(seis.data)[1], test_Stream2Seismogram.stream[1].data
    ).all()
    assert np.isclose(
        np.array(seis.data)[2], test_Stream2Seismogram.stream[2].data
    ).all()


def test_TimeSeriesEnsemble_as_Stream():
    # use data object to verify converter
    tse = get_live_timeseries_ensemble(3)
    stream = tse.toStream()
    assert len(tse.member) == len(stream)
    tse_c = stream.toTimeSeriesEnsemble()
    assert len(tse) == len(tse_c)
    for k in range(3):
        assert np.isclose(tse.member[k].data, tse_c.member[k].data).all()

    # dead member is also dead after conversion
    tse.member[0].kill()
    # Add Emsemble Metadata to verify it gets handled properly
    tse.put_string("foo", "bar")
    tse.put_double("fake_lat", 22.4)
    tse.put_long("fake_evid", 9999)
    stream = tse.toStream()
    tse_c = stream.toTimeSeriesEnsemble()
    for k in range(3):
        assert tse.member[k].live == tse_c.member[k].live
        if tse.member[k].live:
            # the magic 4 here comes from a weird combination of
            # deleting the temp key in tse_c  (-1) + 5 attributes
            # the Trace object converter puts back
            # net, sta, chan, starttime, endtime
            # This test is fragile
            assert len(tse.member[k]) + 4 == len(tse_c.member[k])

    # dead member will be an empty object after conversion
    assert len(tse_c.member[0].data) == 0
    # Confirm the ensemble metadata are carried through
    teststr = tse_c["foo"]
    assert teststr == "bar"
    assert tse_c["fake_lat"] == 22.4
    assert tse_c["fake_evid"] == 9999


def test_SeismogramEnsemble_as_Stream():
    seis_e = get_live_seismogram_ensemble(3)
    stream = seis_e.toStream()
    seis_e_c = stream.toSeismogramEnsemble()
    assert len(seis_e) == len(seis_e_c)
    for k in range(3):
        for i in range(3):
            assert np.isclose(
                seis_e.member[k].data[i], seis_e_c.member[k].data[i]
            ).all()

    # dead member is also dead after conversion
    seis_e.member[0].kill()
    # This tests posting the same junk ensemble metadata as TimeSeriesEnsemble
    seis_e.put_string("foo", "bar")
    seis_e.put_double("fake_lat", 22.4)
    seis_e.put_long("fake_evid", 9999)
    stream = seis_e.toStream()
    seis_e_c = stream.toSeismogramEnsemble()
    for k in range(3):
        assert seis_e_c.member[k].live == seis_e.member[k].live
        if seis_e.member[k].live:
            # the magic 4 here comes from a weird combination of
            # deleting the temp key in tse_c  (-1) + 5 attributes
            # the Trace object converter puts back
            # net, sta, chan, starttime, endtime
            # This test is fragile
            assert len(seis_e.member[k]) + 4 == len(seis_e_c.member[k])
    assert seis_e_c.member[0].data.rows() == 0
    assert seis_e_c.member[0].data.columns() == 0
    assert seis_e_c["foo"] == "bar"
    assert seis_e_c["fake_lat"] == 22.4
    assert seis_e_c["fake_evid"] == 9999


def test_Pf2AttributeNameTbl():
    pf = AntelopePf("python/tests/data/test_import.pf")
    attributes = Pf2AttributeNameTbl(pf, tag="wfprocess")
    names = attributes[0]
    types = attributes[1]
    nullvals = attributes[2]

    assert names == [
        "pwfid",
        "starttime",
        "endtime",
        "time_standard",
        "dir",
        "dfile",
        "foff",
        "dtype",
        "samprate",
        "unkwown",
        "algorithm",
        "lddate",
    ]
    assert len(types) == 12
    assert len(nullvals) == 12
    assert types == [int, float, float, str, str, str, int, str, float, int, str, float]
    assert nullvals["pwfid"] == -1


def test_Textfile2Dataframe():
    pf = AntelopePf("python/tests/data/test_import.pf")
    attributes = Pf2AttributeNameTbl(pf, tag="wfprocess")
    names = attributes[0]
    nullvals = attributes[2]

    textfile = "python/tests/data/testdb.wfprocess"

    for p in [True, False]:
        df = Textfile2Dataframe(textfile, attribute_names=names, parallel=p)
        assert df.shape[0] == 652
        assert df.shape[1] == 12
        assert df.iloc[1].values.tolist() == [
            3103,
            1577912954.62975,
            1577913089.7297499,
            "a",
            "simdata",
            "simdata_22",
            97308.0,
            "c3",
            20.0,
            2703,
            "migsimulation",
            1263164975.47328,
        ]

        #   Test setting null values
        df = Textfile2Dataframe(
            textfile, attribute_names=names, null_values=nullvals, parallel=p
        )
        assert df.shape[0] == 652
        assert df.shape[1] == 12
        assert df.iloc[0].values.tolist() == [
            3102,
            1577912967.53105,
            1577913102.63105,
            "a",
            "simdata",
            "simdata_22",
            None,
            "c3",
            20.0,
            2703,
            "migsimulation",
            1263164975.3866,
        ]

        #   Test turning off one_to_one
        df = Textfile2Dataframe(
            textfile, attribute_names=names, one_to_one=False, parallel=p
        )
        assert df.shape[0] == 651
        assert df.shape[1] == 12

        #   Test add column
        df = Textfile2Dataframe(
            textfile, attribute_names=names, parallel=p, insert_column={"test_col": 1}
        )
        assert df.shape[0] == 652
        assert df.shape[1] == 13
        assert df.at[0, "test_col"] == 1
