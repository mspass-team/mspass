import pytest
import numpy as np
import obspy
import bson.objectid
import sys

sys.path.append("python/tests")
from helper import (get_live_seismogram,
                    get_live_timeseries,
                    get_live_timeseries_ensemble,
                    get_live_seismogram_ensemble)

from mspasspy.ccore import (DoubleVector,
                            dmatrix,
                            ErrorLogger,
                            MDtype,
                            Metadata,
                            MetadataDefinitions,
                            Seismogram,
                            TimeSeries,
                            SeismogramEnsemble,
                            TimeSeriesEnsemble)
from mspasspy.io.converter import (dict2Metadata, 
                                   Metadata2dict, 
                                   TimeSeries2Trace, 
                                   Seismogram2Stream, 
                                   Trace2TimeSeries,
                                   Stream2Seismogram,
                                   TimeSeriesEnsemble2Stream,
                                   Stream2TimeSeriesEnsemble,
                                   SeismogramEnsemble2Stream,
                                   Stream2SeismogramEnsemble)

def setup_function(function):
    ts_size = 255    
    sampling_rate = 20.0


    function.dict1 = {'network': 'IU', 'station': 'ANMO',
                      'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
                      'npts': ts_size, 'sampling_rate': sampling_rate,
                      'channel': 'BHE', 
                      'live': True, '_id': bson.objectid.ObjectId(),
                      'jdate': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
                      'date_str': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
                      'not_defined_date': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000)}
    function.dict2 = {'network': 'IU', 'station': 'ANMO',
                      'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
                      'npts': ts_size, 'sampling_rate': sampling_rate,
                      'channel': 'BHN'}
    function.dict3 = {'network': 'IU', 'station': 'ANMO',
                      'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
                      'npts': ts_size, 'sampling_rate': sampling_rate,
                      'channel': 'BHZ'}
    function.tr1 = obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=function.dict1)
    function.tr2 = obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=function.dict2)
    function.tr3 = obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=function.dict3)
    function.stream = obspy.Stream(traces=[function.tr1, function.tr2, function.tr3])

    function.md1 = Metadata()
    function.md1.put('network', 'IU')
    function.md1.put('npts', ts_size)
    function.md1.put('sampling_rate', sampling_rate)
    function.md1.put('live', True)

    function.ts1 = TimeSeries()
    function.ts1.s = DoubleVector(np.random.rand(ts_size))
    function.ts1.live = True
    function.ts1.dt = 1/sampling_rate
    function.ts1.t0 = 0
    function.ts1.npts = ts_size
    # TODO: need to bind the constructor that can do TimeSeries(md1)
    function.ts1.put('net', 'IU')
    function.ts1.put('npts', ts_size)
    function.ts1.put('sampling_rate', sampling_rate)

    function.seismogram = Seismogram()
    # TODO: the default of seismogram.tref is UTC which is inconsistent with the default 
    # for TimeSeries()
    # TODO: It would be nice to have dmatrix support numpy.ndarray as input
    function.seismogram.u = dmatrix(3, ts_size)
    for i in range(3):
        for j in range(ts_size):
            function.seismogram.u[i,j] = np.random.rand()
    
    function.seismogram.live = True
    function.seismogram.dt = 1/sampling_rate
    function.seismogram.t0 = 0
    function.seismogram.npts = ts_size
    # FIXME: if the following key is network, the Seismogram2Stream will error out 
    # when calling TimeSeries2Trace internally due to the issue when mdef.is_defined(k) 
    # returns True but k is an alias, the mdef.type(k) will error out.
    function.seismogram.put('net', 'IU')
    function.seismogram.put('npts', ts_size)
    function.seismogram.put('sampling_rate', sampling_rate)

    # TODO: Ideally, these two variable should not be required. Default behavior
    # needed such that mdef will be constructed with the default yaml file, and
    # the elog can be converted to string stored in the dictionary
    # function.mdef = MetadataDefinitions()
    # function.elog = ErrorLogger()
    # function.mdef.add('date_str', 'string date for testing', MDtype.String)

def test_dict2Metadata():
    md = dict2Metadata(test_dict2Metadata.dict1)
    assert md.get('network') == test_dict2Metadata.dict1['network']
    assert md.get('station') == test_dict2Metadata.dict1['station']
    assert md.get('npts') == test_dict2Metadata.dict1['npts']
    assert md.get('sampling_rate') == test_dict2Metadata.dict1['sampling_rate']
    assert md.get('channel') == test_dict2Metadata.dict1['channel']

    assert md.get('_id') == test_dict2Metadata.dict1['_id']

    assert md.get('starttime') == test_dict2Metadata.dict1['starttime']
    assert md.get('jdate') == test_dict2Metadata.dict1['jdate']
    assert md.get('date_str') == test_dict2Metadata.dict1['date_str']

def test_Metadata2dict():
    d = Metadata2dict(test_Metadata2dict.md1)
    assert test_Metadata2dict.md1.get('network') == d['network']
    assert test_Metadata2dict.md1.get('live') == d['live']
    assert test_Metadata2dict.md1.get('npts') == d['npts']
    assert test_Metadata2dict.md1.get('sampling_rate') == d['sampling_rate']

def test_TimeSeries2Trace():
    tr = TimeSeries2Trace(test_TimeSeries2Trace.ts1)
    assert tr.stats['delta'] == test_TimeSeries2Trace.ts1.dt
    assert tr.stats['sampling_rate'] == 1.0/test_TimeSeries2Trace.ts1.dt
    assert tr.stats['npts'] == test_TimeSeries2Trace.ts1.npts
    assert tr.stats['starttime'] == obspy.core.UTCDateTime(test_TimeSeries2Trace.ts1.t0)

    assert tr.stats['net'] == test_TimeSeries2Trace.ts1.get('net')
    assert tr.stats['npts'] == test_TimeSeries2Trace.ts1.get('npts')
    assert tr.stats['sampling_rate'] == test_TimeSeries2Trace.ts1.get('sampling_rate')

def test_Trace2TimeSeries():
    # TODO: aliases handling is not tested. Not clear what the
    #  expected behavior should be. 
    ts = Trace2TimeSeries(test_Trace2TimeSeries.tr1)
    assert all(ts.s == test_Trace2TimeSeries.tr1.data)
    assert ts.live == True
    assert ts.dt == test_Trace2TimeSeries.tr1.stats.delta
    assert ts.t0 == test_Trace2TimeSeries.tr1.stats.starttime.timestamp
    assert ts.npts == test_Trace2TimeSeries.tr1.stats.npts

    assert ts.get('network') == test_Trace2TimeSeries.tr1.stats['network']
    assert ts.get('station') == test_Trace2TimeSeries.tr1.stats['station']
    assert ts.get('channel') == test_Trace2TimeSeries.tr1.stats['channel']
    assert ts.get('calib') == test_Trace2TimeSeries.tr1.stats['calib']

def test_Seismogram2Stream():
    strm = Seismogram2Stream(test_Seismogram2Stream.seismogram)
    assert strm[0].stats['delta'] == test_Seismogram2Stream.seismogram.dt
    # FIXME: The sampling_rate defined in Metadata will overwrite 
    # seismogram.dt after the conversion, even if the two are inconsistent.
    assert strm[1].stats['sampling_rate'] == 1.0/test_Seismogram2Stream.seismogram.dt
    assert strm[2].stats['npts'] == test_Seismogram2Stream.seismogram.npts
    assert strm[0].stats['starttime'] == obspy.core.UTCDateTime(test_Seismogram2Stream.seismogram.t0)

    assert strm[1].stats['network'] == test_Seismogram2Stream.seismogram.get('net')
    assert strm[2].stats['npts'] == test_Seismogram2Stream.seismogram.get('npts')

def test_Stream2Seismogram():
    # TODO: need to refine the test as well as the behavior of the function.
    # Right now when cardinal is false, azimuth and dip needs to be defined. 
    seis = Stream2Seismogram(test_Stream2Seismogram.stream, cardinal = True)
    assert all(np.array(seis.u)[0] == test_Stream2Seismogram.stream[0].data)
    assert all(np.array(seis.u)[1] == test_Stream2Seismogram.stream[1].data)
    assert all(np.array(seis.u)[2] == test_Stream2Seismogram.stream[2].data)

def test_TimeSeriesEnsemble_as_Stream():
    # use data object to verify converter
    tse = get_live_timeseries_ensemble(3)
    stream = tse.toStream()
    tse_c = stream.toTimeSeriesEnsemble()
    assert len(tse) == len(tse_c)
    for k in range(3):
        assert all(a == b for a,b in zip(tse.member[k].s, tse_c.member[k].s))

    # dead member is also dead after conversion
    tse.member[0].kill()
    stream = tse.toStream()
    tse_c = stream.toTimeSeriesEnsemble()
    for k in range(3):
        assert tse.member[k].live == tse_c.member[k].live

    # dead member will be an empty object after conversion
    assert len(tse_c.member[0].s) == 0

def test_SeismogramEnsemble_as_Stream():
    seis_e = get_live_seismogram_ensemble(3)
    stream = seis_e.toStream()
    seis_e_c = stream.toSeismogramEnsemble()
    assert len(seis_e) == len(seis_e_c)
    for k in range(3):
        assert all(a.any() == b.any() for a, b in zip(seis_e.member[k].u, seis_e_c.member[k].u))

    # dead member is also dead after conversion
    seis_e.member[0].kill()
    stream = seis_e.toStream()
    seis_e_c = stream.toSeismogramEnsemble()
    for k in range(3):
        assert seis_e_c.member[k].live == seis_e.member[k].live
    assert seis_e_c.member[0].u.rows() == 0
    assert seis_e_c.member[0].u.columns() == 0