import pytest
import numpy as np
import obspy
import bson.objectid

from mspasspy.ccore import (Metadata,
                            MetadataDefinitions,
                            TimeSeries,
                            ErrorLogger,
                            Vector)
from mspasspy.io.converter import (dict2Metadata, 
                                   Metadata2dict, 
                                   TimeSeries2Trace, 
                                   Seismogram2Stream, 
                                   Trace2TimeSeries,
                                   Stream2Seismogram)

def setup_function(function):
    ts_size = 255    
    sampling_rate = 20.0


    dict1 = {'network': 'IU', 'station': 'ANMO',
              'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
              'npts': ts_size, 'sampling_rate': sampling_rate,
              'channel': 'BHE', 'live': True, '_id': bson.objectid.ObjectId()}
    dict2 = {'network': 'IU', 'station': 'ANMO',
              'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
              'npts': ts_size, 'sampling_rate': sampling_rate,
              'channel': 'BHN'}
    dict3 = {'network': 'IU', 'station': 'ANMO',
              'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
              'npts': ts_size, 'sampling_rate': sampling_rate,
              'channel': 'BHZ'}
    tr1 = obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=dict1)
    tr2 = obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=dict2)
    tr3 = obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=dict3)
    stream = obspy.Stream(traces=[tr1, tr2, tr3])

    md1 = Metadata()
    md1.put('network', 'IU')
    md1.put('npts', ts_size)
    md1.put('sampling_rate', sampling_rate)
    md1.put('live', True)

    ts1 = TimeSeries()
    ts1.s = Vector(np.random.rand(ts_size))
    ts1.live = True
    ts1.dt = 1/sampling_rate
    ts1.t0 = 0
    ts1.ns = ts_size
    # TODO: need to bind the constructor that can do TimeSeries(md1)
    ts1.put('network', 'IU')
    ts1.put('npts', ts_size)
    ts1.put('sampling_rate', sampling_rate)

    seismogram = Seismogram()
    # TODO: the default of seismogram.tref is UTC which is inconsistent with the default 
    # for TimeSeries()
    # TODO: It would be nice to have dmatrix support numpy.ndarray as input
    seismogram.u = dmatrix(3, ts_size)
    for i in range(3):
        for j in range(ts_size):
            seismogram.u[i,j] = np.random.rand()
    
    seismogram.live = True
    seismogram.dt = 1/sampling_rate
    seismogram.t0 = 0
    seismogram.ns = ts_size
    # FIXME: if the following key is network, the Seismogram2Stream will error out 
    # when calling TimeSeries2Trace internally due to the issue when mdef.is_defined(k) 
    # returns True but k is an alias, the mdef.type(k) will error out.
    seismogram.put('net', 'IU')
    seismogram.put('npts', ts_size)
    seismogram.put('sampling_rate', sampling_rate)

    # TODO: Ideally, these two variable should not be required. Default behavior
    # needed such that mdef will be constructed with the default yaml file, and
    # the elog can be converted to string stored in the dictionary
    mdef = MetadataDefinitions()
    elog = ErrorLogger()

def test_dict2Metadata():
    md = dict2Metadata(dict1, mdef, elog)
    assert md.get('network') == dict1['network']
    assert md.get('station') == dict1['station']
    assert md.get('npts') == dict1['npts']
    assert md.get('sampling_rate') == dict1['sampling_rate']
    assert md.get('channel') == dict1['channel']

    assert md.get('_id') == str(dict1['_id'])

    # TODO: UTCDateTime is not converted. This can be done by converting 
    # UTCDateTime to string or float with str() or float() or .timestamp
    #assert md.get('starttime') == dict1['starttime']

    # TODO: need to consider the case where an alias is defind in 
    # MetadataDefinitions but not correctly find by a mdef.type()
    # call. This issue applies to many mdef other methods, too. Ideally, 
    # the following line should be tested. Also, this should belong to 
    # a unit test for ccore module.
    # assert len(elog.get_error_log()) == 0

def test_Metadata2dict():
    d = Metadata2dict(md1)
    assert md1.get('network') == d['network']
    assert md1.get('live') == d['live']
    assert md1.get('npts') == d['npts']
    assert md1.get('sampling_rate') == d['sampling_rate']

def test_TimeSeries2Trace():
    tr = TimeSeries2Trace(ts1, mdef)
    assert tr.stats['delta'] == ts1.dt
    assert tr.stats['sampling_rate'] == 1.0/ts1.dt
    assert tr.stats['npts'] == ts1.ns
    assert tr.stats['starttime'] == obspy.core.UTCDateTime(ts1.t0)

    assert tr.stats['network'] == ts1.get('network')
    assert tr.stats['npts'] == ts1.get('npts')
    assert tr.stats['sampling_rate'] == ts1.get('sampling_rate')

def test_Trace2TimeSeries():
    # TODO: aliases handling is not tested. Not clear what the
    #  expected behavior should be. 
    ts = Trace2TimeSeries(tr1, mdef)
    assert all(ts.s == tr1.data)
    assert ts.live == True
    assert ts.dt == tr1.stats.delta
    assert ts.t0 == tr1.stats.starttime.timestamp
    assert ts.ns == tr1.stats.npts

    assert ts.get('network') == tr1.stats['network']
    assert ts.get('station') == tr1.stats['station']
    assert ts.get('channel') == tr1.stats['channel']
    assert ts.get('calib') == tr1.stats['calib']

def test_Seismogram2Stream():
    strm = Seismogram2Stream(seismogram, mdef)
    assert strm[0].stats['delta'] == seismogram.dt
    # FIXME: The sampling_rate defined in Metadata will overwrite 
    # seismogram.dt after the conversion, even if the two are inconsistent.
    assert strm[1].stats['sampling_rate'] == 1.0/seismogram.dt
    assert strm[2].stats['npts'] == seismogram.ns
    assert strm[0].stats['starttime'] == obspy.core.UTCDateTime(seismogram.t0)

    assert strm[1].stats['network'] == seismogram.get('net')
    assert strm[2].stats['npts'] == seismogram.get('npts')

def test_Stream2Seismogram():
    # TODO: need to refine the test as well as the behavior of the function.
    # Right now when cardinal is false, azimuth and dip needs to be defined. 
    seis = Stream2Seismogram(stream, mdef, cardinal = True)
    assert all(np.array(seis.u)[0] == stream[0].data)
    assert all(np.array(seis.u)[1] == stream[1].data)
    assert all(np.array(seis.u)[2] == stream[2].data)