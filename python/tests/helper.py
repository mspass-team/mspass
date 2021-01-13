import numpy as np
import obspy
import bson.objectid
import time
from datetime import datetime

from mspasspy.ccore.utility import (AtomicType,
                                    dmatrix)
from mspasspy.ccore.seismic import (Seismogram,
                                    TimeSeries,
                                    TimeSeriesEnsemble,
                                    SeismogramEnsemble,
                                    DoubleVector, TimeReferenceType)

ts_size = 255
sampling_rate = 20.0


def get_live_seismogram():
    seis = Seismogram()
    seis.set_live()
    seis.set_as_origin('test', '0', '0',
                       AtomicType.SEISMOGRAM)
    seis.dt = 1 / sampling_rate
    seis.npts = ts_size
    # seis.put('net', 'IU')
    seis.put('npts', ts_size)
    seis.put('sampling_rate', sampling_rate)
    seis.tref = TimeReferenceType.UTC
    seis.t0 = 0
    seis['delta'] = 0.1
    seis['calib'] = 0.1
    seis['site_id'] = bson.objectid.ObjectId()
    seis['channel_id'] = [bson.objectid.ObjectId()]
    seis['source_id'] = bson.objectid.ObjectId()
    seis.data = dmatrix(3, ts_size)
    for i in range(3):
        for j in range(ts_size):
            seis.data[i, j] = np.random.rand()
    return seis


def get_live_timeseries():
    ts = TimeSeries()
    ts.set_live()
    ts.dt = 1 / sampling_rate
    ts.npts = ts_size
    # ts.put('net', 'IU')
    ts.put('npts', ts_size)
    ts.put('sampling_rate', sampling_rate)
    ts.tref = TimeReferenceType.UTC
    ts.t0 = datetime.utcnow().timestamp()
    ts['delta'] = 0.1
    ts['calib'] = 0.1
    ts['site_id'] = bson.objectid.ObjectId()
    ts['channel_id'] = bson.objectid.ObjectId()
    ts['source_id'] = bson.objectid.ObjectId()
    ts.set_as_origin('test', '0', '0',
                     AtomicType.TIMESERIES)
    ts.data = DoubleVector(np.random.rand(ts_size))
    return ts

# the following sine wave generation is modified from obspy's test at:
# https://github.com/obspy/obspy/blob/master/obspy/imaging/tests/test_waveform.py
def get_sin_timeseries():
    ts = TimeSeries()
    ts.set_live()
    ts.dt = 1 / sampling_rate
    ts.npts = ts_size
    # ts.put('net', 'IU')
    ts.put('npts', ts_size)
    ts.put('sampling_rate', sampling_rate)
    ts.tref = TimeReferenceType.UTC
    ts.t0 = datetime.utcnow().timestamp()
    ts['delta'] = 0.1
    ts['calib'] = 0.1
    ts['site_id'] = bson.objectid.ObjectId()
    ts['channel_id'] = bson.objectid.ObjectId()
    ts['source_id'] = bson.objectid.ObjectId()
    ts.set_as_origin('test', '0', '0',
                     AtomicType.TIMESERIES)
    curve = np.linspace(0, 2 * np.pi, ts.npts)
    curve = np.sin(curve) + 0.2 * np.sin(10 * curve)
    ts.data = DoubleVector(curve)
    return ts

def get_live_seismogram_ensemble(n):
    seis_e = SeismogramEnsemble()
    for i in range(n):
        seis = get_live_seismogram()
        seis_e.member.append(seis)
    return seis_e


def get_live_timeseries_ensemble(n):
    tse = TimeSeriesEnsemble()
    for i in range(n):
        ts = get_live_timeseries()
        tse.member.append(ts)
    return tse

def get_trace():
    dict1 = {'network': 'IU', 'station': 'ANMO',
             'starttime': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
             'npts': ts_size, 'sampling_rate': sampling_rate,
             'channel': 'BHE',
             'live': True, '_id': bson.objectid.ObjectId(),
             'jdate': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
             'date_str': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000),
             'not_defined_date': obspy.UTCDateTime(2019, 12, 31, 23, 59, 59, 915000)}
    return obspy.Trace(data=np.random.randint(0, 1000, ts_size), header=dict1)

def get_stream():
    return obspy.Stream(traces=[get_trace(), get_trace(), get_trace()])


if __name__ == "__main__":
    data = get_live_seismogram_ensemble(3)
    print(data.member[0]
          )
