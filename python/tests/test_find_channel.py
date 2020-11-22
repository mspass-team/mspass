import pymongo
import datetime
import os
import numpy as np
import struct
from array import array
from pymongo import MongoClient
from mspasspy.ccore.seismic import (BasicTimeSeries,
                                    Seismogram,
                                    TimeReferenceType,
                                    TimeSeries,
                                    DoubleVector)


def find_channel(collection):
    st = datetime.datetime(1990, 1, 1, 6)
    et = datetime.datetime(1990, 1, 4, 6)
    for cr in collection.find({'st': {'$gte': st}, 'et': {'$lte': et}}):
        print(cr)
    # net channel station scheme


def save_data(d):
    di = d.get_string('dir')
    dfile = d.get_string('dfile')
    fname = os.path.join(di, dfile)
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    with open(fname, mode='a+b') as fh:
        foff = fh.seek(0, 2)
        float_array = array('d', d.data)
        d.put('nofbytes', float_array.itemsize * float_array.buffer_info()[1])
        float_array.tofile(fh)
    di = os.path.dirname(os.path.realpath(fname))
    dfile = os.path.basename(os.path.realpath(fname))
    d.put('dir', di)
    d.put('dfile', dfile)
    d.put('foff', foff)


def read_data(d):
    di = d.get_string('dir')
    dfile = d.get_string('dfile')
    foff = d.get('foff')
    fname = os.path.join(di, dfile)
    with open(fname, mode='rb') as fh:
        fh.seek(foff)
        float_array = array('d')
        float_array.fromstring(fh.read(d.get('nofbytes')))
        d.data = DoubleVector(float_array)


if __name__ == "__main__":
    s = TimeSeries()
    s.data = DoubleVector(np.random.rand(255))
    s['dir'] = './'
    s['dfile'] = 'test_op'
    save_data(s)

    s2 = TimeSeries()
    for k in s:
        s2[k] = s[k]
    s2.data = DoubleVector([])
    print(len(s2.data))
    read_data(s2)
    print(len(s2.data))
    assert all(a == b for a, b in zip(s.data, s2.data))
    # client = MongoClient('localhost', 27017)
    # db = client.mspass
    # channels = db.channels
    # find_channel(channels)
