import array
import pickle

import numpy as np
import pytest

from mspasspy.ccore import (dmatrix,
                            ErrorLogger,
                            ErrorSeverity,
                            ExtractComponent,
                            LogData,
                            Metadata,
                            Seismogram,
                            SeismogramEnsemble,
                            TimeSeries,
                            TimeSeriesEnsemble,
                            TimeReferenceType)

def setup_function(function):
    pass

def test_dmatrix():
    dm = dmatrix()
    assert dm.rows() == 0

    dm = dmatrix(9,4)
    assert dm.rows() == 9
    assert dm.columns() == 4

    md = [array.array('l', (0 for _ in range(5))) for _ in range(3)]
    for i in range(3):
        for j in range(5):
            md[i][j] = i*5+j
    dm = dmatrix(md)
    assert np.equal(dm,md).all()

    dm_c = dmatrix(dm)
    assert (dm_c[:] == dm).all()

    dm_c.zero()
    assert not dm_c[:].any()

    md = np.zeros((7,4), dtype=np.double, order='F')
    for i in range(7):
        for j in range(4):
            md[i][j] = i*4+j
    dm = dmatrix(md)
    assert (dm == md).all()

    dm_c = dmatrix(dm)
    dm += dm_c
    assert (dm == md+md).all()
    dm += md
    assert (dm == md+md+md).all()
    assert type(dm) == dmatrix
    dm -= dm_c
    dm -= dm_c
    dm -= md
    assert not dm[:].any()
    assert type(dm) == dmatrix

    dm_c = dmatrix(dm)
    
    md = np.zeros((7,4), dtype=np.single, order='C')
    for i in range(7):
        for j in range(4):
            md[i][j] = i*4+j
    dm = dmatrix(md)
    assert (dm == md).all()

    md = np.zeros((7,4), dtype=np.int, order='F')
    for i in range(7):
        for j in range(4):
            md[i][j] = i*4+j
    dm = dmatrix(md)
    assert (dm == md).all()

    md = np.zeros((7,4), dtype=np.unicode_, order='C')
    for i in range(7):
        for j in range(4):
            md[i][j] = i*4+j
    dm = dmatrix(md)
    assert (dm == np.float_(md)).all()

    md = np.zeros((53,37), dtype=np.double, order='C')
    for i in range(53):
        for j in range(37):
            md[i][j] = i*37+j
    dm = dmatrix(md)
    
    assert dm[17, 23] == md[17, 23]
    assert (dm[17] == md[17]).all()
    assert (dm[::] == md[::]).all()
    assert (dm[3::] == md[3::]).all()
    assert (dm[:5:] == md[:5:]).all()
    assert (dm[::7] == md[::7]).all()
    assert (dm[-3::] == md[-3::]).all()
    assert (dm[:-5:] == md[:-5:]).all()
    assert (dm[::-7] == md[::-7]).all()
    assert (dm[11:41:7] == md[11:41:7]).all()
    assert (dm[-11:-41:-7] == md[-11:-41:-7]).all()
    assert (dm[3::, 13] == md[3::, 13]).all()
    assert (dm[19, :5:] == md[19, :5:]).all()
    assert (dm[::-7,::-11] == md[::-7,::-11]).all()

    with pytest.raises(IndexError, match = 'out of bounds for axis 1'):
        dummy = dm[3,50]
    with pytest.raises(IndexError, match = 'out of bounds for axis 0'):
        dummy = dm[80]
    
    with pytest.raises(IndexError, match = 'out of bounds for axis 1'):
        dm[3,50] = 1.0
    with pytest.raises(IndexError, match = 'out of bounds for axis 0'):
        dm[60,50] = 1

    dm[7,17] = 3.14
    assert dm[7,17] == 3.14

    dm[7,17] = '6.28'
    assert dm[7,17] == 6.28

    dm[7] = 10
    assert (dm[7] == 10).all()

    dm[::] = md
    assert (dm == md).all()

    dm[:,-7] = 3.14
    assert (dm[:,-7] == 3.14).all()

    dm[17,:] = 3.14
    assert (dm[17,:] == 3.14).all()

    dm[3:7,-19:-12] = 3.14
    assert (dm[3:7,-19:-12] == 3.14).all()

def test_ErrorLogger():
    errlog = ErrorLogger()
    assert errlog.log_error('1','2', ErrorSeverity(3)) == 1
    assert errlog[0].algorithm == '1'
    assert errlog[0].message == '2'
    assert errlog[0].badness == ErrorSeverity.Complaint
    assert errlog[0].job_id == errlog.get_job_id()

def test_LogData():
    ld = LogData({"job_id":0, "p_id":1, "algorithm":"alg", "message":"msg", "badness":ErrorSeverity(2)})
    assert ld.job_id == 0
    assert ld.p_id == 1
    assert ld.algorithm == "alg"
    assert ld.message == "msg"
    assert ld.badness == ErrorSeverity.Suspect

def test_Metadata():
    md = Metadata()
    assert repr(md) == 'Metadata({})'
    dic = {1:1}
    md.put('dict', dic)
    val = md.get('dict')
    val[2] = 2
    del val
    dic[3] = 3
    del dic
    md['dict'][4] = 4
    assert md['dict'] == {1: 1, 2: 2, 3: 3, 4: 4}

    md = Metadata({'array': np.array([3, 4])})
    md['dict']      = {1: 1, 2: 2}
    md['str\'i"ng'] = 'str\'i"ng'
    md["str'ing"]   = "str'ing"
    md['double']    = 3.14
    md['bool']      = True
    md['int']       = 7
    md["string"]    = "str\0ing"
    md["string"]    = "str\ning"
    md["str\ting"]  = "str\ting"
    md["str\0ing"]  = "str\0ing"
    md["str\\0ing"] = "str\\0ing"
    md_copy = pickle.loads(pickle.dumps(md))
    for i in md:
        if i == 'array':
            assert (md[i] == md_copy[i]).all()
        else:
            assert md[i] == md_copy[i]

    md = Metadata({
        "<class 'numpy.ndarray'>": np.array([3, 4]),
        "<class 'dict'>"         : {1: 1, 2: 2},
        'string'                 : 'string',
        'double'                 : 3.14,
        'bool'                   : True,
        'long'                   : 7,
        "<class 'bytes'>"        : b'\xba\xd0\xba\xd0',
        "<class 'NoneType'>"     : None })
    for i in md: 
        assert md.type(i) == i
    
    md[b'\xba\xd0'] = b'\xba\xd0'
    md_copy = pickle.loads(pickle.dumps(md))
    for i in md:
        if i == "<class 'numpy.ndarray'>":
            assert (md[i] == md_copy[i]).all()
        else:
            assert md[i] == md_copy[i]

    del md["<class 'numpy.ndarray'>"]
    md_copy.clear("<class 'numpy.ndarray'>")
    assert not "<class 'numpy.ndarray'>" in md
    assert not "<class 'numpy.ndarray'>" in md_copy
    assert md.keys() == md_copy.keys()

    with pytest.raises(TypeError, match = 'Metadata'):
        reversed(md)
    
    md = Metadata({1:1,3:3})
    md_copy = Metadata({2:2,3:30})
    md += md_copy
    assert md.__repr__() == "Metadata({'1': 1, '2': 2, '3': 30})"


@pytest.fixture(params=[Seismogram, SeismogramEnsemble, 
                        TimeSeries, TimeSeriesEnsemble])
def MetadataBase(request):
    return request.param
def test_MetadataBase(MetadataBase):
    md = MetadataBase()
    assert repr(md) == MetadataBase.__name__ + '({})'
    dic = {1:1}
    md.put('dict', dic)
    val = md.get('dict')
    val[2] = 2
    del val
    dic[3] = 3
    del dic
    md['dict'][4] = 4
    assert md['dict'] == {1: 1, 2: 2, 3: 3, 4: 4}

    md = MetadataBase()
    md["<class 'numpy.ndarray'>"]     = np.array([3, 4])
    md["<class 'dict'>"]      = {1: 1, 2: 2}
    md['string'] = 'str\'i"ng'
    md["str'ing"]   = "str'ing"
    md['double']    = 3.14
    md['bool']      = True
    md['long']       = 7
    md["str\ning"]    = "str\0ing"
    md["str\ning"]    = "str\ning"
    md["str\ting"]  = "str\ting"
    md["str\0ing"]  = "str\0ing"
    md["str\\0ing"] = "str\\0ing"
    md["<class 'bytes'>"] = b'\xba\xd0\xba\xd0'
    md["<class 'NoneType'>"] = None
    md[b'\xba\xd0']= b'\xba\xd0'
    md_copy = MetadataBase(md)
    for i in md:
        if i == 'array' or i == "<class 'numpy.ndarray'>":
            assert (md[i] == md_copy[i]).all()
        else:
            assert md[i] == md_copy[i]
    del md["str'ing"], md["str\ning"], md["str\ting"], md["str\0ing"], md["str\\0ing"], md["b'\\xba\\xd0'"]
    for i in md:
        assert md.type(i) == i

    md_copy = MetadataBase(md)
    del md["<class 'numpy.ndarray'>"]
    md_copy.clear("<class 'numpy.ndarray'>")
    assert not "<class 'numpy.ndarray'>" in md
    assert not "<class 'numpy.ndarray'>" in md_copy
    assert md.keys() == md_copy.keys()

    with pytest.raises(TypeError, match = MetadataBase.__name__):
        reversed(md)

def test_TimeSeries():
    ts = TimeSeries()
    ts.ns = 100
    ts.t0 = 0.0
    ts.dt = 0.001
    ts.live = 1
    ts.tref = TimeReferenceType.Relative
    ts.s.append(1.0)
    ts.s.append(2.0)
    ts.s.append(3.0)
    ts.s.append(4.0)
    ts += ts
    for i in range(4) :
        ts.s[i] = i * 0.5
    ts_copy = pickle.loads(pickle.dumps(ts))
    assert ts.s == ts_copy.s
    assert ts.s[3] == 1.5
    assert ts.s[103] == 8
    assert ts.time(100) == 0.1
    assert ts.sample_number(0.0998) == 100

def test_Seismogram():
    seis = Seismogram()
    seis.ns = 100
    assert seis.u.rows() == 3
    assert seis.u.columns() == 100

    seis.t0 = 0.0
    seis.dt = 0.001
    seis.live = 1
    seis.tref = TimeReferenceType.Relative
    seis.u = dmatrix(np.random.rand(3,6))
    assert seis.ns == 6

    seis.ns = 4
    assert seis.u.columns() == 4

    seis.ns = 10
    assert (seis.u[0:3,4:10] == 0).all()

    seis_copy = pickle.loads(pickle.dumps(seis))
    assert seis_copy.t0 == seis.t0
    assert seis_copy.dt == seis.dt
    assert seis_copy.live == seis.live
    assert seis_copy.tref == seis.tref
    assert (seis_copy.u[:] == seis.u[:]).all()

    seis.ns = 0
    assert seis.u.rows() == 0

def test_ExtractComponent():
    seis = Seismogram()
    seis.live = 1
    seis.u = dmatrix(np.random.rand(3,6))
    ts = []
    for i in range(3):
        ts.append(ExtractComponent(seis,i))
    for i in range(3):
        assert (ts[i].s == seis.u[i]).all()