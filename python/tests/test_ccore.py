import array
import copy
import pickle

import numpy as np
import pytest

from mspasspy.ccore.seismic import (CoreSeismogram,
                                    Seismogram,
                                    SeismogramEnsemble,
                                    SlownessVector,
                                    TimeSeries,
                                    TimeSeriesEnsemble,
                                    TimeReferenceType)
from mspasspy.ccore.utility import (AtomicType,
                                    dmatrix,
                                    ErrorLogger,
                                    ErrorSeverity,
                                    LogData,
                                    Metadata,
                                    MetadataDefinitions,
                                    MsPASSError,
                                    ProcessingHistory,
                                    SphericalCoordinate)

from mspasspy.ccore.algorithms.basic import ExtractComponent

def setup_function(function):
    pass

def test_dmatrix():
    dm = dmatrix()
    assert dm.rows() == 0

    dm = dmatrix(9,4)
    assert dm.rows() == 9
    assert dm.columns() == 4
    assert dm.size == 4*9
    assert len(dm) == 9
    assert dm.shape == (9, 4)

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
    assert (dm.transpose() == md.transpose()).all()
    assert (dm * 3.14 == md * 3.14).all()
    assert (2.17 * dm == 2.17 * md).all()
    assert (dm * dm.transpose() == np.matmul(md, md.transpose())).all()

    with pytest.raises(MsPASSError, match = 'size mismatch'):
        dm * dm

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
    assert str(ld) == str(LogData(eval(str(ld))))

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
        
    # Error found with real data
    #dic = {'_format': 'MSEED', 'arrival.time': 1356901212.242550, 'calib': 1.000000, 'chan': 'BHZ', 'delta': 0.025000, 'deltim': -1.000000, 'endtime': 1356904168.544538, 'iphase': 'P', 'loc': '', 'mseed': {'dataquality': 'D', 'number_of_records': 36, 'encoding': 'STEIM2', 'byteorder': '>', 'record_length': 4096, 'filesize': 726344704}, 'net': 'CI', 'npts': 144000, 'phase': 'P', 'sampling_rate': 40.000000, 'site.elev': 0.258000, 'site.lat': 35.126900, 'site.lon': -118.830090, 'site_id': '5fb6a67b37f8eef2f0658e9a', 'sta': 'ARV', 'starttime': 1356900568.569538}
    #md = Metadata(dic)
    #md_copy = pickle.loads(pickle.dumps(md))
    #for i in md:
    #    assert md[i] == md_copy[i]

@pytest.fixture(params=[Seismogram, SeismogramEnsemble, 
                        TimeSeries, TimeSeriesEnsemble])
def MetadataBase(request):
    return request.param
def test_MetadataBase(MetadataBase):
    md = MetadataBase()
    assert MetadataBase.__name__ + "({" in  repr(md)
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
        if i != 'delta' and i != 'npts' and i != 'starttime':
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
    ts.npts = 100
    ts.t0 = 0.0
    ts.dt = 0.001
    ts.live = 1
    ts.tref = TimeReferenceType.Relative
    ts.data.append(1.0)
    ts.data.append(2.0)
    ts.data.append(3.0)
    ts.data.append(4.0)
    ts.sync_npts()
    assert ts.npts == 104
    assert ts.npts == ts['npts']
    ts += ts
    for i in range(4) :
        ts.data[i] = i * 0.5
    ts_copy = pickle.loads(pickle.dumps(ts))
    assert ts.data == ts_copy.data
    assert ts.data[3] == 1.5
    assert ts.data[103] == 8
    assert ts.time(100) == 0.1
    assert ts.sample_number(0.0998) == 100

def test_CoreSeismogram():
    md = Metadata()
    md['delta'] = 0.01
    md['starttime'] = 0.0
    md['npts'] = 100
    # test metadata constructor
    md['tmatrix'] = np.random.rand(3,3)
    cseis = CoreSeismogram(md, False)
    assert (cseis.transformation_matrix == md['tmatrix']).all()
    md['tmatrix'] = dmatrix(np.random.rand(3,3))
    cseis = CoreSeismogram(md, False)
    assert (cseis.transformation_matrix == md['tmatrix']).all()
    md['tmatrix'] = np.random.rand(9)
    cseis = CoreSeismogram(md, False)
    assert (cseis.transformation_matrix == md['tmatrix'].reshape(3,3)).all()
    md['tmatrix'] = np.random.rand(1,9)
    cseis = CoreSeismogram(md, False)
    assert (cseis.transformation_matrix == md['tmatrix'].reshape(3,3)).all()
    md['tmatrix'] = np.random.rand(9,1)
    cseis = CoreSeismogram(md, False)
    assert (cseis.transformation_matrix == md['tmatrix'].reshape(3,3)).all()
    
    md['tmatrix'] = np.random.rand(3,3).tolist()
    cseis = CoreSeismogram(md, False)
    assert np.isclose(cseis.transformation_matrix, np.array(md['tmatrix']).reshape(3,3)).all()
    md['tmatrix'] = np.random.rand(9).tolist()
    cseis = CoreSeismogram(md, False)
    assert np.isclose(cseis.transformation_matrix, np.array(md['tmatrix']).reshape(3,3)).all()

    # test whether the setter of transformation_matrix updates metadata correctly
    tm = np.random.rand(1,9)
    cseis.transformation_matrix = tm
    assert (cseis.transformation_matrix == tm.reshape(3,3)).all()
    assert np.isclose(cseis.transformation_matrix, np.array(cseis['tmatrix']).reshape(3,3)).all()
    tm = np.random.rand(9).tolist()
    cseis.transformation_matrix = tm
    assert np.isclose(cseis.transformation_matrix, np.array(tm).reshape(3,3)).all()
    assert np.isclose(cseis.transformation_matrix, np.array(cseis['tmatrix']).reshape(3,3)).all()

    # test exceptions
    md['tmatrix'] = np.random.rand(4,2)
    with pytest.raises(MsPASSError, match = "should be a 3x3 matrix"):
        CoreSeismogram(md, False)
    md['tmatrix'] = dmatrix(np.random.rand(2,4))
    with pytest.raises(MsPASSError, match = "should be a 3x3 matrix"):
        CoreSeismogram(md, False)
    md['tmatrix'] = 42
    with pytest.raises(MsPASSError, match = "not recognized"):
        CoreSeismogram(md, False)
    md.clear('tmatrix')
    with pytest.raises(MsPASSError, match = "Error trying to extract"):
        CoreSeismogram(md, False)
    md['tmatrix'] = {4:2}
    with pytest.raises(MsPASSError, match = "type is not recognized"):
        CoreSeismogram(md, False)
    
    md['tmatrix'] = np.random.rand(9).tolist()
    md['tmatrix'][3] = 'str'
    with pytest.raises(MsPASSError, match = "should be float"):
        CoreSeismogram(md, False)
    md['tmatrix'] = np.random.rand(3,4).tolist()
    with pytest.raises(MsPASSError, match = "should be a 3x3 list of list"):
        CoreSeismogram(md, False)
    md['tmatrix'] = [1,2,3]
    with pytest.raises(MsPASSError, match = "should be a 3x3 list of list"):
        CoreSeismogram(md, False)
    md['tmatrix'] = np.random.rand(2,2).tolist()
    with pytest.raises(MsPASSError, match = "should be a list of 9 floats or a 3x3 list of list"):
        CoreSeismogram(md, False)
    md['tmatrix'] = np.random.rand(3,3).tolist()
    md['tmatrix'][1][1] = 'str'
    with pytest.raises(MsPASSError, match = "should be float"):
        CoreSeismogram(md, False)


def test_Seismogram():
    seis = Seismogram()
    seis.npts = 100
    assert seis.data.rows() == 3
    assert seis.data.columns() == 100

    seis.t0 = 0.0
    seis.dt = 0.001
    seis.live = 1
    seis.tref = TimeReferenceType.Relative
    seis.data = dmatrix(np.random.rand(3,6))
    assert seis.npts != 6
    seis.sync_npts()
    assert seis.npts == 6
    assert seis.npts == seis['npts']

    seis.npts = 4
    assert seis.data.columns() == 4

    seis.npts = 10
    assert (seis.data[0:3,4:10] == 0).all()

    seis.data = dmatrix(np.random.rand(3,100))
    seis.sync_npts()
    seis_copy = pickle.loads(pickle.dumps(seis))
    assert seis_copy.t0 == seis.t0
    assert seis_copy.dt == seis.dt
    assert seis_copy.live == seis.live
    assert seis_copy.tref == seis.tref
    assert (seis_copy.data[:] == seis.data[:]).all()

    # test the += operator
    seis1 = Seismogram(seis)
    seis2 = Seismogram(seis)
    seis1 += seis2
    assert (np.isclose(seis1.data[:], seis.data + seis.data)).all()

    seis.npts = 0
    assert seis.data.rows() == 0

    seis.npts = 100
    for i in range(3):
        for j in range(100):
            if i == 0:
                seis.data[i,j] = 1.0
            else:
                seis.data[i,j] = 0.0
    seis.data[0,1] = 1.0; seis.data[0,2] = 1.0; seis.data[0,3] = 0.0
    seis.data[1,1] = 1.0; seis.data[1,2] = 1.0; seis.data[1,3] = 0.0
    seis.data[2,1] = 1.0; seis.data[2,2] = 0.0; seis.data[2,3] = 1.0

    sc = SphericalCoordinate()
    sc.phi = 0.0
    sc.theta = np.pi/4
    seis.rotate(sc)
    assert all(np.isclose(seis.data[:,3], [0, -0.707107, 0.707107]))
    seis.rotate_to_standard()
    assert all(seis.data[:,3] == [0, 0, 1])
    sc.phi = -np.pi/4
    seis.data[:,3] = sc.unit_vector
    seis.rotate(sc)
    assert all(seis.data[:,3] == [0, 0, 1])
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:,3], [0.5, -0.5, 0.707107]))
    seis.data[:,3] = [0, 0, 1]

    nu = [np.sqrt(3.0)/3.0, np.sqrt(3.0)/3.0, np.sqrt(3.0)/3.0]
    seis.rotate(nu)
    assert (np.isclose(seis.transformation_matrix, 
            np.array([[ 0.70710678, -0.70710678,  0.        ],
                      [ 0.40824829,  0.40824829, -0.81649658],
                      [ 0.57735027,  0.57735027,  0.57735027]]))).all()
    assert all(np.isclose(seis.data[:,0], [0.707107, 0.408248, 0.57735]))
    assert all(np.isclose(seis.data[:,1], [0, 0, 1.73205]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:,0], [1, 0, 0]))
    assert all(np.isclose(seis.data[:,1], [1, 1, 1]))

    nu = [np.sqrt(3.0)/3.0, np.sqrt(3.0)/3.0, np.sqrt(3.0)/3.0]
    seis.rotate(SphericalCoordinate(nu))
    assert (np.isclose(seis.transformation_matrix, 
            np.array([[ 0.70710678, -0.70710678,  0.        ],
                      [ 0.40824829,  0.40824829, -0.81649658],
                      [ 0.57735027,  0.57735027,  0.57735027]]))).all()
    assert all(np.isclose(seis.data[:,0], [0.707107, 0.408248, 0.57735]))
    assert all(np.isclose(seis.data[:,1], [0, 0, 1.73205]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:,0], [1, 0, 0]))
    assert all(np.isclose(seis.data[:,1], [1, 1, 1]))
    
    sc.phi = np.pi/4
    sc.theta = 0.0
    seis.rotate(sc)
    assert (np.isclose(seis.transformation_matrix, 
            np.array([[ 0.70710678, -0.70710678,  0.],
                      [ 0.70710678,  0.70710678,  0.],
                      [ 0.        ,  0.        ,  1.]]))).all()
    assert all(np.isclose(seis.data[:,0], [0.707107, 0.707107, 0]))
    assert all(np.isclose(seis.data[:,1], [0, 1.41421, 1]))
    assert all(np.isclose(seis.data[:,2], [0, 1.41421, 0]))
    assert all(np.isclose(seis.data[:,3], [0, 0, 1]))
    seis.rotate_to_standard()

    a = np.zeros((3,3))
    a[0][0] =  1.0; a[0][1] =  1.0; a[0][2]  = 1.0
    a[1][0] = -1.0; a[1][1] =  1.0; a[1][2]  = 1.0
    a[2][0] =  0.0; a[2][1] = -1.0; a[2][2]  = 0.0
    seis.transform(a)
    assert all(np.isclose(seis.data[:,0], [1, -1,  0]))
    assert all(np.isclose(seis.data[:,1], [3,  1, -1]))
    assert all(np.isclose(seis.data[:,2], [2,  0, -1]))
    assert all(np.isclose(seis.data[:,3], [1,  1,  0]))
    seis_copy = pickle.loads(pickle.dumps(seis))
    seis_copy.rotate_to_standard()
    assert all(np.isclose(seis_copy.data[:,0], [1, 0, 0]))
    assert all(np.isclose(seis_copy.data[:,1], [1, 1, 1]))
    assert all(np.isclose(seis_copy.data[:,2], [1, 1, 0]))
    assert all(np.isclose(seis_copy.data[:,3], [0, 0, 1]))
    seis.rotate_to_standard()

    seis.rotate(np.pi/4)
    seis.transform(a)
    assert (np.isclose(seis.transformation_matrix, 
            np.array([[  1.41421 ,  0.      , 1],
                      [  0.      ,  1.41421 , 1],
                      [ -0.707107, -0.707107, 0]]))).all()
    assert all(np.isclose(seis.data[:,0], [1.41421, 0, -0.707107]))
    assert all(np.isclose(seis.data[:,1], [2.41421, 2.41421, -1.41421]))
    assert all(np.isclose(seis.data[:,2], [1.41421, 1.41421, -1.41421]))
    assert all(np.isclose(seis.data[:,3], [1, 1, 0]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:,0], [1, 0, 0]))
    assert all(np.isclose(seis.data[:,1], [1, 1, 1]))
    assert all(np.isclose(seis.data[:,2], [1, 1, 0]))
    assert all(np.isclose(seis.data[:,3], [0, 0, 1]))

    uvec = SlownessVector()
    uvec.ux =  0.17085  # cos(-20deg)/5.5
    uvec.uy = -0.062185 # sin(-20deg)/5.5
    seis.free_surface_transformation(uvec, 5.0, 3.5)
    assert (np.isclose(seis.transformation_matrix, 
            np.array([[ -0.171012, -0.469846,  0],
                      [  0.115793, -0.0421458, 0.445447],
                      [ -0.597975,  0.217647,  0.228152]]))).all()

    seis.transformation_matrix = a
    assert (seis.transformation_matrix == a).all()


def test_ExtractComponent():
    seis = Seismogram()
    seis.live = 1
    seis.data = dmatrix(np.random.rand(3,6))
    seis.npts = 6
    ts = []
    for i in range(3):
        ts.append(ExtractComponent(seis,i))
    for i in range(3):
        assert (ts[i].data == seis.data[i]).all()



@pytest.fixture(params=[ProcessingHistory,
                        Seismogram,  
                        TimeSeries])
def ProcessingHistoryBase(request):
    return request.param
def test_ProcessingHistoryBase(ProcessingHistoryBase):
    ph = ProcessingHistoryBase()
    ph.set_jobname("testjob")
    ph.set_jobid("999")
    ph2 = ProcessingHistoryBase(ph)
    assert ph.jobname() == ph2.jobname()
    assert ph.jobid() == ph2.jobid()

    ph.set_as_raw("fakeinput","0","fakeuuid1",AtomicType.SEISMOGRAM)
    ph.new_map("onetoone","0",AtomicType.SEISMOGRAM)
    assert len(ph.get_nodes()) == 1
    phred = ProcessingHistoryBase(ph)
    ph_list = []
    for i in range(4):
        ph_list.append(ProcessingHistoryBase())
        ph_list[i].set_as_raw("fakedataset", "0", "fakeid_"+str(i), AtomicType.SEISMOGRAM)
        ph_list[i].new_map("onetoone", "0", AtomicType.SEISMOGRAM)
    phred.new_ensemble_process("testreduce", "0", AtomicType.SEISMOGRAM, ph_list)
    dic = phred.get_nodes()
    rec = 0
    for i in dic.keys():
        for j in dic[i]:
            print(i, " : ", j.uuid, " , ", j.status)
            rec+=1
    assert rec == 8

    ph_list2 = copy.deepcopy(ph_list)
    ph_list3 = copy.deepcopy(ph_list)
    # a stack in order
    ph_list2[0].accumulate("stack", "stack1", AtomicType.SEISMOGRAM, ph_list2[1])
    ph_list2[0].accumulate("stack", "stack1", AtomicType.SEISMOGRAM, ph_list2[2])
    ph_list2[0].accumulate("stack", "stack1", AtomicType.SEISMOGRAM, ph_list2[3])
    # a stack out of order
    ph_list3[0].accumulate("stack", "stack1", AtomicType.SEISMOGRAM, ph_list3[2])
    ph_list3[3].accumulate("stack", "stack1", AtomicType.SEISMOGRAM, ph_list3[1])
    ph_list3[0].accumulate("stack", "stack1", AtomicType.SEISMOGRAM, ph_list3[3])
    assert len(ph_list2[0].get_nodes()) == 5
    assert len(ph_list3[0].get_nodes()) == 5
    nodes2 = {}
    for k,v in ph_list2[0].get_nodes().items():
        if k not in ph_list2[0].id():
            nodes2[k] = str(v)
    nodes3 = {}
    for k,v in ph_list3[0].get_nodes().items():
        if k not in ph_list3[0].id():
            nodes3[k] = str(v)
    assert nodes2 == nodes3
    nodes2 = []
    for i in ph_list2[0].get_nodes()[ph_list2[0].id()]:
        nodes2.append(str(i))
    nodes3 = []
    for i in ph_list3[0].get_nodes()[ph_list3[0].id()]:
        nodes3.append(str(i))
    assert nodes2.sort() == nodes3.sort()

    phred_copy = pickle.loads(pickle.dumps(phred))
    assert str(phred.get_nodes()) == str(phred_copy.get_nodes())

def test_MsPASSError():
    try:
        x = MetadataDefinitions('foo')
    except MsPASSError as err:
        assert err.message == 'bad file'
        assert err.severity == ErrorSeverity.Invalid
    try: 
        raise MsPASSError('test error1', ErrorSeverity.Informational)
    except MsPASSError as err: 
        assert err.message == 'test error1'
        assert err.severity == ErrorSeverity.Informational
    try: 
        raise MsPASSError('test error2', "Suspect")
    except MsPASSError as err: 
        assert err.message == 'test error2'
        assert err.severity == ErrorSeverity.Suspect
    try: 
        raise MsPASSError('test error3', 123)
    except MsPASSError as err: 
        assert err.message == 'test error3'
        assert err.severity == ErrorSeverity.Fatal
    try: 
        raise MsPASSError("test error4")
    except MsPASSError as err: 
        assert err.message == 'test error4'
        assert err.severity == ErrorSeverity.Fatal