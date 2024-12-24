import array
import copy
import pickle
import sys

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    _CoreSeismogram,
    _CoreTimeSeries,
    DataGap,
    PowerSpectrum,
    Seismogram,
    SeismogramEnsemble,
    SlownessVector,
    TimeSeries,
    TimeSeriesEnsemble,
    TimeReferenceType,
)
from mspasspy.ccore.utility import (
    AtomicType,
    dmatrix,
    ErrorLogger,
    ErrorSeverity,
    LogData,
    Metadata,
    MetadataDefinitions,
    MsPASSError,
    ProcessingHistory,
    SphericalCoordinate,
)

from mspasspy.util.error_logger import PyErrorLogger
from mspasspy.ccore.algorithms.basic import _ExtractComponent
from mspasspy.ccore.algorithms.deconvolution import MTPowerSpectrumEngine
from mspasspy.ccore.algorithms.basic import TimeWindow


def make_constant_data_ts(d, t0=0.0, dt=0.1, nsamp=5, val=1.0):
    """
    Fills TimeSeries (or _CoreTimeSeries) data vector with
    a constant value of a specified length and start time.
    Used for testing arithmetic operators.

    Parameters
    ----------
    d : TYPE
        DESCRIPTION.  TimeSeries or _CoreTimeSeries skeleton to build upon
    t0 : TYPE, optional
        DESCRIPTION. The default is 0.0. data start time
    dt : TYPE, optional
        DESCRIPTION. The default is 0.1.  sample interval
    nsamp : TYPE, optional
        DESCRIPTION. The default is 5.  length of data vector to generate

    Returns
    -------
    None.

    """
    d.npts = nsamp
    d.t0 = t0
    d.dt = dt
    d.set_live()
    for i in range(nsamp):
        d.data[i] = val
    return d


def make_constant_data_seis(d, t0=0.0, dt=0.1, nsamp=5, val=1.0):
    """
    Fills Seismogram (or Seismogram) data vector with
    a constant value of a specified length and start time.
    Used for testing arithmetic operators.

    Parameters
    ----------
    d : TYPE
        DESCRIPTION.  TimeSeries or _CoreTimeSeries skeleton to build upon
    t0 : TYPE, optional
        DESCRIPTION. The default is 0.0. data start time
    dt : TYPE, optional
        DESCRIPTION. The default is 0.1.  sample interval
    nsamp : TYPE, optional
        DESCRIPTION. The default is 5.  length of data vector to generate

    Returns
    -------
    None.

    """
    d.npts = nsamp
    d.t0 = t0
    d.dt = dt
    d.set_live()
    for i in range(nsamp):
        for k in range(3):
            d.data[k, i] = val
    return d


def setup_function(function):
    pass


def test_dmatrix():
    dm = dmatrix()
    assert dm.rows() == 0

    dm = dmatrix(9, 4)
    assert dm.rows() == 9
    assert dm.columns() == 4
    assert dm.size == 4 * 9
    assert len(dm) == 9
    assert dm.shape == (9, 4)

    md = [array.array("l", (0 for _ in range(5))) for _ in range(3)]
    for i in range(3):
        for j in range(5):
            md[i][j] = i * 5 + j
    dm = dmatrix(md)
    assert np.equal(dm, md).all()

    dm_c = dmatrix(dm)
    assert (dm_c[:] == dm).all()

    dm_c.zero()
    assert not dm_c[:].any()

    md = np.zeros((7, 4), dtype=np.double, order="F")
    for i in range(7):
        for j in range(4):
            md[i][j] = i * 4 + j
    dm = dmatrix(md)
    assert (dm == md).all()
    assert (dm.transpose() == md.transpose()).all()
    assert (dm * 3.14 == md * 3.14).all()
    assert (2.17 * dm == 2.17 * md).all()
    assert (dm * dm.transpose() == np.matmul(md, md.transpose())).all()

    with pytest.raises(MsPASSError, match="size mismatch"):
        dm * dm

    dm_c = dmatrix(dm)
    dm += dm_c
    assert (dm == md + md).all()
    dm += md
    assert (dm == md + md + md).all()
    assert type(dm) == dmatrix
    dm -= dm_c
    dm -= dm_c
    dm -= md
    assert not dm[:].any()
    assert type(dm) == dmatrix

    dm_c = dmatrix(dm)

    md = np.zeros((7, 4), dtype=np.single, order="C")
    for i in range(7):
        for j in range(4):
            md[i][j] = i * 4 + j
    dm = dmatrix(md)
    assert (dm == md).all()

    md = np.zeros((7, 4), dtype=int, order="F")
    for i in range(7):
        for j in range(4):
            md[i][j] = i * 4 + j
    dm = dmatrix(md)
    assert (dm == md).all()

    md = np.zeros((7, 4), dtype=str, order="C")
    for i in range(7):
        for j in range(4):
            md[i][j] = i * 4 + j
    dm = dmatrix(md)
    assert (dm == np.float_(md)).all()

    md = np.zeros((53, 37), dtype=np.double, order="C")
    for i in range(53):
        for j in range(37):
            md[i][j] = i * 37 + j
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
    assert (dm[::-7, ::-11] == md[::-7, ::-11]).all()

    with pytest.raises(IndexError, match="out of bounds for axis 1"):
        dummy = dm[3, 50]
    with pytest.raises(IndexError, match="out of bounds for axis 0"):
        dummy = dm[80]

    with pytest.raises(IndexError, match="out of bounds for axis 1"):
        dm[3, 50] = 1.0
    with pytest.raises(IndexError, match="out of bounds for axis 0"):
        dm[60, 50] = 1

    dm[7, 17] = 3.14
    assert dm[7, 17] == 3.14

    dm[7, 17] = "6.28"
    assert dm[7, 17] == 6.28

    dm[7] = 10
    assert (dm[7] == 10).all()

    dm[::] = md
    assert (dm == md).all()

    dm[:, -7] = 3.14
    assert (dm[:, -7] == 3.14).all()

    dm[17, :] = 3.14
    assert (dm[17, :] == 3.14).all()

    dm[3:7, -19:-12] = 3.14
    assert (dm[3:7, -19:-12] == 3.14).all()


def test_ErrorLogger():
    errlog = ErrorLogger()
    assert errlog.log_error("1", "2", ErrorSeverity(3)) == 1
    assert errlog[0].algorithm == "1"
    assert errlog[0].message == "2"
    assert errlog[0].badness == ErrorSeverity.Complaint
    assert errlog[0].job_id == errlog.get_job_id()

    err = MsPASSError("foo", ErrorSeverity.Fatal)
    errlog.log_error(err)
    assert errlog[1].algorithm == "MsPASSError"
    assert errlog[1].message == "foo"
    assert errlog[1].badness == ErrorSeverity.Fatal
    assert errlog[1].job_id == errlog.get_job_id()
    with pytest.raises(TypeError, match="'int' is given"):
        errlog.log_error(123)


def test_PyErrorLogger():
    errlog = PyErrorLogger()
    assert errlog.log_error("1", ErrorSeverity(3)) == 1
    assert errlog[0].algorithm == "test_PyErrorLogger"
    assert errlog[0].message == "1"
    assert errlog[0].badness == ErrorSeverity.Complaint
    assert errlog[0].job_id == errlog.get_job_id()

    class dummy:
        def __init__(self):
            pass

        def func(self, errlog):
            return errlog.log_error("1", ErrorSeverity(3))

    assert dummy().func(errlog) == 2
    assert errlog[1].algorithm == "dummy.func"
    assert errlog[1].message == "1"
    assert errlog[1].badness == ErrorSeverity.Complaint
    assert errlog[1].job_id == errlog.get_job_id()

    assert errlog.log_error("func", "2", ErrorSeverity(3)) == 3
    assert errlog[2].algorithm == "func"
    assert errlog[2].message == "2"
    assert errlog[2].badness == ErrorSeverity.Complaint
    assert errlog[2].job_id == errlog.get_job_id()

    err = MsPASSError("foo", ErrorSeverity.Fatal)
    errlog.log_error(err)
    assert errlog[3].algorithm == "MsPASSError"
    assert errlog[3].message == "foo"
    assert errlog[3].badness == ErrorSeverity.Fatal
    assert errlog[3].job_id == errlog.get_job_id()
    with pytest.raises(TypeError, match="'int' is given"):
        errlog.log_error(123)


def test_LogData():
    ld = LogData(
        {
            "job_id": 0,
            "p_id": 1,
            "algorithm": "alg",
            "message": "msg",
            "badness": ErrorSeverity(2),
        }
    )
    assert ld.job_id == 0
    assert ld.p_id == 1
    assert ld.algorithm == "alg"
    assert ld.message == "msg"
    assert ld.badness == ErrorSeverity.Suspect
    assert str(ld) == str(LogData(eval(str(ld))))

    err = MsPASSError("foo", ErrorSeverity.Fatal)
    ld = LogData(0, "alg", err)
    assert ld.job_id == 0
    assert ld.algorithm == "alg"
    assert ld.message == "foo"
    assert ld.badness == ErrorSeverity.Fatal
    with pytest.raises(TypeError, match="'int' is given"):
        ld = LogData(0, "alg", 123)


def test_Metadata():
    md = Metadata()
    assert repr(md) == "Metadata({})"
    dic = {1: 1}
    md.put("dict", dic)
    val = md.get("dict")
    val[2] = 2
    del val
    dic[3] = 3
    del dic
    md["dict"][4] = 4
    assert md["dict"] == {1: 1, 2: 2, 3: 3, 4: 4}

    md = Metadata({"array": np.array([3, 4])})
    md["dict"] = {1: 1, 2: 2}
    md["str'i\"ng"] = "str'i\"ng"
    md["str'ing"] = "str'ing"
    md["double"] = 3.14
    md["bool"] = True
    md["int"] = 7
    md["string"] = "str\0ing"
    md["string"] = "str\ning"
    md["str\ting"] = "str\ting"
    md["str\0ing"] = "str\0ing"
    md["str\\0ing"] = "str\\0ing"
    md_copy = pickle.loads(pickle.dumps(md))
    for i in md:
        if i == "array":
            assert (md[i] == md_copy[i]).all()
        else:
            assert md[i] == md_copy[i]
    md_copy2 = Metadata(dict(md))
    assert not md_copy2.modified()
    assert md.modified() == md_copy.modified()

    md = Metadata(
        {
            "<class 'numpy.ndarray'>": np.array([3, 4]),
            "<class 'dict'>": {1: 1, 2: 2},
            "string": "string",
            "double": 3.14,
            "bool": True,
            "long": 7,
            "<class 'bytes'>": b"\xba\xd0\xba\xd0",
            "<class 'NoneType'>": None,
        }
    )
    for i in md:
        assert md.type(i) == i

    md[b"\xba\xd0"] = b"\xba\xd0"
    md_copy = pickle.loads(pickle.dumps(md))
    for i in md:
        if i == "<class 'numpy.ndarray'>":
            assert (md[i] == md_copy[i]).all()
        else:
            assert md[i] == md_copy[i]

    del md["<class 'numpy.ndarray'>"]
    md_copy.erase("<class 'numpy.ndarray'>")
    assert not "<class 'numpy.ndarray'>" in md
    assert not "<class 'numpy.ndarray'>" in md_copy
    assert md.keys() == md_copy.keys()

    with pytest.raises(TypeError, match="Metadata"):
        reversed(md)

    md = Metadata({1: 1, 3: 3})
    md_copy = Metadata({2: 2, 3: 30})
    md += md_copy
    assert md.__repr__() == "Metadata({'1': 1, '2': 2, '3': 30})"

    # Test with real data
    dic = {
        "_format": "MSEED",
        "arrival.time": 1356901212.242550,
        "calib": 1.000000,
        "chan": "BHZ",
        "delta": 0.025000,
        "deltim": -1.000000,
        "endtime": 1356904168.544538,
        "iphase": "P",
        "loc": "",
        "mseed": {
            "dataquality": "D",
            "number_of_records": 36,
            "encoding": "STEIM2",
            "byteorder": ">",
            "record_length": 4096,
            "filesize": 726344704,
        },
        "net": "CI",
        "npts": 144000,
        "phase": "P",
        "sampling_rate": 40.000000,
        "site.elev": 0.258000,
        "site.lat": 35.126900,
        "site.lon": -118.830090,
        "site_id": "5fb6a67b37f8eef2f0658e9a",
        "sta": "ARV",
        "starttime": 1356900568.569538,
    }
    md = Metadata(dic)
    md["mod"] = "mod"
    md_copy = pickle.loads(pickle.dumps(md))
    for i in md:
        assert md[i] == md_copy[i]
    assert md.modified() == md_copy.modified()


@pytest.fixture(params=[Seismogram, SeismogramEnsemble, TimeSeries, TimeSeriesEnsemble])
def MetadataBase(request):
    return request.param


def test_MetadataBase(MetadataBase):
    md = MetadataBase()
    assert MetadataBase.__name__ + "({" in repr(md)
    dic = {1: 1}
    md.put("dict", dic)
    val = md.get("dict")
    val[2] = 2
    del val
    dic[3] = 3
    del dic
    md["dict"][4] = 4
    assert md["dict"] == {1: 1, 2: 2, 3: 3, 4: 4}

    md = MetadataBase()
    md["<class 'numpy.ndarray'>"] = np.array([3, 4])
    md["<class 'dict'>"] = {1: 1, 2: 2}
    md["string"] = "str'i\"ng"
    md["str'ing"] = "str'ing"
    md["double"] = 3.14
    md["bool"] = True
    md["long"] = 7
    md["str\ning"] = "str\0ing"
    md["str\ning"] = "str\ning"
    md["str\ting"] = "str\ting"
    md["str\0ing"] = "str\0ing"
    md["str\\0ing"] = "str\\0ing"
    md["<class 'bytes'>"] = b"\xba\xd0\xba\xd0"
    md["<class 'NoneType'>"] = None
    md[b"\xba\xd0"] = b"\xba\xd0"
    md_copy = MetadataBase(md)
    for i in md:
        if i == "array" or i == "<class 'numpy.ndarray'>":
            assert (md[i] == md_copy[i]).all()
        else:
            assert md[i] == md_copy[i]
    del (
        md["str'ing"],
        md["str\ning"],
        md["str\ting"],
        md["str\0ing"],
        md["str\\0ing"],
        md["b'\\xba\\xd0'"],
    )
    for i in md:
        if i != "delta" and i != "npts" and i != "starttime":
            assert md.type(i) == i

    md_copy = MetadataBase(md)
    del md["<class 'numpy.ndarray'>"]
    md_copy.erase("<class 'numpy.ndarray'>")
    assert not "<class 'numpy.ndarray'>" in md
    assert not "<class 'numpy.ndarray'>" in md_copy
    assert md.keys() == md_copy.keys()

    with pytest.raises(TypeError, match=MetadataBase.__name__):
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
    assert ts.npts == ts["npts"]
    ts += ts
    for i in range(4):
        ts.data[i] = i * 0.5
    ts_copy = pickle.loads(pickle.dumps(ts))
    assert ts.data == ts_copy.data
    assert ts.data[3] == 1.5
    assert ts.data[103] == 8
    assert ts.time(100) == 0.1
    assert ts.sample_number(0.0998) == 100
    # starttime method is an alias for t0 included as a convenience
    assert ts.t0 == ts.starttime()
    taxis = ts.time_axis()
    # 104 because of the 4 append operations above
    assert len(taxis) == 104
    assert taxis[0] == ts.t0
    assert taxis[ts.npts - 1] == ts.t0 + (ts.npts - 1) * ts.dt
    # These metadata constructor used for cracking miniseed files
    md = Metadata()
    md["delta"] = 0.01
    md["starttime"] = 0.0
    ts = TimeSeries(md)
    assert ts.npts == 0
    md["npts"] = 100
    ts = TimeSeries(md)
    assert ts.npts == 100
    # this number is volatile. Changes above will make it invalid
    expected_size = 1344
    # we make this test a bit soft by this allowance.  sizeof computation
    # in parent C function is subject to alignment ambiguity that is
    # system dependent so we add this fudge factor for stability
    memory_alignment_allowance = 2
    memlow = expected_size - memory_alignment_allowance
    memhigh = expected_size + memory_alignment_allowance
    memuse = sys.getsizeof(ts)
    assert memuse >= memlow and memuse <= memhigh


def test_CoreSeismogram():
    md = Metadata()
    md["delta"] = 0.01
    md["starttime"] = 0.0
    # test metadata constructor
    # first make sure it handles case with npts not defined
    cseis = _CoreSeismogram(md, False)
    assert cseis.npts == 0
    md["npts"] = 100
    md["tmatrix"] = np.random.rand(3, 3)
    cseis = _CoreSeismogram(md, False)
    assert (cseis.tmatrix == md["tmatrix"]).all()
    md["tmatrix"] = dmatrix(np.random.rand(3, 3))
    cseis = _CoreSeismogram(md, False)
    assert (cseis.tmatrix == md["tmatrix"]).all()
    md["tmatrix"] = np.random.rand(9)
    cseis = _CoreSeismogram(md, False)
    assert (cseis.tmatrix == md["tmatrix"].reshape(3, 3)).all()
    md["tmatrix"] = np.random.rand(1, 9)
    cseis = _CoreSeismogram(md, False)
    assert (cseis.tmatrix == md["tmatrix"].reshape(3, 3)).all()
    md["tmatrix"] = np.random.rand(9, 1)
    cseis = _CoreSeismogram(md, False)
    assert (cseis.tmatrix == md["tmatrix"].reshape(3, 3)).all()

    md["tmatrix"] = np.random.rand(3, 3).tolist()
    cseis = _CoreSeismogram(md, False)
    assert np.isclose(cseis.tmatrix, np.array(md["tmatrix"]).reshape(3, 3)).all()
    md["tmatrix"] = np.random.rand(9).tolist()
    cseis = _CoreSeismogram(md, False)
    assert np.isclose(cseis.tmatrix, np.array(md["tmatrix"]).reshape(3, 3)).all()

    # test whether the setter of tmatrix updates metadata correctly
    tm = np.random.rand(1, 9)
    cseis.tmatrix = tm
    assert (cseis.tmatrix == tm.reshape(3, 3)).all()
    assert np.isclose(cseis.tmatrix, np.array(cseis["tmatrix"]).reshape(3, 3)).all()
    tm = np.random.rand(9).tolist()
    cseis.tmatrix = tm
    assert np.isclose(cseis.tmatrix, np.array(tm).reshape(3, 3)).all()
    assert np.isclose(cseis.tmatrix, np.array(cseis["tmatrix"]).reshape(3, 3)).all()

    # test exceptions
    md["tmatrix"] = np.random.rand(4, 2)
    with pytest.raises(MsPASSError, match="should be a 3x3 matrix"):
        _CoreSeismogram(md, False)
    md["tmatrix"] = dmatrix(np.random.rand(2, 4))
    with pytest.raises(MsPASSError, match="should be a 3x3 matrix"):
        _CoreSeismogram(md, False)
    md["tmatrix"] = 42
    with pytest.raises(MsPASSError, match="not recognized"):
        _CoreSeismogram(md, False)
    md.erase("tmatrix")
    # tmatrix not defined is taken to default to tmatrix being an identity
    # matrix.  We test that condition here
    cseis = _CoreSeismogram(md, False)
    assert np.isclose(cseis.tmatrix, np.eye(3)).all()
    md["tmatrix"] = {4: 2}
    with pytest.raises(MsPASSError, match="type is not recognized"):
        _CoreSeismogram(md, False)

    md["tmatrix"] = np.random.rand(9).tolist()
    md["tmatrix"][3] = "str"
    with pytest.raises(MsPASSError, match="should be float"):
        _CoreSeismogram(md, False)
    md["tmatrix"] = np.random.rand(3, 4).tolist()
    with pytest.raises(MsPASSError, match="should be a 3x3 list of list"):
        _CoreSeismogram(md, False)
    md["tmatrix"] = [1, 2, 3]
    with pytest.raises(MsPASSError, match="should be a 3x3 list of list"):
        _CoreSeismogram(md, False)
    md["tmatrix"] = np.random.rand(2, 2).tolist()
    with pytest.raises(
        MsPASSError, match="should be a list of 9 floats or a 3x3 list of list"
    ):
        _CoreSeismogram(md, False)
    md["tmatrix"] = np.random.rand(3, 3).tolist()
    md["tmatrix"][1][1] = "str"
    with pytest.raises(MsPASSError, match="should be float"):
        _CoreSeismogram(md, False)


def test_Seismogram():
    seis = Seismogram()
    seis.npts = 100
    assert seis.data.rows() == 3
    assert seis.data.columns() == 100

    seis.t0 = 0.0
    seis.dt = 0.001
    seis.live = 1
    seis.tref = TimeReferenceType.Relative
    seis.data = dmatrix(np.random.rand(3, 6))
    assert seis.npts != 6
    seis.sync_npts()
    assert seis.npts == 6
    assert seis.npts == seis["npts"]

    seis.npts = 4
    assert seis.data.columns() == 4

    seis.npts = 10
    assert (seis.data[0:3, 4:10] == 0).all()

    seis.data = dmatrix(np.random.rand(3, 100))
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
                seis.data[i, j] = 1.0
            else:
                seis.data[i, j] = 0.0
    seis.data[0, 1] = 1.0
    seis.data[0, 2] = 1.0
    seis.data[0, 3] = 0.0
    seis.data[1, 1] = 1.0
    seis.data[1, 2] = 1.0
    seis.data[1, 3] = 0.0
    seis.data[2, 1] = 1.0
    seis.data[2, 2] = 0.0
    seis.data[2, 3] = 1.0

    sc = SphericalCoordinate()
    sc.phi = 0.0
    sc.theta = np.pi / 4
    seis.rotate(sc)
    assert all(np.isclose(seis.data[:, 3], [0, -0.707107, 0.707107]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:, 3], [0, 0, 1]))
    sc.phi = -np.pi / 4
    seis.data[:, 3] = sc.unit_vector
    seis.rotate(sc)
    assert all(np.isclose(seis.data[:, 3], [0, 0, 1]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:, 3], [0.5, -0.5, 0.707107]))
    seis.data[:, 3] = [0, 0, 1]

    nu = [np.sqrt(3.0) / 3.0, np.sqrt(3.0) / 3.0, np.sqrt(3.0) / 3.0]
    seis.rotate(nu)
    assert (
        np.isclose(
            seis.tmatrix,
            np.array(
                [
                    [0.70710678, -0.70710678, 0.0],
                    [0.40824829, 0.40824829, -0.81649658],
                    [0.57735027, 0.57735027, 0.57735027],
                ]
            ),
        )
    ).all()
    assert all(np.isclose(seis.data[:, 0], [0.707107, 0.408248, 0.57735]))
    assert all(np.isclose(seis.data[:, 1], [0, 0, 1.73205]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:, 0], [1, 0, 0]))
    assert all(np.isclose(seis.data[:, 1], [1, 1, 1]))

    nu = [np.sqrt(3.0) / 3.0, np.sqrt(3.0) / 3.0, np.sqrt(3.0) / 3.0]
    seis.rotate(SphericalCoordinate(nu))
    assert (
        np.isclose(
            seis.tmatrix,
            np.array(
                [
                    [0.70710678, -0.70710678, 0.0],
                    [0.40824829, 0.40824829, -0.81649658],
                    [0.57735027, 0.57735027, 0.57735027],
                ]
            ),
        )
    ).all()
    assert all(np.isclose(seis.data[:, 0], [0.707107, 0.408248, 0.57735]))
    assert all(np.isclose(seis.data[:, 1], [0, 0, 1.73205]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:, 0], [1, 0, 0]))
    assert all(np.isclose(seis.data[:, 1], [1, 1, 1]))

    sc.phi = np.pi / 4
    sc.theta = 0.0
    seis.rotate(sc)
    assert (
        np.isclose(
            seis.tmatrix,
            np.array(
                [
                    [0.70710678, -0.70710678, 0.0],
                    [0.70710678, 0.70710678, 0.0],
                    [0.0, 0.0, 1.0],
                ]
            ),
        )
    ).all()
    assert all(np.isclose(seis.data[:, 0], [0.707107, 0.707107, 0]))
    assert all(np.isclose(seis.data[:, 1], [0, 1.41421, 1]))
    assert all(np.isclose(seis.data[:, 2], [0, 1.41421, 0]))
    assert all(np.isclose(seis.data[:, 3], [0, 0, 1]))
    seis.rotate_to_standard()

    # test for serialization of SphericalCoordinate
    sc_copy = pickle.loads(pickle.dumps(sc))
    assert sc_copy.radius == sc.radius
    assert sc_copy.theta == sc.theta
    assert sc_copy.phi == sc.phi

    a = np.zeros((3, 3))
    a[0][0] = 1.0
    a[0][1] = 1.0
    a[0][2] = 1.0
    a[1][0] = -1.0
    a[1][1] = 1.0
    a[1][2] = 1.0
    a[2][0] = 0.0
    a[2][1] = -1.0
    a[2][2] = 0.0
    seis.transform(a)
    assert all(np.isclose(seis.data[:, 0], [1, -1, 0]))
    assert all(np.isclose(seis.data[:, 1], [3, 1, -1]))
    assert all(np.isclose(seis.data[:, 2], [2, 0, -1]))
    assert all(np.isclose(seis.data[:, 3], [1, 1, 0]))
    seis_copy = pickle.loads(pickle.dumps(seis))
    seis_copy.rotate_to_standard()
    assert all(np.isclose(seis_copy.data[:, 0], [1, 0, 0]))
    assert all(np.isclose(seis_copy.data[:, 1], [1, 1, 1]))
    assert all(np.isclose(seis_copy.data[:, 2], [1, 1, 0]))
    assert all(np.isclose(seis_copy.data[:, 3], [0, 0, 1]))
    seis.rotate_to_standard()

    seis.rotate(-np.pi / 4)
    seis.transform(a)
    assert (
        np.isclose(
            seis.tmatrix,
            np.array([[1.41421, 0.0, 1], [0.0, 1.41421, 1], [-0.707107, -0.707107, 0]]),
        )
    ).all()
    assert all(np.isclose(seis.data[:, 0], [1.41421, 0, -0.707107]))
    assert all(np.isclose(seis.data[:, 1], [2.41421, 2.41421, -1.41421]))
    assert all(np.isclose(seis.data[:, 2], [1.41421, 1.41421, -1.41421]))
    assert all(np.isclose(seis.data[:, 3], [1, 1, 0]))
    seis.rotate_to_standard()
    assert all(np.isclose(seis.data[:, 0], [1, 0, 0]))
    assert all(np.isclose(seis.data[:, 1], [1, 1, 1]))
    assert all(np.isclose(seis.data[:, 2], [1, 1, 0]))
    assert all(np.isclose(seis.data[:, 3], [0, 0, 1]))

    uvec = SlownessVector()
    uvec.ux = 0.17085  # cos(-20deg)/5.5
    uvec.uy = -0.062185  # sin(-20deg)/5.5
    seis.free_surface_transformation(uvec, 5.0, 3.5)
    assert (
        np.isclose(
            seis.tmatrix,
            np.array(
                [
                    [-0.171012, -0.469846, 0],
                    [0.115793, -0.0421458, 0.445447],
                    [-0.597975, 0.217647, 0.228152],
                ]
            ),
        )
    ).all()

    seis.tmatrix = a
    assert (seis.tmatrix == a).all()

    # test for serialization of SlownessVector
    uvec_copy = pickle.loads(pickle.dumps(uvec))
    assert uvec_copy.ux == uvec.ux
    assert uvec_copy.uy == uvec.uy
    assert uvec_copy.azimuth() == uvec.azimuth()
    # test memory use method
    # we make this test a bit soft by this allowance.  sizeof computation
    # in parent C function is subject to alignment ambiguity that is
    # system dependent so we add this fudge factor for stability
    expected_size = 3080
    memory_alignment_allowance = 2
    memlow = expected_size - memory_alignment_allowance
    memhigh = expected_size + memory_alignment_allowance
    memuse = sys.getsizeof(seis)
    assert memuse >= memlow and memuse <= memhigh


@pytest.fixture(params=[TimeSeriesEnsemble, SeismogramEnsemble])
def Ensemble(request):
    return request.param


def test_Ensemble(Ensemble):
    md = Metadata()
    md["double"] = 3.14
    md["bool"] = True
    md["long"] = 7
    es = Ensemble(md, 3)
    if isinstance(es, TimeSeriesEnsemble):
        d = TimeSeries(10)
        d = make_constant_data_ts(d)
        es.member.append(d)
        es.member.append(d)
        es.member.append(d)
    else:
        d = Seismogram(10)
        d = make_constant_data_seis(d)
        es.member.append(d)
        es.member.append(d)
        es.member.append(d)
    es.set_live()  # new method for LoggingEnsemble needed because default is dead
    es.sync_metadata(["double", "long"])
    assert es.member[0].is_defined("bool")
    assert es.member[0]["bool"] == True
    assert not es.member[0].is_defined("double")
    assert not es.member[0].is_defined("long")
    es.sync_metadata()
    assert es.member[1].is_defined("double")
    assert es.member[1].is_defined("long")
    assert es.member[1]["double"] == 3.14
    assert es.member[1]["long"] == 7
    es.update_metadata(Metadata({"k": "v"}))
    assert es["k"] == "v"
    # From here on we test features not in CoreEnsemble but only in
    # LoggingEnsemble.   Note that we use pybind11 aliasing to
    # define TimeSeriesEnsemble == LoggingEnsemble<TimeSeries> and
    # SeismogramEnsemble == LoggingEnsemble<Seismogram>.
    # Should be initially marked live
    assert es.live
    es.elog.log_error("test_ensemble", "test complaint", ErrorSeverity.Complaint)
    es.elog.log_error("test_ensemble", "test invalid", ErrorSeverity.Invalid)
    assert es.elog.size() == 2
    assert es.live
    es.kill()
    assert es.dead()
    # resurrect es
    es.set_live()
    assert es.live
    # validate checks for for any live members - this tests that feature
    assert es.validate()
    # need this temporary copy for the next test_
    if isinstance(es, TimeSeriesEnsemble):
        escopy = TimeSeriesEnsemble(es)
    else:
        escopy = SeismogramEnsemble(es)
    for d in escopy.member:
        d.kill()
    assert not escopy.validate()
    # Reuse escopy for pickle test
    escopy = pickle.loads(pickle.dumps(es))
    assert escopy.is_defined("bool")
    assert escopy["bool"] == True
    assert escopy.is_defined("double")
    assert escopy.is_defined("long")
    assert escopy["double"] == 3.14
    assert escopy["long"] == 7
    assert escopy.live
    assert escopy.elog.size() == 2
    assert escopy.member[0].is_defined("bool")
    assert escopy.member[0]["bool"] == True
    assert escopy.member[0].is_defined("double")
    assert escopy.member[0].is_defined("long")
    assert es.member[1].is_defined("double")
    assert es.member[1].is_defined("long")
    assert es.member[1]["double"] == 3.14
    assert es.member[1]["long"] == 7
    if isinstance(es, TimeSeriesEnsemble):
        assert es.member[1].data == escopy.member[1].data
    else:
        assert (es.member[1].data[:] == escopy.member[1].data[:]).all()


def test_operators():
    d = _CoreTimeSeries(10)
    d1 = make_constant_data_ts(d, nsamp=10)
    dsave = _CoreTimeSeries(d1)
    d = _CoreTimeSeries(6)
    d2 = make_constant_data_ts(d, t0=-0.2, nsamp=6, val=2.0)
    dsave = _CoreTimeSeries(d1)
    d1 += d2
    assert np.allclose(d1.data, [3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
    d1 = _CoreTimeSeries(dsave)
    d = d1 + d2
    assert np.allclose(d.data, [3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
    d1 = _CoreTimeSeries(dsave)
    d1 *= 2.5
    assert np.allclose(d1.data, [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5])
    d3 = TimeSeries(10)
    d4 = TimeSeries(6)
    d3 = make_constant_data_ts(d3, nsamp=10)
    d4 = make_constant_data_ts(d4, t0=-0.2, nsamp=6, val=2.0)
    dsave = _CoreTimeSeries(d3)
    d3 = TimeSeries(dsave)
    d3 += d4
    assert np.allclose(d3.data, [3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(dsave)
    d = d3 + d4
    assert np.allclose(d.data, [3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
    d1 = _CoreTimeSeries(dsave)
    d3 = TimeSeries(dsave)
    d3 *= 2.5
    assert np.allclose(d3.data, [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5])
    x = np.linspace(-0.7, 1.2, 20)
    for t in x:
        d3 = TimeSeries(dsave)
        d4.t0 = t
        d3 += d4
    # These are selected asserts of the incremental test above
    # visually d4 moves through d3 as the t0 value advance. Assert
    # tests end member: skewed left, inside, and skewed right
    d3 = TimeSeries(dsave)
    d4.t0 = -0.7  # no overlap test
    d3 += d4
    assert np.allclose(d3.data, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(dsave)
    d4.t0 = -0.3  # overlap left
    d3 += d4
    assert np.allclose(d3.data, [3, 3, 3, 1, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(dsave)
    d4.t0 = 0.3  # d4 inside d3 test
    d3 += d4
    assert np.allclose(d3.data, [1, 1, 1, 3, 3, 3, 3, 3, 3, 1])
    d3 = TimeSeries(dsave)
    d4.t0 = 0.7  # partial overlap right
    d3 += d4
    assert np.allclose(d3.data, [1, 1, 1, 1, 1, 1, 1, 3, 3, 3])
    d3 = TimeSeries(dsave)
    d4.t0 = 1.0  # no overlap test right
    d3 += d4
    assert np.allclose(d3.data, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    # Repeat the same test for Seismogram objects
    # This section is edited cut-paste of above
    # Intentionally do not test _CoreSeismogram directly because
    # currently if it works for Seismogram it will for _CoreSeismogram

    d = _CoreSeismogram(10)
    d1 = make_constant_data_seis(d, nsamp=10)
    dsave = _CoreSeismogram(d1)
    d = _CoreSeismogram(6)
    d2 = make_constant_data_seis(d, t0=-0.2, nsamp=6, val=2.0)
    dsave = _CoreSeismogram(d1)
    d1 += d2
    assert np.allclose(
        d1.data,
        np.array(
            [
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )
    d1 = _CoreSeismogram(dsave)
    d = d1 + d2
    assert np.allclose(
        d.data,
        np.array(
            [
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )
    d1 = _CoreSeismogram(dsave)
    d1 *= 2.5
    assert np.allclose(
        d1.data,
        np.array(
            [
                [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5],
                [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5],
                [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5],
            ]
        ),
    )
    d3 = Seismogram(10)
    d4 = Seismogram(6)
    d3 = make_constant_data_seis(d3, nsamp=10)
    d4 = make_constant_data_seis(d4, t0=-0.2, nsamp=6, val=2.0)
    dsave = Seismogram(d3)
    d3 += d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d = d3 + d4
    assert np.allclose(
        d.data,
        np.array(
            [
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d3 *= 2.5
    assert np.allclose(
        d1.data,
        np.array(
            [
                [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5],
                [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5],
                [2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5],
            ]
        ),
    )
    x = np.linspace(-0.7, 1.2, 20)
    for t in x:
        d3 = Seismogram(dsave)
        d4.t0 = t
        d3 += d4

    # These are selected asserts of the incremental test above
    # visually d4 moves through d3 as the t0 value advance. Assert
    # tests end member: skewed left, inside, and skewed right
    d3 = Seismogram(dsave)
    d4.t0 = -0.7  # no overlap test
    d3 += d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ]
        ),
    )

    d3 = Seismogram(dsave)
    d4.t0 = -0.3  # overlap left
    d3 += d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [3, 3, 3, 1, 1, 1, 1, 1, 1, 1],
                [3, 3, 3, 1, 1, 1, 1, 1, 1, 1],
                [3, 3, 3, 1, 1, 1, 1, 1, 1, 1],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = 0.3  # d4 inside d3 test
    d3 += d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 3, 3, 3, 3, 3, 3, 1],
                [1, 1, 1, 3, 3, 3, 3, 3, 3, 1],
                [1, 1, 1, 3, 3, 3, 3, 3, 3, 1],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = 0.7  # partial overlap right
    d3 += d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 1, 1, 1, 1, 3, 3, 3],
                [1, 1, 1, 1, 1, 1, 1, 3, 3, 3],
                [1, 1, 1, 1, 1, 1, 1, 3, 3, 3],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = 1.0  # no overlap test right
    d3 += d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ]
        ),
    )

    # Repeat exactly for - test but different numeric results
    # just omit *= tests
    d = _CoreTimeSeries(10)
    d1 = make_constant_data_ts(d, nsamp=10)
    dsave = _CoreTimeSeries(d1)
    d = _CoreTimeSeries(6)
    d2 = make_constant_data_ts(d, t0=-0.2, nsamp=6, val=2.0)
    dsave = _CoreTimeSeries(d1)
    d1 -= d2
    assert np.allclose(d1.data, [-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
    d1 = _CoreTimeSeries(dsave)
    d = d1 - d2
    assert np.allclose(d.data, [-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(10)
    d4 = TimeSeries(6)
    d3 = make_constant_data_ts(d3, nsamp=10)
    d4 = make_constant_data_ts(d4, t0=-0.2, nsamp=6, val=2.0)
    dsave = _CoreTimeSeries(d3)
    d3 = TimeSeries(dsave)
    d3 -= d4
    assert np.allclose(d3.data, [-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(dsave)
    d = d3 - d4
    assert np.allclose(d.data, [-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
    x = np.linspace(-0.7, 1.2, 20)
    for t in x:
        d3 = TimeSeries(dsave)
        d4.t0 = t
        d3 -= d4
    # These are selected asserts of the incremental test above
    # visually d4 moves through d3 as the t0 value advance. Assert
    # tests end member: skewed left, inside, and skewed right
    d3 = TimeSeries(dsave)
    d4.t0 = -0.7  # no overlap test
    d3 -= d4
    assert np.allclose(d3.data, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(dsave)
    d4.t0 = -0.3  # overlap left
    d3 -= d4
    assert np.allclose(d3.data, [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1])
    d3 = TimeSeries(dsave)
    d4.t0 = 0.3  # d4 inside d3 test
    d3 -= d4
    assert np.allclose(d3.data, [1, 1, 1, -1, -1, -1, -1, -1, -1, 1])
    d3 = TimeSeries(dsave)
    d4.t0 = 0.7  # partial overlap right
    d3 -= d4
    assert np.allclose(d3.data, [1, 1, 1, 1, 1, 1, 1, -1, -1, -1])
    d3 = TimeSeries(dsave)
    d4.t0 = 1.0  # no overlap test right
    d3 -= d4
    assert np.allclose(d3.data, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1])

    # Repeat the same test for Seismogram objects
    # This section is edited cut-paste of above
    # Intentionally do not test _CoreSeismogram directly because
    # currently if it works for Seismogram it will for _CoreSeismogram
    d = _CoreSeismogram(10)
    d1 = make_constant_data_seis(d, nsamp=10)
    dsave = _CoreSeismogram(d1)
    d = _CoreSeismogram(6)
    d2 = make_constant_data_seis(d, t0=-0.2, nsamp=6, val=2.0)
    dsave = _CoreSeismogram(d1)
    d1 -= d2
    assert np.allclose(
        d1.data,
        np.array(
            [
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )
    d1 = _CoreSeismogram(dsave)
    d = d1 - d2
    assert np.allclose(
        d.data,
        np.array(
            [
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )

    d3 = Seismogram(10)
    d4 = Seismogram(6)
    d3 = make_constant_data_seis(d3, nsamp=10)
    d4 = make_constant_data_seis(d4, t0=-0.2, nsamp=6, val=2.0)
    dsave = Seismogram(d3)
    d3 -= d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d = d3 - d4
    assert np.allclose(
        d.data,
        np.array(
            [
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                [-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            ]
        ),
    )

    x = np.linspace(-0.7, 1.2, 20)
    for t in x:
        d3 = Seismogram(dsave)
        d4.t0 = t
        d3 -= d4

    # These are selected asserts of the incremental test above
    # visually d4 moves through d3 as the t0 value advance. Assert
    # tests end member: skewed left, inside, and skewed right
    d3 = Seismogram(dsave)
    d4.t0 = -0.7  # no overlap test
    d3 -= d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = -0.3  # overlap left
    d3 -= d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1],
                [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1],
                [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = 0.3  # d4 inside d3 test
    d3 -= d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, -1, -1, -1, -1, -1, -1, 1],
                [1, 1, 1, -1, -1, -1, -1, -1, -1, 1],
                [1, 1, 1, -1, -1, -1, -1, -1, -1, 1],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = 0.7  # partial overlap right
    d3 -= d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 1, 1, 1, 1, -1, -1, -1],
                [1, 1, 1, 1, 1, 1, 1, -1, -1, -1],
                [1, 1, 1, 1, 1, 1, 1, -1, -1, -1],
            ]
        ),
    )
    d3 = Seismogram(dsave)
    d4.t0 = 1.0  # no overlap test right
    d3 -= d4
    assert np.allclose(
        d3.data,
        np.array(
            [
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            ]
        ),
    )


def test_ExtractComponent():
    seis = Seismogram()
    seis.live = 1
    seis.data = dmatrix(np.random.rand(3, 6))
    seis.npts = 6
    ts = []
    for i in range(3):
        ts.append(_ExtractComponent(seis, i))
    for i in range(3):
        assert (ts[i].data == seis.data[i]).all()


@pytest.fixture(params=[ProcessingHistory, Seismogram, TimeSeries])
def ProcessingHistoryBase(request):
    return request.param


def test_ProcessingHistoryBase(ProcessingHistoryBase):
    ph = ProcessingHistoryBase()
    ph.set_jobname("testjob")
    ph.set_jobid("999")
    assert ph.elog.log_error("1", "2", ErrorSeverity(3)) == 1
    assert ph.elog[0].badness == ErrorSeverity.Complaint

    ph2 = ProcessingHistoryBase(ph)
    assert ph.jobname() == ph2.jobname()
    assert ph.jobid() == ph2.jobid()
    assert ph.elog[0].badness == ph2.elog[0].badness
    assert ph.elog[0].algorithm == ph2.elog[0].algorithm
    assert ph.elog[0].message == ph2.elog[0].message

    ph2 = ProcessingHistoryBase(ph)
    assert ph.jobname() == ph2.jobname()
    assert ph.jobid() == ph2.jobid()

    ph.set_as_raw("fakeinput", "0", "fakeuuid1", AtomicType.SEISMOGRAM)
    ph.new_map("onetoone", "0", AtomicType.SEISMOGRAM)
    assert len(ph.get_nodes()) == 1
    phred = ProcessingHistoryBase(ph)
    ph_list = []
    for i in range(4):
        ph_list.append(ProcessingHistoryBase())
        ph_list[i].set_as_raw(
            "fakedataset", "0", "fakeid_" + str(i), AtomicType.SEISMOGRAM
        )
        ph_list[i].new_map("onetoone", "0", AtomicType.SEISMOGRAM)
    phred.new_ensemble_process("testreduce", "0", AtomicType.SEISMOGRAM, ph_list)
    dic = phred.get_nodes()
    rec = 0
    for i in dic.keys():
        for j in dic[i]:
            print(i, " : ", j.uuid, " , ", j.status)
            rec += 1
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
    for k, v in ph_list2[0].get_nodes().items():
        if k not in ph_list2[0].id():
            nodes2[k] = str(v)
    nodes3 = {}
    for k, v in ph_list3[0].get_nodes().items():
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
        x = MetadataDefinitions("foo")
    except MsPASSError as err:
        print(err.message)
        assert "bad file" in err.message
        assert err.severity == ErrorSeverity.Invalid
    try:
        raise MsPASSError("test error1", ErrorSeverity.Informational)
    except MsPASSError as err:
        assert err.message == "test error1"
        assert err.severity == ErrorSeverity.Informational
    try:
        raise MsPASSError("test error2", "Suspect")
    except MsPASSError as err:
        assert err.message == "test error2"
        assert err.severity == ErrorSeverity.Suspect
    try:
        raise MsPASSError("test error3", 123)
    except MsPASSError as err:
        assert err.message == "test error3"
        assert err.severity == ErrorSeverity.Fatal
    try:
        raise MsPASSError("test error4")
    except MsPASSError as err:
        assert err.message == "test error4"
        assert err.severity == ErrorSeverity.Fatal

    # Add a more realistic example below
    with pytest.raises(MsPASSError, match="LU factorization"):
        d = Seismogram(50)
        d.set_live()
        d.dt = 1.0
        d.t0 = 0.0
        tm = dmatrix(3, 3)
        tm[0, 0] = 1.0
        tm[1, 0] = 1.0
        tm[1, 1] = 0.0
        tm[2, 2] = 1.0
        d.transform(tm)
        d.rotate_to_standard()


def test_PowerSpectrum():
    ts = TimeSeries(100)
    ts.data[0] = 1.0  # delta function - spectrum will be flat
    ts.live = True
    engine = MTPowerSpectrumEngine(100, 5, 10)
    spec = engine.apply(ts)
    # these are BasicSpectrum methods we test
    # nfft goes to next power of 2 for efficiency - with gnu fft 64+1
    assert spec.nf() == 65
    assert spec.live()
    spec.kill()
    assert spec.dead()
    spec.set_live()
    assert spec.Nyquist() == 0.5
    assert spec.f0() == 0.0
    assert spec.dt() == 1.0
    assert spec.rayleigh() == 0.01

    # needed tests for set_df, set_f0, set_dt, set_npts, sample_number
    # Depends upon MTPowerSpectrumEngine default which is to double
    # length as a zero pad.
    spec_copy = PowerSpectrum(spec)
    df_expected = 1.0 / (2.0 * (spec.nf() - 1))
    assert np.isclose(spec.df(), df_expected)
    spec = PowerSpectrum(spec_copy)
    # test setters
    spec.set_f0(1.0)
    assert spec.f0() == 1.0
    spec = PowerSpectrum(spec_copy)
    spec.set_dt(2.0)
    assert spec.dt() == 2.0

    # Repeat with no defaults
    engine = MTPowerSpectrumEngine(100, 5, 10, 512, ts.dt)
    spec = engine.apply(ts)
    assert spec.nf() == int(512 / 2) + 1
    df_expected = 1.0 / (2.0 * (spec.nf() - 1))
    assert np.isclose(spec.df(), df_expected)

    spec_copy = pickle.loads(pickle.dumps(spec))
    assert spec.df() == spec_copy.df()
    assert spec.f0() == spec_copy.f0()
    assert spec.spectrum_type == spec_copy.spectrum_type
    assert np.allclose(spec.spectrum, spec_copy.spectrum)


def test_DataGap():
    # verify default constructor
    dg = DataGap()
    # verify behavior adding one gap
    assert not dg.has_gap()
    tw = TimeWindow(10.0, 20.0)
    dg.add_gap(tw)
    assert dg.has_gap()
    assert dg.is_gap(15.0)
    assert not dg.is_gap(5.0)
    assert not dg.is_gap(100.0)
    gpl = dg.get_gaps()
    assert len(gpl) == 1

    # add another gap that is not overlapping.  Should produce two
    # distince gap definitions
    tw = TimeWindow(100.0, 150.0)
    dg.add_gap(tw)
    gpl = dg.get_gaps()
    assert len(gpl) == 2
    assert dg.is_gap(15.0)
    assert dg.is_gap(125.0)
    assert not dg.is_gap(50.0)

    # test has_gap with a TimeWindow argument (no arg is an existence test)
    tw = TimeWindow(50.0, 125.0)
    assert dg.has_gap(tw)
    tw.end = 75.0
    assert not dg.has_gap(tw)

    # test that adding an overlapping window works correctly.
    # This add_gaps should extend the 10,20 to 10,40
    # from here on we use size method to test length instead of
    # using get_gaps as above - faster but need earlier to test get_gaps
    tw = TimeWindow(15.0, 40.0)
    dg.add_gap(tw)
    n = dg.number_gaps()
    assert n == 2
    # this next text depends upon the fact that the container used in
    # DataGaps is ordered by window start time.   It  could fail if
    # the implementation changed.
    gpl = dg.get_gaps()
    assert np.isclose(gpl[0].start, 10.0)
    assert np.isclose(gpl[0].end, 40.0)

    # test copy constructor
    dg_copy = DataGap(dg)
    gpl = dg_copy.get_gaps()
    assert n == 2
    assert dg_copy.is_gap(12.0)

    # clear gaps discards everything
    dg_copy.clear_gaps()
    n = dg_copy.number_gaps()
    assert n == 0

    # shift the origin by 5.0 s and verify it worked correctly
    # this test also depends upon container have windows in start time order
    # reinitialize so we don't have dependency from above at this point
    dg = DataGap()
    tw = TimeWindow(10.0, 20.0)
    dg.add_gap(tw)
    tw = TimeWindow(100.0, 150.0)
    dg.add_gap(tw)
    dg.translate_origin(5.0)
    n = dg.number_gaps()
    gpl = dg.get_gaps()
    assert n == 2
    tw = gpl[0]
    assert np.isclose(tw.start, 5.0)
    assert np.isclose(tw.end, 15.0)
    tw = gpl[1]
    assert np.isclose(tw.start, 95.0)
    assert np.isclose(tw.end, 145.0)

    # test C++ operator+=
    # copy of above - this probably should be a fixture
    dg = DataGap()
    tw = TimeWindow(10.0, 20.0)
    dg.add_gap(tw)
    tw = TimeWindow(100.0, 150.0)
    dg.add_gap(tw)
    # this one will be used for adding in
    dgrhs = DataGap()
    # first a nonoverlaping window
    tw = TimeWindow(200.0, 250.0)
    dgrhs.add_gap(tw)
    dg += dgrhs
    n = dg.number_gaps()
    assert n == 3
    assert dg.is_gap(225.0)
    # now to add an overlapping window
    assert not dg.is_gap(190.0)
    tw = TimeWindow(185.0, 210.0)
    dg.add_gap(tw)
    n = dg.number_gaps()
    assert n == 3
    assert dg.is_gap(190.0)
    # test the subset method.
    # need to verify it works correctly in several situations
    # first test interval inside range
    # note at this point the content of dg is [ 10, 20; 100, 150, 185,250]
    twtest = TimeWindow(30.0, 160.0)
    dgs = dg.subset(twtest)
    assert dgs.number_gaps() == 1
    # test range larger than self
    twtest = TimeWindow(0.0, 500.0)
    dgs = dg.subset(twtest)
    assert dgs.number_gaps() == 3
    # overlaping left side test
    twtest = TimeWindow(15.0, 500.0)
    dgs = dg.subset(twtest)
    assert dgs.number_gaps() == 3
    # overlapping right side and left side
    twtest = TimeWindow(15.0, 200.0)
    dgs = dg.subset(twtest)
    assert dgs.number_gaps() == 3
    # exclude left
    twtest = TimeWindow(30.0, 200.0)
    dgs = dg.subset(twtest)
    assert dgs.number_gaps() == 2
    # exclude right
    twtest = TimeWindow(0.0, 170.0)
    dgs = dg.subset(twtest)
    assert dgs.number_gaps() == 2
