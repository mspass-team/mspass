import array

import numpy as np
import pytest

from mspasspy.ccore import (dmatrix,
                            Metadata,
                            Seismogram,
                            TimeSeries)

def setup_function(function):
    pass

def test_dmatrix():
    dm = dmatrix()
    assert dm.rows() == 0

    dm = dmatrix(9,4)
    assert dm.rows() == 9
    assert dm.columns() == 4

    a = [array.array('l', (0 for _ in range(5))) for _ in range(3)]
    for i in range(3):
        for j in range(5):
            a[i][j] = i*5+j
    dm = dmatrix(a)
    assert np.equal(dm,a).all()

    dm_c = dmatrix(dm)
    assert (dm_c[:] == dm).all()

    dm_c.zero()
    assert not dm_c[:].any()

    a = np.zeros((7,4), dtype=np.double, order='F')
    for i in range(7):
        for j in range(4):
            a[i][j] = i*4+j
    dm = dmatrix(a)
    assert (dm == a).all()

    dm_c = dmatrix(dm)
    dm += dm_c
    assert (dm == a+a).all()
    dm -= dm_c
    dm -= dm_c
    assert not dm[:].any()

    dm_c = dmatrix(dm)
    
    a = np.zeros((7,4), dtype=np.single, order='C')
    for i in range(7):
        for j in range(4):
            a[i][j] = i*4+j
    dm = dmatrix(a)
    assert (dm == a).all()

    a = np.zeros((7,4), dtype=np.int, order='F')
    for i in range(7):
        for j in range(4):
            a[i][j] = i*4+j
    dm = dmatrix(a)
    assert (dm == a).all()

    a = np.zeros((7,4), dtype=np.unicode_, order='C')
    for i in range(7):
        for j in range(4):
            a[i][j] = i*4+j
    dm = dmatrix(a)
    assert (dm == np.float_(a)).all()

    a = np.zeros((53,37), dtype=np.double, order='C')
    for i in range(53):
        for j in range(37):
            a[i][j] = i*37+j
    dm = dmatrix(a)
    
    assert dm[17, 23] == a[17, 23]
    assert (dm[17] == a[17]).all()
    assert (dm[::] == a[::]).all()
    assert (dm[3::] == a[3::]).all()
    assert (dm[:5:] == a[:5:]).all()
    assert (dm[::7] == a[::7]).all()
    assert (dm[-3::] == a[-3::]).all()
    assert (dm[:-5:] == a[:-5:]).all()
    assert (dm[::-7] == a[::-7]).all()
    assert (dm[11:41:7] == a[11:41:7]).all()
    assert (dm[-11:-41:-7] == a[-11:-41:-7]).all()
    assert (dm[3::, 13] == a[3::, 13]).all()
    assert (dm[19, :5:] == a[19, :5:]).all()
    assert (dm[::-7,::-11] == a[::-7,::-11]).all()

    with pytest.raises(IndexError, match = 'out of bounds for dmatrix'):
        dummy = dm[3,50]
    with pytest.raises(IndexError, match = 'out of bounds for axis 1'):
        dummy = dm[80]
    
    with pytest.raises(IndexError, match = 'out of bounds for dmatrix'):
        dm[3,50] = 1.0
    with pytest.raises(IndexError, match = 'out of bounds for axis 0'):
        dm[60,50] = 1

    dm[7,17] = 3.14
    assert dm[7,17] == 3.14

    dm[7,17] = '3.14'
    assert dm[7,17] == 3.14

    dm[7] = 10
    assert (dm[7] == 10).all()

    dm[::] = a
    assert (dm == a).all()

    dm[:,-7] = 3.14
    assert (dm[:,-7] == 3.14).all()

    dm[17,:] = 3.14
    assert (dm[17,:] == 3.14).all()

    dm[3:7,-19:-12] = 3.14
    assert (dm[3:7,-19:-12] == 3.14).all()
