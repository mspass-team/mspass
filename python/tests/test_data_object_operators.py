#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prototype test program for arithmetic operators.

Created on Mon Dec 28 06:13:20 2020

@author: pavlis
"""
from mspasspy.ccore.seismic import (_CoreTimeSeries,
                                    TimeSeries,
                                    _CoreSeismogram,
                                    Seismogram)
import numpy as np

def make_constant_data_ts(d,t0=0.0,dt=0.1,nsamp=5,val=1.0):
    """
    Fills TimeSeries (or CoreTimeSeries) data vector with 
    a constant value of a specified length and start time.
    Used for testing arithmetic operators.

    Parameters
    ----------
    d : TYPE
        DESCRIPTION.  TimeSeries or CoreTimeSeries skeleton to build upon
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
    d.npts=nsamp
    d.t0=t0
    d.dt=dt
    d.set_live()
    for i in range(nsamp):
        d.data[i]=val 
    return d

def make_constant_data_seis(d,t0=0.0,dt=0.1,nsamp=5,val=1.0):
    """
    Fills Seismogram (or Seismogram) data vector with 
    a constant value of a specified length and start time.
    Used for testing arithmetic operators.

    Parameters
    ----------
    d : TYPE
        DESCRIPTION.  TimeSeries or CoreTimeSeries skeleton to build upon
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
    d.npts=nsamp
    d.t0=t0
    d.dt=dt
    d.set_live()
    for i in range(nsamp):
        for k in range(3):
            d.data[k,i]=val 
    return d

d=_CoreTimeSeries(10)
d1=make_constant_data_ts(d,nsamp=10)
dsave=_CoreTimeSeries(d1)
d=_CoreTimeSeries(6)
d2=make_constant_data_ts(d,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; _CoreTimeSeries')
print(d1)
print(d1.data)
print('right had side for operators; _CoreTimeSeries ')
print(d2)
print(d2.data)
dsave=_CoreTimeSeries(d1)
d1+=d2
print('result of += operator')
print(d1)
print(d1.data)
assert np.allclose(d1.data,[3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
d1=_CoreTimeSeries(dsave)
d=d1+d2
print('result of binary + operator;  should be same as += just computed')
print(d)
print(d.data)
assert np.allclose(d.data,[3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
d1=_CoreTimeSeries(dsave)
print('Data before calling operator *= with val of 2.5')
print(d1)
print(d1.data)
d1 *= 2.5
print(d1)
print(d1.data)
assert np.allclose(d1.data,[2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5])
d3=TimeSeries(10)
d4=TimeSeries(6)
d3=make_constant_data_ts(d3,nsamp=10)
d4=make_constant_data_ts(d4,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; TimeSeries')
print(d3)
print(d3.data)
print('right had side for operators; TimeSeries ')
print(d4)
print(d4.data)
dsave=_CoreTimeSeries(d3)
d3=TimeSeries(dsave)
d3+=d4
print('result of += operator - should be identical to _CoreTimeSeries result')
print(d3)
print(d3.data)
assert np.allclose(d3.data,[3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(dsave)
d=d3+d4
print('result of binary + operator;  should be same as += just computed')
print(d)
print(d.data)
assert np.allclose(d.data,[3, 3, 3, 3, 1, 1, 1, 1, 1, 1])
d1=_CoreTimeSeries(dsave)
d3=TimeSeries(dsave)
print('Data before calling operator *= with val of 2.5')
print(d3)
print(d3.data)
d3 *= 2.5
print('Data after calling *= with val of 2.5')
print(d3)
print(d3.data)
assert np.allclose(d3.data,[2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5])
print('Starting incremental t0 test')
x=np.linspace(-0.7,1.2,20)
for t in x:
    d3=TimeSeries(dsave)
    d4.t0=t
    d3+=d4
    print('d4.t0=',d4.t0)
    print(d3.data)
# These are selected asserts of the incremental test above
# visually d4 moves through d3 as the t0 value advance. Assert 
# tests end member: skewed left, inside, and skewed right
d3=TimeSeries(dsave)
d4.t0=-0.7   # no overlap test
d3+=d4
assert np.allclose(d3.data,[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(dsave)
d4.t0=-0.3   # overlap left
d3+=d4
assert np.allclose(d3.data,[3, 3, 3, 1, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(dsave)
d4.t0=0.3   # d4 inside d3 test
d3+=d4
assert np.allclose(d3.data,[1, 1, 1, 3, 3, 3, 3, 3, 3, 1])
d3=TimeSeries(dsave)
d4.t0=0.7   # partial overlap right
d3+=d4
assert np.allclose(d3.data,[1, 1, 1, 1, 1, 1, 1, 3, 3, 3])
d3=TimeSeries(dsave)
d4.t0=1.0   # no overlap test right
d3+=d4
assert np.allclose(d3.data,[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
# Repeat the same test for Seismogram objects 
# This section is edited cut-paste of above
# Intentionally do not test _CoreSeismogram directly because 
# currently if it works for Seismogram it will for _CoreSeismogram

d=_CoreSeismogram(10)
d1=make_constant_data_seis(d,nsamp=10)
dsave=_CoreSeismogram(d1)
d=_CoreSeismogram(6)
d2=make_constant_data_seis(d,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; _CoreSeismogram')
print(d1)
print(d1.data)
print('right had side for operators; _CoreSeismogram ')
print(d2)
print(d2.data)
dsave=_CoreSeismogram(d1)
d1+=d2
print('result of += operator')
print(d1)
print(d1.data)
assert np.allclose(d1.data,np.array([[3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1. ,1., 1., 1., 1.]]))
d1=_CoreSeismogram(dsave)
d=d1+d2
print('result of binary + operator;  should be same as += just computed')
print(d)
print(d.data)
assert np.allclose(d.data,np.array([[3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1. ,1., 1., 1., 1.]]))
d1=_CoreSeismogram(dsave)
print('Data before calling operator *= with val of 2.5')
print(d1)
print(d1.data)
d1 *= 2.5
print('Data after calling *=operator')
print(d1)
print(d1.data)
assert np.allclose(d1.data,np.array([[2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5],
 [2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5],
 [2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5]]))
d3=Seismogram(10)
d4=Seismogram(6)
d3=make_constant_data_seis(d3,nsamp=10)
d4=make_constant_data_seis(d4,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; Seismogram')
print(d3)
print(d3.data)
print('right had side for operators; Seismogram ')
print(d4)
print(d4.data)
dsave=Seismogram(d3)
d3+=d4
print('result of += operator - should be identical to Seismogram result')
print(d3)
print(d3.data)
assert np.allclose(d3.data,np.array([[3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1. ,1., 1., 1., 1.]]))
d3=Seismogram(dsave)
d=d3+d4
print('result of binary + operator;  should be same as += just computed')
print(d)
print(d.data)
assert np.allclose(d.data,np.array([[3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1., 1., 1., 1., 1.],
 [3., 3., 3., 3., 1., 1. ,1., 1., 1., 1.]]))
d3=Seismogram(dsave)
print('Data before calling operator *= with val of 2.5')
print(d3)
print(d3.data)
d3 *= 2.5
print('Data after calling *=operator')
print(d3)
print(d3.data)
assert np.allclose(d1.data,np.array([[2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5],
 [2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5],
 [2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5,2.5]]))
print('Starting incremental t0 test')
x=np.linspace(-0.7,1.2,20)
for t in x:
    d3=Seismogram(dsave)
    d4.t0=t
    d3+=d4
    print('d4.t0=',d4.t0)
    print(d3.data)
    
# These are selected asserts of the incremental test above
# visually d4 moves through d3 as the t0 value advance. Assert 
# tests end member: skewed left, inside, and skewed right
d3=Seismogram(dsave)
d4.t0=-0.7   # no overlap test
d3+=d4
assert np.allclose(d3.data,np.array([ 
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] ]
    ))

d3=Seismogram(dsave)
d4.t0=-0.3   # overlap left
d3+=d4
assert np.allclose(d3.data,np.array([
    [3, 3, 3, 1, 1, 1, 1, 1, 1, 1],
    [3, 3, 3, 1, 1, 1, 1, 1, 1, 1],
    [3, 3, 3, 1, 1, 1, 1, 1, 1, 1]]
    ))
d3=Seismogram(dsave)
d4.t0=0.3   # d4 inside d3 test
d3+=d4
assert np.allclose(d3.data,np.array([
    [1, 1, 1, 3, 3, 3, 3, 3, 3, 1],
    [1, 1, 1, 3, 3, 3, 3, 3, 3, 1],
    [1, 1, 1, 3, 3, 3, 3, 3, 3, 1]]
    ))
d3=Seismogram(dsave)
d4.t0=0.7   # partial overlap right
d3+=d4
assert np.allclose(d3.data,np.array([
    [1, 1, 1, 1, 1, 1, 1, 3, 3, 3],
    [1, 1, 1, 1, 1, 1, 1, 3, 3, 3],
    [1, 1, 1, 1, 1, 1, 1, 3, 3, 3] ]
    ))
d3=Seismogram(dsave)
d4.t0=1.0   # no overlap test right
d3+=d4
assert np.allclose(d3.data,np.array([
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] ]
    ))


    
# Repeat exactly for - test but different numeric results
# just omit *= tests
d=_CoreTimeSeries(10)
d1=make_constant_data_ts(d,nsamp=10)
dsave=_CoreTimeSeries(d1)
d=_CoreTimeSeries(6)
d2=make_constant_data_ts(d,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; _CoreTimeSeries')
print(d1)
print(d1.data)
print('right had side for operators; _CoreTimeSeries ')
print(d2)
print(d2.data)
dsave=_CoreTimeSeries(d1)
d1-=d2
print('result of -= operator')
print(d1)
print(d1.data)
assert np.allclose(d1.data,[-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
d1=_CoreTimeSeries(dsave)
d=d1-d2
print('result of binary - operator;  should be same as -= just computed')
print(d)
print(d.data)
assert np.allclose(d.data,[-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(10)
d4=TimeSeries(6)
d3=make_constant_data_ts(d3,nsamp=10)
d4=make_constant_data_ts(d4,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; TimeSeries')
print(d3)
print(d3.data)
print('right had side for operators; TimeSeries ')
print(d4)
print(d4.data)
dsave=_CoreTimeSeries(d3)
d3=TimeSeries(dsave)
d3-=d4
print('result of -= operator - should be identical to _CoreTimeSeries result')
print(d3)
print(d3.data)
assert np.allclose(d3.data,[-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(dsave)
d=d3-d4
print('result of binary - operator;  should be same as -= just computed')
print(d)
print(d.data)
assert np.allclose(d.data,[-1, -1, -1, -1, 1, 1, 1, 1, 1, 1])
print('Starting incremental t0 test')
x=np.linspace(-0.7,1.2,20)
for t in x:
    d3=TimeSeries(dsave)
    d4.t0=t
    d3-=d4
    print('d4.t0=',d4.t0)
    print(d3.data)
# These are selected asserts of the incremental test above
# visually d4 moves through d3 as the t0 value advance. Assert 
# tests end member: skewed left, inside, and skewed right
d3=TimeSeries(dsave)
d4.t0=-0.7   # no overlap test
d3-=d4
assert np.allclose(d3.data,[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(dsave)
d4.t0=-0.3   # overlap left
d3-=d4
assert np.allclose(d3.data,[-1, -1, -1, 1, 1, 1, 1, 1, 1, 1])
d3=TimeSeries(dsave)
d4.t0=0.3   # d4 inside d3 test
d3-=d4
assert np.allclose(d3.data,[1, 1, 1, -1, -1, -1, -1, -1, -1, 1])
d3=TimeSeries(dsave)
d4.t0=0.7   # partial overlap right
d3-=d4
assert np.allclose(d3.data,[1, 1, 1, 1, 1, 1, 1, -1, -1, -1])
d3=TimeSeries(dsave)
d4.t0=1.0   # no overlap test right
d3-=d4
assert np.allclose(d3.data,[1, 1, 1, 1, 1, 1, 1, 1, 1, 1])
# Repeat the same test for Seismogram objects 
# This section is edited cut-paste of above
# Intentionally do not test _CoreSeismogram directly because 
# currently if it works for Seismogram it will for _CoreSeismogram

d=_CoreSeismogram(10)
d1=make_constant_data_seis(d,nsamp=10)
dsave=_CoreSeismogram(d1)
d=_CoreSeismogram(6)
d2=make_constant_data_seis(d,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; _CoreSeismogram')
print(d1)
print(d1.data)
print('right had side for operators; _CoreSeismogram ')
print(d2)
print(d2.data)
dsave=_CoreSeismogram(d1)
d1-=d2
print('result of -= operator')
print(d1)
print(d1.data)
assert np.allclose(d1.data,np.array([[-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1. ,1., 1., 1., 1.]]))
d1=_CoreSeismogram(dsave)
d=d1-d2
print('result of binary - operator;  should be same as -= just computed')
print(d)
print(d.data)
assert np.allclose(d.data,np.array([[-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1. ,1., 1., 1., 1.]]))

d3=Seismogram(10)
d4=Seismogram(6)
d3=make_constant_data_seis(d3,nsamp=10)
d4=make_constant_data_seis(d4,t0=-0.2,nsamp=6,val=2.0)
print('left hand side for operator; Seismogram')
print(d3)
print(d3.data)
print('right had side for operators; Seismogram ')
print(d4)
print(d4.data)
dsave=Seismogram(d3)
d3-=d4
print('result of -= operator - should be identical to Seismogram result')
print(d3)
print(d3.data)
assert np.allclose(d3.data,np.array([[-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1. ,1., 1., 1., 1.]]))
d3=Seismogram(dsave)
d=d3-d4
print('result of binary - operator;  should be same as -= just computed')
print(d)
print(d.data)
assert np.allclose(d.data,np.array([[-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1., 1., 1., 1., 1.],
 [-1., -1., -1., -1., 1., 1. ,1., 1., 1., 1.]]))

print('Starting incremental t0 test')
x=np.linspace(-0.7,1.2,20)
for t in x:
    d3=Seismogram(dsave)
    d4.t0=t
    d3-=d4
    print('d4.t0=',d4.t0)
    print(d3.data)
    
# These are selected asserts of the incremental test above
# visually d4 moves through d3 as the t0 value advance. Assert 
# tests end member: skewed left, inside, and skewed right
d3=Seismogram(dsave)
d4.t0=-0.7   # no overlap test
d3-=d4
assert np.allclose(d3.data,np.array([ 
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] ]
    ))

d3=Seismogram(dsave)
d4.t0=-0.3   # overlap left
d3-=d4
assert np.allclose(d3.data,np.array([
    [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1],
    [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1],
    [-1, -1, -1, 1, 1, 1, 1, 1, 1, 1]]
    ))
d3=Seismogram(dsave)
d4.t0=0.3   # d4 inside d3 test
d3-=d4
assert np.allclose(d3.data,np.array([
    [1, 1, 1, -1, -1, -1, -1, -1, -1, 1],
    [1, 1, 1, -1, -1, -1, -1, -1, -1, 1],
    [1, 1, 1, -1, -1, -1, -1, -1, -1, 1]]
    ))
d3=Seismogram(dsave)
d4.t0=0.7   # partial overlap right
d3-=d4
assert np.allclose(d3.data,np.array([
    [1, 1, 1, 1, 1, 1, 1, -1, -1, -1],
    [1, 1, 1, 1, 1, 1, 1, -1, -1, -1],
    [1, 1, 1, 1, 1, 1, 1, -1, -1, -1] ]
    ))
d3=Seismogram(dsave)
d4.t0=1.0   # no overlap test right
d3-=d4
assert np.allclose(d3.data,np.array([
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] ]
    ))


