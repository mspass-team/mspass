#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 07:21:50 2020

@author: pavlis
"""

import numpy as np
import pytest

import mspasspy.ccore as mspass
from mspasspy.algorithms import scale
from mspasspy.algorithms import WindowData


# Build a simple CoreTimeSeries and CoreSeismogram with 
# 100 points and a small number of spikes that allow checking
# by a hand calculation
def setbasics(d, n):
    """
    Takes a child of BasicTimeSeries and defines required attributes with 
    a common set of putters.
    """
    d.npts=n
    d.set_dt(0.01)
    d.t0=0.0
    d.tref=mspass.TimeReferenceType.Relative
    d.live=True

def test_scale():
    dts=mspass.CoreTimeSeries(9)
    dir=setbasics(dts,9)
    d3c=mspass.CoreSeismogram(5)
    setbasics(d3c,5)
    dts.s[0]=3.0
    dts.s[1]=2.0
    dts.s[2]=-4.0
    dts.s[3]=1.0
    dts.s[4]=-100.0
    dts.s[5]=-1.0
    dts.s[6]=5.0
    dts.s[7]=1.0
    dts.s[8]=-6.0
    # MAD o=f above should be 2
    # perf of 0.8 should be 4
    # rms should be just over 10=10.010993957
    print('Starting tests for time series data of amplitude functions')
    ampmad=mspass.MADAmplitude(dts)
    print('MAD amplitude estimate=',ampmad)
    assert(ampmad==3.0)
    amprms=mspass.RMSAmplitude(dts)
    print('RMS amplitude estimate=',amprms)
    assert(round(amprms,2)==100.46)
    amppeak=mspass.PeakAmplitude(dts)
    ampperf80=mspass.PerfAmplitude(dts,0.8)
    print('Peak amplitude=',amppeak)
    print('80% clip level amplitude=',ampperf80)
    assert(amppeak==100.0)
    assert(ampperf80==6.0)
    print('Starting comparable tests for 3c data')
    d3c.u[0,0]=3.0
    d3c.u[0,1]=2.0
    d3c.u[1,2]=-4.0
    d3c.u[2,3]=1.0
    d3c.u[0,4]=np.sqrt(2)*(100.0)
    d3c.u[1,4]=-np.sqrt(2)*(100.0)
    ampmad=mspass.MADAmplitude(d3c)
    print('MAD amplitude estimate=',ampmad)
    amprms=mspass.RMSAmplitude(d3c)
    print('RMS amplitude estimate=',amprms)
    amppeak=mspass.PeakAmplitude(d3c)
    ampperf60=mspass.PerfAmplitude(d3c,0.6)
    print('Peak amplitude=',amppeak)
    print('60% clip level amplitude=',ampperf60)
    assert(amppeak==200.0)
    assert(ampperf60==4.0)
    assert(ampmad==3.0)
    amptest=round(amprms,2)
    assert(amptest==89.48)
    print('Trying scaling functions for TimeSeries')
    # we need a deep copy here since scaling changes the data
    d2=mspass.TimeSeries(dts)
    amp=mspass._scale(d2,mspass.ScalingMethod.Peak,1.0)
    print('Computed peak amplitude=',amp)
    print(d2.s)
    d2=mspass.TimeSeries(dts)
    amp=mspass._scale(d2,mspass.ScalingMethod.Peak,10.0)
    print('Computed peak amplitude with peak set to 10=',amp)
    print(d2.s)
    assert(amp==100.0)
    assert(d2.s[4]==-10.0)
    print('verifying scale has modified and set calib correctly')
    calib=d2.get_double('calib')
    assert(calib==10.0)
    d2=mspass.TimeSeries(dts)
    d2.put('calib',6.0)
    print('test 2 with MAD metric and initial calib of 6')
    amp=mspass._scale(d2,mspass.ScalingMethod.MAD,1.0)
    calib=d2.get_double('calib')
    print('New calib value set=',calib)
    assert(calib==18.0)
    print('Testing 3C scale functions')
    d=mspass.Seismogram(d3c)
    amp=mspass._scale(d,mspass.ScalingMethod.Peak,1.0)
    print('Peak amplitude returned by scale funtion=',amp)
    calib=d.get_double('calib')
    print('Calib value retrieved (assumed inital 1.0)=',calib)
    print('Testing python scale function wrapper - first on a TimeSeries with defaults')
    d2=mspass.TimeSeries(dts)
    amp=scale(d2)
    print('peak amplitude returned =',amp[0])
    assert(amp[0]==100.0)
    d=mspass.Seismogram(d3c)
    amp=scale(d)
    print('peak amplitude returned test Seismogram=',amp[0])
    assert(amp[0]==200.0)
    print('starting tests of scale on ensembles')
    print('first test TimeSeriesEnemble with 5 scaled copies of same vector used earlier in this test')
    ens=mspass.TimeSeriesEnsemble()
    scls=[2.0,4.0,1.0,10.0,5.0]  # note 4 s the median of this vector
    npts=dts.npts
    for i in range(5):
        d=mspass.TimeSeries(dts)
        for k in range(npts):
            d.s[k]*=scls[i]
        d.put('calib',1.0)
        ens.member.append(d)

    # work on a copy because scaling alters data in place
    enscpy=mspass.TimeSeriesEnsemble(ens)
    amps=scale(enscpy)
    print('returned amplitudes for members scaled individually')
    for i in range(5):
        print(amps[i])
        assert(amps[i]==100.0*scls[i])
    enscpy=mspass.TimeSeriesEnsemble(ens)
    amp=scale(enscpy,scale_by_section=True)
    print('average amplitude=',amp[0])
    #assert(amp[0]==4.0)
    avgamp=amp[0]
    for i in range(5):
        calib=enscpy.member[i].get_double("calib")
        print('member number ',i,' calib is ',calib)
        assert(round(calib)==400.0)
        #print(enscpy.member[i].s)

    # similar test for SeismogramEnsemble
    npts=d3c.npts
    ens=mspass.SeismogramEnsemble()
    for i in range(5):
        d=mspass.Seismogram(d3c)
        for k in range(3):
            for j in range(npts):
                d.u[k,j]*=scls[i]
        d.put('calib',1.0)
        ens.member.append(d)
    print('Running comparable tests on SeismogramEnsemble')
    enscpy=mspass.SeismogramEnsemble(ens)
    amps=scale(enscpy)
    print('returned amplitudes for members scaled individually')
    for i in range(5):
        print(amps[i])
        assert(round(amps[i])==round(200.0*scls[i]))
    print('Trying section scaling of same data')
    enscpy=mspass.SeismogramEnsemble(ens)
    amp=scale(enscpy,scale_by_section=True)
    print('average amplitude=',amp[0])
    assert(round(amp[0])==800.0)
    avgamp=amp[0]
    for i in range(5):
        calib=enscpy.member[i].get_double("calib")
        print('member number ',i,' calib is ',calib)
        assert(round(calib)==800.0)
def test_windowdata():
    npts=1000
    ts=mspass.TimeSeries()
    setbasics(ts,npts)
    for i in range(npts):
        ts.s[i]=float(i)
    t3c=mspass.Seismogram()
    setbasics(t3c,npts)
    for k in range(3):
        for i in range(npts):
            t3c.u[k,i]=100*(k+1)+float(i)
    
    win=mspass.TimeWindow(2,3)
    d=WindowData(ts,win)
    print('t y')
    for j in range(d.npts):
        print(d.time(j),d.s[j])
    assert(len(d.s) == 11)
    assert(d.t0==2.0)
    assert(d.endtime() == 3.0)
    d=WindowData(t3c,win)
    print('t x0 x1 x2')
    for j in range(d.npts):
        print(d.time(j),d.u[0,j],d.u[1,j],d.u[2,j])
    assert(d.u.columns() == 11)
    assert(d.t0==2.0)
    assert(d.endtime() == 3.0)
    print('testing error handling')
    t3c.kill()
    d=WindowData(t3c,win)
    assert(d.npts == 0 and (not d.live))
    d=WindowData(ts,win,preserve_history=True)
    print('Error message posted')
    print(d.elog.get_error_log())
    assert(d.elog.size() == 1)
    # this still throws an error but the message will be different
    d=WindowData(ts,win,preserve_history=True,instance='0')
    print('Error message posted')
    print(d.elog.get_error_log())
    assert(d.elog.size() == 1)
