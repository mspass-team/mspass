#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module is a companion to a jupyter notebook tutorial documenting 
the receiver function deconvolution algorithms in MsPASS.   It has a 
string of frozen properties designed just for this tutorial so it is 
best left in this location and used only by the tutorial or students of 
the tutorial interested in what is under the hood.  

The entire purpose of this module is to generate a set of synthetic 
waveforms that can be used to demonstrate deconvolution methods.

Created on Wed Dec 23 10:29:26 2020

@author: Gary L Pavlis
"""
import numpy as np
from scipy import signal
from numpy.random import randn

from mspasspy.ccore.utility import AntelopePf
from mspasspy.ccore.seismic import (Seismogram,
                                    TimeSeries,
                                    TimeSeriesEnsemble,
                                    TimeReferenceType)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.algorithms.CNRDecon import CNRRFDecon,CNRArrayDecon

def make_impulse_vector(lag,imp,n=500):
    """
    Computes a (sparse) vector of impulse functions at a specified set of
    lags.   Used for generating fake data for a number of contexts.
    
    :param lag: is a list of lag values (int in samples) parallel with imp
    :param imp: is a list of values (amplitudes) for each lag.  Algorithm is
       simply to insert imp value at specified lag.  
    :param n: length of output vector returned.
    :return: numpy vector of doubles of length n.  zero where lag,imp not defined.
    """
    if(len(lag)!=len(imp)):
        raise RuntimeError("make_impulse_vector:  lag and imp vectors must be equal length")
    d=np.ndarray(n)
    for i in range(n):
        d[i]=0.0
    for i in range(len(lag)):
        if((lag[i]<0) | (lag[i]>=n)):
            raise RuntimeError("make_impulse_vector:  lag out of range");
        d[lag[i]]=imp[i]
    return d
def addnoise(d,nscale=1.0,padlength=1024,npoles=3,corners=[0.1,1.0]):
    """
    Helper function to add noise to ndarray d.  The approach is a 
    little weird that we shift the data to the right by padlength 
    adding filtered random data to the front if the signal.   
    The code later sets t0 correctly based on padlength - ok 
    for a test program but do not recycle me.
    
    :param d: data to which noise is to be added and padded
    :param scale:  noise scale for gaussian normal noise
    :param padlength:   data padded on front by this many sample of noise
    """
    nd=len(d)
    n=nd+padlength
    dnoise=nscale*randn(n)
    sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=20.0)
    result=signal.sosfilt(sos,dnoise)
    for i in range(nd):
        result[i+padlength]+=d[i]
    return result
def make_wavelet_noise_data(nscale=0.1,ns=2048,padlength=512,
        dt=0.05,npoles=3,corners=[0.08,0.8]):
    wn=TimeSeries(ns)
    wn.t0=0.0
    wn.dt=dt
    wn.tref=TimeReferenceType.Relative
    wn.live=True
    nd=ns+2*padlength
    y=nscale*randn(nd)
    sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=1.0/dt)
    y=signal.sosfilt(sos,y)
    for i in range(ns):
        wn.data[i]=y[i+padlength]
    return(wn)
def make_simulation_wavelet(n=100,dt=0.05,t0=-1.0,
                            imp=(20.0,-15.0,4.0,-1.0),
                            lag=(20,24,35,45),
                            npoles=3,
                            corners=[2.0,6.0]):
    dvec=make_impulse_vector(lag,imp,n)
    fsampling=int(1.0/dt)
    sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=fsampling)
    f=signal.sosfilt(sos,dvec)
    wavelet=TimeSeries(n)
    wavelet.set_t0(t0)
    wavelet.set_dt(dt)
    # This isn't necessary at the moment because relative is the default
    #wavelet.set_tref(TimeReferenceType.Relative)
    wavelet.set_npts(n)
    wavelet.set_live()
    for i in range(n):
        wavelet.data[i]=f[i]
    return wavelet
def make_impulse_data(n=1024,dt=0.05,t0=-5.0):
    # Compute lag for spike at time=0
    lag0=int(-t0/dt)
    z=make_impulse_vector([lag0],[150.0],n)
    rf_lags=(lag0,lag0+50,lag0+60,lag0+150,lag0+180)
    amps1=(10.0,20.0,-60.0,-3.0,2.0)
    amps2=(-15.0,30.0,10.0,-20.0,15.0)
    ns=make_impulse_vector(rf_lags,amps1,n)
    ew=make_impulse_vector(rf_lags,amps2,n)
    d=Seismogram(n)
    d.set_t0(t0)
    d.set_dt(dt)
    d.set_live()
    d.tref=TimeReferenceType.Relative
    for i in range(n):
        d.data[0,i]=ew[i]
        d.data[1,i]=ns[i]
        d.data[2,i]=z[i]
    return d

def convolve_wavelet(d,w):
    """
    Convolves wavelet w with 3C data stored in Seismogram object d 
    to create simulated data d*w.   Returns a copy of d with the data 
    matrix replaced by the convolved data.
    """
    dsim=Seismogram(d)
    # We use scipy convolution routine which requires we copy the 
    # data out of the d.data container one channel at a time.
    wavelet=[]
    n=w.npts
    for i in range(n):
        wavelet.append(w.data[i])
    for k in range(3):
        work=[]
        n=d.npts
        for i in range(n):
            work.append(d.data[k,i])
        # Warning important to put work first and wavelet second in this call
        # or timing will be foobarred
        work=signal.convolve(work,wavelet)
        for i in range(n):
            dsim.data[k,i]=work[i]
    return dsim
def addnoise(d,nscale=1.0,padlength=1024,npoles=3,corners=[0.1,1.0]):
    """
    Helper function to add noise to Seismogram d.  The approach is a 
    little weird in that we shift the data to the right by padlength 
    adding filtered random data to the front of the signal.   
    The padding is compensated by changes to t0 to preserve relative time 
    0.0.  The purpose of this is to allow adding colored noise to 
    a simulated 3C seismogram. 
    
    :param d: Seismogram data to which noise is to be added and padded
    :param nscale:  noise scale for gaussian normal noise
    :param padlength:   data padded on front by this many sample of noise
    :param npoles: number of poles for butterworth filter 
    :param corners:  2 component array with corner frequencies for butterworth bandpass.
    """
    nd=d.npts
    n=nd+padlength
    result=Seismogram(d)
    result.set_npts(n)
    newt0=d.t0-d.dt*padlength
    result.set_t0(newt0)
    # at the time this code was written we had a hole in the ccore 
    # api wherein operator+ and operator+= were not defined. Hence in this 
    # loop we stack the noise and signal inline.
    for k in range(3):
        dnoise=nscale*randn(n)
        sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=20.0)
        nfilt=signal.sosfilt(sos,dnoise)
        for i in range(n):
            result.data[k,i]=nfilt[i]
        for i in range(nd):
            t=d.time(i)
            ii=result.sample_number(t)
            # We don't test range here because we know we won't 
            # go outside bounds because of the logic of this function
            result.data[k,ii]+=d.data[k,i]
    return result
def make_test_data(noise_level=None):
    wavelet=make_simulation_wavelet()
    dimp=make_impulse_data()
    d=convolve_wavelet(dimp,wavelet)
    if noise_level:
        d=addnoise(d,nscale=noise_level)  
    return d
def load_expected_result():
    """
    This function needs to be created after a working version is created 
    in the grapical_test_CNRDecon.py file.   i.e. that script needs to 
    pickle an expected result to a file that this funtion will read. 
    The result is the expected output of the CNRRFDecon function 
    and the CNRArrayDecon function. 
    """
    pass

def test_CNRRFDecon():
    """
    """
    # generate simulation wavelet, error free data, and data with noise
    # copied before use below
    w0 = make_simulation_wavelet()
    d0 = make_test_data()
    d0wn = make_test_data(noise_level=5.0)
    # this creates the expected output of both CNR function with 
    # error free simulation data
    d_e,aout_e,iout_e = load_expected_result()
    
    d = Seismogram(d0)
    pf=AntelopePf('CNRDecon.pf')
    engine=CNRDeconEngine(pf)
    nw = TimeWindow(-45.0,-5.0)
    # useful for test but normal use would use output of broadband_snr_QC
    d['low_f_band_edge'] = 2.0
    d['high_f_band_edge'] = 8.0 
    d_decon,aout,iout = CNRRFDecon(d,engine,noise_window=nw,return_wavelet=True)
    d_e,aout_e,iout_e = load_expected_result()
    # may want to window this to reduce the size of the test data pattern file
    