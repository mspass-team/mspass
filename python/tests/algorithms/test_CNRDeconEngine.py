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

from mspasspy.ccore.utility import AntelopePf,pfread
from mspasspy.ccore.seismic import (Seismogram,
                                    TimeSeries,
                                    TimeSeriesEnsemble,
                                    TimeReferenceType,
                                    DoubleVector)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.algorithms.CNRDecon import CNRRFDecon,CNRArrayDecon
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.algorithms.window import WindowData
from mspasspy.util.seismic import print_metadata


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
        d[i] += result[i+padlength]
    return d
def addnoise_seismogram(d,nscale=0.1):
    """
    Wrapper using previously written addnoise function to 
    add noise to a Seismogram object's data array. 
    """
    for k in range(3):
        x = ExtractComponent(d,k)
        x = addnoise(x.data,nscale=nscale)
        d.data[k,:] = DoubleVector(x)
    return d
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
    matrix replaced by the convolved data.   Note return is a full
    convolution of length d.npts+w.npts-d.dt.   Time 0 of the return 
    is correct for sequence. 
    """
    dsim=Seismogram(d)
    # compute the output length for full convolution
    # for numpy convolve function
    n = d.npts + w.npts - 1
    dsim.set_npts(n)
    # for full convolution the start time needs to be adjusted to 
    # this value
    dsim.t0 = d.t0 + w.t0;
    for k in range(3):
        work=ExtractComponent(d,k)
        convout=np.convolve(work.data,w.data)
        dsim.data[k,:] = DoubleVector(convout)
    return dsim

def make_test_data(noise_level=None,front_pad=40.0):
    """
    Makes test data Seismogram object.   Adds gaussian 
    noise with sigma=noise_level.   Change front_pad to 
    alter padding before t0.   Note front_pad/dt samples 
    are added to front of output and t0 is alterered 
    accordingly.  If noise_level is set that section will be
    filled with filtered data.  Filtering is frozen in addnoise_seismogam
    """
    wavelet=make_simulation_wavelet()
    dimp=make_impulse_data()
    d=convolve_wavelet(dimp,wavelet)
    samples_to_add=int(front_pad/d.dt)
    N = d.npts + samples_to_add
    d2 = Seismogram(d)
    d2.set_npts(N)
    d2.t0 -= samples_to_add*d.dt
    i0 = d2.sample_number(d.t0)
    d2.data[:,i0:] = d.data[:,:]
    if noise_level:
        d2=addnoise_seismogram(d2,nscale=noise_level)  
    return d2
def make_expected_result(wavelet):
    """
    This function computes an ideal output Seismogram from this tes.
    """
    dimp = make_impulse_data()
    dout = convolve_wavelet(dimp,wavelet)
    return dout

def test_CNRRFDecon():
    """
    """
    # generate simulation wavelet, error free data, and data with noise
    # copied before use below
    w0 = make_simulation_wavelet()
    d0 = make_test_data()
    d0wn = make_test_data(noise_level=5.0)
    
    d = Seismogram(d0wn)
    #CHANGE ME - needs a relative path when run with pytest
    pf=pfread('CNRDeconEngine.pf')
    engine=CNRDeconEngine(pf)
    nw = TimeWindow(-45.0,-5.0)
    sw = TimeWindow(-5.0,30.0)
    # useful for test but normal use would use output of broadband_snr_QC
    d['low_f_band_edge'] = 2.0
    d['high_f_band_edge'] = 8.0 
    d_decon,aout,iout = CNRRFDecon(d,
                                   engine,
                                   signal_window=sw,
                                   noise_window=nw,
                                   return_wavelet=True,
                                   )
    print("Metadata container content of decon output")
    print_metadata(d_decon)
    d_e = make_expected_result(iout)
    # may want to window this to reduce the size of the test data pattern file
    ionrm=np.linalg.norm(iout.data)
    e = aout - iout
    enrm = np.linalg.norm(e.data)
    print("computed prediction error=",enrm/ionrm)
    for k in range(3):
        di = ExtractComponent(d_decon,k)
        nrmdi = np.linalg.norm(di.data)
        print("RF estiamte norm=",nrmdi)
        di.data /= nrmdi
        # in these tests the decon output is windowed so we need 
        # to window dei
        dei = ExtractComponent(d_e,k)
        dei = WindowData(dei,di.t0,di.endtime())
        nrmdei = np.linalg.norm(dei.data)
        print("Expected output data vector norm=",nrmdei)
        dei.data /= nrmdei
        e = di - dei
        denrm=np.linalg.norm(di.data)
        enrm = np.linalg.norm(e.data)
        print("Data component {} prediction error={}".format(k,enrm/denrm))
    return

def test_CNRArrayDecon():
    # generate simulation wavelet, error free data, and data with noise
    # copied before use below
    w0 = make_simulation_wavelet()
    d0 = make_test_data()
    d0wn = make_test_data(noise_level=5.0)
    
test_CNRRFDecon()