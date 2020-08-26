#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 07:28:55 2020

This is a test program for CNR3CDecon (Colored noise three component deconvolution).
It is a variant of testdecon necessary because the api to CNR3CDecon is 
fundamentally different than the other decon methods that are children of
ScalarDecon.   CNR3CDecon is intimately linked to mspass while the others
were adapted from code originally developed by Yinzhi Wang for his PhD
dissertation.   
@author: pavlis
"""
import sys
sys.path.append('/home/pavlis/src/mspass/python')
from mspasspy.ccore import AntelopePf
from mspasspy.ccore import dmatrix
from mspasspy.ccore import CoreTimeSeries
from mspasspy.ccore import CoreSeismogram
from mspasspy.ccore import Seismogram
from mspasspy.ccore import TimeSeries
from mspasspy.ccore import TimeReferenceType
from mspasspy.ccore import CNR3CDecon
import numpy as np
from scipy import signal
from scipy import randn
#from scipy import signal
import matplotlib.pyplot as plt

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
def vectors2dmatrix(d):
    """
    Converts a list of three ndarrays in d to a dmatrix that is returned. 
    sizes of three components must match.
    """
    if(len(d)!=3):
        raise RuntimeError("vector2dmatrix:  input must be list of 3 ndarrays")
    n=len(d[0]);
    if((len(d[1])!=n) | (len(d[2])!=n)):
        raise RuntimeError("vector2dmatrix:  input vectors have irregular sizes - must be equal length")
    u=dmatrix(3,n)
    for i in range(3):
        for j in range(n):
            u[i,j]=d[i][j]
    return u
def plot3cs(d):
    """
    Plots a 3C seismogram object's data using matplotlib assumed loaded as plt.
    Use subplots to construct a set of 3 plots with the 3 components.
    
    :param d: is the input data
    """
    n=d.u.columns()
    fig,pltarr=plt.subplots(nrows=3)
    t=np.linspace(d.t0,d.t0+(n-1)*d.dt,num=n)
    y=np.ndarray(n)
    for k in range(3):
        for i in range(n):
            y[i]=d.u[k,i]
        pltarr[k].plot(t,y)
    return
def addnoise(d,nscale=1.0,padlength=1024,npoles=3,corners=[0.25,5.0]):
#def addnoise(d,nscale=1.0,padlength=1024,npoles=3,corners=[2.0,5.0]):
    """
    Helper function to add noise to ndarray d.  
    :param d: data to which noise is to be added and padded
    :param scale:  noise scale for gaussian normal noise
    :param padlength:   data padded on front by this many sample of noise
    """
    nd=len(d)
    n=nd+padlength
    dnoise=nscale*randn(n)
    sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=20.0)
    result=signal.sosfilt(sos,dnoise)
    # uncomment to use white noise
    result=dnoise;
    for i in range(nd):
        result[i+padlength]+=d[i]
    return result
def make_wavelet_noise_data(nscale=0.1,ns=2048,padlength=512,
        dt=0.05,npoles=3,corners=[0.08,0.8]):
    wtmp=CoreTimeSeries(ns)
    wn=TimeSeries(wtmp,'invalid')
    wn.t0=0.0
    wn.dt=dt
    wn.tref=TimeReferenceType.Relative
    wn.live=True
    nd=ns+2*padlength
    y=nscale*randn(nd)
    sos=signal.butter(npoles,corners,btype='bandpass',output='sos',fs=1.0/dt)
    y=signal.sosfilt(sos,y)
    for i in range(ns):
        wn.s[i]=y[i+padlength]
    return(wn)
###MAIN#############
# This is creates the same source wavelet as testdecon.  Could be made
# a library,but that is for later

imp=(20.0,-15.0,4.0,-1.0)
lag=(20,24,35,45)
n=100
dt=0.05
t0w=-1.0  # puts initial pulse at 0

d=make_impulse_vector(lag,imp,n)
t=np.linspace(t0w,t0w+(n-1)*dt,num=n)
#sos=signal.butter(3,[10,30],btype='bandpass',output='sos',fs=100)
sos=signal.butter(3,[2.0,6.0],btype='bandpass',output='sos',fs=20)
f=signal.sosfilt(sos,d)
#f=signal.convolve(d,win)
fig,(ao0,ao1)=plt.subplots(nrows=2)
ao0.plot(t,d)
ao0.set_title('impulse sequence')
ao1.plot(t,f)
ao1.set_title('source wavelet')
#plt.show()
wtmp=CoreTimeSeries(n)
wavelet=TimeSeries(wtmp,'invalid')
wavelet.set_t0(t0w)
wavelet.set_dt(dt)
# This isn't necessary at the moment because relative is the default
#wavelet.set_tref(TimeReferenceType.Relative)
wavelet.set_npts(n)
wavelet.set_live()
for i in range(n):
    wavelet.s[i]=f[i]

# Make 3 vectors with spikes at common time but different amplitude for
# each of the 3 components
impsig0=(100.0,-50.0,40.0,-10.0,5.0)
impsig1=(10.0,20.0,-60.0,-3.0,2.0)
impsig2=(-15.0,30.0,10.0,-20.0,15.0)
lagsig=(100,150,160,250,280)
nsig=1024
sig0=make_impulse_vector(lagsig,impsig0,nsig)
sig1=make_impulse_vector(lagsig,impsig1,nsig)
sig2=make_impulse_vector(lagsig,impsig2,nsig)
dsig0=signal.convolve(sig0,f)
dsig1=signal.convolve(sig1,f)
dsig2=signal.convolve(sig2,f)
icoffset=20
dsig0=dsig0[icoffset:icoffset+nsig]
dsig1=dsig1[icoffset:icoffset+nsig]
dsig2=dsig2[icoffset:icoffset+nsig]

# axar is a 2x2 matrix of plot handles 
fig2,axarr=plt.subplots(2,3)
t0=-5.0
dt=0.05
tsig=np.linspace(t0,t0+(nsig-1)*dt,num=nsig)
axarr[0,0].plot(tsig,sig0)
axarr[0,1].plot(tsig,sig1)
axarr[0,1].set_title('Impulse sequence')
axarr[0,2].plot(tsig,sig2)
axarr[1,0].plot(tsig,dsig0)
axarr[1,1].plot(tsig,dsig1)
axarr[1,1].set_title('Simulated data')
axarr[1,2].plot(tsig,dsig2)
# CNR3CDecon wants a preevent noise window so we need to create a 
# larger seismogram with noise we add to signal
nfullsig=2048
nsc=5   # noise scale factor
#nsc=0.1
padlength=nfullsig-nsig
dsig0=addnoise(dsig0,nsc,padlength)
dsig1=addnoise(dsig1,nsc,padlength)
dsig2=addnoise(dsig2,nsc,padlength)
tsigfull=np.linspace(t0,t0+(nfullsig-1)*dt,num=nfullsig)

# This creates a data matrix to build a 3C seismogram object
u=vectors2dmatrix([dsig0,dsig1,dsig2])
dtmp=CoreSeismogram(nsig)
d=Seismogram(dtmp,'undefined')
t0=t0-dt*padlength
d.set_npts(nfullsig)
d.u=u
d.set_t0(t0)
d.set_dt(dt)
# assume relative is default
#d.set_tref(TimeReferenceType.Relative)
d.set_live()
d.tref=TimeReferenceType.Relative
plot3cs(d)
#plt.show()

pf=AntelopePf('CNR3CDecon.pf')
decon=CNR3CDecon(pf)
decon.loaddata(d,False)
decon.loadnoise_data(d)
# Use defaults for initial test
nwavelet=make_wavelet_noise_data(corners=[0.25,5.0])
decon.loadnoise_wavelet(nwavelet)
decon.loadwavelet(wavelet)
dout=decon.process()
plot3cs(dout)
ao=decon.actual_output()
io=decon.ideal_output()
fig3,(f1,f2)=plt.subplots(nrows=2)
f1.plot(io.s)
f1.set_title('Ideal output')
f2.plot(ao.s)
f2.set_title('Actual output')
plt.show()
