#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 07:49:48 2020

Test python program for deconvolution code.   Some functions in this test
program may be turned into library routines at some point, but that is
to be determined.  
@author: pavlis
"""
import sys
sys.path.append('/home/pavlis/src/mspass/python')
from mspasspy.ccore import WaterLevelDecon
from mspasspy.ccore import LeastSquareDecon
from mspasspy.ccore import MultiTaperSpecDivDecon
from mspasspy.ccore import MultiTaperXcorDecon
from mspasspy.ccore import AntelopePf
#from mspasspy.ccore import dmatrix
from mspasspy.ccore import CoreTimeSeries
#from mspasspy.ccore import CoreSeismogram
#from mspasspy.ccore import Seismogram
from mspasspy.ccore import TimeSeries
import scipy as np
from scipy import signal
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

def plot_wavelets(io,ao,title="Water Level"):
    """
    Plot ideal output(io) and actual output(ao) CoreTimeSeries
    outputs of decon operators. Use title.
    """
    diff=np.ndarray(io.npts)
    for i in range(io.npts):
        diff[i]=ao.s[i]-io.s[i]
    fig,(f1,f2,f3)=plt.subplots(nrows=3)
    f1.plot(io.s)
    f2.plot(ao.s)
    f3.plot(diff)
    f1.set_title(title)
    plt.show()
# some initial testing using only scipy
# first make a proto source wavelet that is minimum phase
imp=(20.0,-15.0,4.0,-1.0)
# This makes it maximum phase - previous reversed
#imp=(-1.0,4.0,-15.0,20.0)
lag=(20,24,35,45)
n=100
dt=0.05
t0w=-1.0  # puts initial pulse at 0

d=make_impulse_vector(lag,imp,n)
t=np.linspace(t0w,t0w+(n-1)*dt,num=n)
#sos=signal.butter(3,[10,30],btype='bandpass',output='sos',fs=100)
#f=signal.sosfilt(sos,d)
b,a=signal.butter(3,[10,30],btype='bandpass',fs=100)
f=signal.lfilter(b,a,d)
#f=signal.convolve(d,win)
fig,(ao0,ao1)=plt.subplots(nrows=2)
ao0.plot(t,d)
ao0.set_title('impulse sequence')
ao1.plot(t,f)
ao1.set_title('source wavelet')
#plt.show()

# now build a more sparse signal to use for simple deconvolution tests
impsig=(100.0,-50.0,40.0,-10.0,5.0)
lagsig=(100,150,160,250,280)
nsig=1024
sig=make_impulse_vector(lagsig,impsig,nsig)
fig2,(b1,b2)=plt.subplots(nrows=2)
t0=-5.0
dt=0.05
tsig=np.linspace(t0,t0+(nsig-1)*dt,num=nsig)
b1.plot(tsig,sig)
b1.set_title('impulse sequence')
dsig=signal.convolve(sig,f)
#print('simulated data length=',len(dsig))
icoffset=20
dsig=dsig[icoffset:icoffset+nsig]
b2.plot(tsig,dsig)
b2.set_title('simulated data')
#plt.show()

# this is a master pf that 
#contains all the parameters for
# each operator in Arr sections
pf=AntelopePf('scalardecontest.pf')
md=pf.get_branch('LeastSquare')
lsop=LeastSquareDecon(md)
# Assume all the pf's read here have the same window start time.
# We need that to set the time scale because these operators to not
# do time bookkeeping
t0old=t0
t0=md.get_double("deconvolution_data_window_start")
if(t0old!=t0):
    print("Warning:  t0 of simulation=",t0old," does not match pf value=",t0)
    print("Plots will show an timing shift of ",t0-t0old)
tsig=np.linspace(t0,t0+(nsig-1)*dt,num=nsig)
fig3,(c1,c2)=plt.subplots(nrows=2)
md=pf.get_branch('WaterLevel')
wlop=WaterLevelDecon(md)
md2=pf.get_branch('MultiTaperSpecDiv')
mtsdop=MultiTaperSpecDivDecon(md2)
md3=pf.get_branch('MultiTaperXcor')
mtxcop=MultiTaperXcorDecon(md3)

# This is a hack method to get ndarray data into a std::vector container
# required by the c api.  Method here assumes constructors initialize
# data vectors to all zeros of length nsig

wavelet=CoreTimeSeries(nsig)
sigts=CoreTimeSeries(nsig)
# Need to offset wavelet for these operators so it has the same t0 
# equivalent as the sig vector.   i0 is the computed interger offset
# to make the output time aligned. 
i0=int((t0w-t0)/dt)
for i in range(len(f)):
    wavelet.s[i+i0]=f[i]
for i in range(len(dsig)):
    sigts.s[i]=dsig[i]
wlop.load(wavelet.s,sigts.s)
wlop.process()
dout=wlop.getresult()
c1.plot(tsig,dout)
c1.set_title('Water level decon')
lsop.load(wavelet.s,sigts.s)
lsop.process()
dout=lsop.getresult()
c2.plot(tsig,dout)
c2.set_title('Least squares damped inverse')
# multitaper methods require a noise vector.  Here we generate simple
# gaussian noise with variance set by a constant
nscale=0.0005
narr=nscale*np.randn(nsig)
n=CoreTimeSeries(nsig)
for i in range(nsig):
    n.s[i]=narr[i]
mtsdop.load(wavelet.s,sigts.s,n.s)
#mtsdop.loadnoise(n.s)
mtsdop.process()
dout=mtsdop.getresult()
fig4,(d1,d2)=plt.subplots(nrows=2)
d1.plot(tsig,dout)
d1.set_title('MultiTaper Spectral Division Method')
mtxcop.load(wavelet.s,sigts.s,n.s)
mtxcop.process()
dout=mtxcop.getresult()
d2.plot(tsig,dout)
d2.set_title('MultiTaper Correlation Method')
plt.show()
# new stuff to compare actual, ideal outputs, and inverse
# wavelets.  Start with water level.
idealout=wlop.ideal_output()
#plt.plot(idealout.s)
ao=wlop.actual_output()
#plt.plot(ao.s)
plot_wavelets(idealout,ao,title="Water level")
idealout=lsop.ideal_output()
ao=lsop.actual_output()
plot_wavelets(idealout,ao,title="Least Square")
idealout=mtsdop.ideal_output()
ao=mtsdop.actual_output()
plot_wavelets(idealout,ao,title="MT spectral division")
idealout=mtxcop.ideal_output()
ao=mtxcop.actual_output()
plot_wavelets(idealout,ao,title="MT correlation method")
inv1=wlop.inverse_wavelet()
inv2=lsop.inverse_wavelet()
inv3=mtsdop.inverse_wavelet()
inv4=mtxcop.inverse_wavelet()
figinv,(f1,f2,f3,f4)=plt.subplots(nrows=4)
f1.plot(inv1.s)
f2.plot(inv2.s)
f3.plot(inv3.s)
f4.plot(inv4.s)
f1.set_title("inverse wavelets")
plt.show()