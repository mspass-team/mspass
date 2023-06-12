#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import mspasspy.ccore.algorithms.deconvolution as Ms
from mspasspy.algorithms.MTPowerSpectrumEngine  import MTPowerSpectrumEngine
import mspasspy.ccore.algorithms.deconvolution as Ms 
from mspasspy.ccore.seismic import TimeSeries
import math
import numpy as np

# for debugging only - remove this comment if plotting is needed for debug
#import matplotlib.pyplot as plt

def build_signal(ts,Ncycles,snr=-1.0)->TimeSeries:
    """
    Loads data vector of input TimeSeries object ts with a 
    sine wave + gaussina noise with a specified signal to noise 
    ratio (snr argument).  Ncycles is the number of cycles of the 
    sine wave for the generated signal. 
    snr less than 1 is taken to mean do not add errors.
    
    Makes not sanity checks on parameters assuning use in test
    environeent where the results have been previously checked.
    """
    T=ts.dt*ts.npts
    Tsin = T/Ncycles
    for i in range(ts.npts):
        ts.data[i] = math.sin(2.0*math.pi*i*ts.dt/Tsin) 
        if snr>0.0:
            ts.data[i] +=  np.random.normal(scale=1.0/snr)
    return ts

def build_test_TimeSeries(n=512,dt=0.1)->TimeSeries:
    """
    Build test signal of length n by calling build_signal and doing 
    other gyrations to build a valid TimeSeries object. 
    """
    ts = TimeSeries(n)
    ts.dt = dt
    ts.t0 = 0.0
    ts.set_live()
    ts = build_signal(ts,32,snr=1.0)
    return ts

def test_MTPowerSpectrumEngine():
    ts = build_test_TimeSeries()
    PrietoEngine=MTPowerSpectrumEngine(ts.npts,4.0,8,nfft=1024,iadapt=1)
    MsPASSEngine=Ms.MTPowerSpectrumEngine(ts.npts,4.0,8,1024,ts.dt)
    spec1=PrietoEngine.apply(ts)
    spec2=MsPASSEngine.apply(ts)
    assert PrietoEngine.time_bandwidth_product() == 4.0
    assert PrietoEngine.number_tapers == 8
    assert PrietoEngine.nfft == 1024
    
    assert MsPASSEngine.nf() == 513
    assert MsPASSEngine.number_tapers() == 8
    assert MsPASSEngine.time_bandwidth_product() == 4.0
    
    # Remove comments on these plots if these tests fail and need checking
    #fig, ax = plt.subplots(2)
    #ax[0].plot(spec1.frequencies(),spec1.spectrum,'-')
    #ax[1].plot(spec2.frequencies(),spec2.spectrum,'-')
    #plt.show()
    nrm1=np.linalg.norm(spec1.spectrum)
    nrm2=np.linalg.norm(spec2.spectrum)
    frac = abs(nrm2-nrm1)/nrm1
    assert frac < 0.01
    # This may not work for automatic setting nfft but works 
    # for test here woth nfft=1024
    assert len(spec1.spectrum) == len(spec2.spectrum)
    assert spec1.df() == spec2.df()
    # This method isn't currently implemented in the Prieto version
    # probably should not be, but maybe should be trapped
    dfnew = MsPASSEngine.set_df(0.001)
    assert dfnew == MsPASSEngine.df()

    