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
import pytest
import pickle
import os
import numpy as np
from scipy import signal
from numpy.random import randn

from mspasspy.ccore.utility import pfread
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    SeismogramEnsemble,
    TimeReferenceType,
    DoubleVector,
)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.algorithms.CNRDecon import CNRRFDecon, CNRArrayDecon
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.algorithms.window import WindowData
from mspasspy.util.seismic import print_metadata


def make_impulse_vector(lag, imp, n=500):
    """
    Computes a (sparse) vector of impulse functions at a specified set of
    lags.   Used for generating fake data for a number of contexts.

    :param lag: is a list of lag values (int in samples) parallel with imp
    :param imp: is a list of values (amplitudes) for each lag.  Algorithm is
       simply to insert imp value at specified lag.
    :param n: length of output vector returned.
    :return: numpy vector of doubles of length n.  zero where lag,imp not defined.
    """
    if len(lag) != len(imp):
        raise RuntimeError(
            "make_impulse_vector:  lag and imp vectors must be equal length"
        )
    d = np.ndarray(n)
    for i in range(n):
        d[i] = 0.0
    for i in range(len(lag)):
        if (lag[i] < 0) | (lag[i] >= n):
            raise RuntimeError("make_impulse_vector:  lag out of range")
        d[lag[i]] = imp[i]
    return d


def addnoise(d, nscale=1.0, padlength=1024, npoles=3, corners=[0.1, 1.0]):
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
    nd = len(d)
    n = nd + padlength
    dnoise = nscale * randn(n)
    sos = signal.butter(npoles, corners, btype="bandpass", output="sos", fs=20.0)
    result = signal.sosfilt(sos, dnoise)
    for i in range(nd):
        d[i] += result[i + padlength]
    return d


def addnoise_seismogram(d, nscale=0.1):
    """
    Wrapper using previously written addnoise function to
    add noise to a Seismogram object's data array.
    """
    for k in range(3):
        x = ExtractComponent(d, k)
        x = addnoise(x.data, nscale=nscale)
        d.data[k, :] = DoubleVector(x)
    return d


def make_wavelet_noise_data(
    nscale=0.1, ns=2048, padlength=512, dt=0.05, npoles=3, corners=[0.08, 0.8]
):
    wn = TimeSeries(ns)
    wn.t0 = 0.0
    wn.dt = dt
    wn.tref = TimeReferenceType.Relative
    wn.live = True
    nd = ns + 2 * padlength
    y = nscale * randn(nd)
    sos = signal.butter(npoles, corners, btype="bandpass", output="sos", fs=1.0 / dt)
    y = signal.sosfilt(sos, y)
    for i in range(ns):
        wn.data[i] = y[i + padlength]
    return wn


def make_simulation_wavelet(
    n=100,
    dt=0.05,
    t0=-1.0,
    imp=(20.0, -15.0, 4.0, -1.0),
    lag=(20, 24, 35, 45),
    npoles=3,
    corners=[2.0, 6.0],
):
    dvec = make_impulse_vector(lag, imp, n)
    fsampling = int(1.0 / dt)
    sos = signal.butter(npoles, corners, btype="bandpass", output="sos", fs=fsampling)
    f = signal.sosfilt(sos, dvec)
    wavelet = TimeSeries(n)
    wavelet.set_t0(t0)
    wavelet.set_dt(dt)
    # This isn't necessary at the moment because relative is the default
    # wavelet.set_tref(TimeReferenceType.Relative)
    wavelet.set_npts(n)
    wavelet.set_live()
    for i in range(n):
        wavelet.data[i] = f[i]
    return wavelet


def make_impulse_data(n=1024, dt=0.05, t0=-5.0):
    # Compute lag for spike at time=0
    lag0 = int(-t0 / dt)
    z = make_impulse_vector([lag0], [150.0], n)
    rf_lags = (lag0, lag0 + 50, lag0 + 60, lag0 + 150, lag0 + 180)
    amps1 = (10.0, 20.0, -60.0, -3.0, 2.0)
    amps2 = (-15.0, 30.0, 10.0, -20.0, 15.0)
    ns = make_impulse_vector(rf_lags, amps1, n)
    ew = make_impulse_vector(rf_lags, amps2, n)
    d = Seismogram(n)
    d.set_t0(t0)
    d.set_dt(dt)
    d.set_live()
    d.tref = TimeReferenceType.Relative
    for i in range(n):
        d.data[0, i] = ew[i]
        d.data[1, i] = ns[i]
        d.data[2, i] = z[i]
    return d


def convolve_wavelet(d, w):
    """
    Convolves wavelet w with 3C data stored in Seismogram object d
    to create simulated data d*w.   Returns a copy of d with the data
    matrix replaced by the convolved data.   Note return is a full
    convolution of length d.npts+w.npts-d.dt.   Time 0 of the return
    is correct for sequence.
    """
    dsim = Seismogram(d)
    # compute the output length for full convolution
    # for numpy convolve function
    n = d.npts + w.npts - 1
    dsim.set_npts(n)
    # for full convolution the start time needs to be adjusted to
    # this value
    dsim.t0 = d.t0 + w.t0
    for k in range(3):
        work = ExtractComponent(d, k)
        convout = np.convolve(work.data, w.data)
        dsim.data[k, :] = DoubleVector(convout)
    return dsim


def make_test_data(noise_level=None, front_pad=40.0):
    """
    Makes test data Seismogram object.   Adds gaussian
    noise with sigma=noise_level.   Change front_pad to
    alter padding before t0.   Note front_pad/dt samples
    are added to front of output and t0 is alterered
    accordingly.  If noise_level is set that section will be
    filled with filtered data.  Filtering is frozen in addnoise_seismogam
    """
    wavelet = make_simulation_wavelet()
    dimp = make_impulse_data()
    d = convolve_wavelet(dimp, wavelet)
    samples_to_add = int(front_pad / d.dt)
    N = d.npts + samples_to_add
    d2 = Seismogram(d)
    d2.set_npts(N)
    d2.t0 -= samples_to_add * d.dt
    i0 = d2.sample_number(d.t0)
    d2.data[:, i0:] = d.data[:, :]
    if noise_level:
        d2 = addnoise_seismogram(d2, nscale=noise_level)
    return d2


def make_expected_result(wavelet):
    """
    This function computes an ideal output Seismogram from this tes.
    """
    dimp = make_impulse_data()
    dout = convolve_wavelet(dimp, wavelet)
    return dout


def verify_decon_output(d_decon, engine, wavelet):
    """
    Standardize test for output of the CNRDeconEngine on
    a single Seismogram passed via arg0.  Regenerates
    expected output on each call.  Inefficient but
    better for test stability.  arg2 (wavelet) is
    needed because actual_output method of engine for
    this operator requires it.
    """
    print("Metadata container content of decon output")
    print_metadata(d_decon)
    iout = engine.ideal_output()
    aout = engine.actual_output(wavelet)
    d_e = make_expected_result(iout)
    # may want to window this to reduce the size of the test data pattern file
    ionrm = np.linalg.norm(iout.data)
    e = aout - iout
    enrm = np.linalg.norm(e.data)
    print("computed prediction error=", enrm / ionrm)
    print("lag of peak for aout=", np.argmax(aout.data))
    print("lag of peak for iout=", np.argmax(iout.data))

    # assert enrm<0.05
    for k in range(3):
        di = ExtractComponent(d_decon, k)
        nrmdi = np.linalg.norm(di.data)
        print("RF estiamte norm=", nrmdi)
        di.data /= nrmdi
        # in these tests the decon output is windowed so we need
        # to window dei
        dei = ExtractComponent(d_e, k)
        dei = WindowData(dei, di.t0, di.endtime())
        nrmdei = np.linalg.norm(dei.data)
        print("Expected output data vector norm=", nrmdei)
        dei.data /= nrmdei
        e = di - dei
        denrm = np.linalg.norm(di.data)
        enrm = np.linalg.norm(e.data)
        print("Data component {} prediction error={}".format(k, enrm / denrm))
        assert enrm < 0.2


def test_CNRRFDecon():
    """
    Test function for CNRRFDecon function.   Error handlers for
    this function are tested in a different pytest function below
    """
    # generate simulation wavelet, error free data, and data with noise
    # copied before use below
    d0wn = make_test_data(noise_level=1.0)
    # necessary for test but normal use would use output of broadband_snr_QC
    d0wn["low_f_band_edge"] = 2.0
    d0wn["high_f_band_edge"] = 8.0

    d = Seismogram(d0wn)
    # use default pf file for this and all tests in this file
    pf = pfread("./data/pf/CNRDeconEngine.pf")
    engine = CNRDeconEngine(pf)
    nw = TimeWindow(-45.0, -5.0)
    sw = TimeWindow(-5.0, 30.0)
    # this is the wavelet used for the actual deconvolution
    rfwavelet0 = ExtractComponent(d, 2)
    rfwavelet0 = WindowData(rfwavelet0, sw.start, sw.end)
    rfwavelet = TimeSeries(rfwavelet0)

    d_decon, aout, iout = CNRRFDecon(
        d,
        engine,
        signal_window=sw,
        noise_window=nw,
        return_wavelet=True,
        use_3C_noise=True,
    )
    verify_decon_output(d_decon, engine, rfwavelet)
    # verify pickle of engine works -important for parallel processng
    # as dask and spark will pickle engine in map/reduce operators
    d = Seismogram(d0wn)
    rfwavelet - TimeSeries(rfwavelet0)
    engine_cpy = pickle.loads(pickle.dumps(engine))
    d_decon2,aout,iout = CNRRFDecon(d,
                                   engine_cpy,
                                   signal_window=sw,
                                   noise_window=nw,
                                   return_wavelet=True,
                                   use_3C_noise=True,
                                   )
    assert d_decon2.live
    assert aout.live
    assert iout.live
    assert d_decon2.npts==d_decon.npts
    assert np.isclose(d_decon.data,d_decon2.data).all()
    
    # verify_decon_output(d_decon, engine, rfwavelet)
    # repeat with 1c noise estimate option and return wavelet off
    d = Seismogram(d0wn)
    rfwavelet = TimeSeries(rfwavelet0)
    d_decon = CNRRFDecon(
        d,
        engine,
        signal_window=sw,
        noise_window=nw,
        return_wavelet=False,
        use_3C_noise=False,
    )
    verify_decon_output(d_decon, engine, rfwavelet)
    # repeat using power spectrum input option
    # use the internal engine to compute the spectrum because
    # the internal engine is tested elsewhere
    d = Seismogram(d0wn)
    rfwavelet = TimeSeries(rfwavelet0)
    n = ExtractComponent(d, 2)
    n = WindowData(n, n.t0, -5.0)  # different from above so df chaanges
    nspec = engine.compute_noise_spectrum(n)
    # in this mode the datum to handle is expected to be windowed
    # to contain the waveform to deconvolve alone
    s = WindowData(d, sw.start, sw.end)
    d_decon = CNRRFDecon(s, engine, noise_spectrum=nspec)
    verify_decon_output(d_decon, engine, rfwavelet)


def test_CNRRFDecon_error_handlers():
    """
    As the name implies this is the pytest code for checking
    all the error handlers in the CNRFDecon function.
    """
    # this copies above - really should be a pytest fixture
    # generate simulation wavelet, error free data, and data with noise
    # copied before use below
    d0wn = make_test_data(noise_level=1.0)
    # necessary for test but normal use would use output of broadband_snr_QC
    d0wn["low_f_band_edge"] = 2.0
    d0wn["high_f_band_edge"] = 8.0

    d = Seismogram(d0wn)
    pf = pfread("./data/pf/CNRDeconEngine.pf")
    engine = CNRDeconEngine(pf)
    nw = TimeWindow(-45.0, -5.0)
    sw = TimeWindow(-5.0, 30.0)
    # first arg type and validity checkers
    with pytest.raises(TypeError, match="illegal type="):
        d_decon = CNRRFDecon("foo", engine, signal_window=sw, noise_window=nw)

    d = Seismogram(d0wn)
    with pytest.raises(TypeError, match="Must be an instance of a CNRDeconEngine"):
        d_decon = CNRRFDecon(d, "foo", signal_window=sw, noise_window=nw)

    d = Seismogram(d0wn)
    with pytest.raises(ValueError, match="Illegal value received"):
        d_decon = CNRRFDecon(
            d,
            engine,
            component=20,
            signal_window=sw,
            noise_window=nw,
        )
    # verify handling of dead datum
    d = Seismogram(d0wn)
    d.kill()
    d_decon = CNRRFDecon(
        d,
        engine,
        signal_window=sw,
        noise_window=nw,
    )
    assert d_decon.dead()
    # this algorithm is expected to return d unaltered but marked dead
    # use these two simple checks only as more would be overkill
    assert d_decon.npts == d_decon.npts
    assert d_decon.t0 == d.t0

    # finally test handlers that kill and p
    d = Seismogram(d0wn)
    # this should cause power spectrum estiamtion to fail which should
    # post an error and kill the output
    nw = TimeWindow(1000.0, 5000.0)
    d_decon = CNRRFDecon(
        d,
        engine,
        signal_window=sw,
        noise_window=nw,
    )
    assert d_decon.dead()
    assert d_decon.elog.size() > 0
    # inconsistent dample rate will kill
    d = Seismogram(d0wn)
    d.dt = 2.0 * d.dt
    d_decon = CNRRFDecon(
        d,
        engine,
        signal_window=sw,
        noise_window=nw,
    )
    assert d_decon.dead()
    assert d_decon.elog.size() > 0

    # partial test of how this could go wrong - may need
    # additional variations
    d = Seismogram(d0wn)
    d_decon = CNRRFDecon(
        d,
        engine,
        signal_window=sw,
        noise_window=nw,
        bandwidth_subdocument_key=["foo", "bar"],
    )
    assert d_decon.dead()
    assert d_decon.elog.size() > 0


def make_ensemble_test_data(N=3):
    """
    Builds and ensemble with N members all with a common signal
    but with different noise components.  Those data are the
    inputs for the array decon method.
    """
    e = SeismogramEnsemble()
    e.set_live()
    for i in range(N):
        s = make_test_data(noise_level=1.0)
        e.member.append(s)
    return e


def test_CNRArrayDecon():
    e0 = make_ensemble_test_data()
    # create a seperate wavelet with lower noise level
    # note noise level of 5.0 is a frozen constant in make_ensemble_data
    d0 = make_test_data(noise_level=1.0)
    w0 = ExtractComponent(d0, 2)
    pf = pfread("./data/pf/CNRDeconEngine.pf")
    engine = CNRDeconEngine(pf)
    nw = TimeWindow(-45.0, -5.0)
    sw = TimeWindow(-5.0, 30.0)
    # need these in wavelet signal when
    w0["low_f_band_edge"] = 2.0
    w0["high_f_band_edge"] = 8.0
    # pattern for seismogram wavelet input
    s0 = WindowData(d0, sw.start, sw.end)

    # run the array method in the standard mode - should succeed
    w = TimeSeries(w0)
    e = SeismogramEnsemble(e0)
    e_d = CNRArrayDecon(
        e,
        w,
        engine,
        use_wavelet_bandwidth=True,
        noise_window=nw,
        signal_window=sw,
        return_wavelet=False,
    )
    assert e_d.live
    # for this simulation every member should have resuls similar
    # to CNRRFDecon ouput so we use the same function to verify
    # the output in a loop
    for d in e_d.member:
        verify_decon_output(d, engine, w)

    # variant with unwindowed seismogram input for wavelet
    w = Seismogram(d0)
    # default uses beam to sset these
    w["low_f_band_edge"] = 2.0
    w["high_f_band_edge"] = 8.0
    e = SeismogramEnsemble(e0)
    # assume default is return_wavelet=True
    e_d = CNRArrayDecon(e, w, engine, noise_window=nw, signal_window=sw)
    # for this simulation every member should have resuls similar
    # to CNRRFDecon ouput so we use the same function to verify
    # the output in a loop
    for d in e_d.member:
        verify_decon_output(d, engine, TimeSeries(w0))

    # variant with use_wavelet_bandwidth option
    w = Seismogram(d0)
    e = SeismogramEnsemble(e0)
    for i in range(len(e.member)):
        e.member[i]["low_f_band_edge"] = 2.0
        e.member[i]["high_f_band_edge"] = 8.0
    e_d = CNRArrayDecon(
        e,
        w,
        engine,
        noise_window=nw,
        signal_window=sw,
        use_wavelet_bandwidth=False,
        return_wavelet=False,
    )
    # for this simulation every member should have resuls similar
    # to CNRRFDecon ouput so we use the same function to verify
    # the output in a loop
    for d in e_d.member:
        verify_decon_output(d, engine, TimeSeries(w0))

    # test noise spectrum input option - this also should work
    # and give almost the same answer as above
    # run the array method in the standard mode - should succeed
    w = WindowData(w0, sw.start, sw.end)
    n = WindowData(w0, w0.t0, -5.0)
    nspec = engine.compute_noise_spectrum(n)
    e = SeismogramEnsemble(e0)
    e_d = CNRArrayDecon(
        e, w, engine, noise_spectrum=nspec, signal_window=sw, return_wavelet=False
    )
    # for this simulation every member should have resuls similar
    # to CNRRFDecon ouput so we use the same function to verify
    # the output in a loop
    for d in e_d.member:
        verify_decon_output(d, engine, w)
    return


def test_CNRArrayDecon_error_handlers():
    """
    As the name implies tests error handlers for array method
    """
    e0 = make_ensemble_test_data()
    # create a seperate wavelet with lower noise level
    # note noise level of 5.0 is a frozen constant in make_ensemble_data
    d0 = make_test_data(noise_level=1.0)
    w0 = ExtractComponent(d0, 2)
    sw = TimeWindow(-5.0, 30.0)
    # pattern for seismogram wavelet input
    s0 = WindowData(d0, sw.start, sw.end)

    pf = pfread("./data/pf/CNRDeconEngine.pf")
    engine = CNRDeconEngine(pf)
    nw = TimeWindow(-45.0, -5.0)
    sw = TimeWindow(-5.0, 30.0)

    # first test handlers for argument errors
    e = SeismogramEnsemble(e0)
    w = Seismogram(s0)
    with pytest.raises(TypeError, match="Illegal type for arg0"):
        e_d = CNRArrayDecon("foo", w, engine, noise_window=nw, signal_window=sw)

    with pytest.raises(TypeError, match="Illegal type for required arg1"):
        e_d = CNRArrayDecon(e, "foo", engine, noise_window=nw, signal_window=sw)

    with pytest.raises(ValueError, match="Illegal argument combination"):
        e_d = CNRArrayDecon(e, w, engine)

    # verify handlling of dead input
    e = SeismogramEnsemble(e0)
    e.kill()
    w = Seismogram(d0)
    e_d = CNRArrayDecon(e, w, engine, noise_window=nw, signal_window=sw)
    assert e_d.dead()

    # dead wavelet creates dead output and an error message
    e = SeismogramEnsemble(e0)
    w = Seismogram(d0)
    w.kill()
    e_d = CNRArrayDecon(e, w, engine, noise_window=nw, signal_window=sw)
    assert e_d.dead()
    assert e_d.elog.size() > 0

    # finally test handlers that log but do not throw exceptions
    e = SeismogramEnsemble(e0)
    w = Seismogram(d0)
    e_d = CNRArrayDecon(
        e, w, engine, noise_window=nw, signal_window=sw, bandwidth_keys=["foo", "bar"]
    )
    assert e_d.dead()
    assert e_d.elog.size() > 0


# test_CNRRFDecon()
# test_CNRRFDecon_error_handlers()
# test_CNRArrayDecon()
# test_CNRRFDecon_error_handlers()
