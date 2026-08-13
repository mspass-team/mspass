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

from mspasspy.ccore.utility import ErrorSeverity, Metadata, MsPASSError, pfread
from mspasspy.ccore.seismic import (
    PowerSpectrum,
    Seismogram,
    TimeSeries,
    SeismogramEnsemble,
    TimeReferenceType,
    DoubleVector,
)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.algorithms.CNRDecon import (
    CNRRFDecon,
    CNRArrayDecon,
    fetch_bandwidth_data,
)
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
    # Generate noise and add minimal offset only to values close to zero
    dnoise_raw = nscale * randn(n)
    # Add very small offset only to values that are very close to zero
    dnoise = np.where(np.abs(dnoise_raw) < 1e-10, dnoise_raw + 1e-8, dnoise_raw)
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
    This function computes the expected output Seismogram from the output
    shaping wavelet used in this test.
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
    iout = engine.output_shaping_wavelet()
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
        dei = WindowData(dei, di.t0, di.endtime(), short_segment_handling="pad")
        nrmdei = np.linalg.norm(dei.data)
        print("Expected output data vector norm=", nrmdei)
        dei.data /= nrmdei
        e = di - dei
        denrm = np.linalg.norm(di.data)
        enrm = np.linalg.norm(e.data)
        print("Data component {} prediction error={}".format(k, enrm / denrm))
        assert enrm < 0.2


@pytest.mark.parametrize(
    "old,new,match",
    [
        (
            "deconvolution_data_window_start -5.0\n"
            "deconvolution_data_window_end 30.0",
            "deconvolution_data_window_start 10.0\n"
            "deconvolution_data_window_end 0.0",
            "deconvolution_data_window",
        ),
        (
            "noise_window_start -105.0\nnoise_window_end -5.0",
            "noise_window_start 0.0\nnoise_window_end -10.0",
            "noise_window",
        ),
    ],
)
def test_CNRDeconEngine_rejects_invalid_windows(tmp_path, old, new, match):
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    pf = tmp_path / "CNRDeconEngine.pf"
    pf.write_text(text.replace(old, new))

    with pytest.raises(MsPASSError, match=match) as excinfo:
        CNRDeconEngine(pfread(str(pf)))
    assert excinfo.value.severity == ErrorSeverity.Fatal


def test_CNRRFDecon():
    """
    Test function for CNRRFDecon function.   Error handlers for
    this function are tested in a different pytest function below
    """
    # generate simulation wavelet, error free data, and data with noise
    # copied before use below
    d0wn = make_test_data(noise_level=0.1)
    # necessary for test but normal use would use output of broadband_snr_QC
    d0wn["low_f_band_edge"] = 2.0
    d0wn["high_f_band_edge"] = 8.0

    d = Seismogram(d0wn)
    # use default pf file for this and all tests in this file
    pf = pfread("./data/pf/CNRDeconEngine.pf")
    # pf = pfread("/geode2/home/u070/pavlis/Quartz/src/mspass/data/pf/CNRDeconEngine.pf")
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
    dumpstring = pickle.dumps(engine)
    engine_cpy = pickle.loads(dumpstring)
    d_decon2, aout, iout = CNRRFDecon(
        d,
        engine_cpy,
        signal_window=sw,
        noise_window=nw,
        return_wavelet=True,
        use_3C_noise=True,
    )
    assert d_decon2.live
    assert aout.live
    assert iout.live
    assert d_decon2.npts == d_decon.npts
    assert np.isclose(d_decon.data, d_decon2.data).all()

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
    d0wn = make_test_data(noise_level=0.1)
    # necessary for test but normal use would use output of broadband_snr_QC
    d0wn["low_f_band_edge"] = 2.0
    d0wn["high_f_band_edge"] = 8.0

    d = Seismogram(d0wn)
    pf = pfread("./data/pf/CNRDeconEngine.pf")
    engine = CNRDeconEngine(pf)
    nw = TimeWindow(-45.0, -5.0)
    sw = TimeWindow(-5.0, 30.0)
    # first arg type and validity checkers
    # this is what the function would throw
    # with pytest.raises(TypeError, match="illegal type="):
    # mspass_func_decorator catches the same error and requires this
    with pytest.raises(TypeError, match="CNRRFDecon:  illegal type="):
        d_decon = CNRRFDecon("foo", engine, signal_window=sw, noise_window=nw)

    d = Seismogram(d0wn)
    CNRRFDecon(d, "foo", signal_window=sw, noise_window=nw)
    assert d.elog.size() >= 1
    assert "must be an instance" in d.elog.get_error_log()[-1].message.lower()

    d = Seismogram(d0wn)
    CNRRFDecon(
        d,
        engine,
        component=20,
        signal_window=sw,
        noise_window=nw,
    )
    assert d.elog.size() >= 1
    assert "component" in d.elog.get_error_log()[-1].message.lower()
    for bad_component in (True, np.bool_(True), 2.0, "2"):
        d = Seismogram(d0wn)
        d_decon = CNRRFDecon(
            d,
            engine,
            component=bad_component,
            signal_window=sw,
            noise_window=nw,
        )
        assert d_decon.dead()
        assert d_decon.elog.size() >= 1
        assert "component" in d_decon.elog.get_error_log()[-1].message.lower()

    d = Seismogram(d0wn)
    d_decon = CNRRFDecon(
        d,
        engine,
        component=np.int64(2),
        signal_window=sw,
        noise_window=nw,
    )
    assert d_decon.live
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
    nw = TimeWindow(-45.0, -5.0)
    for bad_bandwidth in ("2.0", None, [2.0], np.nan):
        d = Seismogram(d0wn)
        d["low_f_band_edge"] = bad_bandwidth
        d["high_f_band_edge"] = 8.0
        assert fetch_bandwidth_data(d, ["low_f_band_edge", "high_f_band_edge"]) == (
            -1.0,
            8.0,
        )
        d_decon = CNRRFDecon(
            d,
            engine,
            signal_window=sw,
            noise_window=nw,
        )
        assert d_decon.dead()
        assert d_decon.elog.size() > 0
        assert "low frequency corner" in d_decon.elog.get_error_log()[-1].message

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
        s = make_test_data(noise_level=0.3)
        e.member.append(s)
    return e


def test_CNRArrayDecon():
    e0 = make_ensemble_test_data()
    # create a seperate wavelet with lower noise level
    # note noise level of 5.0 is a frozen constant in make_ensemble_data
    d0 = make_test_data(noise_level=0.1)
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
    d0 = make_test_data(noise_level=0.1)
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
    # this is what the function would throw
    # with pytest.raises(TypeError, match="Illegal type for arg0"):
    # mspass_func_decorator catches the same error and requires this
    with pytest.raises(
        TypeError, match="mspass_func_wrapper only accepts mspass object"
    ):
        e_d = CNRArrayDecon("foo", w, engine, noise_window=nw, signal_window=sw)

    CNRArrayDecon(e, "foo", engine, noise_window=nw, signal_window=sw)
    errs = e.member[0].elog.get_error_log()
    assert len(errs) >= 1
    assert "illegal type" in errs[-1].message.lower()

    e = SeismogramEnsemble(e0)
    w = Seismogram(s0)
    CNRArrayDecon(e, w, engine)
    errs = e.member[0].elog.get_error_log()
    assert len(errs) >= 1
    assert "illegal argument combination" in errs[-1].message.lower()

    e = SeismogramEnsemble(e0)
    w = Seismogram(d0)
    w["low_f_band_edge"] = 2.0
    w["high_f_band_edge"] = 8.0
    e_d = CNRArrayDecon(
        e,
        w,
        engine,
        beam_component=True,
        noise_window=nw,
        signal_window=sw,
    )
    assert e_d.dead()
    nerrs = e_d.elog.size() + sum(member.elog.size() for member in e_d.member)
    assert nerrs > 0
    messages = [err.message for err in e_d.elog.get_error_log()]
    for member in e_d.member:
        messages.extend(err.message for err in member.elog.get_error_log())
    assert any("beam_component" in message for message in messages)

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


def test_CNRDeconEngine_configured_shaping_survives_legacy_process_and_pickle():
    """Configured shaping must not inherit a previous legacy fl/fh call."""
    d = make_test_data(noise_level=0.1)
    signal_window = TimeWindow(-5.0, 30.0)
    noise_window = TimeWindow(-45.0, -5.0)
    wavelet = WindowData(ExtractComponent(d, 2), signal_window.start, signal_window.end)
    noise = WindowData(ExtractComponent(d, 2), noise_window.start, noise_window.end)
    datum = WindowData(d, signal_window.start, signal_window.end)
    engine = CNRDeconEngine(pfread("./data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(wavelet, noise_spectrum)

    engine.process(datum, noise_spectrum, 0.02, 2.0)
    legacy_shaper = np.asarray(engine.output_shaping_wavelet().data)
    configured = engine.process_configured(datum, noise_spectrum)
    configured_shaper = np.asarray(engine.output_shaping_wavelet().data)
    assert configured.live
    assert not np.allclose(legacy_shaper, configured_shaper)

    restored = pickle.loads(pickle.dumps(engine))
    restored_result = restored.process_configured(datum, noise_spectrum)
    restored_shaper = restored.output_shaping_wavelet()
    assert restored_result.live
    assert np.allclose(restored_result.data, configured.data)
    assert restored_shaper.npts == configured_shaper.size
    assert np.allclose(restored_shaper.data, configured_shaper)


@pytest.mark.parametrize("dead_kind", ["datum", "spectrum"])
def test_CNRDeconEngine_configured_dead_input_preserves_legacy_state(dead_kind):
    """A logged-dead configured return cannot switch the installed shaper."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)
    engine.process(datum, noise_spectrum, 0.02, 2.0)

    qc_before = dict(engine.QCMetrics())
    shaping_before = engine.output_shaping_wavelet()
    actual_before = engine.actual_output(source)
    inverse_before = engine.inverse_wavelet(source, 0.0)
    if dead_kind == "datum":
        dead_datum = Seismogram(datum)
        dead_datum.kill()
        result = engine.process_configured(dead_datum, noise_spectrum)
    else:
        dead_spectrum = PowerSpectrum()
        result = engine.process_configured(datum, dead_spectrum)

    assert result.dead()
    assert result.npts == 0
    assert result.elog.size() >= 1
    assert dict(engine.QCMetrics()) == qc_before
    assert np.array_equal(engine.output_shaping_wavelet().data, shaping_before.data)
    assert np.array_equal(engine.actual_output(source).data, actual_before.data)
    inverse_after = engine.inverse_wavelet(source, 0.0)
    assert np.array_equal(inverse_after.data, inverse_before.data)
    assert inverse_after.t0 == inverse_before.t0
    assert inverse_after.dt == inverse_before.dt


def test_CNRDeconEngine_failed_reinitialization_preserves_inverse_and_timing():
    """A rejected wavelet must not corrupt the last valid inverse phase."""
    d0 = make_test_data(noise_level=0.1)
    d0["low_f_band_edge"] = 0.02
    d0["high_f_band_edge"] = 2.0
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)

    source = make_simulation_wavelet()
    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline = engine.process(datum, noise_spectrum, 0.02, 2.0)

    # The nonfinite sample makes inverse construction fail after its different
    # zero-time lag has been evaluated.  Formerly that lag was committed before
    # the failure and became inconsistent with the still-valid old inverse.
    rejected_wavelet = TimeSeries(source)
    rejected_wavelet.set_t0(source.t0 - 1.0)
    rejected_wavelet.data[0] = np.nan
    alternate_noise = WindowData(ExtractComponent(d0, 2), -35.0, -5.0)
    alternate_spectrum = engine.compute_noise_spectrum(alternate_noise)
    with pytest.raises(MsPASSError, match="NaN"):
        engine.initialize_inverse_operator(rejected_wavelet, alternate_spectrum)

    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)
    assert recovered.live
    assert np.allclose(recovered.data, baseline.data)
    assert np.argmax(np.abs(recovered.data[2, :])) == np.argmax(
        np.abs(baseline.data[2, :])
    )


@pytest.mark.parametrize(
    "invalid_kind,error_match",
    [
        ("empty", "at least one sample"),
        ("lag_outside_fft", "outside the FFT buffer range"),
    ],
)
def test_CNRDeconEngine_rejects_invalid_wavelet_without_corrupting_inverse(
    invalid_kind, error_match
):
    """Wavelet geometry errors are deterministic and transaction-safe."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    source = make_simulation_wavelet()

    if invalid_kind == "empty":
        rejected = TimeSeries(0)
        rejected.set_t0(0.0)
        rejected.set_dt(source.dt)
        rejected.set_live()
    else:
        rejected = TimeSeries(source)
        rejected.set_t0(-300.0)

    # The same validation must be deterministic on a fresh engine and after a
    # valid inverse has been installed.
    with pytest.raises(MsPASSError, match=error_match):
        engine.initialize_inverse_operator(rejected, noise_spectrum)

    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline = engine.process(datum, noise_spectrum, 0.02, 2.0)
    with pytest.raises(MsPASSError, match=error_match):
        engine.initialize_inverse_operator(rejected, noise_spectrum)
    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)

    assert recovered.live
    assert recovered.t0 == baseline.t0
    assert recovered.dt == baseline.dt
    assert recovered.npts == baseline.npts
    assert np.array_equal(recovered.data, baseline.data)


@pytest.mark.parametrize(
    "algorithm", ["colored_noise_damping", "generalized_water_level"]
)
def test_CNRDeconEngine_rejects_zero_wavelet_without_state_change(tmp_path, algorithm):
    """A zero source cannot install or replace an inverse operator."""
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    text = text.replace("algorithm colored_noise_damping", f"algorithm {algorithm}")
    pfpath = tmp_path / f"CNRDeconEngine_zero_wavelet_{algorithm}.pf"
    pfpath.write_text(text)

    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread(str(pfpath)))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    source = make_simulation_wavelet()
    zero_wavelet = TimeSeries(source)
    zero_wavelet.data[:] = DoubleVector(np.zeros(source.npts))

    # The same deterministic Invalid exception is required before an inverse
    # exists and when a valid inverse is already installed.
    with pytest.raises(MsPASSError, match="at least one nonzero sample"):
        engine.initialize_inverse_operator(zero_wavelet, noise_spectrum)

    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline = engine.process(datum, noise_spectrum, 0.02, 2.0)
    actual_before = engine.actual_output(source)
    qc_before = dict(engine.QCMetrics())
    with pytest.raises(MsPASSError, match="at least one nonzero sample"):
        engine.initialize_inverse_operator(zero_wavelet, noise_spectrum)

    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)
    actual_after = engine.actual_output(source)
    qc_after = dict(engine.QCMetrics())
    assert recovered.live
    assert np.isfinite(np.asarray(recovered.data)).all()
    assert np.linalg.norm(recovered.data) > 0.0
    assert np.array_equal(recovered.data, baseline.data)
    assert np.array_equal(actual_after.data, actual_before.data)
    assert qc_after == qc_before


@pytest.mark.parametrize(
    "power,error_match",
    [
        ([], "at least two frequency bins"),
        ([1.0, -0.25], "negative power"),
    ],
)
def test_CNRDeconEngine_rejects_invalid_power_spectrum_at_public_entries(
    power, error_match
):
    """Invalid spectra must raise before inverse or shaping state changes."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    valid_spectrum = engine.compute_noise_spectrum(noise)
    invalid_spectrum = PowerSpectrum(
        Metadata(), DoubleVector(power), 1.0, "invalid", 0.0, 1.0, len(power)
    )

    with pytest.raises(MsPASSError, match=error_match):
        engine.initialize_inverse_operator(source, invalid_spectrum)

    engine.initialize_inverse_operator(source, valid_spectrum)
    baseline = engine.process(datum, valid_spectrum, 0.02, 2.0)
    shaping_before = np.asarray(engine.output_shaping_wavelet().data).copy()
    shifted_source = TimeSeries(source)
    shifted_source.set_t0(source.t0 - 1.0)
    with pytest.raises(MsPASSError, match=error_match):
        engine.initialize_inverse_operator(shifted_source, invalid_spectrum)
    with pytest.raises(MsPASSError, match=error_match):
        engine.process(datum, invalid_spectrum, 0.02, 2.0)
    with pytest.raises(MsPASSError, match=error_match):
        engine.process_configured(datum, invalid_spectrum)

    recovered = engine.process(datum, valid_spectrum, 0.02, 2.0)
    shaping_after = np.asarray(engine.output_shaping_wavelet().data)
    assert recovered.live
    assert np.array_equal(recovered.data, baseline.data)
    assert np.array_equal(shaping_after, shaping_before)


def test_CNRDeconEngine_accepts_zero_power_noise_free_limit():
    """An all-zero PSD is the valid ideal-noise-free synthetic limit."""
    d0 = make_test_data(noise_level=None)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    zero_spectrum = engine.compute_noise_spectrum(noise)
    assert np.count_nonzero(zero_spectrum.spectrum) == 0

    source = make_simulation_wavelet()
    engine.initialize_inverse_operator(source, zero_spectrum)
    result = engine.process(datum, zero_spectrum, 0.02, 2.0)
    values = np.asarray(result.data)
    direct_sample = result.sample_number(0.0)
    vertical_peak = int(np.argmax(np.abs(values[2, :])))
    assert result.live
    assert np.isfinite(values).all()
    assert np.linalg.norm(values) > 0.0
    assert abs(vertical_peak - direct_sample) <= 1


@pytest.mark.parametrize(
    "algorithm", ["colored_noise_damping", "generalized_water_level"]
)
@pytest.mark.parametrize("zero_noise_spectrum", [False, True])
def test_CNRDeconEngine_spectral_null_uses_finite_pseudoinverse(
    tmp_path, algorithm, zero_noise_spectrum
):
    """An exact source DC null has zero inverse gain, never a 0/0 NaN."""
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    text = text.replace("algorithm colored_noise_damping", f"algorithm {algorithm}")
    pfpath = tmp_path / f"CNRDeconEngine_null_{algorithm}.pf"
    pfpath.write_text(text)

    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread(str(pfpath)))
    computed = engine.compute_noise_spectrum(noise)
    if zero_noise_spectrum:
        noise_spectrum = PowerSpectrum(
            Metadata(),
            DoubleVector(np.zeros(len(computed.spectrum))),
            computed.df(),
            "zero_noise",
            computed.f0(),
            computed.dt(),
            noise.npts,
        )
    else:
        noise_spectrum = computed

    # This first-difference source has a mathematically exact DC null.
    wavelet = TimeSeries(2)
    wavelet.set_t0(0.0)
    wavelet.set_dt(engine.get_operator_dt())
    wavelet.set_live()
    wavelet.data[0] = 1.0
    wavelet.data[1] = -1.0

    engine.initialize_inverse_operator(wavelet, noise_spectrum)
    result = engine.process(datum, noise_spectrum, 0.02, 2.0)
    actual = engine.actual_output(wavelet)
    inverse = engine.inverse_wavelet(wavelet, 0.0)
    qc = dict(engine.QCMetrics())

    assert result.live
    assert actual.live
    assert inverse.live
    assert np.isfinite(np.asarray(result.data)).all()
    assert np.isfinite(np.asarray(actual.data)).all()
    assert np.isfinite(np.asarray(inverse.data)).all()
    assert np.linalg.norm(result.data) > 0.0
    assert np.linalg.norm(actual.data) > 0.0
    peak = int(np.argmax(np.abs(np.asarray(actual.data))))
    peak_time = actual.t0 + peak * actual.dt
    assert abs(peak_time) <= actual.dt
    regularized_fraction = qc["cnr_regularization_bandwidth_fraction"]
    assert np.isfinite(regularized_fraction)
    assert 0.0 <= regularized_fraction <= 1.0


@pytest.mark.parametrize(
    "algorithm", ["colored_noise_damping", "generalized_water_level"]
)
@pytest.mark.parametrize("process_path", ["legacy", "configured"])
def test_CNRDeconEngine_zero_psd_qc_is_finite_and_pickle_stable(
    tmp_path, algorithm, process_path
):
    """Ideal-noise-free QC must remain finite and exactly serializable."""
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    text = text.replace("algorithm colored_noise_damping", f"algorithm {algorithm}")
    pfpath = tmp_path / f"CNRDeconEngine_zero_qc_{algorithm}.pf"
    pfpath.write_text(text)

    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    # Freeze one component at exactly zero to exercise the 0/0=no-information
    # convention alongside nonzero-signal/zero-noise capped SNR components.
    for sample in range(datum.npts):
        datum.data[1, sample] = 0.0
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread(str(pfpath)))
    template = engine.compute_noise_spectrum(noise)
    zero_spectrum = PowerSpectrum(
        Metadata(),
        DoubleVector(np.zeros(len(template.spectrum))),
        template.df(),
        "zero_noise",
        template.f0(),
        template.dt(),
        noise.npts,
    )
    engine.initialize_inverse_operator(source, zero_spectrum)

    if process_path == "legacy":
        result = engine.process(datum, zero_spectrum, 0.02, 2.0)
    else:
        result = engine.process_configured(datum, zero_spectrum)
    qc_before = dict(engine.QCMetrics())
    actual_before = engine.actual_output(source)
    inverse_before = engine.inverse_wavelet(source, 0.0)
    shaping_before = engine.output_shaping_wavelet()

    assert result.live
    assert np.isfinite(np.asarray(result.data)).all()
    for value in qc_before.values():
        if isinstance(value, (float, np.floating)):
            assert np.isfinite(value)
    assert qc_before["maxsnr0"] == pytest.approx(10000.0)
    assert qc_before["maxsnr1"] == pytest.approx(0.0)
    assert qc_before["maxsnr2"] == pytest.approx(10000.0)
    assert qc_before["signalbf1"] == pytest.approx(0.0)

    restored = pickle.loads(pickle.dumps(engine))
    assert dict(restored.QCMetrics()) == qc_before
    actual_after = restored.actual_output(source)
    inverse_after = restored.inverse_wavelet(source, 0.0)
    shaping_after = restored.output_shaping_wavelet()
    assert np.array_equal(actual_after.data, actual_before.data)
    assert np.array_equal(inverse_after.data, inverse_before.data)
    assert inverse_after.t0 == inverse_before.t0
    assert inverse_after.dt == inverse_before.dt
    assert np.array_equal(shaping_after.data, shaping_before.data)

    if process_path == "legacy":
        repeated = restored.process(datum, zero_spectrum, 0.02, 2.0)
    else:
        repeated = restored.process_configured(datum, zero_spectrum)
    assert repeated.live
    assert np.isfinite(np.asarray(repeated.data)).all()
    assert np.array_equal(repeated.data, result.data)
    assert dict(restored.QCMetrics()) == qc_before


@pytest.mark.parametrize("process_path", ["legacy", "configured"])
@pytest.mark.parametrize(
    "invalid_kind,error_match",
    [
        ("nan", "nonfinite sample"),
        ("inf", "nonfinite sample"),
        ("overflow", "FFT contains nonfinite values"),
    ],
)
def test_CNRDeconEngine_process_rejects_nonfinite_without_state_change(
    process_path, invalid_kind, error_match
):
    """Invalid/overflowing data cannot partially commit shaping or QC state."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)

    # Install nonconfigured legacy shaping and nontrivial QC.  A failed
    # configured call must restore this state rather than partially switching
    # to the PF shaping wavelet.
    baseline_result = engine.process(datum, noise_spectrum, 0.02, 2.0)
    qc_before = dict(engine.QCMetrics())
    shaping_before = engine.output_shaping_wavelet()
    actual_before = engine.actual_output(source)
    inverse_before = engine.inverse_wavelet(source, 0.0)

    invalid = Seismogram(datum)
    if invalid_kind == "nan":
        invalid.data[1, 7] = np.nan
    elif invalid_kind == "inf":
        invalid.data[1, 7] = np.inf
    else:
        huge = np.finfo(np.float64).max
        for component in range(3):
            for sample in range(invalid.npts):
                invalid.data[component, sample] = huge

    with pytest.raises(MsPASSError, match=error_match):
        if process_path == "legacy":
            engine.process(invalid, noise_spectrum, 0.08, 0.8)
        else:
            engine.process_configured(invalid, noise_spectrum)

    assert dict(engine.QCMetrics()) == qc_before
    shaping_after = engine.output_shaping_wavelet()
    actual_after = engine.actual_output(source)
    inverse_after = engine.inverse_wavelet(source, 0.0)
    assert np.array_equal(shaping_after.data, shaping_before.data)
    assert np.array_equal(actual_after.data, actual_before.data)
    assert np.array_equal(inverse_after.data, inverse_before.data)
    assert inverse_after.t0 == inverse_before.t0
    assert inverse_after.dt == inverse_before.dt

    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)
    assert recovered.live
    assert np.isfinite(np.asarray(recovered.data)).all()
    assert np.array_equal(recovered.data, baseline_result.data)


@pytest.mark.parametrize("process_path", ["legacy", "configured"])
@pytest.mark.parametrize(
    "invalid_kind,error_match",
    [
        ("nonfinite_t0", "start time must be finite"),
        ("empty", "at least one sample"),
        ("oversized", "exceeds the configured FFT buffer"),
    ],
)
def test_CNRDeconEngine_process_rejects_invalid_geometry_transactionally(
    process_path, invalid_kind, error_match
):
    """Datum geometry errors are rejected before shaping or QC can change."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline_result = engine.process(datum, noise_spectrum, 0.02, 2.0)
    qc_before = dict(engine.QCMetrics())
    shaping_before = engine.output_shaping_wavelet()
    actual_before = engine.actual_output(source)
    inverse_before = engine.inverse_wavelet(source, 0.0)
    nfft = qc_before["decon_operator_nfft"]

    if invalid_kind == "nonfinite_t0":
        invalid = Seismogram(datum)
        invalid.set_t0(np.nan)
    else:
        invalid = Seismogram(0 if invalid_kind == "empty" else nfft + 1)
        invalid.set_t0(datum.t0)
        invalid.set_dt(datum.dt)
        invalid.set_live()

    with pytest.raises(MsPASSError, match=error_match):
        if process_path == "legacy":
            engine.process(invalid, noise_spectrum, 0.08, 0.8)
        else:
            engine.process_configured(invalid, noise_spectrum)

    assert dict(engine.QCMetrics()) == qc_before
    assert np.array_equal(engine.output_shaping_wavelet().data, shaping_before.data)
    actual_after = engine.actual_output(source)
    inverse_after = engine.inverse_wavelet(source, 0.0)
    assert np.array_equal(actual_after.data, actual_before.data)
    assert np.array_equal(inverse_after.data, inverse_before.data)
    assert inverse_after.t0 == inverse_before.t0

    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)
    assert recovered.live
    assert np.array_equal(recovered.data, baseline_result.data)


@pytest.mark.parametrize("process_path", ["legacy", "configured"])
def test_CNRDeconEngine_process_accepts_exact_nfft_datum(process_path):
    """The fixed FFT buffer length is inclusive, not an off-by-one reject."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)
    nfft = dict(engine.QCMetrics())["decon_operator_nfft"]
    exact = Seismogram(nfft)
    exact.set_t0(datum.t0)
    exact.set_dt(datum.dt)
    exact.set_live()
    exact.data[:, : datum.npts] = datum.data

    if process_path == "legacy":
        result = engine.process(exact, noise_spectrum, 0.02, 2.0)
    else:
        result = engine.process_configured(exact, noise_spectrum)
    assert result.live
    assert result.npts == nfft
    assert result.t0 == exact.t0
    assert result.dt == exact.dt
    assert np.isfinite(np.asarray(result.data)).all()


@pytest.mark.parametrize(
    "invalid_kind,error_match",
    [
        ("parent_dt", "parent sample interval"),
        ("frequency_coverage", "does not cover operator Nyquist"),
    ],
)
def test_CNRDeconEngine_rejects_incompatible_external_power_spectrum(
    invalid_kind, error_match
):
    """External PSDs must match dt and cover the CNR grid within half a bin."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    computed = engine.compute_noise_spectrum(noise)
    power = DoubleVector(computed.spectrum)
    valid_external = PowerSpectrum(
        Metadata(),
        power,
        computed.df(),
        "valid_external",
        computed.f0(),
        computed.dt(),
        noise.npts,
    )
    if invalid_kind == "parent_dt":
        incompatible = PowerSpectrum(
            Metadata(),
            power,
            computed.df(),
            "wrong_parent_dt",
            computed.f0(),
            0.1,
            noise.npts,
        )
    else:
        incompatible = PowerSpectrum(
            Metadata(),
            power,
            computed.df() / 2.0,
            "truncated_frequency_range",
            computed.f0(),
            computed.dt(),
            noise.npts,
        )

    with pytest.raises(MsPASSError, match=error_match):
        engine.initialize_inverse_operator(source, incompatible)

    engine.initialize_inverse_operator(source, valid_external)
    baseline = engine.process(datum, valid_external, 0.02, 2.0)
    shaping_before = np.asarray(engine.output_shaping_wavelet().data).copy()
    shifted_source = TimeSeries(source)
    shifted_source.set_t0(source.t0 - 1.0)
    with pytest.raises(MsPASSError, match=error_match):
        engine.initialize_inverse_operator(shifted_source, incompatible)
    with pytest.raises(MsPASSError, match=error_match):
        engine.process(datum, incompatible, 0.02, 2.0)
    with pytest.raises(MsPASSError, match=error_match):
        engine.process_configured(datum, incompatible)

    recovered = engine.process(datum, valid_external, 0.02, 2.0)
    shaping_after = np.asarray(engine.output_shaping_wavelet().data)
    assert baseline.live
    assert recovered.live
    assert np.isfinite(recovered.data).all()
    assert np.array_equal(recovered.data, baseline.data)
    assert np.array_equal(shaping_after, shaping_before)


def test_CNRDeconEngine_accepts_odd_fft_noise_spectrum_terminal():
    """The final odd-FFT bin represents the half-bin through Nyquist."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    reference_engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    nfft = 601
    dt = engine.get_operator_dt()
    df = 1.0 / (nfft * dt)
    power_values = [1.0 + index / 100.0 for index in range(nfft // 2 + 1)]
    power = DoubleVector(power_values)
    odd_spectrum = PowerSpectrum(Metadata(), power, df, "odd_fft", 0.0, dt, nfft)
    reference_spectrum = PowerSpectrum(
        Metadata(),
        DoubleVector(power_values + [power_values[-1]]),
        df,
        "terminal_extended",
        0.0,
        dt,
        nfft,
    )

    assert odd_spectrum.frequency(odd_spectrum.nf() - 1) == pytest.approx(
        odd_spectrum.Nyquist() - df / 2.0
    )
    engine.initialize_inverse_operator(source, odd_spectrum)
    reference_engine.initialize_inverse_operator(source, reference_spectrum)
    result = engine.process(datum, odd_spectrum, 0.02, 2.0)
    reference_result = reference_engine.process(datum, reference_spectrum, 0.02, 2.0)
    assert result.live
    assert np.isfinite(np.asarray(result.data)).all()
    assert np.array_equal(result.data, reference_result.data)
    assert np.array_equal(
        engine.inverse_wavelet(source, 0.0).data,
        reference_engine.inverse_wavelet(source, 0.0).data,
    )
    assert dict(engine.QCMetrics()) == dict(reference_engine.QCMetrics())

    truncated = PowerSpectrum(
        Metadata(),
        DoubleVector([1.0] * (nfft // 2)),
        df,
        "truncated_odd_fft",
        0.0,
        dt,
        nfft,
    )
    with pytest.raises(MsPASSError, match="does not cover operator Nyquist"):
        engine.initialize_inverse_operator(source, truncated)


def test_CNRDeconEngine_rejects_overflowing_frequency_grid_transactionally():
    """Finite f0/df are insufficient when their full PSD grid overflows."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    valid_spectrum = engine.compute_noise_spectrum(noise)
    double_max = np.finfo(np.float64).max
    overflowing = PowerSpectrum(
        Metadata(),
        DoubleVector([1.0] * 4),
        double_max / 2.0,
        "overflowing_grid",
        -double_max,
        source.dt,
        4,
    )

    with pytest.raises(MsPASSError, match="frequency-grid span or endpoint"):
        engine.initialize_inverse_operator(source, overflowing)

    engine.initialize_inverse_operator(source, valid_spectrum)
    baseline = engine.process(datum, valid_spectrum, 0.02, 2.0)
    actual_before = engine.actual_output(source)
    shaping_before = engine.output_shaping_wavelet()
    qc_before = dict(engine.QCMetrics())
    shifted_source = TimeSeries(source)
    shifted_source.set_t0(source.t0 - 1.0)
    with pytest.raises(MsPASSError, match="frequency-grid span or endpoint"):
        engine.initialize_inverse_operator(shifted_source, overflowing)
    with pytest.raises(MsPASSError, match="frequency-grid span or endpoint"):
        engine.process(datum, overflowing, 0.02, 2.0)
    with pytest.raises(MsPASSError, match="frequency-grid span or endpoint"):
        engine.process_configured(datum, overflowing)

    recovered = engine.process(datum, valid_spectrum, 0.02, 2.0)
    assert np.array_equal(recovered.data, baseline.data)
    assert np.array_equal(engine.actual_output(source).data, actual_before.data)
    assert np.array_equal(engine.output_shaping_wavelet().data, shaping_before.data)
    assert dict(engine.QCMetrics()) == qc_before


def test_CNRDeconEngine_accepts_near_double_limit_frequency_grid():
    """A huge but finite span/endpoint remains a valid PSD grid."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    double_max = np.finfo(np.float64).max
    boundary = PowerSpectrum(
        Metadata(),
        DoubleVector([1.0] * 4),
        double_max / 4.0,
        "finite_boundary_grid",
        -double_max / 2.0,
        source.dt,
        4,
    )

    engine.initialize_inverse_operator(source, boundary)
    legacy = engine.process(datum, boundary, 0.02, 2.0)
    configured = engine.process_configured(datum, boundary)
    assert legacy.live
    assert configured.live
    assert np.isfinite(np.asarray(legacy.data)).all()
    assert np.isfinite(np.asarray(configured.data)).all()


def test_CNRDeconEngine_rejects_wavelet_dt_mismatch_transactionally():
    """Inverse phase and samples are invalid when wavelet dt differs."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    source = make_simulation_wavelet()
    mismatched = TimeSeries(source)
    mismatched.set_dt(0.1)

    with pytest.raises(MsPASSError, match="sample interval"):
        engine.initialize_inverse_operator(mismatched, noise_spectrum)

    matching = TimeSeries(source)
    matching.set_dt(engine.get_operator_dt())
    engine.initialize_inverse_operator(matching, noise_spectrum)
    baseline = engine.process(datum, noise_spectrum, 0.02, 2.0)
    with pytest.raises(MsPASSError, match="sample interval"):
        engine.initialize_inverse_operator(mismatched, noise_spectrum)
    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)

    assert recovered.live
    assert np.array_equal(recovered.data, baseline.data)
    assert np.argmax(np.abs(recovered.data[2, :])) == np.argmax(
        np.abs(baseline.data[2, :])
    )


def test_CNRDeconEngine_process_rejects_datum_dt_mismatch_before_mutation():
    """Both direct process paths must enforce the operator sample interval."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    source = make_simulation_wavelet()
    engine.initialize_inverse_operator(source, noise_spectrum)

    baseline = engine.process(datum, noise_spectrum, 0.02, 2.0)
    shaping_before = np.asarray(engine.output_shaping_wavelet().data).copy()
    mismatched = Seismogram(datum)
    mismatched.set_dt(0.1)
    with pytest.raises(MsPASSError, match="datum sample interval"):
        engine.process(mismatched, noise_spectrum, 0.02, 2.0)
    with pytest.raises(MsPASSError, match="datum sample interval"):
        engine.process_configured(mismatched, noise_spectrum)

    shaping_after = np.asarray(engine.output_shaping_wavelet().data)
    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)
    configured = engine.process_configured(datum, noise_spectrum)
    assert recovered.live
    assert configured.live
    assert recovered.t0 == baseline.t0
    assert recovered.dt == baseline.dt
    assert recovered.npts == baseline.npts
    assert np.array_equal(recovered.data, baseline.data)
    assert np.array_equal(shaping_after, shaping_before)


@pytest.mark.parametrize("use_external_wavelet", [False, True])
def test_CNRRFDecon_generalized_water_level_accepts_short_wavelet(
    tmp_path, use_external_wavelet
):
    """Normal CNR wrapper wavelets are shorter than the padded FFT buffer."""
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    pfpath = tmp_path / "CNRDeconEngine_gwl.pf"
    pfpath.write_text(
        text.replace(
            "algorithm colored_noise_damping",
            "algorithm generalized_water_level",
        )
    )
    d0 = make_test_data(noise_level=0.1)
    d0["low_f_band_edge"] = 0.02
    d0["high_f_band_edge"] = 2.0
    engine = CNRDeconEngine(pfread(str(pfpath)))
    external_wavelet = make_simulation_wavelet() if use_external_wavelet else None

    rf = CNRRFDecon(
        d0,
        engine,
        signal_window=TimeWindow(-5.0, 30.0),
        noise_window=TimeWindow(-45.0, -5.0),
        external_wavelet=external_wavelet,
    )
    values = np.asarray(rf.data)
    direct_sample = rf.sample_number(0.0)
    vertical_peak = int(np.argmax(np.abs(values[2, :])))
    assert rf.live
    assert np.isfinite(values).all()
    assert abs(vertical_peak - direct_sample) <= 1


@pytest.mark.parametrize(
    "algorithm", ["colored_noise_damping", "generalized_water_level"]
)
def test_CNRDeconEngine_exact_nfft_wavelet_uses_all_samples(tmp_path, algorithm):
    """Exact-buffer inverse construction must not scalar-fill from sample 0."""
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    text = text.replace("algorithm colored_noise_damping", f"algorithm {algorithm}")
    pfpath = tmp_path / f"CNRDeconEngine_{algorithm}.pf"
    pfpath.write_text(text)

    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread(str(pfpath)))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    nfft = dict(engine.QCMetrics())["decon_operator_nfft"]

    source = make_simulation_wavelet()
    wavelet = TimeSeries(nfft)
    wavelet.set_t0(datum.t0)
    wavelet.set_dt(datum.dt)
    wavelet.set_live()
    offset = wavelet.sample_number(source.t0)
    wavelet.data[offset : offset + source.npts] = source.data
    assert wavelet.data[0] == 0.0
    assert np.count_nonzero(wavelet.data) > 1

    engine.initialize_inverse_operator(wavelet, noise_spectrum)
    result = engine.process(datum, noise_spectrum, 0.02, 2.0)
    values = np.asarray(result.data)
    direct_sample = datum.sample_number(0.0)
    vertical_peak = int(np.argmax(np.abs(values[2, :])))
    assert result.live
    assert np.isfinite(values).all()
    assert np.linalg.norm(values) > 0.0
    assert abs(vertical_peak - direct_sample) <= 1


@pytest.mark.parametrize(
    "algorithm", ["colored_noise_damping", "generalized_water_level"]
)
def test_CNRDeconEngine_rejects_oversized_wavelet_without_state_change(
    tmp_path, algorithm
):
    """An nfft+1 wavelet must raise, not enter GSL with an empty buffer."""
    with open("data/pf/CNRDeconEngine.pf", encoding="utf-8") as fp:
        text = fp.read()
    text = text.replace("algorithm colored_noise_damping", f"algorithm {algorithm}")
    pfpath = tmp_path / f"CNRDeconEngine_oversized_{algorithm}.pf"
    pfpath.write_text(text)
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread(str(pfpath)))
    noise_spectrum = engine.compute_noise_spectrum(noise)
    source = make_simulation_wavelet()
    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline = engine.process(datum, noise_spectrum, 0.02, 2.0)

    nfft = dict(engine.QCMetrics())["decon_operator_nfft"]
    oversized = TimeSeries(nfft + 1)
    oversized.set_t0(datum.t0)
    oversized.set_dt(datum.dt)
    oversized.set_live()
    offset = oversized.sample_number(source.t0)
    oversized.data[offset : offset + source.npts] = source.data
    with pytest.raises(MsPASSError, match="exceeds the fixed FFT buffer"):
        engine.initialize_inverse_operator(oversized, noise_spectrum)

    recovered = engine.process(datum, noise_spectrum, 0.02, 2.0)
    assert recovered.live
    assert np.array_equal(recovered.data, baseline.data)


def test_CNRDeconEngine_actual_output_returns_windowable_timeseries():
    """The public resolution kernel must carry a valid TimeSeries timebase."""
    d0 = make_test_data(noise_level=0.1)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)

    for method in (engine.actual_output, engine.resolution_kernel):
        result = method(source)
        assert isinstance(result, TimeSeries)
        assert result.live
        assert result.dt == engine.get_operator_dt()
        assert np.isfinite(np.asarray(result.data)).all()
        windowed = WindowData(result, -5.0, 30.0)
        assert isinstance(windowed, TimeSeries)
        assert windowed.live
        assert windowed.t0 == -5.0
        assert np.isfinite(np.asarray(windowed.data)).all()


def test_CNRDeconEngine_inverse_wavelet_returns_windowable_timeseries():
    """The public inverse diagnostic must have a finite, usable timebase."""
    d0 = make_test_data(noise_level=0.1)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)

    result = engine.inverse_wavelet(source, 0.0)
    assert isinstance(result, TimeSeries)
    assert result.live
    assert result.dt == engine.get_operator_dt()
    assert np.isfinite(result.t0)
    assert np.isfinite(np.asarray(result.data)).all()
    windowed = WindowData(result, 0.0, 30.0)
    assert isinstance(windowed, TimeSeries)
    assert windowed.live
    assert windowed.t0 == pytest.approx(0.0)
    assert np.isfinite(np.asarray(windowed.data)).all()


def test_CNRDeconEngine_inverse_wavelet_window_start_invariance():
    """Source extraction start cannot move the physical inverse response."""
    d0 = make_test_data(noise_level=0.1)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = TimeSeries(1)
    source.set_t0(0.0)
    source.set_dt(0.05)
    source.set_live()
    source.data[0] = 1.0
    peak_times = []
    spectrum_engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    spectrum_template = spectrum_engine.compute_noise_spectrum(noise)
    # Freeze the regularization input so the expected peak is independent of
    # the random-noise realization used elsewhere in this module.
    noise_spectrum = PowerSpectrum(
        Metadata(),
        DoubleVector(np.ones(len(spectrum_template.spectrum))),
        spectrum_template.df(),
        "constant_positive_noise",
        spectrum_template.f0(),
        spectrum_template.dt(),
        noise.npts,
    )

    for window_start in (-8.0, -5.0, -3.0):
        engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
        npts = int(round((20.0 - window_start) / source.dt)) + 1
        embedded = TimeSeries(npts)
        embedded.set_t0(window_start)
        embedded.set_dt(source.dt)
        embedded.set_live()
        offset = embedded.sample_number(source.t0)
        embedded.data[offset : offset + source.npts] = source.data

        engine.initialize_inverse_operator(embedded, noise_spectrum)
        inverse = engine.inverse_wavelet(embedded, 0.0)
        values = np.asarray(inverse.data)
        peak_index = int(np.argmax(np.abs(values)))
        peak_time = inverse.t0 + inverse.dt * peak_index
        peak_times.append(peak_time)
        assert inverse.live
        assert np.isfinite(values).all()
        assert inverse.t0 == pytest.approx(0.0)
        assert inverse.sample_number(0.0) == 0

        shifted = engine.inverse_wavelet(embedded, 7.5)
        assert shifted.live
        assert shifted.t0 - inverse.t0 == pytest.approx(7.5)
        assert shifted.t0 == pytest.approx(7.5)
        assert shifted.sample_number(7.5) == 0
        assert np.array_equal(shifted.data, inverse.data)

    # A unit impulse has an inverse shaped impulse whose dominant peak is at
    # its zero-time reference.  Test both that expected physical value and
    # exact invariance to otherwise irrelevant extraction-window padding.
    assert peak_times == pytest.approx([0.0, 0.0, 0.0], abs=source.dt)


@pytest.mark.parametrize(
    "invalid_kind,error_match",
    [
        ("dead", "marked dead"),
        ("empty", "at least one sample"),
        ("sample_interval", "sample interval"),
        ("nonfinite_t0", "start time must be finite"),
        ("nonfinite_shift", "time shift must be finite"),
    ],
)
def test_CNRDeconEngine_inverse_wavelet_rejects_invalid_timebase(
    invalid_kind, error_match
):
    """Invalid inverse diagnostics return logged dead data without mutation."""
    d0 = make_test_data(noise_level=0.1)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline = engine.inverse_wavelet(source, 0.0)

    invalid = TimeSeries(source)
    shift = 0.0
    if invalid_kind == "dead":
        invalid.kill()
    elif invalid_kind == "empty":
        invalid = TimeSeries(0)
        invalid.set_t0(0.0)
        invalid.set_dt(source.dt)
        invalid.set_live()
    elif invalid_kind == "sample_interval":
        invalid.set_dt(2.0 * source.dt)
    elif invalid_kind == "nonfinite_t0":
        invalid.set_t0(np.nan)
    else:
        shift = np.nan

    result = engine.inverse_wavelet(invalid, shift)
    assert isinstance(result, TimeSeries)
    assert result.dead()
    assert result.npts == 0
    assert result.elog.size() >= 1
    assert error_match in result.elog.get_error_log()[-1].message

    recovered = engine.inverse_wavelet(source, 0.0)
    assert recovered.live
    assert np.isfinite(recovered.t0)
    assert np.isfinite(np.asarray(recovered.data)).all()
    assert recovered.t0 == baseline.t0
    assert recovered.dt == baseline.dt
    assert np.array_equal(recovered.data, baseline.data)


@pytest.mark.parametrize("method_name", ["actual_output", "resolution_kernel"])
@pytest.mark.parametrize(
    "invalid_kind,error_match",
    [
        ("dead", "marked dead"),
        ("empty", "at least one sample"),
        ("sample_interval", "sample interval"),
        ("lag_outside_fft", "outside the FFT buffer range"),
        ("oversized", "exceeds the fixed FFT buffer"),
        ("nonfinite", "nonfinite samples"),
        ("all_zero", "at least one nonzero sample"),
    ],
)
def test_CNRDeconEngine_actual_output_rejects_invalid_wavelets(
    method_name, invalid_kind, error_match
):
    """Invalid public inputs return a logged dead object, never live NaNs."""
    d0 = make_test_data(noise_level=0.1)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    noise_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, noise_spectrum)
    baseline = engine.actual_output(source)
    nfft = dict(engine.QCMetrics())["decon_operator_nfft"]

    if invalid_kind == "empty":
        invalid = TimeSeries(0)
        invalid.set_t0(0.0)
        invalid.set_dt(source.dt)
        invalid.set_live()
    elif invalid_kind == "sample_interval":
        invalid = TimeSeries(source)
        invalid.set_dt(2.0 * source.dt)
    elif invalid_kind == "lag_outside_fft":
        invalid = TimeSeries(source)
        invalid.set_t0(-300.0)
    elif invalid_kind == "oversized":
        invalid = TimeSeries(nfft + 1)
        invalid.set_t0(source.t0)
        invalid.set_dt(source.dt)
        invalid.set_live()
        invalid.data[: source.npts] = source.data
    elif invalid_kind == "nonfinite":
        invalid = TimeSeries(source)
        invalid.data[0] = np.nan
    elif invalid_kind == "all_zero":
        invalid = TimeSeries(source)
        invalid.data[:] = DoubleVector(np.zeros(source.npts))
    else:
        invalid = TimeSeries(source)
        invalid.kill()

    result = getattr(engine, method_name)(invalid)
    assert isinstance(result, TimeSeries)
    assert result.dead()
    assert result.npts == 0
    assert result.elog.size() >= 1
    assert error_match in result.elog.get_error_log()[-1].message

    # Diagnostics must be side-effect free: the installed inverse still
    # produces the identical finite resolution kernel after a rejected call.
    recovered = engine.actual_output(source)
    assert recovered.live
    assert np.isfinite(np.asarray(recovered.data)).all()
    assert np.array_equal(recovered.data, baseline.data)


@pytest.mark.parametrize(
    "fractional_dt_error,accepted",
    [(0.00005, True), (0.0002, False)],
)
def test_CNRDeconEngine_timeseries_noise_uses_cnr_dt_tolerance(
    fractional_dt_error, accepted
):
    """CNR noise input uses its 1e-4 contract, not MTP's looser tolerance."""
    d0 = make_test_data(noise_level=0.1)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    source = make_simulation_wavelet()
    valid_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, valid_spectrum)
    baseline = engine.actual_output(source)

    candidate = TimeSeries(noise)
    candidate.set_dt(noise.dt * (1.0 + fractional_dt_error))
    computed = engine.compute_noise_spectrum(candidate)
    if accepted:
        assert computed.live
        engine.initialize_inverse_operator_TS(source, candidate)
    else:
        assert computed.dead()
        assert computed.elog.size() >= 1
        assert "sample interval" in computed.elog.get_error_log()[-1].message
        with pytest.raises(MsPASSError, match="noise data sample interval"):
            engine.initialize_inverse_operator_TS(source, candidate)

    recovered = engine.actual_output(source)
    assert recovered.live
    assert np.isfinite(np.asarray(recovered.data)).all()
    if not accepted:
        assert np.array_equal(recovered.data, baseline.data)


@pytest.mark.parametrize("three_component", [False, True])
@pytest.mark.parametrize("npts", range(4))
def test_CNRDeconEngine_short_noise_spectrum_input_is_safe(three_component, npts):
    """Noise shorter than the taper count is rejected before DPSS setup."""
    d0 = make_test_data(noise_level=0.1)
    scalar_noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    baseline = engine.compute_noise_spectrum(scalar_noise)

    if three_component:
        short_noise = Seismogram(npts)
        short_noise.set_t0(0.0)
        short_noise.set_dt(engine.get_operator_dt())
        short_noise.set_live()
        result = engine.compute_noise_spectrum_3C(short_noise)
    else:
        short_noise = TimeSeries(npts)
        short_noise.set_t0(0.0)
        short_noise.set_dt(engine.get_operator_dt())
        short_noise.set_live()
        result = engine.compute_noise_spectrum(short_noise)

    assert result.dead()
    assert result.elog.size() >= 1
    assert "at least 4 samples" in result.elog.get_error_log()[-1].message

    # Recomputing the same valid spectrum confirms rejection happened before
    # replacement of the cached multitaper engine.
    recovered = engine.compute_noise_spectrum(scalar_noise)
    assert recovered.live
    assert recovered.dt() == baseline.dt()
    assert recovered.df() == baseline.df()
    assert np.array_equal(recovered.spectrum, baseline.spectrum)


@pytest.mark.parametrize("three_component", [False, True])
@pytest.mark.parametrize("invalid_kind", ["nan", "inf", "overflow"])
def test_CNRDeconEngine_noise_spectrum_rejects_invalid_samples_before_resize(
    three_component, invalid_kind
):
    """Invalid noise cannot poison or resize the cached multitaper engine."""
    d0 = make_test_data(noise_level=0.1)
    valid_scalar = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    valid_3c = WindowData(d0, -45.0, -5.0)
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    if three_component:
        baseline = engine.compute_noise_spectrum_3C(valid_3c)
        invalid = Seismogram(valid_3c.npts - 1)
        invalid.set_t0(valid_3c.t0)
        invalid.set_dt(valid_3c.dt)
        invalid.set_live()
        if invalid_kind == "nan":
            invalid.data[1, 0] = np.nan
        elif invalid_kind == "inf":
            invalid.data[1, 0] = np.inf
        else:
            huge = np.finfo(np.float64).max
            for component in range(3):
                for sample in range(invalid.npts):
                    invalid.data[component, sample] = huge
        result = engine.compute_noise_spectrum_3C(invalid)
    else:
        baseline = engine.compute_noise_spectrum(valid_scalar)
        invalid = TimeSeries(valid_scalar.npts - 1)
        invalid.set_t0(valid_scalar.t0)
        invalid.set_dt(valid_scalar.dt)
        invalid.set_live()
        if invalid_kind == "nan":
            invalid.data[0] = np.nan
        elif invalid_kind == "inf":
            invalid.data[0] = np.inf
        else:
            invalid.data[:] = DoubleVector(
                np.full(invalid.npts, np.finfo(np.float64).max)
            )
        result = engine.compute_noise_spectrum(invalid)

    assert result.dead()
    assert result.elog.size() >= 1
    message = result.elog.get_error_log()[-1].message
    if invalid_kind == "overflow":
        assert "too large" in message
    else:
        assert "nonfinite samples" in message

    if three_component:
        recovered = engine.compute_noise_spectrum_3C(valid_3c)
    else:
        recovered = engine.compute_noise_spectrum(valid_scalar)
    assert recovered.live
    assert recovered.dt() == baseline.dt()
    assert recovered.df() == baseline.df()
    assert np.array_equal(recovered.spectrum, baseline.spectrum)


@pytest.mark.parametrize("npts", range(4))
def test_CNRDeconEngine_short_timeseries_noise_init_preserves_inverse(npts):
    """The TimeSeries initializer rejects short noise before state changes."""
    d0 = make_test_data(noise_level=0.1)
    datum = WindowData(d0, -5.0, 30.0)
    noise = WindowData(ExtractComponent(d0, 2), -45.0, -5.0)
    source = make_simulation_wavelet()
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    short_noise = TimeSeries(npts)
    short_noise.set_t0(0.0)
    short_noise.set_dt(engine.get_operator_dt())
    short_noise.set_live()

    with pytest.raises(MsPASSError, match="at least 4 samples"):
        engine.initialize_inverse_operator_TS(source, short_noise)

    valid_spectrum = engine.compute_noise_spectrum(noise)
    engine.initialize_inverse_operator(source, valid_spectrum)
    baseline = engine.process(datum, valid_spectrum, 0.02, 2.0)
    actual_before = engine.actual_output(source)
    qc_before = dict(engine.QCMetrics())

    with pytest.raises(MsPASSError, match="at least 4 samples"):
        engine.initialize_inverse_operator_TS(source, short_noise)

    recovered = engine.process(datum, valid_spectrum, 0.02, 2.0)
    actual_after = engine.actual_output(source)
    assert recovered.live
    assert np.isfinite(np.asarray(recovered.data)).all()
    assert np.array_equal(recovered.data, baseline.data)
    assert np.array_equal(actual_after.data, actual_before.data)
    assert dict(engine.QCMetrics()) == qc_before


# test_CNRRFDecon()
# test_CNRRFDecon_error_handlers()
# test_CNRArrayDecon()
# test_CNRRFDecon_error_handlers()
