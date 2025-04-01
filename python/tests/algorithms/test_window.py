#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  2 07:21:50 2020

@author: pavlis
"""

import numpy as np
import pytest

from mspasspy.ccore.seismic import (
    TimeReferenceType,
    _CoreTimeSeries,
    _CoreSeismogram,
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.algorithms.amplitudes import (
    MADAmplitude,
    RMSAmplitude,
    PeakAmplitude,
    PercAmplitude,
    ScalingMethod,
    _scale,
)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.window import scale
from mspasspy.algorithms.window import WindowData, TopMute, WindowData_autopad


# Build a simple _CoreTimeSeries and _CoreSeismogram with
# 100 points and a small number of spikes that allow checking
# by a hand calculation
def setbasics(d, n):
    """
    Takes a child of BasicTimeSeries and defines required attributes with
    a common set of putters.
    """
    d.npts = n
    d.set_dt(0.01)
    d.t0 = 0.0
    d.tref = TimeReferenceType.Relative
    d.live = True


def test_scale():
    dts = _CoreTimeSeries(9)
    setbasics(dts, 9)
    d3c = _CoreSeismogram(5)
    setbasics(d3c, 5)
    dts.data[0] = 3.0
    dts.data[1] = 2.0
    dts.data[2] = -4.0
    dts.data[3] = 1.0
    dts.data[4] = -100.0
    dts.data[5] = -1.0
    dts.data[6] = 5.0
    dts.data[7] = 1.0
    dts.data[8] = -6.0
    # MAD o=f above should be 2
    # perf of 0.8 should be 4
    # rms should be just over 10=10.010993957
    print("Starting tests for time series data of amplitude functions")
    ampmad = MADAmplitude(dts)
    print("MAD amplitude estimate=", ampmad)
    assert ampmad == 3.0
    amprms = RMSAmplitude(dts)
    print("RMS amplitude estimate=", amprms)
    assert round(amprms, 2) == 33.49
    amppeak = PeakAmplitude(dts)
    ampperf80 = PercAmplitude(dts, 0.8)
    print("Peak amplitude=", amppeak)
    print("80% clip level amplitude=", ampperf80)
    assert amppeak == 100.0
    assert ampperf80 == 6.0
    print("Starting comparable tests for 3c data")
    d3c.data[0, 0] = 3.0
    d3c.data[0, 1] = 2.0
    d3c.data[1, 2] = -4.0
    d3c.data[2, 3] = 1.0
    d3c.data[0, 4] = np.sqrt(2) * (100.0)
    d3c.data[1, 4] = -np.sqrt(2) * (100.0)
    ampmad = MADAmplitude(d3c)
    print("MAD amplitude estimate=", ampmad)
    amprms = RMSAmplitude(d3c)
    print("RMS amplitude estimate=", amprms)
    amppeak = PeakAmplitude(d3c)
    ampperf60 = PercAmplitude(d3c, 0.6)
    print("Peak amplitude=", amppeak)
    print("60% clip level amplitude=", ampperf60)
    assert amppeak == 200.0
    assert ampperf60 == 4.0
    assert ampmad == 3.0
    amptest = round(amprms, 2)
    assert amptest == 89.48
    print("Trying scaling functions for TimeSeries")
    # we need a deep copy here since scaling changes the data
    d2 = TimeSeries(dts)
    # This is an invalid TimeWindow used to trigger automatic conversion to full data range
    win = TimeWindow(0.0, -1.0)
    amp = _scale(d2, ScalingMethod.Peak, 1.0, win)
    print("Computed peak amplitude=", amp)
    print(d2.data)
    d2 = TimeSeries(dts)
    amp = _scale(d2, ScalingMethod.Peak, 10.0, win)
    print("Computed peak amplitude with peak set to 10=", amp)
    print(d2.data)
    assert amp == 100.0
    assert d2.data[4] == -10.0
    print("verifying scale has modified and set calib correctly")
    calib = d2.get_double("calib")
    assert calib == 10.0
    d2 = TimeSeries(dts)
    d2.put("calib", 6.0)
    print("test 2 with MAD metric and initial calib of 6")
    amp = _scale(d2, ScalingMethod.MAD, 1.0, win)
    calib = d2.get_double("calib")
    print("New calib value set=", calib)
    assert calib == 18.0
    print("Testing 3C scale functions")
    d = Seismogram(d3c)
    amp = _scale(d, ScalingMethod.Peak, 1.0, win)
    print("Peak amplitude returned by scale funtion=", amp)
    calib = d.get_double("calib")
    print("Calib value retrieved (assumed initial 1.0)=", calib)
    print("Testing python scale function wrapper - first on a TimeSeries with defaults")
    d2 = TimeSeries(dts)
    dscaled = scale(d2)
    amp = dscaled["amplitude"]
    print("peak amplitude returned =", amp)
    assert amp == 100.0
    d = Seismogram(d3c)
    dscaled = scale(d)
    amp = dscaled["amplitude"]
    print("peak amplitude returned test Seismogram=", amp)
    assert amp == 200.0
    print("starting tests of scale on ensembles")
    print(
        "first test TimeSeriesEnsemble with 5 scaled copies of same vector used earlier in this test"
    )
    # test handling of all zeros in sample arrays for both TimeSeries and Seismogram
    # testing only with rms - this is mostly for handlers not the numerics anyway
    d2 = TimeSeries(dts)
    for i in range(d2.npts):
        d2.data[i] = 0.0
    d2 = scale(d2)
    assert d2.live
    assert d2.elog.size() == 1
    assert np.count_nonzero(d2.data) == 0

    d = Seismogram(d3c)
    # trick to initialize data matrix to all 0s - uses a special feature
    # of the c++ api of the set_npts method
    d.set_npts(d3c.npts)
    d = scale(d)
    assert d.live
    assert d.elog.size() == 1
    assert np.count_nonzero(d.data) == 0

    ens = TimeSeriesEnsemble()
    scls = [2.0, 4.0, 1.0, 10.0, 5.0]  # note 4 s the median of this vector
    npts = dts.npts
    for i in range(5):
        d = TimeSeries(dts)
        for k in range(npts):
            d.data[k] *= scls[i]
        d.put("calib", 1.0)
        ens.member.append(d)

    # work on a copy because scaling alters data in place
    enscpy = TimeSeriesEnsemble(ens)
    enscpy = scale(enscpy)
    print("returned amplitudes for members scaled individually")
    for i in range(5):
        amp = enscpy.member[i].get_double("amplitude")
        print(amp)
        assert amp == 100.0 * scls[i]
    enscpy = TimeSeriesEnsemble(ens)
    amp = scale(enscpy, scale_by_section=True)
    print("average amplitude=", amp[0])
    # assert(amp[0]==4.0)
    avgamp = amp[0]
    for i in range(5):
        calib = enscpy.member[i].get_double("calib")
        print("member number ", i, " calib is ", calib)
        assert round(calib) == 400.0
        # print(enscpy.member[i].data)

    # similar test for SeismogramEnsemble
    npts = d3c.npts
    ens = SeismogramEnsemble()
    for i in range(5):
        d = Seismogram(d3c)
        for k in range(3):
            for j in range(npts):
                d.data[k, j] *= scls[i]
        d.put("calib", 1.0)
        ens.member.append(d)
    print("Running comparable tests on SeismogramEnsemble")
    enscpy = SeismogramEnsemble(ens)
    enscpy = scale(enscpy)
    print("returned amplitudes for members scaled individually")
    for i in range(5):
        amp = enscpy.member[i].get_double("amplitude")
        print(amp)
        assert round(amp) == round(200.0 * scls[i])
    print("Trying section scaling of same data")
    enscpy = SeismogramEnsemble(ens)
    enscpy = scale(enscpy, scale_by_section=True)
    amp = enscpy["amplitude"]
    print("average amplitude=", amp)
    assert np.isclose(amp, 800.0)
    for i in range(5):
        calib = enscpy.member[i].get_double("calib")
        print("member number ", i, " calib is ", calib)
        assert np.isclose(amp, 800.0)


def test_windowdata():
    """
    This pytest function tests WindowData for the default configuration where
    the "short_segment_handling" argument defaults to "kill".   It mostly tests
    the case where the window requested is consistent with the data.  It touches
    some of the error cases, but the related function test_windowdata_exceptions
    covers that and more.  i.e. all the error handling functions are tested
    in test_windowdata_exceptions.
    """
    npts = 1000
    ts = TimeSeries()
    setbasics(ts, npts)
    for i in range(npts):
        ts.data[i] = float(i)
    se = Seismogram()
    setbasics(se, npts)
    for k in range(3):
        for i in range(npts):
            se.data[k, i] = 100 * (k + 1) + float(i)
    # these patterns have t0=0 and endtime 9.99 because
    # d.dt is 0.01
    # make copies because WindowData can alter content
    ts0 = TimeSeries(ts)
    se0 = Seismogram(se)
    # ensembles are this length with first datum having a different t0
    Nmembers = 3
    offset_m0 = 1.0
    ts_ens0 = TimeSeriesEnsemble(3)
    se_ens0 = SeismogramEnsemble(3)
    for i in range(Nmembers):
        ts_ens0.member.append(TimeSeries(ts0))
        se_ens0.member.append(Seismogram(se0))
    ts_ens0.member[0].t0 += offset_m0
    se_ens0.member[0].t0 += offset_m0

    # first test case where the operator should work and return
    # live (all live for ensemble) results
    d = WindowData(ts, 2, 3)
    assert d.live
    # print("t y")
    # for j in range(d.npts):
    #    print(d.time(j), d.data[j])
    assert len(d.data) == 101
    assert np.isclose(d.t0, 2.0)
    assert np.isclose(d.endtime(), 3.0)
    d = WindowData(se, 2, 3)
    assert d.live
    # print("t x0 x1 x2")
    # for j in range(d.npts):
    #    print(d.time(j), d.data[0, j], d.data[1, j], d.data[2, j])
    assert d.data.columns() == 101
    assert np.isclose(d.t0, 2.0)
    assert np.isclose(d.endtime(), 3.0)
    se = Seismogram(se0)
    # verify windowing uses a round operator in converting times to
    # compute the sample number of the window to retrieve.
    # first test left on TimeSeries and Seismogram
    ts = TimeSeries(ts0)
    dt = ts.dt
    stime2p = 2.0 + 0.6 * dt
    d = WindowData(ts, stime2p, 3.0)
    # WindowData finds nearest sample to window start time but
    # sets t0 of the output to the computed time for the nearest
    # sample.   That means t0 of the windowed data is usually not
    # exactly matching window start time
    istart = ts.sample_number(stime2p)
    t0_expected = ts.time(istart)
    assert np.isclose(d.t0, t0_expected)
    # should be one less than with start 2.0 which is 101
    assert d.npts == 100
    se = Seismogram(se0)
    dt = ts.dt
    stime2p = 2.0 + 0.6 * dt
    istart = ts.sample_number(stime2p)
    t0_expected = ts.time(istart)
    d = WindowData(se, stime2p, 3.0)
    assert np.isclose(d.t0, t0_expected)
    assert d.npts == 100
    # now rounding test for right (endtime of window)
    ts = TimeSeries(ts0)
    dt = ts.dt
    d = WindowData(ts, 2.0, 3.0 + 0.6 * dt)
    # now it should be one more than 2.0 to 3.0 window
    assert d.npts == 102
    se = Seismogram(se0)
    dt = ts.dt
    d = WindowData(se, 2.0, 3.0 + 0.6 * dt)
    assert d.npts == 102

    # print("verifying previously killed data are returned unaltered")
    se.kill()
    d = WindowData(se, 2, 3)
    assert d.dead()
    assert d.npts == 1000
    ts = TimeSeries(ts0)
    ts.kill()
    d = WindowData(ts, 2, 3)
    assert d.dead()
    assert d.npts == 1000
    se = Seismogram(se0)
    se.kill()
    d = WindowData(se, 2, 3)
    assert d.dead()
    assert d.npts == 1000
    # test the t0shift argument.
    # We only test this for TimeSeries because the exact same
    # code would be used by Seismogram data and ensembles
    # a complication is that we need to changes the times
    # completely and make the data look like UTC data
    # that requires a few special tricks used in the
    # dithering with TimeSeries object below
    ts = TimeSeries(ts0)
    UTC_offset = 10000.0
    ts.set_t0(UTC_offset)
    # this is method normal users should never use except for simulations
    ts.force_t0_shift(UTC_offset)
    ts.tref = TimeReferenceType.UTC
    # save this for use with string mode for t0shift next
    ts_UTC = TimeSeries(ts)
    d = WindowData(ts, 2.0, 3.0, t0shift=UTC_offset)
    assert d.live
    assert np.isclose(d.t0, UTC_offset + 2.0)
    assert np.isclose(d.endtime(), UTC_offset + 3.0)
    # repeat putting the shift in Metadata and using "shifttest" as the Metadata key
    ts = TimeSeries(ts_UTC)
    ts["shifttest"] = UTC_offset
    d = WindowData(ts, 2.0, 3.0, t0shift="shifttest")
    assert d.live
    assert np.isclose(d.t0, UTC_offset + 2.0)
    assert np.isclose(d.endtime(), UTC_offset + 3.0)

    # Data collected with a UTC timing system often have
    # timing precision better than the sample interval.
    # This tests that WindowData preserves subsample timing.
    ts = TimeSeries(ts0)
    # skew t0 by 1/4 sample
    ts.set_t0(1.0 + 0.25 * ts.dt)
    d = WindowData(ts, 2.0, 3.0)
    assert d.live
    assert d.t0 == 2.0 + 0.25 * ts.dt
    assert d.endtime() == 3.0 + 0.25 * ts.dt

    # verify ensembles work for a valid time interval
    ts_ens = TimeSeriesEnsemble(ts_ens0)
    se_ens = SeismogramEnsemble(se_ens0)
    e = WindowData(ts_ens, 2, 3)
    e3 = WindowData(se_ens, 2, 3)
    assert e.live
    assert e3.live
    assert len(e.member) == 3
    for i in range(Nmembers):
        assert e.member[i].live
        assert e3.member[i].live

    # test window that will kill member 0 of ensembles only
    # note member 0 has t0 of 1.0 while the other have t0 == 0
    ts_ens = TimeSeriesEnsemble(ts_ens0)
    se_ens = SeismogramEnsemble(se_ens0)
    e = WindowData(ts_ens, 0.5, 3)
    e3 = WindowData(se_ens, 0.5, 3)
    assert e.live
    assert e3.live
    assert len(e.member) == 3
    for i in range(Nmembers):
        if i == 0:
            assert e.member[i].dead()
            assert e3.member[i].dead()
        else:
            assert e.member[i].live
            assert e3.member[i].live
    # repeat exact same test but with "kill" specified explicitly
    ts_ens = TimeSeriesEnsemble(ts_ens0)
    se_ens = SeismogramEnsemble(se_ens0)
    e = WindowData(ts_ens, 0.5, 3, short_segment_handling="kill")
    e3 = WindowData(se_ens, 0.5, 3, short_segment_handling="kill")
    assert e.live
    assert e3.live
    assert len(e.member) == 3
    for i in range(Nmembers):
        if i == 0:
            assert e.member[i].dead()
            assert e3.member[i].dead()
        else:
            assert e.member[i].live
            assert e3.member[i].live


def test_windowdata_exceptions():
    """
    This function tests the wide range possible error handling for
    the WindowData function.  It is a companion to test_WindowData
    that mainly tests the cases where the data are processed without
    any problems.

    Most of these tests are applied only to atomic data and usually only
    only TimeSeries.  That is done because, at this time at least,
    the error handling is all with atomic data and all tests with
    time ranges visit the same code base for both TimeSeries and
    Seismogram data.
    """
    # This duplicates the same data generator as test_WindowData
    # it probably should be turned into a pytest fixture for this file
    npts = 1000
    ts = TimeSeries()
    setbasics(ts, npts)
    for i in range(npts):
        ts.data[i] = float(i)
    se = Seismogram()
    setbasics(se, npts)
    for k in range(3):
        for i in range(npts):
            se.data[k, i] = 100 * (k + 1) + float(i)
    # these patterns have t0=0 and endtime 9.99 because
    # d.dt is 0.01
    # make copies because WindowData can alter content
    ts0 = TimeSeries(ts)
    se0 = Seismogram(se)
    # ensembles are this length with first datum having a different t0
    Nmembers = 3
    offset_m0 = 1.0
    ts_ens0 = TimeSeriesEnsemble(3)
    se_ens0 = SeismogramEnsemble(3)
    for i in range(Nmembers):
        ts_ens0.member.append(TimeSeries(ts0))
        se_ens0.member.append(Seismogram(se0))
    ts_ens0.member[0].t0 += offset_m0
    se_ens0.member[0].t0 += offset_m0

    # First test argument mistake handler.  i.e. these cases throw
    # exceptions instead of posting elog messages
    ts = TimeSeries(ts0)
    with pytest.raises(ValueError, match="illegal option"):
        d = WindowData(ts, 2, 3, short_segment_handling="notvalid")
    ts = TimeSeries(ts0)
    # WindowData handles both atomic and ensemble data \
    # this exception is handled by the function itself, no the decorators
    with pytest.raises(TypeError, match="Illegal type for arg0="):
        d = WindowData(0.0, 2.0, 3.0)

    # We don't test explicit kill mode.  That is default and assume
    # the explicit use is tested in test_WindowData
    # First run the special, dogmatic test where the window range is outside
    # the data range.  That should always kill
    # interval less than t0
    stime = -5.0
    etime = -2.0
    ts = TimeSeries(ts0)
    d = WindowData(ts, stime, etime)
    assert d.dead()
    ts = TimeSeries(ts0)
    d = WindowData(ts, stime, etime, short_segment_handling="truncate")
    assert d.dead()
    ts = TimeSeries(ts0)
    d = WindowData(ts, stime, etime, short_segment_handling="pad")
    assert d.dead()
    # interval greater than endtime()
    stime = 20.0
    etime = 22.0
    ts = TimeSeries(ts0)
    d = WindowData(ts, stime, etime)
    assert d.dead()
    ts = TimeSeries(ts0)
    d = WindowData(ts, stime, etime, short_segment_handling="truncate")
    assert d.dead()
    ts = TimeSeries(ts0)
    d = WindowData(ts, stime, etime, short_segment_handling="pad")
    assert d.dead()
    # Now do a set of tests in truncate mode
    # each test runs with error logging turned on and in silent mode
    #
    # test truncation on the left (starttime)
    # with logging
    stime = -0.5
    etime = 2.0
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="truncate", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == 0.0
    npts_expected = round((etime - d.t0) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 1
    # silent mode
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts,
        stime,
        etime,
        short_segment_handling="truncate",
        log_recoverable_errors=False,
    )
    assert d.live
    assert d.t0 == 0.0
    npts_expected = round((etime - d.t0) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 0
    # similar test but truncated on the right
    stime = 2.0
    etime = 15.0
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="truncate", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == 2.0
    npts_expected = round((ts0.endtime() - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 1
    # silent mode
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts,
        stime,
        etime,
        short_segment_handling="truncate",
        log_recoverable_errors=False,
    )
    assert d.live
    assert d.t0 == 2.0
    npts_expected = round((ts0.endtime() - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 0

    # Similar tests in pad mode
    # each test runs with error logging turned on and in silent mode
    #
    # test padding on the left (starttime)
    # with logging
    stime = -0.5
    etime = 2.0
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="pad", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == stime
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 1
    padend = d.sample_number(0.0) - 1
    for i in range(padend):
        assert d.data[i] == 0.0
    # silent mode
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="pad", log_recoverable_errors=False
    )
    assert d.live
    assert d.t0 == stime
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 0
    # verify subsample timing is preserved
    stime = -0.5
    etime = 2.0
    ts = TimeSeries(ts0)
    # dither t0 by 1/4 sample
    dt = ts0.dt
    ts.t0 += 0.25 * dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="pad", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == stime + 0.25 * dt
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 1
    padend = d.sample_number(0.0) - 1
    for i in range(padend):
        assert d.data[i] == 0.0

    # similar test but padding on the right
    stime = 2.0
    etime = 15.0
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="pad", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == stime
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    pad_start = d.sample_number(ts0.endtime()) + 1
    for i in range(pad_start, d.npts):
        assert d.data[i] == 0.0
    assert d.elog.size() == 1
    # silent mode
    ts = TimeSeries(ts0)
    dt = ts0.dt
    d = WindowData(
        ts, stime, etime, short_segment_handling="pad", log_recoverable_errors=False
    )
    assert d.live
    assert d.t0 == stime
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 0
    # in truncate mode TimeSeries and Seismogram handling do not
    # require independent tests (at least at this time July 2024)
    # with padding, however, a different section of code is required
    # to handle the matrix padding done in python.  This section
    # tests that for left and right padding.  We don't retest
    # silent mode because that is the same for TimeSeries and
    # Seismogram data
    stime = -0.5
    etime = 2.0
    se = Seismogram(se0)
    dt = se0.dt
    d = WindowData(
        se, stime, etime, short_segment_handling="pad", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == stime
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    assert d.elog.size() == 1
    padend = d.sample_number(0.0) - 1
    for i in range(3):
        for j in range(padend):
            assert d.data[i, j] == 0.0

    # similar test but padding on the right
    stime = 2.0
    etime = 15.0
    se = Seismogram(se0)
    dt = se0.dt
    d = WindowData(
        se, stime, etime, short_segment_handling="pad", log_recoverable_errors=True
    )
    assert d.live
    assert d.t0 == stime
    npts_expected = round((etime - stime) / dt) + 1
    assert d.npts == npts_expected
    pad_start = d.sample_number(se0.endtime()) + 1
    for i in range(3):
        for j in range(pad_start, d.npts):
            assert d.data[i, j] == 0.0
    assert d.elog.size() == 1


def test_WindowData_autopad():
    """
    Tests function added September 2024 that uses a soft test
    for the data range.   Returns a fixed length signal that
    is padded unless the actual time span is too short as
    defined by the fractional_mismatch_limit argument.
    The function is a higher level function using WindowData
    so tests are limited to the features it adds and
    additional exceptions it handles.
    """
    # this duplicates code in test_WindowData
    # TODO:  this should be converted to a pytest fixture
    npts = 1000
    ts = TimeSeries()
    setbasics(ts, npts)
    for i in range(npts):
        ts.data[i] = float(i)
    se = Seismogram()
    setbasics(se, npts)
    for k in range(3):
        for i in range(npts):
            se.data[k, i] = 100 * (k + 1) + float(i)
    # these patterns have t0=0 and endtime 9.99 because
    # d.dt is 0.01
    # make copies because WindowData can alter content
    ts0 = TimeSeries(ts)
    se0 = Seismogram(se)

    # first verify the function works correctly if the
    # stime:etime range is entirely within the data
    ts = TimeSeries(ts0)
    ts = WindowData_autopad(ts, ts.time(10), ts.time(50))
    assert ts.live
    assert ts.npts == 41

    se = Seismogram(se0)
    se = WindowData_autopad(se, se.time(10), se.time(50))
    assert se.live
    assert se.npts == 41

    # verify automatic zero padding of first and last sample
    # when stime and etime are specified as that
    ts = TimeSeries(ts0)
    # the time method works with negative and resolves her
    # to one sample before start and one sample after end time
    ts = WindowData_autopad(ts, ts.time(-1), ts.time(npts))
    assert ts.live
    assert ts.npts == npts + 2
    # isclose is not needed here as this is a hard set 0
    assert ts.data[0] == 0.0
    assert ts.data[npts + 1] == 0.0
    # similar for Seismogram
    se = Seismogram(se0)
    # the time method works with negative and resolves her
    # to one sample before start and one sample after end time
    se = WindowData_autopad(se, se.time(-1), se.time(npts))
    assert se.live
    assert se.npts == npts + 2
    for k in range(3):
        assert se.data[k, 0] == 0.0
        assert se.data[k, npts + 1] == 0.0

    # test handling of ensembles via the mspass_func_decorator
    # note the function itself only handles atomic data
    # Test only TimeSeriesEnsemble
    ens = TimeSeriesEnsemble(2)
    for i in range(2):
        ens.member.append(ts)
    ens.set_live()
    ens = WindowData_autopad(ens, ts.time(10), ts.time(50))
    for i in range(2):
        assert ens.member[i].live

    # only test this for TimeSeries - no reason to think it would behave differently for Seismogram
    ts = TimeSeries(ts0)
    ts = WindowData_autopad(ts, ts.t0, ts.time(3 * npts))
    assert ts.dead()
    # this is actually 2 because it passes through WindowDataAtomic that also posts a log message
    # Using >= to make the test more robust in the event that changes
    assert ts.elog.size() >= 1


def test_TopMute():
    ts = TimeSeries(100)
    seis = Seismogram(100)
    # Fill data arrays with all ones to show form of mute
    for i in range(100):
        ts.data[i] = 1.0
    seis.data[:, :] = 1.0
    ts.dt = 0.1
    ts.t0 = 0.0
    seis.dt = 0.1
    seis.t0 = 0.0
    ts.live = True
    seis.live = True

    ts2 = TimeSeries(ts)
    seis2 = Seismogram(seis)
    lmute = TopMute(2.0, 4.0, "linear")
    lmute.apply(ts2)
    lmute.apply(seis2)
    ini_index = ts2.sample_number(2.0)
    las_index = ts2.sample_number(4.0)
    mid_index = int((ini_index + las_index) / 2)
    assert np.isclose(ts2.data[mid_index], 0.5)
    assert np.isclose(seis2.data[:, mid_index], 0.5).all()
    assert np.isclose(ts2.data[ini_index], 0.0)
    assert np.isclose(seis2.data[:, ini_index], 0.0).all()
    assert np.isclose(ts2.data[las_index], 1)
    assert np.isclose(seis2.data[:, las_index], 1).all()

    ts2 = TimeSeries(ts)
    seis2 = Seismogram(seis)
    cmute = TopMute(2.0, 4.0, "cosine")
    cmute.apply(ts2)
    cmute.apply(seis2)
    ini_index = ts2.sample_number(2.0)
    las_index = ts2.sample_number(4.0)
    mid_index = int((ini_index + las_index) / 2)
    assert np.isclose(ts2.data[mid_index], 0.5)
    assert np.isclose(seis2.data[:, mid_index], 0.5).all()
    assert np.isclose(ts2.data[ini_index], 0.0)
    assert np.isclose(seis2.data[:, ini_index], 0.0).all()
    assert np.isclose(ts2.data[las_index], 1)
    assert np.isclose(seis2.data[:, las_index], 1).all()

    failmute = TopMute(-0.1, 4.0, "cosine")
    failmute.apply(ts2)
    assert not ts2.live

    with pytest.raises(TypeError, match="TopMute.apply:  usage error."):
        failmute.apply([1, 2, 3])


test_TopMute()
