#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests that amplitude functions (MADAmplitude, PeakAmplitude, RMSAmplitude, PercAmplitude)
are picklable when captured in closures for Dask serialization (issue #680).

Without the functor+py::pickle fix, cloudpickle.dumps fails with
TypeError: cannot pickle 'PyCapsule' object.
"""

import cloudpickle

from mspasspy.ccore.algorithms.amplitudes import (
    MADAmplitude,
    PeakAmplitude,
    RMSAmplitude,
    PercAmplitude,
)
from mspasspy.ccore.seismic import _CoreTimeSeries, TimeReferenceType


def _make_timeseries_5():
    """CoreTimeSeries [1,2,3,4,5] for MAD=3.0, Peak=5, RMS≈3.0."""
    d = _CoreTimeSeries(5)
    d.npts = 5
    d.set_dt(0.01)
    d.t0 = 0.0
    d.tref = TimeReferenceType.Relative
    d.live = True
    for i in range(5):
        d.data[i] = float(i + 1)
    return d


def _make_timeseries_window():
    """CoreTimeSeries from test_window: Peak=100, RMS≈33.49, Perc(0.8)=6."""
    d = _CoreTimeSeries(9)
    d.npts = 9
    d.set_dt(0.01)
    d.t0 = 0.0
    d.tref = TimeReferenceType.Relative
    d.live = True
    vals = [3.0, 2.0, -4.0, 1.0, -100.0, -1.0, 5.0, 1.0, -6.0]
    for i, v in enumerate(vals):
        d.data[i] = v
    return d


def test_MADAmplitude_captured_in_closure_picklable():
    """
    Regression for issue #680: MADAmplitude must be picklable when captured in
    a closure for Dask. Without the functor+py::pickle fix, cloudpickle.dumps
    fails with TypeError: cannot pickle 'PyCapsule' object.
    """

    def _compute():
        d = _make_timeseries_5()
        return MADAmplitude(d)  # MAD = median of |values| = 3.0

    pickled = cloudpickle.dumps(_compute)
    fn_restored = cloudpickle.loads(pickled)
    assert fn_restored() == 3.0, f"Round-trip pickle: expected 3.0, got {fn_restored()}"


def test_PeakAmplitude_captured_in_closure_picklable():
    """
    PeakAmplitude must be picklable when captured in a closure for Dask.
    """

    def _compute():
        d = _make_timeseries_window()
        return PeakAmplitude(d)  # Peak = 100.0

    pickled = cloudpickle.dumps(_compute)
    fn_restored = cloudpickle.loads(pickled)
    assert (
        fn_restored() == 100.0
    ), f"Round-trip pickle: expected 100.0, got {fn_restored()}"


def test_RMSAmplitude_captured_in_closure_picklable():
    """
    RMSAmplitude must be picklable when captured in a closure for Dask.
    """

    def _compute():
        d = _make_timeseries_window()
        return round(RMSAmplitude(d), 2)  # ≈ 33.49

    pickled = cloudpickle.dumps(_compute)
    fn_restored = cloudpickle.loads(pickled)
    assert (
        fn_restored() == 33.49
    ), f"Round-trip pickle: expected 33.49, got {fn_restored()}"


def test_PercAmplitude_captured_in_closure_picklable():
    """
    PercAmplitude must be picklable when captured in a closure for Dask.
    """

    def _compute():
        d = _make_timeseries_window()
        return PercAmplitude(d, 0.8)  # 80% clip = 6.0

    pickled = cloudpickle.dumps(_compute)
    fn_restored = cloudpickle.loads(pickled)
    assert fn_restored() == 6.0, f"Round-trip pickle: expected 6.0, got {fn_restored()}"
