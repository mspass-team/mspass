#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest
from pathlib import Path

from mspasspy.ccore.utility import pfread
from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.seismic import TimeReferenceType, DoubleVector
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import TimeDomainGIDDecon
from mspasspy.algorithms.TimeDomainGIDDecon import TimeDomainGIDRFDecon
from mspasspy.algorithms.basic import ExtractComponent

from decon_data_generators import (
    addnoise,
    convolve_wavelet,
    make_impulse_data,
    make_simulation_wavelet,
)


def _make_gid_test_data(noise_level=0.02):
    wavelet = make_simulation_wavelet()
    impulses = make_impulse_data()
    data = convolve_wavelet(impulses, wavelet)
    if noise_level is None:
        noise_level = 0.0
    return addnoise(data, nscale=noise_level, padlength=800)


def _assert_valid_rf(rf):
    assert rf.live
    assert rf.npts > 0
    assert np.isfinite(rf.data).all()
    assert np.linalg.norm(rf.data) > 0.0


def _make_single_spike_convolution_data():
    n = 1000
    dt = 0.05
    t0 = -25.0
    time_axis = np.arange(n) * dt + t0
    wavelet = np.exp(-((time_axis / 0.12) ** 2))
    wavelet /= np.max(wavelet)
    model = np.zeros((3, n))
    spike_sample = round((0.0 - t0) / dt)
    model[:, spike_sample] = [0.2, -0.1, 1.0]
    data_matrix = np.vstack(
        [
            np.convolve(model[k], wavelet, mode="same")
            + 1.0e-8 * np.sin(0.37 * np.arange(n) + k)
            for k in range(3)
        ]
    )

    data = Seismogram(n)
    data.set_t0(t0)
    data.set_dt(dt)
    data.set_live()
    data.tref = TimeReferenceType.Relative
    for k in range(3):
        data.data[k, :] = DoubleVector(data_matrix[k, :])
    return data


def _pf_with_mode(tmp_path, pf_name, branch_name, mode):
    src = Path("./data/pf") / pf_name
    text = src.read_text()
    text = text.replace(
        f"{branch_name} &Arr{{\n        deconvolution_type least_square",
        f"{branch_name} &Arr{{\n        deconvolution_type {mode}",
    )
    dst = tmp_path / pf_name
    dst.write_text(text)
    return pfread(str(dst))


def test_TimeDomainGIDDecon_binding_and_wrapper():
    np.random.seed(13)
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    signal_window = TimeWindow(-8.0, 20.0)
    noise_window = TimeWindow(-25.0, -8.0)

    rf, actual_output, ideal_output = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=signal_window,
        noise_window=noise_window,
        return_wavelet=True,
    )

    _assert_valid_rf(rf)
    assert actual_output.live
    assert ideal_output.live
    assert rf.is_defined("TimeDomainGIDDecon_properties")
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0

    # The synthetic direct-arrival vector has component ratios
    # u0/u2=-15/150 and u1/u2=10/150.  The sparse GID output may be shifted
    # by shaping convolution, but the recovered dominant vector should preserve
    # those ratios.
    zrf = ExtractComponent(rf, 2)
    peak_sample = int(np.argmax(np.abs(zrf.data)))
    assert signal_window.start <= zrf.time(peak_sample) <= signal_window.end
    assert np.isclose(
        rf.data[0, peak_sample] / rf.data[2, peak_sample], -0.1, atol=2.0e-3
    )
    assert np.isclose(
        rf.data[1, peak_sample] / rf.data[2, peak_sample],
        10.0 / 150.0,
        atol=2.0e-3,
    )


def test_TimeDomainGIDDecon_engine_reuse_is_stable():
    np.random.seed(13)
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    signal_window = TimeWindow(-8.0, 20.0)
    noise_window = TimeWindow(-25.0, -8.0)

    rf1 = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=signal_window,
        noise_window=noise_window,
    )
    rf2 = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=signal_window,
        noise_window=noise_window,
    )

    _assert_valid_rf(rf1)
    _assert_valid_rf(rf2)
    assert np.allclose(rf1.data, rf2.data)


def test_TimeDomainGIDDecon_validates_single_spike_recovery():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)

    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-24.0, -12.0),
    )

    _assert_valid_rf(rf)
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_Linf_final"] < qc["residual_Linf_initial"]
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]

    zrf = ExtractComponent(rf, 2)
    peak_sample = int(np.argmax(np.abs(zrf.data)))
    assert np.isclose(
        rf.data[0, peak_sample] / rf.data[2, peak_sample], 0.2, atol=2.0e-3
    )
    assert np.isclose(
        rf.data[1, peak_sample] / rf.data[2, peak_sample], -0.1, atol=2.0e-3
    )


@pytest.mark.parametrize("mode", ["least_square", "water_level", "multi_taper", "cnr3c"])
def test_TimeDomainGIDDecon_inverse_modes_are_valid(tmp_path, mode):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution", mode
    )
    engine = TimeDomainGIDDecon(pf)

    rf, actual_output, ideal_output = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-24.0, -12.0),
        return_wavelet=True,
    )

    _assert_valid_rf(rf)
    assert actual_output.live
    assert ideal_output.live
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]


def test_TimeDomainGIDRFDecon_argument_validation():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    data = _make_gid_test_data(noise_level=None)
    impl = TimeDomainGIDRFDecon.__wrapped__

    with pytest.raises(TypeError):
        impl("not a seismogram", engine)
    with pytest.raises(TypeError):
        impl(data, object())
    with pytest.raises(TypeError):
        impl(data, engine, signal_window=(-8.0, 20.0))
    with pytest.raises(TypeError):
        impl(data, engine, noise_window=(-25.0, -8.0))

    dead = Seismogram(data)
    dead.kill()
    assert impl(dead, engine).dead()
    dead_result = impl(dead, engine, return_wavelet=True)
    assert dead_result[0].dead()
    assert dead_result[1] is None
    assert dead_result[2] is None


def test_TimeDomainGIDRFDecon_error_return_and_optional_qc():
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")

    # A too-short analysis window cannot contain the inverse wavelet transient.
    engine = TimeDomainGIDDecon(pf)
    bad_result = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-2.0, 2.0),
        noise_window=TimeWindow(-25.0, -8.0),
        return_wavelet=True,
    )
    assert bad_result[0].dead()
    assert bad_result[1] is None
    assert bad_result[2] is None

    engine = TimeDomainGIDDecon(pf)
    rf = TimeDomainGIDRFDecon(data, engine, QCdata_key=None)
    _assert_valid_rf(rf)
    assert not rf.is_defined("TimeDomainGIDDecon_properties")
