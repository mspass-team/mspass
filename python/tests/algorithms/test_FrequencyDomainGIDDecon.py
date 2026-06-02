#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import numpy as np

from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import FrequencyDomainGIDDecon
from mspasspy.ccore.utility import pfread
from mspasspy.ccore.seismic import Seismogram
from mspasspy.algorithms.FrequencyDomainGIDDecon import FrequencyDomainGIDRFDecon

from test_TimeDomainGIDDecon import (
    _assert_actual_and_output_shaping_are_distinct,
    _assert_single_spike_recovery,
    _assert_valid_rf,
    _make_external_wavelet_3c_data,
    _make_gid_test_data,
    _make_single_spike_convolution_data,
    _ns_gid_pf,
    _pf_with_mode,
)


def _run_frequency_gid(data, pf):
    engine = FrequencyDomainGIDDecon(pf)
    return FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )


def test_FrequencyDomainGIDDecon_binding_and_wrapper():
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)

    rf, actual_output, output_shaping_wavelet = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
        return_wavelet=True,
    )

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    assert rf.is_defined("FrequencyDomainGIDDecon_properties")
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]


def test_FrequencyDomainGIDDecon_validates_single_spike_recovery():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")

    rf, actual_output, output_shaping_wavelet = _run_frequency_gid(data, pf)

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_Linf_final"] < qc["residual_Linf_initial"]
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]

    _assert_single_spike_recovery(rf, ratio_tolerance=5.0e-2)


def test_FrequencyDomainNSGID_uses_external_wavelet_and_gain_cap(tmp_path):
    data, wavelet, spike_times = _make_external_wavelet_3c_data(noise_level=2.0e-4)
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        gain_max=30.0,
    )
    engine = FrequencyDomainGIDDecon(pf)
    rf = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
        external_wavelet=wavelet,
    )

    _assert_valid_rf(rf)
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["ns_gid_enabled"]
    assert qc["ns_gid_external_wavelet_used"]
    assert qc["ns_gid_gain_max_actual"] <= 30.0 * (1.0 + 1.0e-10)
    assert qc["ns_gid_number_spikes"] <= 2 * len(spike_times)
    support = np.where(np.linalg.norm(np.asarray(rf.data), axis=0) > 1.0e-8)[0]
    picked_times = [rf.time(int(i)) for i in support]
    assert picked_times
    expected_times = [t - wavelet.t0 for t in spike_times[:2]]
    for t in expected_times:
        assert min(abs(t - p) for p in picked_times) < 0.15


@pytest.mark.parametrize("mode", ["least_square", "water_level", "multi_taper", "cnr"])
def test_FrequencyDomainGIDDecon_inverse_modes_are_valid(tmp_path, mode):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
        mode,
    )

    rf, actual_output, output_shaping_wavelet = _run_frequency_gid(data, pf)

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    qc = rf["FrequencyDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]
    _assert_single_spike_recovery(rf, ratio_tolerance=5.0e-2)


def test_FrequencyDomainGIDRFDecon_argument_validation():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")
    engine = FrequencyDomainGIDDecon(pf)
    impl = FrequencyDomainGIDRFDecon.__wrapped__

    with pytest.raises(TypeError):
        impl("not a seismogram", engine)
    with pytest.raises(TypeError):
        impl(data, object())
    with pytest.raises(TypeError):
        impl(data, engine, signal_window=(-8.0, 20.0))
    with pytest.raises(TypeError):
        impl(data, engine, noise_window=(-25.0, -8.0))

    dead = data
    dead.kill()
    assert impl(dead, engine).dead()
    dead_result = impl(dead, engine, return_wavelet=True)
    assert dead_result[0].dead()
    assert dead_result[1] is None
    assert dead_result[2] is None


def test_FrequencyDomainGIDRFDecon_error_return_and_optional_qc():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/FrequencyDomainGIDDecon.pf")

    zero_data = Seismogram(data)
    zero_data.data[:, :] = 0.0
    engine = FrequencyDomainGIDDecon(pf)
    bad_result = FrequencyDomainGIDRFDecon(
        zero_data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )
    assert bad_result[0].dead()
    assert bad_result[1] is None
    assert bad_result[2] is None

    engine = FrequencyDomainGIDDecon(pf)
    rf = FrequencyDomainGIDRFDecon(data, engine, QCdata_key=None)
    _assert_valid_rf(rf)
    assert not rf.is_defined("FrequencyDomainGIDDecon_properties")
