#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest
import numpy as np

from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import FrequencyDomainGIDDecon
from mspasspy.ccore.utility import Metadata, MsPASSError, pfread
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum, Seismogram
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


def test_FrequencyDomainGIDDecon_rejects_leaf_window_drift(tmp_path):
    text = open("data/pf/FrequencyDomainGIDDecon.pf", encoding="utf-8").read()
    text = text.replace(
        "least_square &Arr{\n        target_sample_interval 0.05\n        operator_nfft 512\n        deconvolution_data_window_start -5.0",
        "least_square &Arr{\n        target_sample_interval 0.05\n        operator_nfft 512\n        deconvolution_data_window_start -4.0",
    )
    pfname = tmp_path / "FrequencyDomainGIDDecon_bad_leaf.pf"
    pfname.write_text(text)

    with pytest.raises(MsPASSError, match="not consistent with GID parameters"):
        FrequencyDomainGIDDecon(pfread(str(pfname)))


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


def test_FrequencyDomainGIDRFDecon_clears_external_state_between_calls(tmp_path):
    data = _make_gid_test_data(noise_level=1.0e-4)
    from decon_data_generators import make_simulation_wavelet

    external_wavelet = make_simulation_wavelet()
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)

    rf_external = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
        external_wavelet=external_wavelet,
    )
    assert rf_external.live
    assert rf_external["FrequencyDomainGIDDecon_properties"][
        "ns_gid_external_wavelet_used"
    ]

    rf_internal = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
    )
    assert rf_internal.live
    assert not rf_internal["FrequencyDomainGIDDecon_properties"][
        "ns_gid_external_wavelet_used"
    ]


def test_FrequencyDomainGIDDecon_rejects_empty_external_noise(tmp_path):
    from mspasspy.ccore.seismic import TimeSeries

    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    empty_noise = TimeSeries(0)
    empty_noise.set_live()

    with pytest.raises(MsPASSError, match="external noise is empty"):
        engine.loadnoise(empty_noise)


def test_FrequencyDomainGIDDecon_rejects_invalid_external_noise_spectrum(tmp_path):
    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    dead_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0]), 1.0, "dead", 0.0, 1.0, 1
    )
    dead_spectrum.kill()
    live_empty_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([]), 1.0, "empty", 0.0, 1.0, 0
    )
    one_bin_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0]), 1.0, "one_bin", 0.0, 1.0, 1
    )

    with pytest.raises(MsPASSError, match="PowerSpectrum is marked dead"):
        engine.loadnoise(dead_spectrum)
    with pytest.raises(MsPASSError, match="at least two frequency bins"):
        engine.loadnoise(live_empty_spectrum)
    with pytest.raises(MsPASSError, match="at least two frequency bins"):
        engine.loadnoise(one_bin_spectrum)


def test_FrequencyDomainGIDDecon_rejects_dead_external_wavelet_and_noise(tmp_path):
    from mspasspy.ccore.seismic import TimeSeries

    pf = _ns_gid_pf(
        tmp_path,
        "FrequencyDomainGIDDecon.pf",
        "frequency_domain_gid_deconvolution",
    )
    engine = FrequencyDomainGIDDecon(pf)
    dead_wavelet = TimeSeries(8)
    dead_wavelet.kill()
    dead_noise = TimeSeries(8)
    dead_noise.kill()

    with pytest.raises(MsPASSError, match="external wavelet is marked dead"):
        engine.loadwavelet(dead_wavelet)
    with pytest.raises(MsPASSError, match="external noise is marked dead"):
        engine.loadnoise(dead_noise)


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
    bad_window_result = FrequencyDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-2.0, 2.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )
    assert bad_window_result[0].dead()
    assert bad_window_result[1] is None
    assert bad_window_result[2] is None

    engine = FrequencyDomainGIDDecon(pf)
    rf = FrequencyDomainGIDRFDecon(data, engine, QCdata_key=None)
    _assert_valid_rf(rf)
    assert not rf.is_defined("FrequencyDomainGIDDecon_properties")
