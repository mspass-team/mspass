#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest
from pathlib import Path

from mspasspy.ccore.utility import Metadata, MsPASSError, pfread
from mspasspy.ccore.seismic import PowerSpectrum, Seismogram, TimeSeries
from mspasspy.ccore.seismic import TimeReferenceType, DoubleVector
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import NoiseStableDecon, TimeDomainGIDDecon
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


def _assert_actual_and_output_shaping_are_distinct(
    actual_output, output_shaping_wavelet
):
    actual = np.asarray(actual_output.data)
    shaping = np.asarray(output_shaping_wavelet.data)
    if actual.shape == shaping.shape:
        assert not np.allclose(actual, shaping, atol=1.0e-10)
    else:
        assert actual.size != shaping.size


def _assert_single_spike_recovery(rf, ratio_tolerance):
    zrf = ExtractComponent(rf, 2)
    peak_sample = int(np.argmax(np.abs(zrf.data)))
    assert np.isclose(
        rf.data[0, peak_sample] / rf.data[2, peak_sample],
        0.2,
        atol=ratio_tolerance,
    )
    assert np.isclose(
        rf.data[1, peak_sample] / rf.data[2, peak_sample],
        -0.1,
        atol=ratio_tolerance,
    )


def _make_single_spike_convolution_data():
    n = 1400
    dt = 0.05
    t0 = -45.0
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


def _make_external_wavelet_3c_data(noise_level=1.0e-4):
    n = 1400
    dt = 0.05
    t0 = -45.0
    w = TimeSeries(101)
    w.set_t0(-2.5)
    w.set_dt(dt)
    w.set_live()
    x = np.arange(w.npts) * dt + w.t0
    wv = np.exp(-0.5 * (x / 0.16) ** 2) - 0.35 * np.exp(
        -0.5 * ((x - 0.35) / 0.22) ** 2
    )
    wv /= np.max(np.abs(wv))
    for i, v in enumerate(wv):
        w.data[i] = v

    model = np.zeros((3, n))
    spike_times = [0.0, 3.0, 8.0, 18.0]
    amps = np.array(
        [[0.45, -0.25, 0.18, 0.12], [-0.2, 0.15, -0.12, 0.05], [0.0, 0.0, 0.0, 0.0]]
    )
    for it, t in enumerate(spike_times):
        isamp = int(round((t - t0) / dt))
        model[:, isamp] = amps[:, it]
    data = Seismogram(n)
    data.set_t0(t0)
    data.set_dt(dt)
    data.set_live()
    data.tref = TimeReferenceType.Relative
    rng = np.random.default_rng(421)
    for k in range(3):
        y = np.convolve(model[k], np.array(w.data), mode="same")
        y += noise_level * rng.standard_normal(n)
        data.data[k, :] = DoubleVector(y)
    return data, w, spike_times


def _ns_gid_pf(tmp_path, pf_name, branch_name, gain_max=30.0, peak_sigma=3.0):
    src = Path("./data/pf") / pf_name
    text = src.read_text()
    text = text.replace(
        f"{branch_name} &Arr{{\n        deconvolution_type least_square",
        f"{branch_name} &Arr{{\n        deconvolution_type ns_gid",
    )
    text = text.replace("ns_gid_gain_max 1.0e3", f"ns_gid_gain_max {gain_max}")
    text = text.replace(
        "ns_gid_peak_sigma_threshold 4.0",
        f"ns_gid_peak_sigma_threshold {peak_sigma}",
    )
    text = text.replace(
        "ns_gid_peak_probability_threshold 0.995",
        "ns_gid_peak_probability_threshold 0.999",
    )
    dst = tmp_path / pf_name
    dst.write_text(text)
    return pfread(str(dst))


def test_NoiseStableDecon_enforces_gain_cap_on_notched_wavelet():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    md = pf.get_branch("deconvolution_operator_type").get_branch("ns_gid")
    md["ns_gid_gain_max"] = 12.5
    op = NoiseStableDecon(md)
    n = 501
    t = np.arange(n) * 0.05
    wavelet = np.sin(2.0 * np.pi * 0.55 * t) - np.sin(2.0 * np.pi * 0.56 * t)
    data = np.zeros(n)
    data[250] = 1.0
    op.loadwavelet(DoubleVector(wavelet))
    op.loaddata(DoubleVector(data))
    op.loadnoise(DoubleVector(0.02 * np.ones(n)))
    op.process()
    qc = dict(op.QCMetrics())
    assert qc["ns_gid_gain_max_actual"] <= 12.5 * (1.0 + 1.0e-10)
    assert qc["ns_gid_operator_nfft"] >= 2 * n - 1


def test_TimeDomainNSGID_uses_external_wavelet_and_rejects_noise_spikes(tmp_path):
    data, wavelet, spike_times = _make_external_wavelet_3c_data(noise_level=2.0e-4)
    pf = _ns_gid_pf(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution"
    )
    engine = TimeDomainGIDDecon(pf)
    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
        external_wavelet=wavelet,
    )
    _assert_valid_rf(rf)
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["ns_gid_enabled"]
    assert qc["ns_gid_external_wavelet_used"]
    assert qc["ns_gid_gain_max_actual"] <= qc["ns_gid_gain_max_requested"] * (
        1.0 + 1.0e-10
    )
    assert qc["ns_gid_number_spikes"] <= 2 * len(spike_times)
    assert qc["ns_gid_stop_reason"] in {
        "candidate_not_significant",
        "fractional_improvement_floor",
        "residual_reached_noise_floor",
        "converged",
    }
    support = np.where(np.linalg.norm(np.asarray(rf.data), axis=0) > 1.0e-8)[0]
    picked_times = [rf.time(int(i)) for i in support]
    assert picked_times
    expected_times = [t - wavelet.t0 for t in spike_times[:2]]
    for t in expected_times:
        assert min(abs(t - p) for p in picked_times) < 0.15


def test_TimeDomainGIDRFDecon_clears_external_state_between_calls(tmp_path):
    data = _make_gid_test_data(noise_level=1.0e-4)
    external_wavelet = make_simulation_wavelet()
    pf = _ns_gid_pf(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution"
    )
    engine = TimeDomainGIDDecon(pf)

    rf_external = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
        external_wavelet=external_wavelet,
    )
    assert rf_external.live
    assert rf_external["TimeDomainGIDDecon_properties"][
        "ns_gid_external_wavelet_used"
    ]

    rf_internal = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
    )
    assert rf_internal.live
    assert not rf_internal["TimeDomainGIDDecon_properties"][
        "ns_gid_external_wavelet_used"
    ]


def test_TimeDomainGIDDecon_rejects_empty_external_noise(tmp_path):
    pf = _ns_gid_pf(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution"
    )
    engine = TimeDomainGIDDecon(pf)
    empty_noise = TimeSeries(0)
    empty_noise.set_live()

    with pytest.raises(MsPASSError, match="external noise is empty"):
        engine.loadnoise(empty_noise)


def test_TimeDomainGIDDecon_rejects_invalid_external_noise_spectrum(tmp_path):
    pf = _ns_gid_pf(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution"
    )
    engine = TimeDomainGIDDecon(pf)
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
    missing_dc_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0]), 1.0, "missing_dc", 1.0, 1.0, 2
    )
    below_dc_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0]), 1.0, "below_dc", -10.0, 1.0, 2
    )
    dc_at_last_bin_spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0]), 1.0, "dc_at_last", -1.0, 1.0, 2
    )

    with pytest.raises(MsPASSError, match="PowerSpectrum is marked dead"):
        engine.loadnoise(dead_spectrum)
    with pytest.raises(MsPASSError, match="at least two frequency bins"):
        engine.loadnoise(live_empty_spectrum)
    with pytest.raises(MsPASSError, match="at least two frequency bins"):
        engine.loadnoise(one_bin_spectrum)
    with pytest.raises(MsPASSError, match="cover DC"):
        engine.loadnoise(missing_dc_spectrum)
    with pytest.raises(MsPASSError, match="cover DC"):
        engine.loadnoise(below_dc_spectrum)
    with pytest.raises(MsPASSError, match="cover DC"):
        engine.loadnoise(dc_at_last_bin_spectrum)


@pytest.mark.parametrize("mode", ["multi_taper", "least_square", "water_level", "cnr"])
def test_TimeDomainGIDDecon_rejects_non_ns_power_spectrum_noise(tmp_path, mode):
    pf = _pf_with_mode(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        mode,
    )
    engine = TimeDomainGIDDecon(pf)
    spectrum = PowerSpectrum(
        Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
    )

    with pytest.raises(MsPASSError, match="only supported for ns_gid"):
        engine.loadnoise(spectrum)


def test_TimeDomainGIDDecon_rejects_dead_external_wavelet_and_noise(tmp_path):
    pf = _ns_gid_pf(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution"
    )
    engine = TimeDomainGIDDecon(pf)
    dead_wavelet = TimeSeries(8)
    dead_wavelet.kill()
    dead_noise = TimeSeries(8)
    dead_noise.kill()

    with pytest.raises(MsPASSError, match="external wavelet is marked dead"):
        engine.loadwavelet(dead_wavelet)
    with pytest.raises(MsPASSError, match="external noise is marked dead"):
        engine.loadnoise(dead_noise)


def test_TimeDomainGIDDecon_rejects_external_timeseries_dt_mismatch(tmp_path):
    pf = _ns_gid_pf(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution"
    )
    engine = TimeDomainGIDDecon(pf)
    wavelet = TimeSeries(8)
    wavelet.set_live()
    wavelet.set_dt(0.2)
    noise = TimeSeries(8)
    noise.set_live()
    noise.set_dt(0.2)

    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.loadwavelet(wavelet)
    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.loadnoise(noise)


def test_TimeDomainNSGID_rejects_pure_noise(tmp_path):
    data, wavelet, _ = _make_external_wavelet_3c_data(noise_level=0.0)
    rng = np.random.default_rng(314)
    data.data[:, :] = 0.002 * rng.standard_normal((3, data.npts))
    pf = _ns_gid_pf(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        peak_sigma=8.0,
    )
    engine = TimeDomainGIDDecon(pf)
    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
        external_wavelet=wavelet,
    )
    assert rf.live
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["ns_gid_enabled"]
    assert qc["ns_gid_number_spikes"] <= 2
    assert qc["ns_gid_stop_reason"] in {
        "candidate_not_significant",
        "fractional_improvement_floor",
        "residual_reached_noise_floor",
        "residual_linf_floor",
    }


def test_TimeDomainGIDDecon_binding_and_wrapper():
    np.random.seed(13)
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    signal_window = TimeWindow(-8.0, 20.0)
    noise_window = TimeWindow(-25.0, -8.0)

    rf, actual_output, output_shaping_wavelet = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=signal_window,
        noise_window=noise_window,
        return_wavelet=True,
    )

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    assert rf.is_defined("TimeDomainGIDDecon_properties")
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0

    # This broad multi-arrival synthetic is a wrapper smoke test.  Strict
    # vector-ratio validation is done below with the isolated single-spike
    # synthetic, where the peak sample is mathematically unambiguous.
    zrf = ExtractComponent(rf, 2)
    peak_sample = int(np.argmax(np.abs(zrf.data)))
    assert signal_window.start <= zrf.time(peak_sample) <= signal_window.end
    assert np.isclose(
        rf.data[0, peak_sample] / rf.data[2, peak_sample], -0.1, atol=2.0e-3
    )
    assert np.isclose(
        rf.data[1, peak_sample] / rf.data[2, peak_sample],
        10.0 / 150.0,
        atol=1.0e-2,
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
        noise_window=TimeWindow(-35.0, -5.0),
    )

    _assert_valid_rf(rf)
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_Linf_final"] < qc["residual_Linf_initial"]
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]

    _assert_single_spike_recovery(rf, ratio_tolerance=2.0e-3)


@pytest.mark.parametrize("mode", ["least_square", "water_level", "multi_taper", "cnr"])
def test_TimeDomainGIDDecon_inverse_modes_are_valid(tmp_path, mode):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution", mode
    )
    engine = TimeDomainGIDDecon(pf)

    rf, actual_output, output_shaping_wavelet = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
        return_wavelet=True,
    )

    _assert_valid_rf(rf)
    assert actual_output.live
    assert output_shaping_wavelet.live
    _assert_actual_and_output_shaping_are_distinct(
        actual_output, output_shaping_wavelet
    )
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] > 0
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]
    _assert_single_spike_recovery(rf, ratio_tolerance=5.0e-2)


def test_TimeDomainGIDDecon_changeparameter_handles_cnr_mode(tmp_path):
    pf = _pf_with_mode(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution", "cnr"
    )
    engine = TimeDomainGIDDecon(pf)
    cnr_md = pf.get_branch("deconvolution_operator_type").get_branch("cnr")
    cnr_md["sample_shift"] = 100

    engine.changeparameter(cnr_md)


def test_TimeDomainGIDDecon_changeparameter_rejects_leaf_window_drift(tmp_path):
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["deconvolution_data_window_start"] = (
        leaf_md.get_double("deconvolution_data_window_start") + 1.0
    )

    with pytest.raises(MsPASSError, match="does not match"):
        engine.changeparameter(leaf_md)


def test_TimeDomainGIDDecon_changeparameter_rejects_leaf_dt_drift():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["target_sample_interval"] = 0.1

    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.changeparameter(leaf_md)


def test_TimeDomainGIDDecon_changeparameter_rejects_gid_level_metadata():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    gid_md = pf.get_branch("deconvolution_operator_type").get_branch(
        "time_domain_gid_deconvolution"
    )

    with pytest.raises(MsPASSError, match="GID-level"):
        engine.changeparameter(gid_md)


@pytest.mark.parametrize(
    "key,value",
    [
        ("residual_fractional_improvement_floor", 1.0e-3),
        ("ns_gid_refit_interval", 2),
    ],
)
def test_TimeDomainGIDDecon_changeparameter_rejects_gid_keys_on_leaf(key, value):
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md[key] = value

    with pytest.raises(MsPASSError, match=key):
        engine.changeparameter(leaf_md)


def test_TimeDomainGIDDecon_accepts_legacy_cnr3c_mode_alias(tmp_path):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution", "cnr3c"
    )
    engine = TimeDomainGIDDecon(pf)

    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )

    _assert_valid_rf(rf)


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
