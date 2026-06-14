#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pickle
import pytest
import cloudpickle
from pathlib import Path

from mspasspy.ccore.utility import ErrorSeverity, Metadata, MsPASSError, pfread
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

DEFAULT_GID_DECONVOLUTION_TYPE = "ns_gid"


def _replace_gid_deconvolution_type(text, branch_name, mode):
    old = (
        f"{branch_name} &Arr{{\n        "
        f"deconvolution_type {DEFAULT_GID_DECONVOLUTION_TYPE}"
    )
    new = f"{branch_name} &Arr{{\n        deconvolution_type {mode}"
    assert text.count(old) == 1
    return text.replace(old, new, 1)


def _distributed_roundtrip(obj):
    distributed_protocol = pytest.importorskip("distributed.protocol")
    header, frames = distributed_protocol.serialize(obj)
    return distributed_protocol.deserialize(header, frames)


def _make_gid_test_data(noise_level=0.02):
    wavelet = make_simulation_wavelet()
    impulses = make_impulse_data()
    data = convolve_wavelet(impulses, wavelet)
    if noise_level is None:
        noise_level = 0.0
    return addnoise(data, nscale=noise_level, padlength=800)


def _make_external_noise(npts=300, dt=0.05, t0=-35.0, scale=1.0):
    noise = TimeSeries(npts)
    noise.set_t0(t0)
    noise.set_dt(dt)
    noise.set_live()
    for i in range(noise.npts):
        noise.data[i] = scale * (0.01 * np.sin(0.17 * i) + 0.004 * np.cos(0.07 * i))
    return noise


def _assert_valid_rf(rf):
    assert rf.live
    assert rf.npts > 0
    assert np.isfinite(rf.data).all()
    assert np.linalg.norm(rf.data) > 0.0


def _assert_single_penalty_footprint(lag_weights, effective_width):
    valid = lag_weights > 0.0
    changed = np.flatnonzero(valid & (lag_weights < 1.0 - 1.0e-10))
    assert changed.size == effective_width
    assert changed.size > 1
    assert np.all(np.diff(changed) == 1)
    footprint = lag_weights[changed]
    center = changed.size // 2
    assert np.argmin(footprint) == center
    assert footprint[center] == pytest.approx(float(lag_weights[valid].min()))
    assert footprint[center] < footprint[0]
    assert np.allclose(footprint, footprint[::-1], atol=1.0e-10)


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


def _assert_group_sparse_qc(qc):
    assert qc["group_sparse_enabled"]
    assert "ns_gid_enabled" not in dict(qc)
    assert qc["group_sparse_inverse_operator"] == "ns_gid"
    assert qc["group_sparse_lambda_requested"] == pytest.approx(0.0)
    assert qc["group_sparse_lambda_scale"] == pytest.approx(1.0)
    assert qc["group_sparse_lambda_used"] >= 0.0
    assert qc["group_sparse_noise_threshold"] > 0.0
    assert qc["group_sparse_lambda_used"] == pytest.approx(
        qc["group_sparse_lambda_scale"] * qc["group_sparse_noise_threshold"]
    )
    assert qc["group_sparse_active_threshold"] == pytest.approx(0.02)
    assert qc["group_sparse_active_threshold_scale"] == pytest.approx(1.0)
    assert qc["group_sparse_active_threshold_quantile"] == pytest.approx(0.90)
    assert qc["group_sparse_active_threshold_quantile_value"] >= 0.0
    assert (
        qc["group_sparse_active_threshold_used"] >= qc["group_sparse_active_threshold"]
    )
    assert qc["group_sparse_iterations"] == qc["iteration_count"]
    assert qc["group_sparse_active_groups"] == qc["gid_number_spikes"]
    assert qc["group_sparse_objective_final"] <= qc["group_sparse_objective_initial"]
    assert qc["group_sparse_fractional_improvement_final"] >= 0.0
    assert qc["group_sparse_inverse_operator_nfft"] > 0
    assert qc["group_sparse_inverse_gain_max_actual"] <= (
        qc["group_sparse_inverse_gain_max_requested"] * (1.0 + 1.0e-10)
    )
    assert qc["group_sparse_inverse_noise_amplification"] >= 0.0
    assert 0.0 <= qc["group_sparse_inverse_effective_bandwidth_fraction"] <= 1.0
    assert not qc["group_sparse_inverse_external_wavelet_used"]
    assert not qc["group_sparse_inverse_external_noise_used"]
    assert not qc["group_sparse_inverse_external_noise_spectrum_used"]
    assert not any(key.startswith("gid_penalty") for key in dict(qc))
    assert not any(key.startswith("lag_weight_penalty") for key in dict(qc))
    assert qc["gid_stop_reason"] in {
        "group_sparse_converged",
        "group_sparse_max_iterations",
    }


def _assert_group_sparse_disabled_qc(qc):
    keys = set(dict(qc).keys())
    assert not any(key.startswith("group_sparse") for key in keys)


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
    text = _replace_gid_deconvolution_type(text, branch_name, mode)
    dst = tmp_path / pf_name
    dst.write_text(text)
    return pfread(str(dst))


def _pf_with_short_decon_window(tmp_path, pf_name, branch_name=None, mode=None):
    src = Path("./data/pf") / pf_name
    text = src.read_text()
    if mode is not None:
        text = _replace_gid_deconvolution_type(text, branch_name, mode)
    text = text.replace(
        "deconvolution_data_window_start -5.0",
        "deconvolution_data_window_start -0.5",
    )
    text = text.replace(
        "deconvolution_data_window_end 20.0",
        "deconvolution_data_window_end 0.5",
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
    wv = np.exp(-0.5 * (x / 0.16) ** 2) - 0.35 * np.exp(-0.5 * ((x - 0.35) / 0.22) ** 2)
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
    text = _replace_gid_deconvolution_type(text, branch_name, "ns_gid")
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
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
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


def test_TimeDomainGIDRFDecon_preserves_engine_external_state_between_calls(tmp_path):
    data = _make_gid_test_data(noise_level=1.0e-4)
    external_wavelet = make_simulation_wavelet()
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
    engine = TimeDomainGIDDecon(pf)

    rf_external = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
        external_wavelet=external_wavelet,
    )
    assert rf_external.live
    assert rf_external["TimeDomainGIDDecon_properties"]["ns_gid_external_wavelet_used"]

    rf_internal = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
    )
    assert rf_internal.live
    assert rf_internal["TimeDomainGIDDecon_properties"]["ns_gid_external_wavelet_used"]


def test_TimeDomainGIDDecon_rejects_empty_external_noise(tmp_path):
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
    engine = TimeDomainGIDDecon(pf)
    empty_noise = TimeSeries(0)
    empty_noise.set_live()

    with pytest.raises(MsPASSError, match="external noise is empty"):
        engine.loadnoise(empty_noise)


def test_TimeDomainGIDDecon_rejects_invalid_external_noise_spectrum(tmp_path):
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
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
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
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
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
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
    assert qc["decon_operator"] == "TimeDomainGIDDecon"
    assert qc["decon_processed"]
    assert qc["gid_processed"]
    assert qc["gid_stop_reason"] in {
        "converged",
        "fractional_improvement_floor",
        "lag_weight_linf_floor",
        "lag_weight_l2_floor",
        "max_iterations",
        "no_acceptable_candidate",
        "residual_linf_floor",
    }
    assert qc["gid_penalty_function"] == "adaptive_memory"
    assert qc["gid_penalty_scale_factor"] == pytest.approx(0.35)
    assert qc["gid_penalty_width"] == 5
    assert qc["gid_penalty_effective_width"] >= 1
    assert qc["gid_adaptive_penalty_enabled"]
    assert qc["gid_penalty_last_immediate_strength"] == pytest.approx(
        qc["gid_penalty_last_confidence"]
    )
    assert qc["gid_penalty_last_decay_factor"] == pytest.approx(
        qc["gid_penalty_last_confidence"] * qc["gid_penalty_last_specificity"]
    )
    assert qc["iteration_count"] > 0
    assert qc["gid_actual_o_fir_npts"] > 0
    assert 0 <= qc["gid_actual_o_fir_zero_lag_index"] < qc["gid_actual_o_fir_npts"]
    assert qc["gid_actual_o_fir_peak_normalized"]
    assert qc["gid_actual_o_fir_npts"] <= actual_output.npts
    lag_weights = np.asarray(engine.lag_weight_vector())
    assert lag_weights.size > 0
    assert np.isfinite(lag_weights).all()
    assert 0.0 <= lag_weights.min() <= lag_weights.max() <= 1.0
    assert qc["lag_weight_Linf_final"] == pytest.approx(float(np.max(lag_weights)))
    assert qc["lag_weight_L2_final"] == pytest.approx(
        float(np.linalg.norm(lag_weights))
    )

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


def test_TimeDomainGIDDecon_penalty_reduces_multispike_residual(tmp_path):
    np.random.seed(13)
    data = _make_gid_test_data(noise_level=None)
    signal_window = TimeWindow(-8.0, 20.0)
    noise_window = TimeWindow(-25.0, -8.0)

    penalty_pf = tmp_path / "TimeDomainGIDDecon_penalty.pf"
    no_penalty_pf = tmp_path / "TimeDomainGIDDecon_no_penalty.pf"
    text = Path("./data/pf/TimeDomainGIDDecon.pf").read_text()
    text = _replace_gid_deconvolution_type(
        text, "time_domain_gid_deconvolution", "least_square"
    )
    penalty_pf.write_text(text)
    no_penalty_pf.write_text(
        text.replace(
            "lag_weight_penalty_function adaptive_memory",
            "lag_weight_penalty_function none",
        )
    )

    penalty_rf = TimeDomainGIDRFDecon(
        data,
        TimeDomainGIDDecon(pfread(str(penalty_pf))),
        signal_window=signal_window,
        noise_window=noise_window,
    )
    no_penalty_rf = TimeDomainGIDRFDecon(
        data,
        TimeDomainGIDDecon(pfread(str(no_penalty_pf))),
        signal_window=signal_window,
        noise_window=noise_window,
    )

    penalty_qc = penalty_rf["TimeDomainGIDDecon_properties"]
    no_penalty_qc = no_penalty_rf["TimeDomainGIDDecon_properties"]
    assert penalty_qc["gid_penalty_function"] == "adaptive_memory"
    assert penalty_qc["gid_penalty_effective_width"] >= 1
    assert no_penalty_qc["gid_penalty_function"] == "none"
    assert no_penalty_qc["gid_penalty_scale_factor"] == pytest.approx(0.35)
    assert no_penalty_qc["gid_penalty_width"] == 5
    assert no_penalty_qc["gid_penalty_effective_width"] == 1
    assert penalty_qc["residual_L2_final"] < no_penalty_qc["residual_L2_final"]
    assert penalty_qc["lag_weight_L2_final"] < no_penalty_qc["lag_weight_L2_final"]


@pytest.mark.parametrize(
    "penalty_function", ["shaping_wavelet", "resolution_kernel", "adaptive_memory"]
)
def test_TimeDomainGIDDecon_kernel_penalty_modes_are_adaptive(
    tmp_path, penalty_function
):
    data = _make_gid_test_data(noise_level=None)
    signal_window = TimeWindow(-8.0, 20.0)
    noise_window = TimeWindow(-25.0, -8.0)

    pf = tmp_path / f"TimeDomainGIDDecon_{penalty_function}.pf"
    text = Path("./data/pf/TimeDomainGIDDecon.pf").read_text()
    text = _replace_gid_deconvolution_type(
        text, "time_domain_gid_deconvolution", "least_square"
    )
    text = text.replace(
        "lag_weight_penalty_function adaptive_memory",
        f"lag_weight_penalty_function {penalty_function}",
    )
    text = text.replace("lag_weight_function_width 5", "lag_weight_function_width 101")
    text = text.replace("maximum_iterations 100", "maximum_iterations 1")
    pf.write_text(text)

    engine = TimeDomainGIDDecon(pfread(str(pf)))
    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=signal_window,
        noise_window=noise_window,
    )

    assert rf.live
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["gid_penalty_function"] == penalty_function
    assert qc["gid_penalty_width"] == 101
    assert qc["gid_penalty_effective_width"] != qc["gid_penalty_width"]
    assert qc["gid_adaptive_penalty_enabled"] == (penalty_function == "adaptive_memory")
    assert 0.0 <= qc["gid_penalty_last_confidence"] < 1.0
    assert 0.0 <= qc["gid_penalty_last_immediate_strength"] < 1.0
    assert 0.0 <= qc["gid_penalty_last_specificity"] <= 1.0
    assert 0.0 <= qc["gid_penalty_last_decay_factor"] < 1.0
    assert qc["gid_penalty_valid_lags"] > 0
    if penalty_function == "adaptive_memory":
        assert qc["gid_penalty_effective_width"] >= 1
        assert qc["gid_penalty_noise_amplitude"] > 0.0
        assert qc["gid_penalty_memory_Linf_final"] > 0.0
        assert qc["gid_penalty_memory_L2_final"] > 0.0
    else:
        assert qc["gid_penalty_effective_width"] > 1
        assert qc["gid_penalty_memory_Linf_final"] == pytest.approx(0.0)
        assert qc["gid_penalty_memory_L2_final"] == pytest.approx(0.0)
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]
    lag_weights = np.asarray(engine.lag_weight_vector())
    assert lag_weights.size > 0
    assert np.isfinite(lag_weights).all()
    assert 0.0 <= lag_weights.min() <= lag_weights.max() <= 1.0
    assert qc["lag_weight_L2_final"] == pytest.approx(
        float(np.linalg.norm(lag_weights))
    )
    _assert_single_penalty_footprint(lag_weights, qc["gid_penalty_effective_width"])


def test_TimeDomainGIDDecon_iteration_cap_returns_best_result(tmp_path):
    data = _make_gid_test_data(noise_level=None)
    capped_pf = tmp_path / "TimeDomainGIDDecon_capped.pf"
    text = Path("./data/pf/TimeDomainGIDDecon.pf").read_text()
    capped_pf.write_text(text.replace("maximum_iterations 100", "maximum_iterations 1"))
    engine = TimeDomainGIDDecon(pfread(str(capped_pf)))

    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
    )

    assert rf.live
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["iteration_count"] == 1
    assert qc["gid_iterations"] == 1
    assert qc["gid_stop_reason"] == "max_iterations"
    assert not qc["gid_converged"]
    assert qc["residual_L2_final"] < qc["residual_L2_initial"]


def test_TimeDomainGIDRFDecon_uses_custom_algorithm_name_in_qc():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)

    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
        alg_name="CustomTimeGID",
    )

    assert rf.live
    assert rf["TimeDomainGIDDecon_properties"]["algorithm"] == "CustomTimeGID"


def test_TimeDomainGIDDecon_engine_is_pickleable_for_wrapper_use():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    restored = pickle.loads(pickle.dumps(engine))

    rf1 = TimeDomainGIDRFDecon(
        Seismogram(data),
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    rf2 = TimeDomainGIDRFDecon(
        Seismogram(data),
        restored,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )

    assert rf1.live
    assert rf2.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf2.data))

    restored_after_use = pickle.loads(pickle.dumps(engine))
    rf3 = TimeDomainGIDRFDecon(
        Seismogram(data),
        restored_after_use,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert rf3.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf3.data))

    restored_cloudpickle = cloudpickle.loads(cloudpickle.dumps(engine))
    rf4 = TimeDomainGIDRFDecon(
        Seismogram(data),
        restored_cloudpickle,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert rf4.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf4.data))

    changed = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["damping_factor"] = 100.0
    changed.changeparameter(leaf_md)
    rf_changed = TimeDomainGIDRFDecon(
        Seismogram(data),
        changed,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    for restored_changed in (
        pickle.loads(pickle.dumps(changed)),
        cloudpickle.loads(cloudpickle.dumps(changed)),
    ):
        rf_restored = TimeDomainGIDRFDecon(
            Seismogram(data),
            restored_changed,
            signal_window=TimeWindow(-10.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
        )
        assert rf_restored.live
        assert np.allclose(np.asarray(rf_changed.data), np.asarray(rf_restored.data))


def test_TimeDomainGIDDecon_engine_is_dask_serializable_for_wrapper_use():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)

    rf1 = TimeDomainGIDRFDecon(
        Seismogram(data),
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    restored_dask = _distributed_roundtrip(engine)
    rf2 = TimeDomainGIDRFDecon(
        Seismogram(data),
        restored_dask,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )

    assert rf1.live
    assert rf2.live
    assert np.allclose(np.asarray(rf1.data), np.asarray(rf2.data))

    changed = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["damping_factor"] = 100.0
    changed.changeparameter(leaf_md)
    rf_changed = TimeDomainGIDRFDecon(
        Seismogram(data),
        changed,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    restored_changed = _distributed_roundtrip(changed)
    rf_restored = TimeDomainGIDRFDecon(
        Seismogram(data),
        restored_changed,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert rf_restored.live
    assert np.allclose(np.asarray(rf_changed.data), np.asarray(rf_restored.data))


def test_TimeDomainGIDDecon_pickle_preserves_external_wavelet_and_noise(tmp_path):
    data, wavelet, _ = _make_external_wavelet_3c_data(noise_level=2.0e-4)
    noise = _make_external_noise()
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
    engine = TimeDomainGIDDecon(pf)
    engine.loadwavelet(wavelet)
    engine.loadnoise(noise)
    restored = pickle.loads(pickle.dumps(engine))

    rf = TimeDomainGIDRFDecon(
        data,
        restored,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
    )

    assert rf.live
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["ns_gid_external_wavelet_used"]
    assert qc["ns_gid_external_noise_used"]

    spectrum_engine = TimeDomainGIDDecon(pf)
    spectrum_engine.loadwavelet(wavelet)
    spectrum_engine.loadnoise(
        PowerSpectrum(
            Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
        )
    )
    restored_spectrum = pickle.loads(pickle.dumps(spectrum_engine))
    rf_spectrum = TimeDomainGIDRFDecon(
        data,
        restored_spectrum,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
    )
    assert rf_spectrum.live
    qc_spectrum = rf_spectrum["TimeDomainGIDDecon_properties"]
    assert qc_spectrum["ns_gid_external_wavelet_used"]
    assert qc_spectrum["ns_gid_external_noise_spectrum_used"]


def test_TimeDomainGIDDecon_group_sparse_honors_external_noise_spectrum(tmp_path):
    data, wavelet, _ = _make_external_wavelet_3c_data(noise_level=2.0e-4)
    pf = _pf_with_mode(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        "group_sparse",
    )
    engine = TimeDomainGIDDecon(pf)
    engine.loadwavelet(wavelet)
    engine.loadnoise(
        PowerSpectrum(
            Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
        )
    )

    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 22.0),
        noise_window=TimeWindow(-35.0, -8.0),
    )

    assert rf.live
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["group_sparse_inverse_external_wavelet_used"]
    assert qc["group_sparse_inverse_external_noise_spectrum_used"]
    assert qc["group_sparse_noise_threshold"] > 0.0


def test_TimeDomainGIDDecon_clear_external_state_drops_pickle_payload(tmp_path):
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
    engine = TimeDomainGIDDecon(pf)
    baseline_size = len(pickle.dumps(engine))

    large_wavelet = _make_external_noise(npts=50000)
    engine.loadwavelet(large_wavelet)
    loaded_wavelet_size = len(pickle.dumps(engine))
    assert loaded_wavelet_size > baseline_size + 100000

    engine.clear_external_wavelet()
    cleared_wavelet_size = len(pickle.dumps(engine))
    assert cleared_wavelet_size < baseline_size * 2

    large_noise = _make_external_noise(npts=50000)
    engine.loadnoise(large_noise)
    loaded_noise_size = len(pickle.dumps(engine))
    assert loaded_noise_size > baseline_size + 100000

    engine.clear_external_noise()
    cleared_noise_size = len(pickle.dumps(engine))
    assert cleared_noise_size < baseline_size * 2


def test_TimeDomainGIDDecon_switching_external_noise_drops_inactive_payload(
    tmp_path,
):
    pf = _ns_gid_pf(tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution")
    engine = TimeDomainGIDDecon(pf)
    baseline_size = len(pickle.dumps(engine))

    engine.loadnoise(_make_external_noise(npts=50000))
    loaded_timeseries_size = len(pickle.dumps(engine))
    assert loaded_timeseries_size > baseline_size + 100000

    engine.loadnoise(
        PowerSpectrum(
            Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
        )
    )
    spectrum_size = len(pickle.dumps(engine))
    assert spectrum_size < baseline_size * 2


def test_TimeDomainGIDDecon_powerspectrum_noise_still_requires_residual_noise(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
    )
    engine = TimeDomainGIDDecon(pf)
    engine.loadnoise(
        PowerSpectrum(
            Metadata(), DoubleVector([1.0, 1.0, 1.0]), 1.0, "valid", -1.0, 1.0, 3
        )
    )
    assert engine.load(data, TimeWindow(-10.0, 20.0)) == 0
    with pytest.raises(MsPASSError, match="valid noise window has not been loaded"):
        engine.process()


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


def test_TimeDomainGIDDecon_repeated_process_preserves_loaded_noise():
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    assert engine.load(data, TimeWindow(-8.0, 20.0), TimeWindow(-25.0, -8.0)) == 0

    engine.process()
    first_result = np.asarray(engine.getresult().data, dtype=np.float64).copy()
    first_lag_weights = np.asarray(engine.lag_weight_vector(), dtype=np.float64).copy()
    first_qc = dict(engine.QCMetrics())

    engine.process()
    second_result = np.asarray(engine.getresult().data, dtype=np.float64)
    second_lag_weights = np.asarray(engine.lag_weight_vector(), dtype=np.float64)
    second_qc = dict(engine.QCMetrics())

    assert np.allclose(second_result, first_result)
    assert np.allclose(second_lag_weights, first_lag_weights)
    assert second_qc["gid_stop_reason"] == first_qc["gid_stop_reason"]
    for key in (
        "iteration_count",
        "gid_penalty_noise_amplitude",
        "residual_L2_final",
        "lag_weight_L2_final",
        "gid_penalty_last_confidence",
        "gid_penalty_memory_L2_final",
    ):
        assert second_qc[key] == pytest.approx(first_qc[key])


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


@pytest.mark.parametrize(
    "mode", ["least_square", "water_level", "multi_taper", "cnr", "group_sparse"]
)
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
    if mode == "group_sparse":
        _assert_group_sparse_qc(qc)
    else:
        _assert_group_sparse_disabled_qc(qc)
    _assert_single_spike_recovery(rf, ratio_tolerance=5.0e-2)


def test_TimeDomainGIDDecon_cnr_honors_external_noise(tmp_path):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution", "cnr"
    )

    outputs = []
    for scale in (0.1, 100.0):
        engine = TimeDomainGIDDecon(pf)
        rf = TimeDomainGIDRFDecon(
            Seismogram(data),
            engine,
            signal_window=TimeWindow(-10.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
            external_noise=_make_external_noise(npts=600, scale=scale),
        )
        _assert_valid_rf(rf)
        outputs.append(np.asarray(rf.data, dtype=np.float64))

    difference_norm = np.linalg.norm(outputs[0] - outputs[1])
    assert difference_norm / np.linalg.norm(outputs[0]) > 1.0e-3


def test_TimeDomainGIDDecon_short_kernel_crop_is_bounded(tmp_path):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_short_decon_window(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        "least_square",
    )
    engine = TimeDomainGIDDecon(pf)

    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-1.5, 1.5),
        noise_window=TimeWindow(-35.0, -5.0),
    )

    _assert_valid_rf(rf)
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["gid_actual_o_fir_npts"] <= rf.npts


def test_TimeDomainGIDDecon_changeparameter_handles_cnr_mode(tmp_path):
    pf = _pf_with_mode(
        tmp_path, "TimeDomainGIDDecon.pf", "time_domain_gid_deconvolution", "cnr"
    )
    engine = TimeDomainGIDDecon(pf)
    cnr_md = pf.get_branch("deconvolution_operator_type").get_branch("cnr")

    engine.changeparameter(cnr_md)


def test_TimeDomainGIDDecon_preprocess_outputs_are_guarded():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    qc = dict(engine.QCMetrics())

    assert qc["gid_processed"] is False
    assert qc["iteration_count"] == 0
    assert qc["residual_Linf_final"] == 0.0
    assert qc["residual_L2_final"] == 0.0
    for method in (
        engine.getresult,
        engine.sparse_output,
        engine.actual_output,
        engine.inverse_wavelet,
    ):
        with pytest.raises(MsPASSError, match="process must be called first"):
            method()


def test_TimeDomainGIDDecon_changeparameter_invalidates_outer_state():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    engine.changeparameter(leaf_md)

    qc = dict(engine.QCMetrics())
    assert qc["gid_processed"] is False
    assert qc["iteration_count"] == 0
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.actual_output()


def test_TimeDomainGIDDecon_failed_changeparameter_invalidates_without_poisoning_leaf(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        "least_square",
    )
    engine = TimeDomainGIDDecon(pf)
    TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["damping_factor"] = -1.0
    with pytest.raises(MsPASSError, match="damping_factor"):
        engine.changeparameter(leaf_md)

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    engine.process()
    assert dict(engine.QCMetrics())["gid_processed"] is True


def test_TimeDomainGIDDecon_load_invalidates_outer_state():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    assert engine.load(data, TimeWindow(-10.0, 20.0)) == 0

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()


def test_TimeDomainGIDDecon_failed_load_invalidates_and_clears_old_data():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    assert engine.load(data, TimeWindow(30.0, 40.0)) == 1

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    with pytest.raises(MsPASSError, match="valid data window"):
        engine.process()


def test_TimeDomainGIDDecon_failed_combined_load_invalidates_old_output():
    data = _make_single_spike_convolution_data()
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-10.0, 20.0),
        noise_window=TimeWindow(-35.0, -5.0),
    )
    assert dict(engine.QCMetrics())["gid_processed"] is True

    assert engine.load(data, TimeWindow(30.0, 40.0), TimeWindow(-35.0, -5.0)) == 1

    assert dict(engine.QCMetrics())["gid_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.getresult()
    with pytest.raises(MsPASSError, match="valid data window"):
        engine.process()


def test_TimeDomainGIDDecon_combined_load_preserves_external_noise_until_clear(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        gain_max=20.0,
    )
    dwin = TimeWindow(-10.0, 20.0)
    nwin = TimeWindow(-35.0, -5.0)
    external_noise = TimeSeries(300)
    external_noise.set_t0(-35.0)
    external_noise.set_dt(0.05)
    external_noise.set_live()
    for i in range(external_noise.npts):
        external_noise.data[i] = 100.0 * np.sin(0.11 * i)

    windowed_engine = TimeDomainGIDDecon(pf)
    assert windowed_engine.load(data, dwin, nwin) == 0
    windowed_engine.process()

    switched_engine = TimeDomainGIDDecon(pf)
    switched_engine.loadnoise(external_noise)
    assert switched_engine.load(data, dwin, nwin) == 0
    switched_engine.process()
    switched_qc = dict(switched_engine.QCMetrics())
    assert switched_qc["ns_gid_external_noise_used"]

    switched_engine.clear_external_noise()
    assert switched_engine.load(data, dwin, nwin) == 0
    switched_engine.process()

    windowed_qc = dict(windowed_engine.QCMetrics())
    switched_qc = dict(switched_engine.QCMetrics())
    assert not switched_qc["ns_gid_external_noise_used"]
    assert np.isclose(
        switched_qc["ns_gid_noise_amplification"],
        windowed_qc["ns_gid_noise_amplification"],
    )
    assert np.isclose(
        switched_qc["ns_gid_gain_max_actual"],
        windowed_qc["ns_gid_gain_max_actual"],
    )
    assert np.allclose(
        np.asarray(switched_engine.getresult().data),
        np.asarray(windowed_engine.getresult().data),
    )


def test_TimeDomainGIDDecon_replacing_external_noise_refreshes_threshold(tmp_path):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
    )
    dwin = TimeWindow(-10.0, 20.0)
    low_noise = _make_external_noise(scale=1.0)
    high_noise = _make_external_noise(scale=1000.0)

    engine = TimeDomainGIDDecon(pf)
    engine.loadnoise(low_noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    low_threshold = dict(engine.QCMetrics())["ns_gid_peak_threshold"]

    engine.loadnoise(high_noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    high_threshold = dict(engine.QCMetrics())["ns_gid_peak_threshold"]

    assert high_threshold > 10.0 * low_threshold


def test_TimeDomainGIDDecon_group_sparse_external_noise_sets_lambda(tmp_path):
    data = _make_single_spike_convolution_data()
    pf = _pf_with_mode(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
        "group_sparse",
    )
    dwin = TimeWindow(-10.0, 20.0)
    low_noise = _make_external_noise(scale=1.0)
    high_noise = _make_external_noise(scale=1000.0)

    engine = TimeDomainGIDDecon(pf)
    engine.loadnoise(low_noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    low_qc = dict(engine.QCMetrics())

    engine.loadnoise(high_noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    high_qc = dict(engine.QCMetrics())

    assert low_qc["group_sparse_inverse_external_noise_used"]
    assert high_qc["group_sparse_inverse_external_noise_used"]
    assert high_qc["group_sparse_noise_threshold"] > (
        10.0 * low_qc["group_sparse_noise_threshold"]
    )
    assert high_qc["group_sparse_lambda_used"] > (
        10.0 * low_qc["group_sparse_lambda_used"]
    )


def test_TimeDomainGIDDecon_failed_external_noise_replacement_preserves_state(
    tmp_path,
):
    data = _make_single_spike_convolution_data()
    pf = _ns_gid_pf(
        tmp_path,
        "TimeDomainGIDDecon.pf",
        "time_domain_gid_deconvolution",
    )
    dwin = TimeWindow(-10.0, 20.0)
    noise = _make_external_noise(scale=2.0)

    engine = TimeDomainGIDDecon(pf)
    engine.loadnoise(noise)
    assert engine.load(data, dwin) == 0
    engine.process()
    original_qc = dict(engine.QCMetrics())

    bad_noise = TimeSeries(noise)
    bad_noise.set_dt(noise.dt * 2.0)
    with pytest.raises(MsPASSError, match="target_sample_interval"):
        engine.loadnoise(bad_noise)

    assert engine.load(data, dwin) == 0
    engine.process()
    recovered_qc = dict(engine.QCMetrics())
    assert recovered_qc["ns_gid_external_noise_used"]
    assert np.isclose(
        recovered_qc["ns_gid_peak_threshold"],
        original_qc["ns_gid_peak_threshold"],
    )


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


def test_TimeDomainGIDDecon_changeparameter_rejects_leaf_shaping_dt_drift():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md["shaping_wavelet_dt"] = 0.1

    with pytest.raises(MsPASSError, match="shaping_wavelet_dt"):
        engine.changeparameter(leaf_md)


@pytest.mark.parametrize(
    "old,new,match",
    [
        (
            "residual_noise_rms_probability_floor 0.95",
            "residual_noise_rms_probability_floor -0.5",
            "residual_noise_rms_probability_floor",
        ),
        (
            "ns_gid_peak_probability_threshold 0.995",
            "ns_gid_peak_probability_threshold 2.0",
            "ns_gid_peak_probability_threshold",
        ),
        (
            "lag_weight_function_width 5",
            "lag_weight_function_width -3",
            "lag_weight_function_width",
        ),
        (
            "lag_weight_penalty_scale_factor 0.35",
            "lag_weight_penalty_scale_factor 0.0",
            "lag_weight_penalty_scale_factor",
        ),
        (
            "lag_weight_penalty_function adaptive_memory",
            "lag_weight_penalty_function invalid_penalty",
            "lag_weight_penalty_function",
        ),
        (
            "group_sparse_lambda 0.0",
            "group_sparse_lambda -1.0",
            "group_sparse_lambda",
        ),
        (
            "group_sparse_lambda_scale 1.0",
            "group_sparse_lambda_scale -1.0",
            "group_sparse_lambda_scale",
        ),
        (
            "group_sparse_tolerance 1.0e-4",
            "group_sparse_tolerance 0.0",
            "group_sparse_tolerance",
        ),
        (
            "group_sparse_max_iterations 200",
            "group_sparse_max_iterations 0",
            "group_sparse_max_iterations",
        ),
        (
            "group_sparse_active_threshold 0.02",
            "group_sparse_active_threshold -0.01",
            "group_sparse_active_threshold",
        ),
        (
            "group_sparse_active_threshold_scale 1.0",
            "group_sparse_active_threshold_scale -0.01",
            "group_sparse_active_threshold_scale",
        ),
        (
            "group_sparse_active_threshold_quantile 0.90",
            "group_sparse_active_threshold_quantile 1.5",
            "group_sparse_active_threshold_quantile",
        ),
        (
            "maximum_iterations 100",
            "maximum_iterations 0",
            "maximum_iterations",
        ),
        (
            "maximum_iterations 100",
            "maximum_iterations -1",
            "maximum_iterations",
        ),
        (
            "noise_component 2",
            "noise_component 3",
            "noise_component",
        ),
        (
            "noise_component 2",
            "noise_component -1",
            "noise_component",
        ),
        (
            "full_data_window_start -8.0\n        full_data_window_end 20.0",
            "full_data_window_start 20.0\n        full_data_window_end -8.0",
            "full_data_window",
        ),
        (
            "deconvolution_data_window_start -5.0\n        deconvolution_data_window_end 20.0",
            "deconvolution_data_window_start 10.0\n        deconvolution_data_window_end 0.0",
            "deconvolution_data_window",
        ),
        (
            "noise_window_start -35.0\n        noise_window_end -5.0",
            "noise_window_start 0.0\n        noise_window_end -10.0",
            "noise_window",
        ),
    ],
)
def test_TimeDomainGIDDecon_rejects_invalid_probability_and_lag_parameters(
    tmp_path, old, new, match
):
    text = open("data/pf/TimeDomainGIDDecon.pf", encoding="utf-8").read()
    pf = tmp_path / "TimeDomainGIDDecon.pf"
    pf.write_text(text.replace(old, new))

    with pytest.raises(MsPASSError, match=match) as excinfo:
        TimeDomainGIDDecon(pfread(str(pf)))
    assert excinfo.value.severity == ErrorSeverity.Fatal


def test_TimeDomainGIDDecon_defaults_missing_lag_penalty_function_to_none(
    tmp_path,
):
    text = open("data/pf/TimeDomainGIDDecon.pf", encoding="utf-8").read()
    text = text.replace("        lag_weight_penalty_function adaptive_memory\n", "")
    pf = tmp_path / "TimeDomainGIDDecon_legacy_no_penalty_function.pf"
    pf.write_text(text)

    data = _make_gid_test_data(noise_level=None)
    engine = TimeDomainGIDDecon(pfread(str(pf)))
    rf = TimeDomainGIDRFDecon(
        data,
        engine,
        signal_window=TimeWindow(-8.0, 20.0),
        noise_window=TimeWindow(-25.0, -8.0),
    )

    _assert_valid_rf(rf)
    qc = rf["TimeDomainGIDDecon_properties"]
    assert qc["gid_penalty_function"] == "none"
    assert qc["gid_penalty_effective_width"] == 1


def test_TimeDomainGIDDecon_reports_missing_fixed_penalty_width(tmp_path):
    text = open("data/pf/TimeDomainGIDDecon.pf", encoding="utf-8").read()
    text = text.replace(
        "lag_weight_penalty_function adaptive_memory",
        "lag_weight_penalty_function cosine_taper",
    )
    text = text.replace("        lag_weight_function_width 5\n", "")
    pf = tmp_path / "TimeDomainGIDDecon_missing_penalty_width.pf"
    pf.write_text(text)

    with pytest.raises(MsPASSError, match="lag_weight_function_width") as excinfo:
        TimeDomainGIDDecon(pfread(str(pf)))
    assert excinfo.value.severity == ErrorSeverity.Fatal


def test_TimeDomainGIDDecon_changeparameter_rejects_gid_level_metadata():
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    gid_md = pf.get_branch("deconvolution_operator_type").get_branch(
        "time_domain_gid_deconvolution"
    )

    with pytest.raises(MsPASSError, match="GID-level") as excinfo:
        engine.changeparameter(gid_md)
    assert excinfo.value.severity == ErrorSeverity.Fatal


@pytest.mark.parametrize(
    "key,value",
    [
        ("residual_fractional_improvement_floor", 1.0e-3),
        ("ns_gid_refit_interval", 2),
        ("lag_weight_penalty_scale_factor", 0.5),
        ("lag_weight_function_width", 5),
        ("group_sparse_lambda_scale", 1.0),
        ("noise_window_start", -30.0),
        ("noise_window_end", -3.0),
    ],
)
def test_TimeDomainGIDDecon_changeparameter_rejects_gid_keys_on_leaf(key, value):
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)
    leaf_md = pf.get_branch("deconvolution_operator_type").get_branch("least_square")
    leaf_md[key] = value

    with pytest.raises(MsPASSError, match=key) as excinfo:
        engine.changeparameter(leaf_md)
    assert excinfo.value.severity == ErrorSeverity.Fatal


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
    assert bad_result[0].elog.size() > 0

    engine = TimeDomainGIDDecon(pf)
    rf = TimeDomainGIDRFDecon(data, engine, QCdata_key=None)
    _assert_valid_rf(rf)
    assert not rf.is_defined("TimeDomainGIDDecon_properties")


def test_TimeDomainGIDDecon_rejects_invalid_runtime_noise_window():
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)

    with pytest.raises(MsPASSError, match="noise_window") as excinfo:
        engine.load(data, TimeWindow(-10.0, 20.0), TimeWindow(0.0, -10.0))

    assert excinfo.value.severity == ErrorSeverity.Fatal

    engine = TimeDomainGIDDecon(pf)
    with pytest.raises(MsPASSError, match="noise_window") as excinfo:
        engine.loadnoise(data, TimeWindow(0.0, -10.0))

    assert excinfo.value.severity == ErrorSeverity.Fatal


def test_TimeDomainGIDDecon_rejects_invalid_runtime_signal_window():
    data = _make_gid_test_data(noise_level=None)
    pf = pfread("./data/pf/TimeDomainGIDDecon.pf")
    engine = TimeDomainGIDDecon(pf)

    with pytest.raises(MsPASSError, match="signal_window") as excinfo:
        engine.load(data, TimeWindow(20.0, -10.0), TimeWindow(-35.0, -5.0))

    assert excinfo.value.severity == ErrorSeverity.Fatal
