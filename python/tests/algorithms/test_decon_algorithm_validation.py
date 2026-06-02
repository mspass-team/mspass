#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest
from scipy import signal

from mspasspy.algorithms.CNRDecon import CNRRFDecon
from mspasspy.algorithms.FrequencyDomainGIDDecon import FrequencyDomainGIDRFDecon
from mspasspy.algorithms.RFdeconProcessor import RFdeconProcessor
from mspasspy.algorithms.TimeDomainGIDDecon import TimeDomainGIDRFDecon
from mspasspy.algorithms.basic import ExtractComponent
from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import (
    CNRDeconEngine,
    FrequencyDomainGIDDecon,
    NoiseStableDecon,
    TimeDomainGIDDecon,
)
from mspasspy.ccore.seismic import DoubleVector, Seismogram, TimeReferenceType
from mspasspy.ccore.utility import Metadata, pfread

from decon_data_generators import (
    addnoise,
    convolve_wavelet,
    make_impulse_data,
    make_simulation_wavelet,
)


SIGNAL_WINDOW = TimeWindow(-5.0, 30.0)
NOISE_WINDOW = TimeWindow(-45.0, -5.0)
DIRECT_SAMPLE = 100
EXPECTED_EW_Z_RATIO = -15.0 / 150.0
EXPECTED_NS_Z_RATIO = 10.0 / 150.0
EXPECTED_TRANSVERSE_RATIOS = {
    0.0: (EXPECTED_EW_Z_RATIO, EXPECTED_NS_Z_RATIO),
    7.5: (-20.0 / 150.0, -3.0 / 150.0),
    9.0: (15.0 / 150.0, 2.0 / 150.0),
}
STRESS_SPIKES = {
    0.0: (-15.0, 10.0, 150.0),
    1.2: (12.0, 18.0, 0.0),
    1.7: (-10.0, -22.0, 0.0),
    2.5: (20.0, 20.0, 0.0),
    3.0: (15.0, -30.0, 0.0),
    4.2: (-18.0, 26.0, 0.0),
    4.75: (16.0, -24.0, 0.0),
    6.8: (-24.0, -16.0, 0.0),
    7.35: (22.0, 15.0, 0.0),
    8.9: (15.0, 12.0, 0.0),
    10.4: (-14.0, 19.0, 0.0),
    10.95: (13.0, -18.0, 0.0),
}


def _make_validation_data(noise_level=0.0):
    np.random.seed(12345)
    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_level, padlength=800)
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
    return data


def _notch_filter_vector(x, dt, notches):
    spec = np.fft.rfft(np.asarray(x, dtype=np.float64))
    freq = np.fft.rfftfreq(len(x), dt)
    response = np.ones_like(freq)
    for center, width, depth in notches:
        response *= 1.0 - depth * np.exp(-0.5 * ((freq - center) / width) ** 2)
    return np.fft.irfft(spec * response, len(x))


def _make_complex_colored_validation_data(return_truth=False):
    np.random.seed(24680)
    wavelet = make_simulation_wavelet(corners=[0.4, 7.0])
    notched_wavelet = _notch_filter_vector(
        wavelet.data, wavelet.dt, [(1.2, 0.12, 0.75), (4.5, 0.20, 0.65)]
    )
    wavelet.data = DoubleVector(notched_wavelet.tolist())
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=0.025, padlength=900, corners=[0.04, 2.8])

    # Add a weak, coherent low-frequency field and a higher-frequency
    # transverse noise term.  This mimics colored seismic noise without
    # destroying the known receiver-function arrivals.
    t = np.arange(data.npts) * data.dt + data.t0
    for component, scale, phase in [(0, 0.010, 0.2), (1, 0.014, 1.1), (2, 0.008, 2.0)]:
        data.data[component, :] += scale * np.sin(2.0 * np.pi * 0.18 * t + phase)
    sos = signal.butter(3, [2.5, 6.0], btype="bandpass", output="sos", fs=1.0 / data.dt)
    hf_noise = signal.sosfilt(sos, np.random.default_rng(24680).standard_normal(data.npts))
    data.data[0, :] += 0.006 * hf_noise
    data.data[1, :] -= 0.004 * hf_noise
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
    if return_truth:
        return data, truth, wavelet
    return data


def _make_stress_truth(n=1500, dt=0.05, t0=-5.0):
    truth = Seismogram(n)
    truth.set_t0(t0)
    truth.set_dt(dt)
    truth.set_live()
    truth.tref = TimeReferenceType.Relative
    for arrival_time, amplitudes in STRESS_SPIKES.items():
        sample = int(round((arrival_time - t0) / dt))
        if 0 <= sample < n:
            for component, amplitude in enumerate(amplitudes):
                truth.data[component, sample] = amplitude
    return truth


def _make_stress_colored_validation_data(noise_scale=0.025, return_truth=False):
    np.random.seed(97531)
    wavelet = make_simulation_wavelet(corners=[0.35, 7.5])
    notched_wavelet = _notch_filter_vector(
        wavelet.data,
        wavelet.dt,
        [(0.9, 0.10, 0.70), (2.6, 0.18, 0.80), (5.2, 0.25, 0.65)],
    )
    wavelet.data = DoubleVector(notched_wavelet.tolist())
    truth = _make_stress_truth()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_scale, padlength=900, corners=[0.04, 3.0])
    t = np.arange(data.npts) * data.dt + data.t0
    for component, scale, phase in [(0, 0.012, 0.4), (1, 0.016, 1.7), (2, 0.009, 2.4)]:
        data.data[component, :] += scale * np.sin(2.0 * np.pi * 0.16 * t + phase)
    sos = signal.butter(3, [2.2, 6.5], btype="bandpass", output="sos", fs=1.0 / data.dt)
    hf_noise = signal.sosfilt(sos, np.random.default_rng(97531).standard_normal(data.npts))
    data.data[0, :] += 0.006 * hf_noise
    data.data[1, :] -= 0.005 * hf_noise
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
    if return_truth:
        return data, truth, wavelet
    return data


def _scalar_rf_matrix(algorithm, data):
    processor = RFdeconProcessor(alg=algorithm, pf="data/pf/RFdeconProcessor.pf")
    processor.loadwavelet(data, dtype="Seismogram", component=2, window=True)
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    components = []
    for component in range(3):
        processor.loaddata(data, dtype="Seismogram", component=component, window=True)
        components.append(np.asarray(processor.apply()))
    npts = min(len(x) for x in components)
    return np.vstack([x[:npts] for x in components])


def _scalar_rf_matrix_external_wavelet(algorithm, data, wavelet):
    processor = RFdeconProcessor(alg=algorithm, pf="data/pf/RFdeconProcessor.pf")
    processor.loadwavelet(wavelet, dtype="TimeSeries")
    processor.loadnoise(data, dtype="Seismogram", component=2, window=True)
    components = []
    for component in range(3):
        processor.loaddata(data, dtype="Seismogram", component=component, window=True)
        components.append(np.asarray(processor.apply()))
    npts = min(len(x) for x in components)
    return np.vstack([x[:npts] for x in components])


def _noise_stable_metadata():
    md = Metadata()
    md.put_double("deconvolution_data_window_start", SIGNAL_WINDOW.start)
    md.put_double("deconvolution_data_window_end", SIGNAL_WINDOW.end)
    md.put_double("target_sample_interval", 0.05)
    md.put_long("operator_nfft", 1024)
    md.put_double("shaping_wavelet_dt", 0.05)
    md.put_string("shaping_wavelet_type", "ricker")
    md.put_double("shaping_wavelet_frequency", 1.0)
    md.put_double("shaping_wavelet_frequency_for_inverse", 0.5)
    md.put_double("ns_gid_mu_min", 3.0e-3)
    md.put_double("ns_gid_alpha", 1.0)
    md.put_double("ns_gid_noise_floor", 1.0e-12)
    md.put_double("ns_gid_gain_max", 1.0e3)
    md.put_double("ns_gid_snr_taper_low", 1.0)
    md.put_double("ns_gid_snr_taper_high", 3.0)
    md.put_bool("ns_gid_use_reliability_taper", False)
    return md


def _noise_stable_rf_matrix(data, external_wavelet=None):
    wavelet = external_wavelet if external_wavelet is not None else ExtractComponent(data, 2)
    if external_wavelet is None:
        wavelet = WindowData(wavelet, SIGNAL_WINDOW.start, SIGNAL_WINDOW.end)
    noise = ExtractComponent(data, 2)
    noise = WindowData(noise, NOISE_WINDOW.start, NOISE_WINDOW.end)
    components = []
    for component in range(3):
        d = ExtractComponent(data, component)
        d = WindowData(d, SIGNAL_WINDOW.start, SIGNAL_WINDOW.end)
        engine = NoiseStableDecon(_noise_stable_metadata())
        engine.loadnoise(noise)
        engine.load(wavelet.data, d.data)
        engine.process()
        components.append(np.asarray(engine.getresult()))
    npts = min(len(x) for x in components)
    return np.vstack([x[:npts] for x in components])


def _cnr_rf_matrix(data, external_wavelet=None):
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    rf = CNRRFDecon(
        data,
        engine,
        signal_window=SIGNAL_WINDOW,
        noise_window=NOISE_WINDOW,
        external_wavelet=external_wavelet,
    )
    assert rf.live
    return np.asarray(rf.data)


def _normalized_correlation(x, y):
    npts = min(x.shape[1], y.shape[1])
    xv = x[:, :npts].ravel()
    yv = y[:, :npts].ravel()
    return np.dot(xv, yv) / (np.linalg.norm(xv) * np.linalg.norm(yv))


def _assert_scalar_result_is_not_discrete_sparse_output(matrix, name):
    """Scalar inverse operators should produce finite-bandwidth traces."""
    x = np.asarray(matrix, dtype=np.float64)
    assert np.count_nonzero(np.abs(x) > 1.0e-10) > 0.35 * x.size, name
    for component in range(x.shape[0]):
        y = np.abs(x[component, :])
        if np.max(y) <= 0.0:
            continue
        active = np.count_nonzero(y > 0.02 * np.max(y))
        assert active > 0.02 * y.size, name


def _pf_with_gid_mode(tmp_path, pfname, branch_name, mode):
    # The C++ engines expect an AntelopePf tree, so write a temporary pf file
    # after changing only the GID inverse mode under test.
    text = open(pfname, encoding="utf-8").read()
    text = text.replace(
        f"{branch_name} &Arr{{\n        deconvolution_type least_square",
        f"{branch_name} &Arr{{\n        deconvolution_type {mode}",
    )
    dst = tmp_path / pfname.split("/")[-1]
    dst.write_text(text)
    return pfread(str(dst))


def _pf_with_gid_mode_and_replacements(
    tmp_path, pfname, branch_name, mode, replacements
):
    text = open(pfname, encoding="utf-8").read()
    text = text.replace(
        f"{branch_name} &Arr{{\n        deconvolution_type least_square",
        f"{branch_name} &Arr{{\n        deconvolution_type {mode}",
    )
    for old, new in replacements.items():
        text = text.replace(old, new)
    suffix = abs(hash((pfname, branch_name, mode, tuple(replacements.items()))))
    dst = tmp_path / f"{pfname.split('/')[-1]}.{suffix}.pf"
    dst.write_text(text)
    return pfread(str(dst))


def _assert_direct_arrival_is_recovered(matrix, ratio_tol=2.0e-2):
    z = matrix[2, :]
    search = slice(DIRECT_SAMPLE - 3, DIRECT_SAMPLE + 4)
    peak_sample = DIRECT_SAMPLE - 3 + int(np.argmax(np.abs(z[search])))
    assert abs(peak_sample - DIRECT_SAMPLE) <= 2
    assert np.isclose(
        matrix[0, peak_sample] / matrix[2, peak_sample],
        EXPECTED_EW_Z_RATIO,
        atol=ratio_tol,
    )
    assert np.isclose(
        matrix[1, peak_sample] / matrix[2, peak_sample],
        EXPECTED_NS_Z_RATIO,
        atol=ratio_tol,
    )


def _assert_colored_transverse_arrivals_are_recovered(
    matrix, t0=SIGNAL_WINDOW.start, dt=0.05, ratio_tol=7.0e-2
):
    z0_sample = int(round((0.0 - t0) / dt))
    z0 = matrix[2, z0_sample]
    assert abs(z0) > 1.0e-8
    for arrival_time, expected in EXPECTED_TRANSVERSE_RATIOS.items():
        sample = int(round((arrival_time - t0) / dt))
        ew_ratio = matrix[0, sample] / z0
        ns_ratio = matrix[1, sample] / z0
        assert np.isclose(ew_ratio, expected[0], atol=ratio_tol), arrival_time
        assert np.isclose(ns_ratio, expected[1], atol=ratio_tol), arrival_time


def _assert_colored_gid_arrival_signs_are_recovered(matrix, t0, dt):
    expected_signs = [
        (2.5, (1.0, 1.0)),
        (3.0, (1.0, -1.0)),
        (7.5, (-1.0, -1.0)),
        (9.0, (1.0, 1.0)),
    ]
    for arrival_time, expected in expected_signs:
        sample = int(round((arrival_time - t0) / dt))
        ew = matrix[0, sample]
        ns = matrix[1, sample]
        assert np.sign(ew) == expected[0], arrival_time
        assert np.sign(ns) == expected[1], arrival_time
        assert abs(ew) > 1.0e-3, arrival_time
        assert abs(ns) > 1.0e-3, arrival_time


def _local_peak(matrix, component, sample, half_width=2):
    start = max(0, sample - half_width)
    stop = min(matrix.shape[1], sample + half_width + 1)
    window = matrix[component, start:stop]
    return window[int(np.argmax(np.abs(window)))]


def _assert_stress_arrivals_recovered(matrix, t0, dt, ratio_tol=9.0e-2):
    z0_sample = int(round((0.0 - t0) / dt))
    z0 = matrix[2, z0_sample]
    assert abs(z0) > 1.0e-8
    for arrival_time, amplitudes in STRESS_SPIKES.items():
        sample = int(round((arrival_time - t0) / dt))
        for component in (0, 1):
            expected = amplitudes[component]
            if expected == 0.0:
                continue
            recovered = _local_peak(matrix, component, sample) / z0
            assert np.sign(recovered) == np.sign(expected), (
                arrival_time,
                component,
            )
            assert np.isclose(recovered, expected / 150.0, atol=ratio_tol), (
                arrival_time,
                component,
            )


def _assert_shifted_stress_arrivals_recovered(
    matrix, t0, dt, shift, ratio_tol=1.8e-1
):
    z0_sample = int(round((shift - t0) / dt))
    z0 = matrix[2, z0_sample]
    assert abs(z0) > 1.0e-8
    for arrival_time, amplitudes in STRESS_SPIKES.items():
        sample = int(round((arrival_time + shift - t0) / dt))
        for component in (0, 1):
            expected = amplitudes[component]
            if expected == 0.0:
                continue
            recovered = _local_peak(matrix, component, sample) / z0
            assert np.sign(recovered) == np.sign(expected), (
                arrival_time,
                component,
            )
            assert np.isclose(recovered, expected / 150.0, atol=ratio_tol), (
                arrival_time,
                component,
            )


def _assert_shifted_direct_arrival_recovered(
    matrix, t0, dt, shift, ratio_tol=8.0e-2
):
    z0_sample = int(round((shift - t0) / dt))
    z0 = matrix[2, z0_sample]
    assert abs(z0) > 1.0e-8
    ew_ratio = _local_peak(matrix, 0, z0_sample) / z0
    ns_ratio = _local_peak(matrix, 1, z0_sample) / z0
    assert np.isclose(ew_ratio, EXPECTED_EW_Z_RATIO, atol=ratio_tol)
    assert np.isclose(ns_ratio, EXPECTED_NS_Z_RATIO, atol=ratio_tol)


def _assert_stress_gid_signs_recovered(matrix, t0, dt):
    for arrival_time, amplitudes in STRESS_SPIKES.items():
        if arrival_time == 0.0:
            continue
        sample = int(round((arrival_time - t0) / dt))
        for component in (0, 1):
            expected = amplitudes[component]
            if expected == 0.0:
                continue
            recovered = _local_peak(matrix, component, sample)
            assert np.sign(recovered) == np.sign(expected), (
                arrival_time,
                component,
            )
            assert abs(recovered) > 1.0e-3, (arrival_time, component)


def _classify_gid_spike_detections(
    matrix, t0, dt, detection_threshold=0.02, match_tolerance=0.15
):
    truth_times = np.asarray(sorted(STRESS_SPIKES.keys()), dtype=np.float64)
    return _classify_spike_detections_against_times(
        matrix, t0, dt, truth_times, detection_threshold, match_tolerance
    )


def _classify_spike_detections_against_times(
    matrix, t0, dt, truth_times, detection_threshold=0.02, match_tolerance=0.15
):
    transverse_amp = np.sqrt(matrix[0, :] ** 2 + matrix[1, :] ** 2)
    peak = np.max(transverse_amp)
    if peak <= 0.0:
        return {
            "detections": 0,
            "true_positive": 0,
            "false_positive": 0,
            "false_negative": len(truth_times),
            "precision": 0.0,
            "recall": 0.0,
            "f1": 0.0,
        }
    candidate_samples = np.where(transverse_amp >= detection_threshold * peak)[0]
    detection_samples = []
    groups = []
    for sample in candidate_samples:
        if not groups or sample - groups[-1][-1] > 2:
            groups.append([sample])
        else:
            groups[-1].append(sample)
    for group in groups:
        detection_samples.append(max(group, key=lambda i: transverse_amp[i]))
    detection_times = t0 + dt * np.asarray(detection_samples, dtype=np.float64)
    matched_truth = set()
    true_positive = 0
    for detection_time in detection_times:
        distances = np.abs(truth_times - detection_time)
        truth_index = int(np.argmin(distances))
        if (
            distances[truth_index] <= match_tolerance
            and truth_index not in matched_truth
        ):
            matched_truth.add(truth_index)
            true_positive += 1
    detections = len(detection_times)
    false_positive = detections - true_positive
    false_negative = len(truth_times) - true_positive
    precision = true_positive / detections if detections else 0.0
    recall = true_positive / len(truth_times)
    f1 = (
        2.0 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0
    )
    return {
        "detections": detections,
        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def _truth_matrix_for_times(truth, times):
    truth_data = np.asarray(truth.data)
    truth_matrix = np.zeros((3, len(times)))
    samples = np.rint((times - truth.t0) / truth.dt).astype(int)
    valid = np.logical_and(samples >= 0, samples < truth.npts)
    truth_matrix[:, valid] = truth_data[:, samples[valid]]
    return truth_matrix


def _shift_truth_time(truth, shift):
    shifted = Seismogram(truth)
    shifted.set_t0(truth.t0 + shift)
    return shifted


def _slice_matrix_to_window(matrix, src_t0, dt, out_t0, out_npts):
    result = np.zeros((matrix.shape[0], out_npts))
    start = int(round((out_t0 - src_t0) / dt))
    src_start = max(0, start)
    dst_start = max(0, -start)
    ncopy = min(matrix.shape[1] - src_start, out_npts - dst_start)
    if ncopy > 0:
        result[:, dst_start : dst_start + ncopy] = matrix[
            :, src_start : src_start + ncopy
        ]
    return result


def _plot_rf_overlay(plot_dir, filename, title, results, truth, t0, dt):
    if plot_dir is None:
        return
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError as err:
        pytest.fail(
            "--decon-validation-plots requires matplotlib; install it to write "
            f"validation plots ({err})"
        )

    npts = min(matrix.shape[1] for matrix in results.values())
    t = t0 + np.arange(npts) * dt
    truth_matrix = _truth_matrix_for_times(truth, t)
    component_names = ["EW", "NS", "Z"]
    colors = plt.get_cmap("tab10")
    linestyles = ["-", "--", "-.", ":"]

    fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True)
    for component, axis in enumerate(axes):
        axis.plot(
            t,
            truth_matrix[component, :],
            color="black",
            linewidth=1.4,
            label="truth spike train",
        )
        truth_peak = np.max(np.abs(truth_matrix[component, :]))
        if truth_peak == 0.0:
            truth_peak = 1.0
        offset_step = 1.25 * truth_peak
        axis.axhline(0.0, color="0.2", linewidth=0.6)
        for offset_index, (name, matrix) in enumerate(results.items()):
            scaled = matrix[component, :npts]
            peak = np.max(np.abs(scaled))
            if peak > 0.0:
                scaled = scaled / peak * truth_peak
            offset = (offset_index + 1) * offset_step
            axis.axhline(offset, color="0.88", linewidth=0.5)
            axis.plot(
                t,
                scaled + offset,
                linewidth=1.1,
                alpha=0.95,
                color=colors(offset_index % 10),
                linestyle=linestyles[offset_index % len(linestyles)],
                label=name,
            )
        axis.axvline(0.0, color="0.5", linestyle="--", linewidth=0.8)
        axis.set_ylabel(component_names[component])
        axis.grid(True, color="0.85", linewidth=0.6)
    axes[-1].set_xlabel("Lag time relative to direct P sample (s)")
    axes[0].legend(loc="upper right", ncol=3, fontsize="small")
    fig.suptitle(title + " (method traces are vertically offset)")
    fig.tight_layout()
    fig.savefig(plot_dir / filename, dpi=150)
    plt.close(fig)


def _plot_complex_colored_results(plot_dir, results, truth, wavelet):
    if plot_dir is None:
        return
    try:
        import matplotlib

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt
    except ImportError as err:
        pytest.fail(
            "--decon-validation-plots requires matplotlib; install it to write "
            f"validation plots ({err})"
        )

    _plot_rf_overlay(
        plot_dir,
        "complex_colored_scalar_methods.png",
        "Scalar and CNR deconvolution validation on colored 3C synthetic",
        results,
        truth,
        SIGNAL_WINDOW.start,
        truth.dt,
    )

    tw = wavelet.t0 + np.arange(wavelet.npts) * wavelet.dt
    fig, axis = plt.subplots(1, 1, figsize=(10, 3))
    axis.plot(tw, np.asarray(wavelet.data), color="black", linewidth=1.2)
    axis.set_title("Notched source wavelet used for validation")
    axis.set_xlabel("Time (s)")
    axis.set_ylabel("Amplitude")
    axis.grid(True, color="0.85", linewidth=0.6)
    fig.tight_layout()
    fig.savefig(plot_dir / "complex_colored_validation_wavelet.png", dpi=150)
    plt.close(fig)


def _plot_stress_results(plot_dir, results, truth, wavelet, t0, dt, prefix):
    if plot_dir is None:
        return
    _plot_rf_overlay(
        plot_dir,
        f"{prefix}_stress_spike_results.png",
        f"{prefix} stress validation with close opposite-polarity spikes",
        results,
        truth,
        t0,
        dt,
    )


def _plot_gid_mode_results(plot_dir, engine_name, results, truth, t0, dt):
    _plot_rf_overlay(
        plot_dir,
        f"{engine_name}_inverse_modes.png",
        f"{engine_name} validation by inverse-operator mode",
        results,
        truth,
        t0,
        dt,
    )


def _plot_gid_sparse_results(plot_dir, engine_name, results, truth, t0, dt, suffix):
    _plot_rf_overlay(
        plot_dir,
        f"{engine_name}_{suffix}_sparse_results.png",
        f"{engine_name} raw sparse spike output ({suffix})",
        results,
        truth,
        t0,
        dt,
    )


def _plot_external_wavelet_results(plot_dir, results, truth, t0, dt):
    _plot_rf_overlay(
        plot_dir,
        "external_wavelet_all_methods.png",
        "External prepared wavelet validation across deconvolution methods",
        results,
        truth,
        t0,
        dt,
    )


@pytest.mark.parametrize("noise_level", [0.0, 1.0e-6])
@pytest.mark.parametrize(
    "algorithm", ["LeastSquares", "WaterLevel", "MultiTaperXcor", "MultiTaperSpecDiv"]
)
def test_existing_scalar_decon_methods_recover_noise_free_direct_arrival(
    noise_level, algorithm
):
    data = _make_validation_data(noise_level=noise_level)
    rf = _scalar_rf_matrix(algorithm, data)
    _assert_direct_arrival_is_recovered(rf)


@pytest.mark.parametrize("noise_level", [0.0, 1.0e-6])
def test_cnr_decon_recovers_noise_free_direct_arrival(noise_level):
    data = _make_validation_data(noise_level=noise_level)
    rf = _cnr_rf_matrix(data)
    _assert_direct_arrival_is_recovered(rf)


def test_existing_decon_methods_are_consistent_for_noise_free_input():
    data = _make_validation_data(noise_level=0.0)
    results = {
        "LeastSquares": _scalar_rf_matrix("LeastSquares", data),
        "WaterLevel": _scalar_rf_matrix("WaterLevel", data),
        "MultiTaperXcor": _scalar_rf_matrix("MultiTaperXcor", data),
        "MultiTaperSpecDiv": _scalar_rf_matrix("MultiTaperSpecDiv", data),
        "NoiseStable": _noise_stable_rf_matrix(data),
        "CNR": _cnr_rf_matrix(data),
    }

    assert _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.97
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperXcor"]) > 0.88
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperSpecDiv"]) > 0.88
    assert _normalized_correlation(results["WaterLevel"], results["MultiTaperXcor"]) > 0.93
    assert _normalized_correlation(results["WaterLevel"], results["MultiTaperSpecDiv"]) > 0.93
    assert _normalized_correlation(results["MultiTaperXcor"], results["MultiTaperSpecDiv"]) > 0.93
    assert _normalized_correlation(results["CNR"], results["LeastSquares"]) > 0.75
    assert _normalized_correlation(results["CNR"], results["WaterLevel"]) > 0.79
    assert _normalized_correlation(results["CNR"], results["MultiTaperXcor"]) > 0.87
    assert _normalized_correlation(results["CNR"], results["MultiTaperSpecDiv"]) > 0.86


def test_scalar_methods_are_consistent_for_complex_colored_3c_synthetic(
    decon_validation_plot_dir,
):
    data, truth, wavelet = _make_complex_colored_validation_data(return_truth=True)
    results = {
        "LeastSquares": _scalar_rf_matrix("LeastSquares", data),
        "WaterLevel": _scalar_rf_matrix("WaterLevel", data),
        "MultiTaperXcor": _scalar_rf_matrix("MultiTaperXcor", data),
        "MultiTaperSpecDiv": _scalar_rf_matrix("MultiTaperSpecDiv", data),
        "NoiseStable": _noise_stable_rf_matrix(data),
        "CNR": _cnr_rf_matrix(data),
    }

    for name, result in results.items():
        assert np.isfinite(result).all()
        _assert_scalar_result_is_not_discrete_sparse_output(result, name)
        _assert_direct_arrival_is_recovered(result, ratio_tol=1.0e-1)
        _assert_colored_transverse_arrivals_are_recovered(result)

    assert _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.95
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperXcor"]) > 0.84
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperSpecDiv"]) > 0.84
    assert _normalized_correlation(results["MultiTaperXcor"], results["MultiTaperSpecDiv"]) > 0.90
    assert _normalized_correlation(results["CNR"], results["LeastSquares"]) > 0.70
    assert _normalized_correlation(results["CNR"], results["MultiTaperXcor"]) > 0.82
    assert (
        np.max(np.abs(results["MultiTaperXcor"] - results["MultiTaperSpecDiv"]))
        > 1.0e-2
    )
    _plot_complex_colored_results(decon_validation_plot_dir, results, truth, wavelet)


def test_scalar_methods_recover_stress_spikes_with_colored_noise(
    decon_validation_plot_dir,
    decon_validation_noise_scale,
):
    data, truth, wavelet = _make_stress_colored_validation_data(
        noise_scale=0.01,
        return_truth=True,
    )
    results = {
        "LeastSquares": _scalar_rf_matrix("LeastSquares", data),
        "WaterLevel": _scalar_rf_matrix("WaterLevel", data),
        "MultiTaperXcor": _scalar_rf_matrix("MultiTaperXcor", data),
        "MultiTaperSpecDiv": _scalar_rf_matrix("MultiTaperSpecDiv", data),
        "NoiseStable": _noise_stable_rf_matrix(data),
        "CNR": _cnr_rf_matrix(data),
    }

    for name, result in results.items():
        assert np.isfinite(result).all()
        _assert_scalar_result_is_not_discrete_sparse_output(result, name)
        _assert_stress_arrivals_recovered(result, SIGNAL_WINDOW.start, truth.dt)

    assert _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.93
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperXcor"]) > 0.82
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperSpecDiv"]) > 0.82
    assert _normalized_correlation(results["MultiTaperXcor"], results["MultiTaperSpecDiv"]) > 0.90
    assert _normalized_correlation(results["CNR"], results["LeastSquares"]) > 0.58
    assert _normalized_correlation(results["CNR"], results["MultiTaperXcor"]) > 0.68
    assert (
        np.max(np.abs(results["MultiTaperXcor"] - results["MultiTaperSpecDiv"]))
        > 1.0e-2
    )
    plot_results = results
    plot_truth = truth
    plot_wavelet = wavelet
    if decon_validation_plot_dir is not None and decon_validation_noise_scale != 0.01:
        plot_data, plot_truth, plot_wavelet = _make_stress_colored_validation_data(
            noise_scale=decon_validation_noise_scale,
            return_truth=True,
        )
        plot_results = {
            "LeastSquares": _scalar_rf_matrix("LeastSquares", plot_data),
            "WaterLevel": _scalar_rf_matrix("WaterLevel", plot_data),
            "MultiTaperXcor": _scalar_rf_matrix("MultiTaperXcor", plot_data),
            "MultiTaperSpecDiv": _scalar_rf_matrix("MultiTaperSpecDiv", plot_data),
            "NoiseStable": _noise_stable_rf_matrix(plot_data),
            "CNR": _cnr_rf_matrix(plot_data),
        }
    _plot_stress_results(
        decon_validation_plot_dir,
        plot_results,
        plot_truth,
        plot_wavelet,
        SIGNAL_WINDOW.start,
        plot_truth.dt,
        f"scalar_noise_{decon_validation_noise_scale:g}",
    )


def test_external_wavelet_validation_across_deconvolution_methods(
    tmp_path, decon_validation_plot_dir
):
    data, truth, wavelet = _make_stress_colored_validation_data(
        noise_scale=0.01,
        return_truth=True,
    )
    results = {
        "LeastSquares": _scalar_rf_matrix_external_wavelet(
            "LeastSquares", data, wavelet
        ),
        "WaterLevel": _scalar_rf_matrix_external_wavelet("WaterLevel", data, wavelet),
        "MultiTaperXcor": _scalar_rf_matrix_external_wavelet(
            "MultiTaperXcor", data, wavelet
        ),
        "MultiTaperSpecDiv": _scalar_rf_matrix_external_wavelet(
            "MultiTaperSpecDiv", data, wavelet
        ),
        "NoiseStable": _noise_stable_rf_matrix(data, external_wavelet=wavelet),
        "CNR": _cnr_rf_matrix(data, external_wavelet=wavelet),
    }
    for engine_class, wrapper, pfname, branch_name in [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
        ),
    ]:
        pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, "ns_gid")
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
            external_wavelet=wavelet,
        )
        assert rf.live
        results[f"{engine_class.__name__}:ns_gid"] = np.asarray(rf.data)
        results[f"{engine_class.__name__}:ns_gid_sparse"] = np.asarray(
            engine.sparse_output().data
        )
        qc = dict(rf[f"{engine_class.__name__}_properties"])
        assert qc["ns_gid_external_wavelet_used"]
        assert qc["ns_gid_gain_max_actual"] <= qc["ns_gid_gain_max_requested"] * (
            1.0 + 1.0e-10
        )

    reference = results["LeastSquares"]
    amp = np.sqrt(reference[0, :] ** 2 + reference[1, :] ** 2 + reference[2, :] ** 2)
    external_wavelet_shift = SIGNAL_WINDOW.start + 0.05 * int(np.argmax(amp))
    shifted_truth = _shift_truth_time(truth, external_wavelet_shift)
    for name, result in results.items():
        assert np.isfinite(result).all(), name
        if "GIDDecon" not in name and name != "NoiseStable":
            _assert_shifted_direct_arrival_recovered(
                result, SIGNAL_WINDOW.start, truth.dt, external_wavelet_shift
            )

    assert _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.95
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperXcor"]) > 0.82
    assert _normalized_correlation(results["MultiTaperXcor"], results["MultiTaperSpecDiv"]) > 0.35
    assert (
        np.max(np.abs(results["MultiTaperXcor"] - results["MultiTaperSpecDiv"]))
        > 1.0e-2
    )
    assert _normalized_correlation(results["CNR"], results["LeastSquares"]) > 0.55

    shifted_truth_times = (
        np.asarray(sorted(STRESS_SPIKES.keys()), dtype=np.float64)
        + external_wavelet_shift
    )
    td_metrics = _classify_spike_detections_against_times(
        results["TimeDomainGIDDecon:ns_gid_sparse"],
        -8.0,
        truth.dt,
        shifted_truth_times,
    )
    fd_metrics = _classify_spike_detections_against_times(
        results["FrequencyDomainGIDDecon:ns_gid_sparse"],
        -8.0,
        truth.dt,
        shifted_truth_times,
    )
    assert td_metrics["recall"] >= 0.95
    assert td_metrics["precision"] >= 0.45
    assert fd_metrics["recall"] >= 0.95
    assert fd_metrics["precision"] >= 0.75

    plot_npts = int(round((20.0 - SIGNAL_WINDOW.start) / truth.dt)) + 1
    plot_results = {}
    for name, result in results.items():
        if name.endswith("_sparse"):
            continue
        src_t0 = -8.0 if "GIDDecon" in name else SIGNAL_WINDOW.start
        plot_results[name] = _slice_matrix_to_window(
            result, src_t0, truth.dt, SIGNAL_WINDOW.start, plot_npts
        )

    _plot_external_wavelet_results(
        decon_validation_plot_dir,
        plot_results,
        shifted_truth,
        SIGNAL_WINDOW.start,
        truth.dt,
    )


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, modes, qc_key",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            ["least_square", "water_level", "multi_taper", "cnr", "ns_gid"],
            "TimeDomainGIDDecon_properties",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            ["least_square", "water_level", "multi_taper", "cnr", "ns_gid"],
            "FrequencyDomainGIDDecon_properties",
        ),
    ],
)
def test_gid_methods_recover_colored_multi_spike_rf_for_all_inverse_modes(
    tmp_path,
    decon_validation_plot_dir,
    engine_class,
    wrapper,
    pfname,
    branch_name,
    modes,
    qc_key,
):
    plot_results = {}
    plot_sparse_results = {}
    plot_t0 = None
    plot_dt = None
    data, truth, _ = _make_complex_colored_validation_data(return_truth=True)
    for mode in modes:
        pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, mode)
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
        )
        assert rf.live, mode
        assert rf[qc_key]["residual_L2_final"] < rf[qc_key]["residual_L2_initial"]
        plot_results[mode] = np.asarray(rf.data)
        plot_t0 = rf.t0
        plot_dt = rf.dt
        sparse = engine.sparse_output()
        plot_sparse_results[mode] = np.asarray(sparse.data)
        _assert_colored_gid_arrival_signs_are_recovered(
            np.asarray(sparse.data), sparse.t0, sparse.dt
        )
        zrf = ExtractComponent(rf, 2)
        peak_sample = int(np.argmax(np.abs(zrf.data)))
        assert abs(zrf.time(peak_sample)) <= 0.15, mode
        assert np.isclose(
            rf.data[0, peak_sample] / rf.data[2, peak_sample],
            EXPECTED_EW_Z_RATIO,
            atol=5.0e-2,
        ), mode
        assert np.isclose(
            rf.data[1, peak_sample] / rf.data[2, peak_sample],
            EXPECTED_NS_Z_RATIO,
            atol=5.0e-2,
        ), mode
    _plot_gid_mode_results(
        decon_validation_plot_dir,
        engine_class.__name__,
        plot_results,
        truth,
        plot_t0,
        plot_dt,
    )
    _plot_gid_sparse_results(
        decon_validation_plot_dir,
        engine_class.__name__,
        plot_sparse_results,
        truth,
        plot_t0,
        plot_dt,
        "inverse_modes",
    )


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
        ),
    ],
)
def test_gid_output_shaping_wavelet_is_configurable_and_separate_from_sparse_support(
    tmp_path, engine_class, wrapper, pfname, branch_name
):
    data, _, _ = _make_stress_colored_validation_data(
        noise_scale=0.0,
        return_truth=True,
    )
    pf_default = _pf_with_gid_mode(tmp_path, pfname, branch_name, "ns_gid")
    pf_wide = _pf_with_gid_mode_and_replacements(
        tmp_path,
        pfname,
        branch_name,
        "ns_gid",
        {"shaping_wavelet_frequency 1.0": "shaping_wavelet_frequency 2.0"},
    )

    shaped_outputs = []
    sparse_outputs = []
    for pf in (pf_default, pf_wide):
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
        )
        assert rf.live
        shaped_outputs.append(np.asarray(rf.data))
        sparse_outputs.append(np.asarray(engine.sparse_output().data))

    assert np.allclose(sparse_outputs[0], sparse_outputs[1], atol=1.0e-10)
    assert not np.allclose(shaped_outputs[0], shaped_outputs[1], atol=1.0e-5)
    assert _normalized_correlation(shaped_outputs[0], shaped_outputs[1]) < 0.999


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, modes, qc_key",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            ["least_square", "water_level", "multi_taper", "cnr", "ns_gid"],
            "TimeDomainGIDDecon_properties",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            ["least_square", "water_level", "multi_taper", "cnr", "ns_gid"],
            "FrequencyDomainGIDDecon_properties",
        ),
    ],
)
def test_gid_methods_recover_stress_spike_signs_for_all_inverse_modes(
    tmp_path,
    decon_validation_plot_dir,
    decon_validation_noise_scale,
    engine_class,
    wrapper,
    pfname,
    branch_name,
    modes,
    qc_key,
):
    plot_results = {}
    plot_sparse_results = {}
    plot_t0 = None
    plot_dt = None
    data, truth, _ = _make_stress_colored_validation_data(
        noise_scale=0.01,
        return_truth=True,
    )
    for mode in modes:
        pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, mode)
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
        )
        assert rf.live, mode
        assert rf[qc_key]["residual_L2_final"] < rf[qc_key]["residual_L2_initial"]
        matrix = np.asarray(rf.data)
        sparse = engine.sparse_output()
        _assert_stress_gid_signs_recovered(
            np.asarray(sparse.data), sparse.t0, sparse.dt
        )
        plot_results[mode] = matrix
        plot_sparse_results[mode] = np.asarray(sparse.data)
        plot_t0 = rf.t0
        plot_dt = rf.dt

    plot_truth = truth
    if decon_validation_plot_dir is not None and decon_validation_noise_scale != 0.01:
        plot_data, plot_truth, _ = _make_stress_colored_validation_data(
            noise_scale=decon_validation_noise_scale,
            return_truth=True,
        )
        plot_results = {}
        plot_sparse_results = {}
        plot_t0 = None
        plot_dt = None
        for mode in modes:
            pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, mode)
            engine = engine_class(pf)
            rf = wrapper(
                plot_data,
                engine,
                signal_window=TimeWindow(-8.0, 20.0),
                noise_window=TimeWindow(-35.0, -5.0),
            )
            if rf.live:
                plot_results[mode] = np.asarray(rf.data)
                plot_sparse_results[mode] = np.asarray(engine.sparse_output().data)
                plot_t0 = rf.t0
                plot_dt = rf.dt

    _plot_stress_results(
        decon_validation_plot_dir,
        plot_results,
        plot_truth,
        None,
        plot_t0,
        plot_dt,
        f"{engine_class.__name__}_noise_{decon_validation_noise_scale:g}",
    )
    _plot_gid_sparse_results(
        decon_validation_plot_dir,
        engine_class.__name__,
        plot_sparse_results,
        plot_truth,
        plot_t0,
        plot_dt,
        f"noise_{decon_validation_noise_scale:g}_stress",
    )


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, strict_replacements",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            {
                "residual_fractional_improvement_floor 0.0001":
                    "residual_fractional_improvement_floor 0.01"
            },
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            {"residual_ratio_floor 0.01": "residual_ratio_floor 0.35"},
        ),
    ],
)
def test_gid_detection_precision_recall_tracks_noise_and_stopping_thresholds(
    tmp_path, engine_class, wrapper, pfname, branch_name, strict_replacements
):
    noise_levels = [0.0, 0.01, 0.03]
    loose_metrics = []
    strict_metrics = []
    for noise_scale in noise_levels:
        data, _, _ = _make_stress_colored_validation_data(
            noise_scale=noise_scale,
            return_truth=True,
        )
        for replacements, metrics_list in [
            ({}, loose_metrics),
            (strict_replacements, strict_metrics),
        ]:
            pf = _pf_with_gid_mode_and_replacements(
                tmp_path,
                pfname,
                branch_name,
                "least_square",
                replacements,
            )
            engine = engine_class(pf)
            rf = wrapper(
                data,
                engine,
                signal_window=TimeWindow(-8.0, 20.0),
                noise_window=TimeWindow(-35.0, -5.0),
            )
            assert rf.live, (engine_class.__name__, noise_scale, replacements)
            sparse = engine.sparse_output()
            metrics_list.append(
                _classify_gid_spike_detections(
                    np.asarray(sparse.data), sparse.t0, sparse.dt
                )
            )

    for metrics in loose_metrics:
        assert metrics["recall"] >= 0.95
        assert metrics["precision"] >= 0.60
        assert metrics["f1"] >= 0.75
        assert metrics["false_positive"] <= 8

    for loose, strict in zip(loose_metrics, strict_metrics):
        assert strict["detections"] <= loose["detections"]
        assert strict["precision"] >= loose["precision"]
        assert strict["false_positive"] <= loose["false_positive"]
        if engine_class is FrequencyDomainGIDDecon:
            assert strict["recall"] < loose["recall"]
            assert strict["precision"] >= 0.99
        else:
            assert strict["recall"] >= 0.95


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, qc_key",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            "TimeDomainGIDDecon_properties",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            "FrequencyDomainGIDDecon_properties",
        ),
    ],
)
def test_ns_gid_detection_precision_recall_tracks_max_spike_threshold(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key
):
    for noise_scale in [0.0, 0.01, 0.03]:
        data, _, _ = _make_stress_colored_validation_data(
            noise_scale=noise_scale,
            return_truth=True,
        )
        loose_pf = _pf_with_gid_mode_and_replacements(
            tmp_path, pfname, branch_name, "ns_gid", {}
        )
        strict_pf = _pf_with_gid_mode_and_replacements(
            tmp_path,
            pfname,
            branch_name,
            "ns_gid",
            {"ns_gid_max_spikes 0": "ns_gid_max_spikes 12"},
        )
        metrics = []
        qcs = []
        for pf in (loose_pf, strict_pf):
            engine = engine_class(pf)
            rf = wrapper(
                data,
                engine,
                signal_window=TimeWindow(-8.0, 20.0),
                noise_window=TimeWindow(-35.0, -5.0),
            )
            assert rf.live, (engine_class.__name__, noise_scale)
            qcs.append(dict(rf[qc_key]))
            sparse = engine.sparse_output()
            metrics.append(
                _classify_gid_spike_detections(
                    np.asarray(sparse.data), sparse.t0, sparse.dt
                )
            )

        loose, strict = metrics
        loose_qc, strict_qc = qcs
        assert loose["recall"] >= 0.95
        assert strict["recall"] >= 0.95
        assert strict["detections"] <= loose["detections"]
        assert strict["false_positive"] <= loose["false_positive"]
        assert strict["precision"] >= loose["precision"]
        assert strict_qc["ns_gid_stop_reason"] in {
            "max_spikes",
            "residual_reached_noise_floor",
            "candidate_not_significant",
        }
        assert strict_qc["ns_gid_number_spikes"] <= 12
        assert loose_qc["ns_gid_gain_max_actual"] <= (
            loose_qc["ns_gid_gain_max_requested"] * (1.0 + 1.0e-10)
        )
