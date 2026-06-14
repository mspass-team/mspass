#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import warnings

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
DEFAULT_GID_DECONVOLUTION_TYPE = "ns_gid"
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
CLOSE_ARRIVAL_SPIKES = {
    0.0: (-12.0, 8.0, 150.0),
    1.00: (32.0, -24.0, 0.0),
    1.20: (-27.0, 21.0, 0.0),
    1.40: (13.0, -10.0, 0.0),
    3.00: (-30.0, -22.0, 0.0),
    3.20: (26.0, 18.0, 0.0),
    5.10: (18.0, -16.0, 0.0),
    7.40: (-20.0, 15.0, 0.0),
}


def _import_matplotlib_pyplot():
    try:
        from pyparsing.exceptions import PyparsingDeprecationWarning
    except ImportError:
        warning_category = Warning
    else:
        warning_category = PyparsingDeprecationWarning

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            category=warning_category,
            module=r"matplotlib\._fontconfig_pattern",
        )
        warnings.filterwarnings(
            "ignore",
            category=warning_category,
            module=r"matplotlib\._mathtext",
        )
        try:
            import matplotlib

            matplotlib.use("Agg", force=True)
            import matplotlib.pyplot as plt
        except ImportError as err:
            pytest.fail(
                "--decon-validation-plots requires matplotlib; install it to write "
                f"validation plots ({err})"
            )
    return plt


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


def _make_complex_colored_validation_data(noise_scale=0.025, return_truth=False):
    np.random.seed(24680)
    wavelet = make_simulation_wavelet(corners=[0.4, 7.0])
    notched_wavelet = _notch_filter_vector(
        wavelet.data, wavelet.dt, [(1.2, 0.12, 0.75), (4.5, 0.20, 0.65)]
    )
    wavelet.data = DoubleVector(notched_wavelet.tolist())
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_scale, padlength=900, corners=[0.04, 2.8])

    # Add a weak, coherent low-frequency field and a higher-frequency
    # transverse noise term.  This mimics colored seismic noise without
    # destroying the known receiver-function arrivals.
    t = np.arange(data.npts) * data.dt + data.t0
    for component, scale, phase in [(0, 0.010, 0.2), (1, 0.014, 1.1), (2, 0.008, 2.0)]:
        data.data[component, :] += scale * np.sin(2.0 * np.pi * 0.18 * t + phase)
    sos = signal.butter(3, [2.5, 6.0], btype="bandpass", output="sos", fs=1.0 / data.dt)
    hf_noise = signal.sosfilt(
        sos, np.random.default_rng(24680).standard_normal(data.npts)
    )
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
    hf_noise = signal.sosfilt(
        sos, np.random.default_rng(97531).standard_normal(data.npts)
    )
    data.data[0, :] += 0.006 * hf_noise
    data.data[1, :] -= 0.005 * hf_noise
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
    if return_truth:
        return data, truth, wavelet
    return data


def _make_weak_stress_colored_validation_data(noise_scale=0.025, return_truth=False):
    """Stress fixture with weak converted phases and a strong direct arrival."""
    _, truth, wavelet = _make_stress_colored_validation_data(
        noise_scale=0.0, return_truth=True
    )
    direct_sample = int(round((0.0 - truth.t0) / truth.dt))
    for sample in range(truth.npts):
        if sample == direct_sample:
            continue
        truth.data[0, sample] *= 0.35
        truth.data[1, sample] *= 0.35
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_scale, padlength=900, corners=[0.04, 3.0])
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
    if return_truth:
        return data, truth, wavelet
    return data


def _make_direct_only_noisy_validation_data(noise_scale=3.0):
    np.random.seed(424242)
    wavelet = make_simulation_wavelet(corners=[0.35, 7.5])
    truth = _make_stress_truth()
    truth.data[:, :] = 0.0
    direct_sample = int(round((0.0 - truth.t0) / truth.dt))
    truth.data[2, direct_sample] = STRESS_SPIKES[0.0][2]
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_scale, padlength=900, corners=[0.04, 3.0])
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
    return data


def _make_close_arrival_truth(n=900, dt=0.05, t0=-5.0):
    truth = Seismogram(n)
    truth.set_t0(t0)
    truth.set_dt(dt)
    truth.set_live()
    truth.tref = TimeReferenceType.Relative
    for arrival_time, amplitudes in CLOSE_ARRIVAL_SPIKES.items():
        sample = int(round((arrival_time - t0) / dt))
        if 0 <= sample < n:
            for component, amplitude in enumerate(amplitudes):
                truth.data[component, sample] = amplitude
    return truth


def _make_close_arrival_cycling_validation_data(noise_scale=0.015, return_truth=False):
    np.random.seed(314159)
    wavelet = make_simulation_wavelet(corners=[0.45, 6.5])
    notched_wavelet = _notch_filter_vector(
        wavelet.data,
        wavelet.dt,
        [(1.0, 0.10, 0.65), (3.2, 0.18, 0.55)],
    )
    wavelet.data = DoubleVector(notched_wavelet.tolist())
    truth = _make_close_arrival_truth()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_scale, padlength=900, corners=[0.05, 2.6])
    t = np.arange(data.npts) * data.dt + data.t0
    for component, scale, phase in [
        (0, 0.006, 0.1),
        (1, 0.007, 1.3),
        (2, 0.003, 2.1),
    ]:
        data.data[component, :] += scale * np.sin(2.0 * np.pi * 0.18 * t + phase)
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
    wavelet = (
        external_wavelet if external_wavelet is not None else ExtractComponent(data, 2)
    )
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


def _noise_stable_qc(data):
    wavelet = WindowData(
        ExtractComponent(data, 2), SIGNAL_WINDOW.start, SIGNAL_WINDOW.end
    )
    noise = WindowData(ExtractComponent(data, 2), NOISE_WINDOW.start, NOISE_WINDOW.end)
    d = WindowData(ExtractComponent(data, 0), SIGNAL_WINDOW.start, SIGNAL_WINDOW.end)
    engine = NoiseStableDecon(_noise_stable_metadata())
    engine.loadnoise(noise)
    engine.load(wavelet.data, d.data)
    engine.process()
    return dict(engine.QCMetrics())


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
    """Scalar inverse operators should not be literal spike trains."""
    x = np.asarray(matrix, dtype=np.float64)
    assert np.count_nonzero(np.abs(x) > 1.0e-10) > 0.35 * x.size, name


def _pf_with_gid_mode(tmp_path, pfname, branch_name, mode):
    # The C++ engines expect an AntelopePf tree, so write a temporary pf file
    # after changing only the GID inverse mode under test.
    text = open(pfname, encoding="utf-8").read()
    text = _replace_once(
        text,
        (
            f"{branch_name} &Arr{{\n        "
            f"deconvolution_type {DEFAULT_GID_DECONVOLUTION_TYPE}"
        ),
        f"{branch_name} &Arr{{\n        deconvolution_type {mode}",
        pfname,
    )
    dst = tmp_path / pfname.split("/")[-1]
    dst.write_text(text)
    return pfread(str(dst))


def _replace_once(text, old, new, context):
    count = text.count(old)
    assert count == 1, f"{context}: expected one occurrence of {old!r}, found {count}"
    return text.replace(old, new)


def test_replace_once_rejects_missing_or_duplicate_targets():
    assert _replace_once("alpha beta", "alpha", "gamma", "unit") == "gamma beta"
    with pytest.raises(AssertionError, match="expected one occurrence"):
        _replace_once("alpha beta", "delta", "gamma", "unit")
    with pytest.raises(AssertionError, match="expected one occurrence"):
        _replace_once("alpha alpha", "alpha", "gamma", "unit")


def _pf_with_gid_mode_and_replacements(
    tmp_path, pfname, branch_name, mode, replacements
):
    text = open(pfname, encoding="utf-8").read()
    text = _replace_once(
        text,
        (
            f"{branch_name} &Arr{{\n        "
            f"deconvolution_type {DEFAULT_GID_DECONVOLUTION_TYPE}"
        ),
        f"{branch_name} &Arr{{\n        deconvolution_type {mode}",
        pfname,
    )
    for old, new in replacements.items():
        text = _replace_once(text, old, new, pfname)
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


def _assert_shifted_stress_arrivals_recovered(matrix, t0, dt, shift, ratio_tol=1.8e-1):
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
    matrix, t0, dt, shift, ratio_tol=8.0e-2, name=""
):
    z0_sample = int(round((shift - t0) / dt))
    z0 = matrix[2, z0_sample]
    assert abs(z0) > 1.0e-8
    ew_ratio = _local_peak(matrix, 0, z0_sample) / z0
    ns_ratio = _local_peak(matrix, 1, z0_sample) / z0
    assert np.isclose(ew_ratio, EXPECTED_EW_Z_RATIO, atol=ratio_tol), name
    assert np.isclose(ns_ratio, EXPECTED_NS_Z_RATIO, atol=ratio_tol), name


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
    matrix, t0, dt, detection_threshold=0.05, match_tolerance=0.15
):
    truth_times = np.asarray(sorted(STRESS_SPIKES.keys()), dtype=np.float64)
    return _classify_spike_detections_against_times(
        matrix, t0, dt, truth_times, detection_threshold, match_tolerance
    )


def _classify_close_arrival_spike_detections(
    matrix, t0, dt, detection_threshold=0.04, match_tolerance=0.11
):
    truth_times = np.asarray(sorted(CLOSE_ARRIVAL_SPIKES.keys()), dtype=np.float64)
    return _classify_spike_detections_against_times(
        matrix, t0, dt, truth_times, detection_threshold, match_tolerance
    )


def _detected_sparse_spike_times(matrix, t0, dt, detection_threshold=0.04):
    transverse_amp = np.sqrt(matrix[0, :] ** 2 + matrix[1, :] ** 2)
    peak = np.max(transverse_amp)
    if peak <= 0.0:
        return np.asarray([], dtype=np.float64)
    candidate_samples = np.where(transverse_amp >= detection_threshold * peak)[0]
    groups = []
    for sample in candidate_samples:
        if not groups or sample - groups[-1][-1] > 2:
            groups.append([sample])
        else:
            groups[-1].append(sample)
    detection_samples = [
        max(group, key=lambda sample: transverse_amp[sample]) for group in groups
    ]
    return t0 + dt * np.asarray(detection_samples, dtype=np.float64)


def _count_unmatched_close_arrival_revisits(
    matrix,
    t0,
    dt,
    detection_threshold=0.04,
    match_tolerance=0.11,
    coherence_window=0.35,
):
    truth_times = np.asarray(sorted(CLOSE_ARRIVAL_SPIKES.keys()), dtype=np.float64)
    detection_times = _detected_sparse_spike_times(
        matrix, t0, dt, detection_threshold=detection_threshold
    )
    matched_detections = set()
    for truth_time in truth_times:
        unmatched = [
            (index, abs(detection_time - truth_time))
            for index, detection_time in enumerate(detection_times)
            if index not in matched_detections
        ]
        if not unmatched:
            continue
        index, distance = min(unmatched, key=lambda item: item[1])
        if distance <= match_tolerance:
            matched_detections.add(index)
    revisits = 0
    for index, detection_time in enumerate(detection_times):
        if index in matched_detections:
            continue
        if np.any(np.abs(truth_times - detection_time) <= coherence_window):
            revisits += 1
    return revisits


def _assert_group_sparse_detection_metrics(metrics):
    assert metrics["recall"] >= 0.95
    assert metrics["precision"] >= 0.75
    assert metrics["false_positive"] <= 2


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
    f1 = 2.0 * precision * recall / (precision + recall) if precision + recall else 0.0
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


def _plot_rf_overlay(
    plot_dir, filename, title, results, truth, t0, dt, failed_methods=None
):
    if plot_dir is None:
        return
    plt = _import_matplotlib_pyplot()

    if results:
        npts = min(matrix.shape[1] for matrix in results.values())
    else:
        npts = truth.npts
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
            label="truth sparse impulse response",
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
    failed_methods = failed_methods or []
    for name in failed_methods:
        axes[0].plot(
            [],
            [],
            color="0.45",
            linestyle=":",
            linewidth=1.2,
            label=f"{name} (failed)",
        )

    axes[-1].set_xlabel("Lag time relative to direct P sample (s)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.suptitle(
        title + " (method traces are vertically offset and normalized independently)"
    )
    bottom_margin = 0.22 if labels else 0.08
    fig.tight_layout(rect=(0.0, bottom_margin, 1.0, 0.95))
    if handles:
        max_label = max(len(label) for label in labels)
        ncol = 1 if max_label > 55 else min(3, len(labels))
        fig.legend(
            handles,
            labels,
            loc="lower center",
            bbox_to_anchor=(0.5, 0.01),
            ncol=ncol,
            fontsize="x-small",
            frameon=False,
        )
    fig.savefig(plot_dir / filename, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _plot_offset_components(
    axis,
    matrix,
    t0,
    dt,
    component_names=("EW", "NS", "Z"),
    title=None,
    color="C0",
    linewidth=1.0,
):
    data = np.asarray(matrix)
    npts = data.shape[1]
    t = t0 + np.arange(npts) * dt
    scale = np.nanmax(np.abs(data))
    if scale == 0.0 or not np.isfinite(scale):
        scale = 1.0
    offset_step = 1.8
    for component, component_name in enumerate(component_names):
        offset = offset_step * (len(component_names) - component - 1)
        axis.plot(
            t,
            data[component, :] / scale + offset,
            color=color,
            linewidth=linewidth,
        )
        axis.text(
            0.01,
            offset,
            component_name,
            transform=axis.get_yaxis_transform(),
            va="center",
            ha="left",
            fontsize="small",
        )
    if title is not None:
        axis.set_title(title)
    axis.set_yticks([])
    axis.grid(True, color="0.88", linewidth=0.6)
    return t


def _windowed_threec_rms(matrix, t0, dt, window):
    data = np.asarray(matrix, dtype=np.float64)
    t = t0 + np.arange(data.shape[1]) * dt
    mask = (t >= window.start) & (t <= window.end)
    if not np.any(mask):
        return 0.0
    return float(np.sqrt(np.mean(data[:, mask] ** 2)))


def _plot_sparse_impulse_norm(axis, truth, title=None):
    truth_matrix = np.asarray(truth.data)
    t = truth.t0 + np.arange(truth.npts) * truth.dt
    impulse_norm = np.linalg.norm(truth_matrix, axis=0)
    scale = np.max(np.abs(impulse_norm))
    if scale == 0.0:
        scale = 1.0
    nonzero = np.flatnonzero(impulse_norm)
    axis.vlines(
        t[nonzero],
        0.0,
        impulse_norm[nonzero] / scale,
        color="black",
        linewidth=1.0,
    )
    axis.axhline(0.0, color="0.45", linewidth=0.8)
    axis.set_ylim(-0.05, 1.05)
    axis.set_yticks([])
    if title is not None:
        axis.set_title(title)
    axis.grid(True, color="0.9", linewidth=0.6)


def _plot_sparse_impulse_components(
    axis,
    truth,
    component_names=("EW", "NS", "Z"),
    title=None,
):
    truth_matrix = np.asarray(truth.data)
    t = truth.t0 + np.arange(truth.npts) * truth.dt
    scale = np.nanmax(np.abs(truth_matrix))
    if scale == 0.0 or not np.isfinite(scale):
        scale = 1.0
    offset_step = 1.8
    for component, component_name in enumerate(component_names):
        values = truth_matrix[component, :] / scale
        nonzero = np.flatnonzero(values)
        offset = offset_step * (len(component_names) - component - 1)
        axis.axhline(offset, color="0.7", linewidth=0.7)
        axis.vlines(
            t[nonzero],
            offset,
            offset + values[nonzero],
            color="black",
            linewidth=1.0,
        )
        axis.text(
            0.01,
            offset,
            component_name,
            transform=axis.get_yaxis_transform(),
            va="center",
            ha="left",
            fontsize="small",
        )
    if title is not None:
        axis.set_title(title)
    axis.set_yticks([])
    axis.set_ylim(-1.1, offset_step * (len(component_names) - 1) + 1.1)
    axis.grid(True, color="0.9", linewidth=0.6)


def _plot_complex_colored_results(plot_dir, results, truth, wavelet, data, noise_scale):
    if plot_dir is None:
        return
    _plot_rf_overlay(
        plot_dir,
        "complex_colored_scalar_methods.png",
        "Scalar and CNR deconvolution validation on colored 3C synthetic",
        results,
        truth,
        SIGNAL_WINDOW.start,
        truth.dt,
    )

    plt = _import_matplotlib_pyplot()
    tw = wavelet.t0 + np.arange(wavelet.npts) * wavelet.dt

    fig, axes = plt.subplots(
        3,
        1,
        figsize=(12, 6.8),
        sharex=False,
        gridspec_kw={"height_ratios": [1.0, 1.5, 1.2]},
    )
    axes[0].plot(tw, np.asarray(wavelet.data), color="black", linewidth=1.2)
    axes[0].set_title("Notched source wavelet")
    axes[0].set_ylabel("Amp")
    axes[0].grid(True, color="0.85", linewidth=0.6)

    pre_event_rms = _windowed_threec_rms(data.data, data.t0, data.dt, NOISE_WINDOW)
    signal_rms = _windowed_threec_rms(data.data, data.t0, data.dt, SIGNAL_WINDOW)
    _plot_offset_components(
        axes[1],
        data.data,
        data.t0,
        data.dt,
        title=(
            "Convolved 3C data with colored noise "
            f"(normalized display, noise scale={noise_scale:g})"
        ),
        color="C0",
    )
    axes[1].text(
        0.99,
        0.94,
        f"pre-event RMS={pre_event_rms:.3g}\nsignal RMS={signal_rms:.3g}",
        transform=axes[1].transAxes,
        ha="right",
        va="top",
        fontsize="small",
        bbox={"facecolor": "white", "edgecolor": "0.75", "alpha": 0.85},
    )
    axes[1].set_xlim(SIGNAL_WINDOW.start - 2.0, 18.0)

    _plot_sparse_impulse_components(
        axes[2],
        truth,
        title="True sparse 3C impulse response",
    )
    axes[2].set_xlim(SIGNAL_WINDOW.start - 2.0, 18.0)
    axes[2].set_xlabel("Lag time relative to direct P sample (s)")
    fig.suptitle("Complex colored validation setup")
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.96))
    fig.savefig(plot_dir / "complex_colored_validation_wavelet.png", dpi=150)
    plt.close(fig)


def _plot_stress_results(
    plot_dir, results, truth, wavelet, t0, dt, prefix, failed_methods=None
):
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
        failed_methods=failed_methods,
    )


def _plot_gid_mode_results(plot_dir, engine_name, results, truth, t0, dt):
    _plot_rf_overlay(
        plot_dir,
        f"{engine_name}_inverse_modes.png",
        f"{engine_name} validation by core inverse mode and penalty helper",
        results,
        truth,
        t0,
        dt,
    )


def _plot_gid_sparse_results(
    plot_dir, engine_name, results, truth, t0, dt, suffix, failed_methods=None
):
    _plot_rf_overlay(
        plot_dir,
        f"{engine_name}_{suffix}_sparse_results.png",
        f"{engine_name} raw sparse spike output by core inverse and penalty ({suffix})",
        results,
        truth,
        t0,
        dt,
        failed_methods=failed_methods,
    )


def _gid_plot_label(core_method, qc):
    qc = dict(qc)
    if qc.get("group_sparse_enabled", False):
        return (
            f"group_sparse/{qc['group_sparse_inverse_operator']}"
            f" lam={qc['group_sparse_lambda_used']:.3g}"
            f" thr={qc['group_sparse_active_threshold_used']:.3g}"
            f" k={qc['group_sparse_active_groups']}"
            f" it={qc['group_sparse_iterations']}"
        )
    try:
        adaptive_enabled = qc["gid_adaptive_penalty_enabled"]
    except Exception:
        adaptive_enabled = False
    label = (
        f"{core_method}/{qc['gid_penalty_function']}"
        f" s={qc['gid_penalty_scale_factor']:g}"
        f" W={qc['gid_penalty_effective_width']}"
    )
    if adaptive_enabled:
        label += (
            f" conf={qc['gid_penalty_last_confidence']:.2f}"
            f" decay={qc['gid_penalty_last_decay_factor']:.2f}"
        )
    return label


def _plot_gid_penalty_comparison(
    plot_dir,
    engine_name,
    core_method,
    shaped_results,
    sparse_results,
    lag_weights,
    truth,
    shaped_t0,
    sparse_t0,
    dt,
    weight_t0,
    suffix,
    failed_methods=None,
):
    if plot_dir is None:
        return
    failed_methods = failed_methods or []
    _plot_rf_overlay(
        plot_dir,
        f"{engine_name}_{suffix}_{core_method}_penalty_shaped_results.png",
        (
            f"{engine_name} penalty comparison with {core_method} core inverse, "
            "shaped receiver functions"
        ),
        shaped_results,
        truth,
        shaped_t0,
        dt,
        failed_methods=failed_methods,
    )
    _plot_rf_overlay(
        plot_dir,
        f"{engine_name}_{suffix}_{core_method}_penalty_sparse_results.png",
        (
            f"{engine_name} penalty comparison with {core_method} core inverse, "
            "raw sparse outputs"
        ),
        sparse_results,
        truth,
        sparse_t0,
        dt,
        failed_methods=failed_methods,
    )

    plt = _import_matplotlib_pyplot()
    if lag_weights:
        npts = min(len(weights) for weights in lag_weights.values())
        t = weight_t0 + np.arange(npts) * dt
    else:
        npts = truth.npts
        t = truth.t0 + np.arange(npts) * truth.dt
    colors = plt.get_cmap("tab10")
    fig, (axis, truth_axis) = plt.subplots(
        2,
        1,
        figsize=(12, 4.8),
        sharex=True,
        gridspec_kw={"height_ratios": [3.0, 1.0], "hspace": 0.08},
    )
    for index, (name, weights) in enumerate(lag_weights.items()):
        axis.plot(
            t,
            weights[:npts],
            linewidth=1.3,
            color=colors(index % 10),
            label=name,
        )
    for name in failed_methods:
        axis.plot(
            [],
            [],
            color="0.45",
            linestyle=":",
            linewidth=1.2,
            label=f"{name} (failed)",
        )
    if not lag_weights:
        axis.text(
            0.5,
            0.5,
            "No live penalty runs for this diagnostic noise level",
            transform=axis.transAxes,
            ha="center",
            va="center",
            color="0.35",
        )
    for arrival_time in STRESS_SPIKES:
        axis.axvline(arrival_time, color="0.75", linewidth=0.8, linestyle=":")
    axis.axhline(1.0, color="0.85", linewidth=0.8)
    axis.set_ylim(-0.05, 1.05)
    axis.set_ylabel("Final lag weight")
    axis.set_title(
        f"{engine_name} final lag weights after GID iteration ({core_method} core)"
    )
    axis.grid(True, color="0.88", linewidth=0.6)
    handles, labels = axis.get_legend_handles_labels()
    if handles:
        axis.legend(
            handles,
            labels,
            loc="upper left",
            bbox_to_anchor=(1.01, 1.0),
            fontsize="x-small",
            frameon=False,
        )
    _plot_sparse_impulse_norm(
        truth_axis,
        truth,
    )
    truth_axis.set_ylabel("True\nimpulse")
    truth_axis.set_xlabel("Lag time relative to direct P sample (s)")
    truth_axis.set_xlim(t[0], t[-1])
    fig.subplots_adjust(left=0.08, right=0.74, top=0.86, bottom=0.14, hspace=0.08)
    fig.savefig(
        plot_dir / f"{engine_name}_{suffix}_{core_method}_penalty_lag_weights.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close(fig)


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
    display_results = _apply_common_display_filter(results, dt)
    _plot_rf_overlay(
        plot_dir,
        "external_wavelet_all_methods_display_filtered.png",
        "External prepared wavelet validation with common display filter",
        display_results,
        truth,
        t0,
        dt,
    )


def _apply_common_display_filter(results, dt, frequency=1.0):
    """Apply a plotting-only Ricker filter to compare visual bandwidths.

    The numerical validation uses the raw operator outputs.  This helper is
    only used for optional plots so scalar inverse methods are not given a
    hidden shaping wavelet in the algorithm under test.
    """

    half_width = max(1, int(round(2.0 / (frequency * dt))))
    t = np.arange(-half_width, half_width + 1, dtype=np.float64) * dt
    arg = (np.pi * frequency * t) ** 2
    kernel = (1.0 - 2.0 * arg) * np.exp(-arg)
    peak = np.max(np.abs(kernel))
    if peak > 0.0:
        kernel = kernel / peak
    filtered = {}
    for name, matrix in results.items():
        filtered[name] = np.vstack(
            [signal.fftconvolve(component, kernel, mode="same") for component in matrix]
        )
    return filtered


@pytest.mark.parametrize("noise_level", [0.0, 1.0e-6])
@pytest.mark.parametrize(
    "algorithm",
    [
        "LeastSquares",
        "TimeDomainLeastSquares",
        "WaterLevel",
        "MultiTaperPowerXcor",
        "MultiTaperPowerSpecDiv",
    ],
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
        "TimeDomainLeastSquares": _scalar_rf_matrix("TimeDomainLeastSquares", data),
        "WaterLevel": _scalar_rf_matrix("WaterLevel", data),
        "MultiTaperPowerXcor": _scalar_rf_matrix("MultiTaperPowerXcor", data),
        "MultiTaperPowerSpecDiv": _scalar_rf_matrix("MultiTaperPowerSpecDiv", data),
        "NoiseStable": _noise_stable_rf_matrix(data),
        "CNR": _cnr_rf_matrix(data),
    }

    assert (
        _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.97
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["TimeDomainLeastSquares"]
        )
        > 0.93
    )
    assert (
        _normalized_correlation(results["LeastSquares"], results["MultiTaperPowerXcor"])
        > 0.88
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.88
    )
    assert (
        _normalized_correlation(
            results["TimeDomainLeastSquares"], results["MultiTaperPowerXcor"]
        )
        > 0.86
    )
    assert (
        _normalized_correlation(results["WaterLevel"], results["MultiTaperPowerXcor"])
        > 0.93
    )
    assert (
        _normalized_correlation(
            results["WaterLevel"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.90
    )
    assert (
        _normalized_correlation(
            results["MultiTaperPowerXcor"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.93
    )


def test_scalar_methods_are_consistent_for_complex_colored_3c_synthetic(
    decon_validation_plot_dir,
    decon_validation_noise_scale,
):
    data, truth, wavelet = _make_complex_colored_validation_data(return_truth=True)
    results = {
        "LeastSquares": _scalar_rf_matrix("LeastSquares", data),
        "TimeDomainLeastSquares": _scalar_rf_matrix("TimeDomainLeastSquares", data),
        "WaterLevel": _scalar_rf_matrix("WaterLevel", data),
        "MultiTaperPowerXcor": _scalar_rf_matrix("MultiTaperPowerXcor", data),
        "MultiTaperPowerSpecDiv": _scalar_rf_matrix("MultiTaperPowerSpecDiv", data),
        "NoiseStable": _noise_stable_rf_matrix(data),
        "CNR": _cnr_rf_matrix(data),
    }

    for name, result in results.items():
        assert np.isfinite(result).all()
        _assert_scalar_result_is_not_discrete_sparse_output(result, name)
        _assert_direct_arrival_is_recovered(result, ratio_tol=1.0e-1)
        _assert_colored_transverse_arrivals_are_recovered(result)

    cnr_engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    cnr_rf = CNRRFDecon(
        data,
        cnr_engine,
        signal_window=SIGNAL_WINDOW,
        noise_window=NOISE_WINDOW,
    )
    assert cnr_rf.live
    assert "CNRFDecon_properties" in cnr_rf
    cnr_qc = dict(cnr_rf["CNRFDecon_properties"])
    assert cnr_qc["decon_operator"] == "CNRDeconEngine"
    assert "decon_processed" in cnr_qc
    assert cnr_qc["prediction_error"] >= 0.0

    noise_stable_qc = _noise_stable_qc(data)
    assert noise_stable_qc["decon_operator"] == "NoiseStableDecon"
    assert noise_stable_qc["decon_processed"]
    assert noise_stable_qc["ns_gid_gain_max_actual"] <= (
        noise_stable_qc["ns_gid_gain_max_requested"] * (1.0 + 1.0e-10)
    )

    assert (
        _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.95
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["TimeDomainLeastSquares"]
        )
        > 0.92
    )
    assert (
        _normalized_correlation(results["LeastSquares"], results["MultiTaperPowerXcor"])
        > 0.84
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.84
    )
    assert (
        _normalized_correlation(
            results["MultiTaperPowerXcor"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.90
    )
    plot_data = data
    plot_truth = truth
    plot_wavelet = wavelet
    plot_results = results
    if decon_validation_plot_dir is not None and decon_validation_noise_scale != 0.025:
        plot_data, plot_truth, plot_wavelet = _make_complex_colored_validation_data(
            noise_scale=decon_validation_noise_scale,
            return_truth=True,
        )
        plot_results = {
            "LeastSquares": _scalar_rf_matrix("LeastSquares", plot_data),
            "TimeDomainLeastSquares": _scalar_rf_matrix(
                "TimeDomainLeastSquares", plot_data
            ),
            "WaterLevel": _scalar_rf_matrix("WaterLevel", plot_data),
            "MultiTaperPowerXcor": _scalar_rf_matrix("MultiTaperPowerXcor", plot_data),
            "MultiTaperPowerSpecDiv": _scalar_rf_matrix(
                "MultiTaperPowerSpecDiv", plot_data
            ),
            "NoiseStable": _noise_stable_rf_matrix(plot_data),
            "CNR": _cnr_rf_matrix(plot_data),
        }
    _plot_complex_colored_results(
        decon_validation_plot_dir,
        plot_results,
        plot_truth,
        plot_wavelet,
        plot_data,
        decon_validation_noise_scale,
    )


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
        "TimeDomainLeastSquares": _scalar_rf_matrix("TimeDomainLeastSquares", data),
        "WaterLevel": _scalar_rf_matrix("WaterLevel", data),
        "MultiTaperPowerXcor": _scalar_rf_matrix("MultiTaperPowerXcor", data),
        "MultiTaperPowerSpecDiv": _scalar_rf_matrix("MultiTaperPowerSpecDiv", data),
        "NoiseStable": _noise_stable_rf_matrix(data),
        "CNR": _cnr_rf_matrix(data),
    }

    for name, result in results.items():
        assert np.isfinite(result).all()
        _assert_scalar_result_is_not_discrete_sparse_output(result, name)
        _assert_stress_arrivals_recovered(result, SIGNAL_WINDOW.start, truth.dt)

    assert (
        _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.93
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["TimeDomainLeastSquares"]
        )
        > 0.90
    )
    assert (
        _normalized_correlation(results["LeastSquares"], results["MultiTaperPowerXcor"])
        > 0.82
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.82
    )
    assert (
        _normalized_correlation(
            results["MultiTaperPowerXcor"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.90
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
            "TimeDomainLeastSquares": _scalar_rf_matrix(
                "TimeDomainLeastSquares", plot_data
            ),
            "WaterLevel": _scalar_rf_matrix("WaterLevel", plot_data),
            "MultiTaperPowerXcor": _scalar_rf_matrix("MultiTaperPowerXcor", plot_data),
            "MultiTaperPowerSpecDiv": _scalar_rf_matrix(
                "MultiTaperPowerSpecDiv", plot_data
            ),
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
        "TimeDomainLeastSquares": _scalar_rf_matrix_external_wavelet(
            "TimeDomainLeastSquares", data, wavelet
        ),
        "WaterLevel": _scalar_rf_matrix_external_wavelet("WaterLevel", data, wavelet),
        "MultiTaperPowerXcor": _scalar_rf_matrix_external_wavelet(
            "MultiTaperPowerXcor", data, wavelet
        ),
        "MultiTaperPowerSpecDiv": _scalar_rf_matrix_external_wavelet(
            "MultiTaperPowerSpecDiv", data, wavelet
        ),
        "NoiseStable": _noise_stable_rf_matrix(data, external_wavelet=wavelet),
        "CNR": _cnr_rf_matrix(data, external_wavelet=wavelet),
    }
    gid_sparse_names = []
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
        modes = ("ns_gid", "group_sparse")
        for mode in modes:
            pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, mode)
            engine = engine_class(pf)
            rf = wrapper(
                data,
                engine,
                signal_window=TimeWindow(-8.0, 20.0),
                noise_window=TimeWindow(-35.0, -5.0),
                external_wavelet=wavelet,
            )
            assert rf.live
            prefix = f"{engine_class.__name__}:{mode}"
            results[prefix] = np.asarray(rf.data)
            sparse_name = f"{prefix}_sparse"
            results[sparse_name] = np.asarray(engine.sparse_output().data)
            gid_sparse_names.append(sparse_name)
            qc = dict(rf[f"{engine_class.__name__}_properties"])
            if mode == "ns_gid":
                assert qc["ns_gid_external_wavelet_used"]
                assert qc["ns_gid_gain_max_actual"] <= qc[
                    "ns_gid_gain_max_requested"
                ] * (1.0 + 1.0e-10)
            else:
                assert qc["group_sparse_enabled"]
                assert qc["group_sparse_inverse_external_wavelet_used"]
                assert qc["group_sparse_inverse_gain_max_actual"] <= qc[
                    "group_sparse_inverse_gain_max_requested"
                ] * (1.0 + 1.0e-10)

    reference = results["LeastSquares"]
    amp = np.sqrt(reference[0, :] ** 2 + reference[1, :] ** 2 + reference[2, :] ** 2)
    external_wavelet_shift = SIGNAL_WINDOW.start + 0.05 * int(np.argmax(amp))
    shifted_truth = _shift_truth_time(truth, external_wavelet_shift)
    for name, result in results.items():
        assert np.isfinite(result).all(), name
        if "GIDDecon" not in name and name != "NoiseStable":
            _assert_shifted_direct_arrival_recovered(
                result,
                SIGNAL_WINDOW.start,
                truth.dt,
                external_wavelet_shift,
                name=name,
            )

    assert (
        _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.95
    )
    assert (
        _normalized_correlation(
            results["LeastSquares"], results["TimeDomainLeastSquares"]
        )
        > 0.90
    )
    assert (
        _normalized_correlation(results["LeastSquares"], results["MultiTaperPowerXcor"])
        > 0.84
    )
    assert (
        _normalized_correlation(
            results["MultiTaperPowerXcor"], results["MultiTaperPowerSpecDiv"]
        )
        > 0.95
    )

    shifted_truth_times = (
        np.asarray(sorted(STRESS_SPIKES.keys()), dtype=np.float64)
        + external_wavelet_shift
    )
    for sparse_name in gid_sparse_names:
        metrics = _classify_spike_detections_against_times(
            results[sparse_name],
            -8.0,
            truth.dt,
            shifted_truth_times,
        )
        assert metrics["recall"] >= 0.95, sparse_name
        if "FrequencyDomainGIDDecon" in sparse_name or "group_sparse" in sparse_name:
            assert metrics["precision"] >= 0.75, sparse_name
        else:
            assert metrics["precision"] >= 0.45, sparse_name

    plot_npts = int(round((20.0 - SIGNAL_WINDOW.start) / truth.dt)) + 1
    plot_results = {}
    for name, result in results.items():
        if "GIDDecon" in name and not name.endswith("_sparse"):
            continue
        plot_name = name.replace("_sparse", ":sparse")
        src_t0 = -8.0 if name.endswith("_sparse") else SIGNAL_WINDOW.start
        plot_results[name] = _slice_matrix_to_window(
            result, src_t0, truth.dt, SIGNAL_WINDOW.start, plot_npts
        )
        if plot_name != name:
            plot_results[plot_name] = plot_results.pop(name)

    _plot_external_wavelet_results(
        decon_validation_plot_dir,
        plot_results,
        shifted_truth,
        SIGNAL_WINDOW.start,
        truth.dt,
    )


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
def test_group_sparse_adaptive_support_threshold_controls_clustered_coefficients(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key
):
    data, truth, wavelet = _make_stress_colored_validation_data(
        noise_scale=0.01,
        return_truth=True,
    )
    reference = _scalar_rf_matrix_external_wavelet("LeastSquares", data, wavelet)
    amp = np.sqrt(reference[0, :] ** 2 + reference[1, :] ** 2 + reference[2, :] ** 2)
    external_wavelet_shift = SIGNAL_WINDOW.start + truth.dt * int(np.argmax(amp))
    shifted_truth_times = (
        np.asarray(sorted(STRESS_SPIKES.keys()), dtype=np.float64)
        + external_wavelet_shift
    )

    def run_group_sparse(replacements=None):
        replacements = replacements or {}
        pf = _pf_with_gid_mode_and_replacements(
            tmp_path,
            pfname,
            branch_name,
            "group_sparse",
            replacements,
        )
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
            external_wavelet=wavelet,
        )
        assert rf.live
        sparse = engine.sparse_output()
        metrics = _classify_spike_detections_against_times(
            np.asarray(sparse.data),
            sparse.t0,
            sparse.dt,
            shifted_truth_times,
        )
        return metrics, dict(rf[qc_key])

    default_metrics, default_qc = run_group_sparse()
    fixed_floor_metrics, fixed_floor_qc = run_group_sparse(
        {
            "group_sparse_active_threshold 0.02": (
                "group_sparse_active_threshold 0.000000001"
            ),
            "group_sparse_active_threshold_scale 1.0": (
                "group_sparse_active_threshold_scale 0.0"
            ),
        }
    )

    assert default_qc["group_sparse_active_threshold_quantile"] == pytest.approx(0.90)
    assert (
        default_qc["group_sparse_active_threshold_used"]
        > default_qc["group_sparse_active_threshold"]
    )
    assert default_metrics["precision"] == pytest.approx(1.0)
    assert default_metrics["recall"] == pytest.approx(1.0)
    assert default_qc["group_sparse_active_groups"] == len(STRESS_SPIKES)

    assert fixed_floor_qc["group_sparse_active_threshold_used"] == pytest.approx(1.0e-9)
    assert fixed_floor_qc["group_sparse_active_groups"] > 100
    assert fixed_floor_metrics["precision"] < default_metrics["precision"]
    assert fixed_floor_metrics["f1"] < default_metrics["f1"]


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, modes, qc_key",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            [
                "least_square",
                "water_level",
                "multi_taper",
                "cnr",
                "ns_gid",
                "group_sparse",
            ],
            "TimeDomainGIDDecon_properties",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            [
                "least_square",
                "water_level",
                "multi_taper",
                "cnr",
                "ns_gid",
                "group_sparse",
            ],
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
        qc = rf[qc_key]
        assert qc["residual_L2_final"] < qc["residual_L2_initial"]
        plot_label = _gid_plot_label(mode, qc)
        plot_results[plot_label] = np.asarray(rf.data)
        plot_t0 = rf.t0
        plot_dt = rf.dt
        sparse = engine.sparse_output()
        plot_sparse_results[plot_label] = np.asarray(sparse.data)
        sparse_matrix = np.asarray(sparse.data)
        metrics = _classify_spike_detections_against_times(
            sparse_matrix,
            sparse.t0,
            sparse.dt,
            np.asarray([2.5, 3.0, 7.5, 9.0], dtype=np.float64),
            detection_threshold=0.05,
            match_tolerance=0.20,
        )
        if mode == "group_sparse":
            _assert_group_sparse_detection_metrics(metrics)
            assert qc["group_sparse_enabled"]
        else:
            assert metrics["recall"] >= 0.95, mode
            assert metrics["f1"] >= 0.30, mode
            assert metrics["false_positive"] <= 16, mode
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
        {
            "lag_weight_function_width 5\n\n"
            "        shaping_wavelet_dt 0.05\n"
            "        shaping_wavelet_type ricker\n"
            "        shaping_wavelet_frequency 1.0": (
                "lag_weight_function_width 5\n\n"
                "        shaping_wavelet_dt 0.05\n"
                "        shaping_wavelet_type ricker\n"
                "        shaping_wavelet_frequency 2.0"
            )
        },
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
            [
                "least_square",
                "water_level",
                "multi_taper",
                "cnr",
                "ns_gid",
                "group_sparse",
            ],
            "TimeDomainGIDDecon_properties",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            [
                "least_square",
                "water_level",
                "multi_taper",
                "cnr",
                "ns_gid",
                "group_sparse",
            ],
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
        qc = rf[qc_key]
        assert qc["residual_L2_final"] < qc["residual_L2_initial"]
        plot_label = _gid_plot_label(mode, qc)
        matrix = np.asarray(rf.data)
        sparse = engine.sparse_output()
        sparse_matrix = np.asarray(sparse.data)
        if mode == "group_sparse":
            metrics = _classify_gid_spike_detections(
                sparse_matrix, sparse.t0, sparse.dt
            )
            _assert_group_sparse_detection_metrics(metrics)
            assert qc["group_sparse_enabled"]
        else:
            _assert_stress_gid_signs_recovered(sparse_matrix, sparse.t0, sparse.dt)
        plot_results[plot_label] = matrix
        plot_sparse_results[plot_label] = sparse_matrix
        plot_t0 = rf.t0
        plot_dt = rf.dt

    plot_truth = truth
    skipped_plot_modes = []
    if decon_validation_plot_dir is not None and decon_validation_noise_scale != 0.01:
        plot_data, plot_truth, _ = _make_stress_colored_validation_data(
            noise_scale=decon_validation_noise_scale,
            return_truth=True,
        )
        plot_results = {}
        plot_sparse_results = {}
        skipped_plot_modes = []
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
                qc = rf[qc_key]
                plot_label = _gid_plot_label(mode, qc)
                plot_results[plot_label] = np.asarray(rf.data)
                plot_sparse_results[plot_label] = np.asarray(
                    engine.sparse_output().data
                )
                plot_t0 = rf.t0
                plot_dt = rf.dt
            else:
                skipped_plot_modes.append(mode)

    _plot_stress_results(
        decon_validation_plot_dir,
        plot_results,
        plot_truth,
        None,
        plot_t0,
        plot_dt,
        f"{engine_class.__name__}_noise_{decon_validation_noise_scale:g}",
        failed_methods=skipped_plot_modes,
    )
    _plot_gid_sparse_results(
        decon_validation_plot_dir,
        engine_class.__name__,
        plot_sparse_results,
        plot_truth,
        plot_t0,
        plot_dt,
        f"noise_{decon_validation_noise_scale:g}_stress",
        failed_methods=skipped_plot_modes,
    )


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
def test_group_sparse_recovers_weak_arrivals_like_adaptive_memory(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key
):
    for noise_scale in (0.0, 0.03):
        data, _, _ = _make_weak_stress_colored_validation_data(
            noise_scale=noise_scale,
            return_truth=True,
        )
        results = {}
        for label, mode in [
            ("adaptive_memory_least_square", "least_square"),
            ("adaptive_memory_ns_gid", "ns_gid"),
            ("group_sparse", "group_sparse"),
        ]:
            pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, mode)
            engine = engine_class(pf)
            rf = wrapper(
                data,
                engine,
                signal_window=TimeWindow(-8.0, 20.0),
                noise_window=TimeWindow(-35.0, -5.0),
            )
            assert rf.live, (engine_class.__name__, label, noise_scale)
            qc = dict(rf[qc_key])
            sparse = engine.sparse_output()
            metrics = _classify_gid_spike_detections(
                np.asarray(sparse.data),
                sparse.t0,
                sparse.dt,
                detection_threshold=0.025,
            )
            results[label] = (metrics, qc)

        legacy_metrics, legacy_qc = results["adaptive_memory_least_square"]
        assert legacy_qc["gid_penalty_function"] == "adaptive_memory"
        assert legacy_metrics["recall"] >= 0.95, noise_scale
        assert legacy_metrics["precision"] >= 0.75, noise_scale

        stable_metrics, stable_qc = results["adaptive_memory_ns_gid"]
        assert stable_qc["gid_penalty_function"] == "adaptive_memory"
        assert stable_qc["deconvolution_type"] == DEFAULT_GID_DECONVOLUTION_TYPE
        assert stable_metrics["recall"] >= 0.95, noise_scale
        assert stable_metrics["precision"] >= 0.95, noise_scale
        assert stable_metrics["f1"] >= legacy_metrics["f1"] - 1.0e-12
        assert stable_metrics["false_positive"] <= legacy_metrics["false_positive"]

        group_metrics, group_qc = results["group_sparse"]
        assert group_qc["group_sparse_enabled"]
        assert group_qc["group_sparse_active_threshold"] == pytest.approx(0.02)
        assert group_qc["group_sparse_active_threshold_quantile"] == pytest.approx(0.90)
        assert (
            group_qc["group_sparse_active_threshold_used"]
            >= group_qc["group_sparse_active_threshold"]
        )
        assert group_metrics["recall"] >= stable_metrics["recall"] - 1.0e-12
        assert group_metrics["precision"] >= stable_metrics["precision"] - 1.0e-12
        assert group_metrics["f1"] >= legacy_metrics["f1"] - 1.0e-12
        assert group_metrics["false_positive"] <= stable_metrics["false_positive"]


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
def test_gid_lag_penalty_improves_stress_fit_and_plots_diagnostics(
    tmp_path,
    decon_validation_plot_dir,
    decon_validation_noise_scale,
    engine_class,
    wrapper,
    pfname,
    branch_name,
    qc_key,
):
    def run_cases(noise_scale, require_live=True):
        data, truth, _ = _make_stress_colored_validation_data(
            noise_scale=noise_scale,
            return_truth=True,
        )
        shaped_results = {}
        sparse_results = {}
        lag_weight_results = {}
        qcs = {}
        detection_metrics = {}
        failed_methods = []
        shaped_t0 = sparse_t0 = weight_t0 = -8.0
        dt = truth.dt
        cases = [
            (
                "none",
                {
                    "lag_weight_penalty_function adaptive_memory": "lag_weight_penalty_function none"
                },
            ),
            (
                "boxcar",
                {
                    "lag_weight_penalty_function adaptive_memory": "lag_weight_penalty_function boxcar"
                },
            ),
            (
                "shaping_wavelet",
                {
                    "lag_weight_penalty_function adaptive_memory": "lag_weight_penalty_function shaping_wavelet"
                },
            ),
            (
                "resolution_kernel",
                {
                    "lag_weight_penalty_function adaptive_memory": "lag_weight_penalty_function resolution_kernel"
                },
            ),
            ("adaptive_memory", {}),
            (
                "cosine_taper",
                {
                    "lag_weight_penalty_function adaptive_memory": "lag_weight_penalty_function cosine_taper"
                },
            ),
        ]
        for label, replacements in cases:
            pf = _pf_with_gid_mode_and_replacements(
                tmp_path,
                pfname,
                branch_name,
                "least_square",
                replacements,
            )
            engine = engine_class(pf)
            try:
                rf = wrapper(
                    data,
                    engine,
                    signal_window=TimeWindow(-8.0, 20.0),
                    noise_window=TimeWindow(-35.0, -5.0),
                )
            except Exception as err:
                if require_live:
                    raise
                failed_methods.append(f"{label} ({type(err).__name__})")
                continue
            if not rf.live:
                if require_live:
                    assert rf.live, label
                failed_methods.append(label)
                continue
            qc = rf[qc_key]
            plot_label = (
                f"{_gid_plot_label('least_square', qc)}; "
                f"iters={qc['iteration_count']} L2={qc['residual_L2_final']:.3g}"
            )
            shaped_results[plot_label] = np.asarray(rf.data)
            sparse = engine.sparse_output()
            sparse_matrix = np.asarray(sparse.data)
            sparse_results[plot_label] = sparse_matrix
            detection_metrics[qc["gid_penalty_function"]] = (
                _classify_gid_spike_detections(sparse_matrix, sparse.t0, sparse.dt)
            )
            lag_weight_results[plot_label] = np.asarray(engine.lag_weight_vector())
            qcs[qc["gid_penalty_function"]] = qc
            shaped_t0 = rf.t0
            sparse_t0 = sparse.t0
            weight_t0 = engine.deconvolution_window_start()
            dt = rf.dt
        return (
            truth,
            shaped_results,
            sparse_results,
            lag_weight_results,
            qcs,
            shaped_t0,
            sparse_t0,
            weight_t0,
            dt,
            detection_metrics,
            failed_methods,
        )

    (
        truth,
        shaped_results,
        sparse_results,
        lag_weight_results,
        qcs,
        shaped_t0,
        sparse_t0,
        weight_t0,
        dt,
        detection_metrics,
        failed_methods,
    ) = run_cases(0.01)

    assert failed_methods == []
    assert set(qcs) == {
        "none",
        "boxcar",
        "cosine_taper",
        "shaping_wavelet",
        "resolution_kernel",
        "adaptive_memory",
    }
    assert qcs["adaptive_memory"]["gid_penalty_scale_factor"] == pytest.approx(0.35)
    assert detection_metrics["adaptive_memory"]["recall"] >= 0.95
    assert detection_metrics["adaptive_memory"]["precision"] >= 0.95
    assert detection_metrics["adaptive_memory"]["false_positive"] <= 1
    assert qcs["cosine_taper"]["gid_penalty_effective_width"] == 5
    for adaptive_penalty in ("shaping_wavelet", "resolution_kernel"):
        assert qcs[adaptive_penalty]["gid_penalty_effective_width"] > 1
        assert (
            qcs[adaptive_penalty]["gid_penalty_effective_width"]
            != qcs[adaptive_penalty]["gid_penalty_width"]
        )
    assert qcs["adaptive_memory"]["gid_adaptive_penalty_enabled"]
    assert qcs["adaptive_memory"]["gid_penalty_effective_width"] >= 1
    assert qcs["adaptive_memory"]["gid_penalty_noise_amplitude"] > 0.0
    assert 0.0 <= qcs["adaptive_memory"]["gid_penalty_last_confidence"] < 1.0
    assert 0.0 <= qcs["adaptive_memory"]["gid_penalty_last_immediate_strength"] < 1.0
    assert 0.0 <= qcs["adaptive_memory"]["gid_penalty_last_specificity"] <= 1.0
    assert 0.0 <= qcs["adaptive_memory"]["gid_penalty_last_decay_factor"] < 1.0
    assert qcs["adaptive_memory"][
        "gid_penalty_last_immediate_strength"
    ] == pytest.approx(qcs["adaptive_memory"]["gid_penalty_last_confidence"])
    assert qcs["adaptive_memory"]["gid_penalty_last_decay_factor"] == pytest.approx(
        qcs["adaptive_memory"]["gid_penalty_last_confidence"]
        * qcs["adaptive_memory"]["gid_penalty_last_specificity"]
    )
    assert qcs["adaptive_memory"]["gid_penalty_memory_Linf_final"] > 0.0
    assert (
        qcs["adaptive_memory"]["iteration_count"]
        <= qcs["none"]["iteration_count"]
        + qcs["adaptive_memory"]["gid_penalty_effective_width"]
    )
    assert (
        qcs["adaptive_memory"]["residual_L2_final"]
        <= qcs["none"]["residual_L2_final"] * 1.05
    )
    assert (
        qcs["adaptive_memory"]["lag_weight_L2_final"]
        < qcs["none"]["lag_weight_L2_final"]
    )

    if decon_validation_plot_dir is not None and decon_validation_noise_scale != 0.01:
        (
            truth,
            shaped_results,
            sparse_results,
            lag_weight_results,
            _,
            shaped_t0,
            sparse_t0,
            weight_t0,
            dt,
            _,
            failed_methods,
        ) = run_cases(decon_validation_noise_scale, require_live=False)

    _plot_gid_penalty_comparison(
        decon_validation_plot_dir,
        engine_class.__name__,
        "least_square",
        shaped_results,
        sparse_results,
        lag_weight_results,
        truth,
        shaped_t0,
        sparse_t0,
        dt,
        weight_t0,
        f"noise_{decon_validation_noise_scale:g}",
        failed_methods=failed_methods,
    )


def _adaptive_memory_penalty_replacements():
    return {}


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
def test_gid_adaptive_memory_penalty_bounds_close_arrival_revisits(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key
):
    data = _make_close_arrival_cycling_validation_data(noise_scale=0.015)
    cases = {
        "resolution_kernel": {
            "lag_weight_penalty_function adaptive_memory": (
                "lag_weight_penalty_function resolution_kernel"
            ),
            "lag_weight_function_width 5": "lag_weight_function_width 51",
        },
        "adaptive_memory": _adaptive_memory_penalty_replacements(),
    }
    results = {}
    for label, replacements in cases.items():
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
        assert rf.live, label
        sparse = engine.sparse_output()
        sparse_matrix = np.asarray(sparse.data)
        results[label] = {
            "qc": dict(rf[qc_key]),
            "metrics": _classify_close_arrival_spike_detections(
                sparse_matrix, sparse.t0, sparse.dt
            ),
            "revisits": _count_unmatched_close_arrival_revisits(
                sparse_matrix, sparse.t0, sparse.dt
            ),
        }

    kernel = results["resolution_kernel"]
    adaptive = results["adaptive_memory"]
    adaptive_qc = adaptive["qc"]
    assert adaptive_qc["gid_penalty_function"] == "adaptive_memory"
    assert adaptive_qc["gid_adaptive_penalty_enabled"]
    assert adaptive_qc["gid_penalty_effective_width"] >= 1
    assert adaptive_qc["gid_penalty_noise_amplitude"] > 0.0
    assert 0.0 <= adaptive_qc["gid_penalty_last_confidence"] < 1.0
    assert 0.0 <= adaptive_qc["gid_penalty_last_immediate_strength"] < 1.0
    assert 0.0 <= adaptive_qc["gid_penalty_last_specificity"] <= 1.0
    assert 0.0 <= adaptive_qc["gid_penalty_last_decay_factor"] < 1.0
    assert adaptive_qc["gid_penalty_memory_Linf_final"] > 0.0
    assert adaptive_qc["gid_penalty_memory_L2_final"] > 0.0

    assert adaptive["metrics"]["recall"] >= 0.75
    assert (
        adaptive["metrics"]["false_positive"] <= kernel["metrics"]["false_positive"] + 2
    )
    assert adaptive["revisits"] <= kernel["revisits"] + 1
    assert adaptive_qc["residual_L2_final"] < adaptive_qc["residual_L2_initial"]


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
def test_gid_none_penalty_preserves_dense_close_arrival_support(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key
):
    data = _make_close_arrival_cycling_validation_data(noise_scale=0.015)
    cases = {
        "none": {
            "lag_weight_penalty_function adaptive_memory": (
                "lag_weight_penalty_function none"
            ),
        },
        "adaptive_memory": _adaptive_memory_penalty_replacements(),
    }
    results = {}
    for label, replacements in cases.items():
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
        assert rf.live, label
        sparse = engine.sparse_output()
        results[label] = {
            "qc": dict(rf[qc_key]),
            "metrics": _classify_close_arrival_spike_detections(
                np.asarray(sparse.data), sparse.t0, sparse.dt
            ),
        }

    no_penalty = results["none"]
    adaptive = results["adaptive_memory"]
    assert no_penalty["qc"]["gid_penalty_function"] == "none"
    assert adaptive["qc"]["gid_penalty_function"] == "adaptive_memory"
    assert no_penalty["metrics"]["recall"] >= adaptive["metrics"]["recall"]
    assert (
        no_penalty["metrics"]["false_positive"] <= adaptive["metrics"]["false_positive"]
    )


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
@pytest.mark.parametrize("noise_scale", [3.0, 10.0, 30.0, 100.0])
def test_gid_adaptive_memory_does_not_overlearn_noise_maxima(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key, noise_scale
):
    data = _make_direct_only_noisy_validation_data(noise_scale=noise_scale)
    cases = {
        "none": {
            "lag_weight_penalty_function adaptive_memory": (
                "lag_weight_penalty_function none"
            ),
        },
        "adaptive_memory": _adaptive_memory_penalty_replacements(),
    }
    results = {}
    for label, replacements in cases.items():
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
        assert rf.live, label
        sparse = engine.sparse_output()
        sparse_matrix = np.asarray(sparse.data)
        results[label] = {
            "qc": dict(rf[qc_key]),
            "peak_count": len(
                _detected_sparse_spike_times(
                    sparse_matrix,
                    sparse.t0,
                    sparse.dt,
                    detection_threshold=0.08,
                )
            ),
            "transverse_l2": np.linalg.norm(sparse_matrix[:2, :]),
        }

    no_penalty = results["none"]
    adaptive = results["adaptive_memory"]
    adaptive_qc = adaptive["qc"]
    assert adaptive_qc["gid_penalty_function"] == "adaptive_memory"
    assert adaptive_qc["gid_penalty_noise_amplitude"] > 0.0
    assert 0.0 <= adaptive_qc["gid_penalty_last_confidence"] < 1.0
    assert 0.0 <= adaptive_qc["gid_penalty_last_immediate_strength"] < 1.0
    assert (
        adaptive_qc["gid_penalty_last_decay_factor"]
        <= adaptive_qc["gid_penalty_last_immediate_strength"] + 1.0e-12
    )
    assert adaptive["peak_count"] <= no_penalty["peak_count"] + 2
    assert adaptive["transverse_l2"] <= no_penalty["transverse_l2"] * 1.10 + 1.0e-10
    if noise_scale >= 10.0:
        assert abs(adaptive["peak_count"] - no_penalty["peak_count"]) <= 2


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
def test_legacy_gid_modes_continue_with_elevated_noise_floor(
    tmp_path, engine_class, wrapper, pfname, branch_name, qc_key
):
    data, _, _ = _make_stress_colored_validation_data(
        noise_scale=0.3,
        return_truth=True,
    )
    for mode in ["least_square", "water_level", "multi_taper", "cnr"]:
        pf = _pf_with_gid_mode(
            tmp_path,
            pfname,
            branch_name,
            mode,
        )
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-35.0, -5.0),
        )
        assert rf.live, mode
        qc = rf[qc_key]
        assert qc["iteration_count"] > 5, mode
        sparse = engine.sparse_output()
        metrics = _classify_gid_spike_detections(
            np.asarray(sparse.data), sparse.t0, sparse.dt
        )
        assert metrics["recall"] >= 0.95, mode
        assert metrics["f1"] >= 0.50, mode
        assert metrics["false_positive"] <= 15, mode


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, strict_replacements",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            {
                "residual_fractional_improvement_floor 0.0001": "residual_fractional_improvement_floor 0.01"
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
        assert metrics["f1"] >= 0.75 - 1.0e-12
        assert metrics["false_positive"] <= 8

    for loose, strict in zip(loose_metrics, strict_metrics):
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
