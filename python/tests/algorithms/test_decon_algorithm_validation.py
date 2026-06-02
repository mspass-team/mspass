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
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.algorithms.deconvolution import (
    CNRDeconEngine,
    FrequencyDomainGIDDecon,
    TimeDomainGIDDecon,
)
from mspasspy.ccore.seismic import DoubleVector
from mspasspy.ccore.utility import pfread

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


def _cnr_rf_matrix(data):
    engine = CNRDeconEngine(pfread("data/pf/CNRDeconEngine.pf"))
    rf = CNRRFDecon(
        data,
        engine,
        signal_window=SIGNAL_WINDOW,
        noise_window=NOISE_WINDOW,
    )
    assert rf.live
    return np.asarray(rf.data)


def _normalized_correlation(x, y):
    npts = min(x.shape[1], y.shape[1])
    xv = x[:, :npts].ravel()
    yv = y[:, :npts].ravel()
    return np.dot(xv, yv) / (np.linalg.norm(xv) * np.linalg.norm(yv))


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


def _truth_matrix_for_times(truth, times):
    truth_data = np.asarray(truth.data)
    truth_matrix = np.zeros((3, len(times)))
    samples = np.rint((times - truth.t0) / truth.dt).astype(int)
    valid = np.logical_and(samples >= 0, samples < truth.npts)
    truth_matrix[:, valid] = truth_data[:, samples[valid]]
    return truth_matrix


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
        "Scalar deconvolution validation on colored 3C synthetic",
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
    }

    for result in results.values():
        assert np.isfinite(result).all()
        _assert_direct_arrival_is_recovered(result, ratio_tol=1.0e-1)
        _assert_colored_transverse_arrivals_are_recovered(result)

    assert _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.95
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperXcor"]) > 0.84
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperSpecDiv"]) > 0.84
    assert _normalized_correlation(results["MultiTaperXcor"], results["MultiTaperSpecDiv"]) > 0.90
    assert (
        np.max(np.abs(results["MultiTaperXcor"] - results["MultiTaperSpecDiv"]))
        > 1.0e-2
    )
    _plot_complex_colored_results(decon_validation_plot_dir, results, truth, wavelet)


@pytest.mark.parametrize(
    "engine_class, wrapper, pfname, branch_name, modes, qc_key",
    [
        (
            TimeDomainGIDDecon,
            TimeDomainGIDRFDecon,
            "data/pf/TimeDomainGIDDecon.pf",
            "time_domain_gid_deconvolution",
            ["least_square", "water_level", "multi_taper", "cnr3c"],
            "TimeDomainGIDDecon_properties",
        ),
        (
            FrequencyDomainGIDDecon,
            FrequencyDomainGIDRFDecon,
            "data/pf/FrequencyDomainGIDDecon.pf",
            "frequency_domain_gid_deconvolution",
            ["least_square", "water_level", "multi_taper", "cnr"],
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
        _assert_colored_gid_arrival_signs_are_recovered(
            np.asarray(rf.data), rf.t0, rf.dt
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
