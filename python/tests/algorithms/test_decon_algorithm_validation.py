#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest

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


def _make_validation_data(noise_level=0.0):
    np.random.seed(12345)
    wavelet = make_simulation_wavelet()
    truth = make_impulse_data()
    data = convolve_wavelet(truth, wavelet)
    data = addnoise(data, nscale=noise_level, padlength=800)
    data["low_f_band_edge"] = 0.02
    data["high_f_band_edge"] = 2.0
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


@pytest.mark.parametrize("noise_level", [0.0, 1.0e-6])
@pytest.mark.parametrize(
    "algorithm", ["LeastSquares", "WaterLevel", "MultiTaperXcor"]
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
        "CNR": _cnr_rf_matrix(data),
    }

    assert _normalized_correlation(results["LeastSquares"], results["WaterLevel"]) > 0.97
    assert _normalized_correlation(results["LeastSquares"], results["MultiTaperXcor"]) > 0.88
    assert _normalized_correlation(results["WaterLevel"], results["MultiTaperXcor"]) > 0.93
    assert _normalized_correlation(results["CNR"], results["LeastSquares"]) > 0.75
    assert _normalized_correlation(results["CNR"], results["WaterLevel"]) > 0.79
    assert _normalized_correlation(results["CNR"], results["MultiTaperXcor"]) > 0.87


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
def test_gid_methods_recover_noise_free_multi_spike_rf_for_all_inverse_modes(
    tmp_path, engine_class, wrapper, pfname, branch_name, modes, qc_key
):
    for mode in modes:
        data = _make_validation_data(noise_level=0.0)
        pf = _pf_with_gid_mode(tmp_path, pfname, branch_name, mode)
        engine = engine_class(pf)
        rf = wrapper(
            data,
            engine,
            signal_window=TimeWindow(-8.0, 20.0),
            noise_window=TimeWindow(-25.0, -8.0),
        )
        assert rf.live, mode
        assert rf[qc_key]["residual_L2_final"] < rf[qc_key]["residual_L2_initial"]
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
