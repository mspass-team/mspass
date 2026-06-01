#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest

from mspasspy.ccore.algorithms.deconvolution import (
    DoubleVector,
    LeastSquareDecon,
    WaterLevelDecon,
)
from mspasspy.ccore.utility import MsPASSError, pfread


def _write_scalar_pf(tmp_path, *, damping=1.0e-6, water_level=1.0e-6, nfft=64):
    pf = tmp_path / "scalar_decon.pf"
    pf.write_text(
        f"""
target_sample_interval 1.0
operator_nfft {nfft}
deconvolution_data_window_start 0.0
deconvolution_data_window_end {float(nfft - 1)}
damping_factor {damping:.12f}
water_level {water_level:.12f}
shaping_wavelet_dt 1.0
shaping_wavelet_type none
"""
    )
    return pfread(str(pf))


def _double_vector(values):
    return DoubleVector(np.asarray(values, dtype=np.float64).tolist())


def _run_scalar_engine(engine_class, pf, wavelet, data):
    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()
    return np.asarray(engine.getresult(), dtype=np.float64)


def _fft_linear_convolution(wavelet, model):
    nout = len(wavelet) + len(model) - 1
    nfft = 1 << (nout - 1).bit_length()
    return np.fft.ifft(np.fft.fft(wavelet, nfft) * np.fft.fft(model, nfft)).real[
        :nout
    ]


def test_direct_and_fft_padded_linear_convolution_agree():
    wavelet = np.array([0.25, -0.5, 1.0, 0.5, -0.125])
    model = np.zeros(17)
    model[[0, 3, 9, 16]] = [1.0, -0.4, 0.25, 0.7]

    direct = np.convolve(wavelet, model, mode="full")
    via_fft = _fft_linear_convolution(wavelet, model)

    assert np.allclose(via_fft, direct, atol=1.0e-12)


def test_circular_convolution_is_not_silently_equal_to_linear_convolution():
    wavelet = np.array([1.0, 0.0, 0.0, 0.5])
    model = np.zeros(8)
    model[-1] = 2.0

    linear = np.convolve(wavelet, model, mode="full")
    circular = np.fft.ifft(np.fft.fft(wavelet, 8) * np.fft.fft(model, 8)).real

    assert not np.allclose(circular, linear[:8])
    assert np.isclose(linear[-1], 1.0)
    assert np.isclose(circular[2], 1.0)


@pytest.mark.parametrize("engine_class", [LeastSquareDecon, WaterLevelDecon])
def test_scalar_fft_decon_identity_wavelet_is_identity(tmp_path, engine_class):
    pf = _write_scalar_pf(tmp_path, nfft=32)
    model = np.zeros(32)
    model[[0, 5, 12, 31]] = [1.0, -0.4, 0.25, 0.75]
    wavelet = np.zeros(32)
    wavelet[0] = 1.0

    recovered = _run_scalar_engine(engine_class, pf, wavelet, model)

    assert len(recovered) == len(model)
    assert np.allclose(recovered, model, atol=1.0e-5)


def test_scalar_fft_decon_process_is_idempotent_after_single_load(tmp_path):
    pf = _write_scalar_pf(tmp_path, nfft=32)
    model = np.zeros(32)
    model[[2, 10]] = [1.0, -0.5]
    wavelet = np.zeros(32)
    wavelet[0] = 1.0
    engine = LeastSquareDecon(pf)
    engine.load(_double_vector(wavelet), _double_vector(model))

    engine.process()
    first = np.asarray(engine.getresult(), dtype=np.float64)
    engine.process()
    second = np.asarray(engine.getresult(), dtype=np.float64)

    assert len(second) == len(first)
    assert np.allclose(second, first, atol=1.0e-12)


def test_damped_least_squares_is_stable_for_notched_wavelet(tmp_path):
    pf = _write_scalar_pf(tmp_path, damping=0.1, nfft=64)
    wavelet = np.zeros(64)
    wavelet[:2] = [1.0, -1.0]
    model = np.zeros(64)
    model[[3, 18, 40]] = [1.0, 0.2, -0.35]
    rng = np.random.default_rng(20260602)
    data = np.convolve(wavelet, model, mode="full")[:64]
    data += 1.0e-4 * rng.standard_normal(data.size)

    recovered = _run_scalar_engine(LeastSquareDecon, pf, wavelet, data)

    assert np.isfinite(recovered).all()
    assert np.linalg.norm(recovered) < 20.0 * np.linalg.norm(model)


@pytest.mark.parametrize(
    "pf_kwargs, engine_class",
    [
        ({"damping": 0.0}, LeastSquareDecon),
        ({"water_level": 0.0}, WaterLevelDecon),
    ],
)
def test_unregularized_frequency_domain_decon_is_rejected(
    tmp_path, pf_kwargs, engine_class
):
    pf = _write_scalar_pf(tmp_path, **pf_kwargs)

    with pytest.raises(MsPASSError):
        engine_class(pf)


def test_zero_wavelet_is_rejected_by_damped_least_squares(tmp_path):
    pf = _write_scalar_pf(tmp_path, nfft=16)
    wavelet = np.zeros(16)
    data = np.ones(16)

    engine = LeastSquareDecon(pf)
    engine.load(_double_vector(wavelet), _double_vector(data))

    with pytest.raises(MsPASSError):
        engine.process()
