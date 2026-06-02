#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest

from mspasspy.ccore.algorithms.deconvolution import (
    DoubleVector,
    LeastSquareDecon,
    TimeDomainLeastSquareDecon,
    WaterLevelDecon,
)
from mspasspy.ccore.seismic import _CoreTimeSeries  # noqa: F401
from mspasspy.ccore.utility import MsPASSError, pfread


def _write_scalar_pf(
    tmp_path,
    *,
    damping=1.0e-6,
    water_level=1.0e-6,
    nfft=64,
    window_start=0.0,
    window_end=None,
):
    if window_end is None:
        window_end = float(nfft - 1)
    pf = tmp_path / "scalar_decon.pf"
    pf.write_text(
        f"""
target_sample_interval 1.0
operator_nfft {nfft}
deconvolution_data_window_start {window_start:.12f}
deconvolution_data_window_end {window_end:.12f}
damping_factor {damping:.12f}
water_level {water_level:.12f}
shaping_wavelet_dt 1.0
shaping_wavelet_type none
"""
    )
    return pfread(str(pf))


def _write_time_domain_ls_pf(
    tmp_path,
    *,
    damping=1.0e-12,
    model_length=None,
    window_start=0.0,
    window_end=63.0,
):
    model_length_line = ""
    if model_length is not None:
        model_length_line = f"model_length {model_length}\n"
    pf = tmp_path / "time_domain_ls.pf"
    pf.write_text(
        f"""
target_sample_interval 1.0
operator_nfft 64
deconvolution_data_window_start {window_start:.12f}
deconvolution_data_window_end {window_end:.12f}
damping_factor {damping:.12f}
{model_length_line}shaping_wavelet_dt 1.0
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


def test_scalar_fft_decon_uses_minimal_linear_padding(tmp_path):
    n = 25
    pf = _write_scalar_pf(tmp_path, nfft=n, window_end=float(n - 1))
    model = np.zeros(n)
    model[[0, 7, 24]] = [1.0, -0.25, 0.5]
    wavelet = np.zeros(n)
    wavelet[0] = 1.0
    engine = LeastSquareDecon(pf)
    engine.load(_double_vector(wavelet), _double_vector(model))
    engine.process()

    expected_nfft = 1 << ((2 * n - 1) - 1).bit_length()

    assert expected_nfft == 64
    assert engine.actual_output().npts == expected_nfft
    assert len(engine.getresult()) == n
    assert np.allclose(np.asarray(engine.getresult()), model, atol=1.0e-5)


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


@pytest.mark.parametrize(
    "engine_class, pf_factory",
    [
        (LeastSquareDecon, _write_scalar_pf),
        (WaterLevelDecon, _write_scalar_pf),
        (TimeDomainLeastSquareDecon, _write_time_domain_ls_pf),
    ],
)
def test_scalar_decon_methods_share_nonzero_lag_convention(
    tmp_path, engine_class, pf_factory
):
    model = np.zeros(16)
    model[[0, 2, 7, 15]] = [0.25, 1.0, -0.5, 0.75]
    wavelet = np.zeros(16)
    zero_lag_sample = 4
    wavelet[zero_lag_sample] = 1.0
    pf = pf_factory(
        tmp_path,
        window_start=-float(zero_lag_sample),
        window_end=float(model.size - zero_lag_sample - 1),
        model_length=model.size,
    ) if engine_class is TimeDomainLeastSquareDecon else pf_factory(
        tmp_path,
        nfft=model.size,
        window_start=-float(zero_lag_sample),
        window_end=float(model.size - zero_lag_sample - 1),
    )

    recovered = _run_scalar_engine(engine_class, pf, wavelet, model)

    assert len(recovered) == len(model)
    assert np.allclose(recovered, model, atol=1.0e-5)


@pytest.mark.parametrize("engine_class", [LeastSquareDecon, WaterLevelDecon])
def test_scalar_fft_decon_reduced_padding_does_not_wrap_boundary_lags(
    tmp_path, engine_class
):
    n = 25
    zero_lag_sample = 8
    model = np.zeros(n)
    model[[0, 1, n - 2, n - 1]] = [0.4, -0.8, 0.25, 1.0]
    wavelet = np.zeros(n)
    wavelet[zero_lag_sample] = 1.0
    pf = _write_scalar_pf(
        tmp_path,
        nfft=n,
        window_start=-float(zero_lag_sample),
        window_end=float(n - zero_lag_sample - 1),
    )

    recovered = _run_scalar_engine(engine_class, pf, wavelet, model)

    assert len(recovered) == n
    assert np.argmax(np.abs(recovered)) == n - 1
    assert np.allclose(recovered, model, atol=1.0e-5)


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


def test_time_domain_least_squares_recovers_cropped_linear_convolution(tmp_path):
    model = np.zeros(24)
    model[[0, 4, 11, 23]] = [1.0, -0.5, 0.25, 0.75]
    wavelet = np.array([1.0, -0.3, 0.1, 0.05])
    data = np.convolve(wavelet, model, mode="full")[: model.size]
    pf = _write_time_domain_ls_pf(tmp_path, model_length=model.size)

    recovered = _run_scalar_engine(TimeDomainLeastSquareDecon, pf, wavelet, data)

    assert len(recovered) == len(model)
    assert np.allclose(recovered, model, atol=1.0e-8)


def test_time_domain_least_squares_uses_linear_not_circular_boundaries(tmp_path):
    model = np.zeros(8)
    model[-1] = 2.0
    wavelet = np.array([1.0, 0.0, 0.5])
    data = np.convolve(wavelet, model, mode="full")[: model.size]
    pf = _write_time_domain_ls_pf(tmp_path, model_length=model.size)

    recovered = _run_scalar_engine(TimeDomainLeastSquareDecon, pf, wavelet, data)

    assert np.argmax(np.abs(recovered)) == model.size - 1
    assert recovered[1] == pytest.approx(0.0, abs=1.0e-8)
    assert np.allclose(recovered, model, atol=1.0e-8)


def test_time_domain_least_squares_regularizes_ill_conditioned_problem(tmp_path):
    wavelet = np.array([1.0, -1.0])
    model = np.zeros(32)
    model[[4, 18]] = [1.0, -0.5]
    rng = np.random.default_rng(20260602)
    data = np.convolve(wavelet, model, mode="full")[: model.size]
    data += 1.0e-5 * rng.standard_normal(data.size)
    pf = _write_time_domain_ls_pf(tmp_path, damping=0.1, model_length=model.size)

    recovered = _run_scalar_engine(TimeDomainLeastSquareDecon, pf, wavelet, data)

    assert np.isfinite(recovered).all()
    assert np.linalg.norm(recovered) < 10.0 * np.linalg.norm(model)
