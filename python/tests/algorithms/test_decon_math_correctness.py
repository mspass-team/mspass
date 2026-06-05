#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pytest
import subprocess
import sys
from scipy.signal import windows

from mspasspy.ccore.algorithms.deconvolution import (
    DoubleVector,
    LeastSquareDecon,
    MultiTaperPowerSpecDivDecon,
    MultiTaperPowerXcorDecon,
    MultiTaperSpecDivDecon,
    MultiTaperXcorDecon,
    NoiseStableDecon,
    TimeDomainLeastSquareDecon,
    WaterLevelDecon,
)
from mspasspy.ccore.seismic import PowerSpectrum
from mspasspy.ccore.utility import ErrorSeverity, Metadata, MsPASSError, pfread


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
    nfft=64,
    window_start=0.0,
    window_end=63.0,
    shaping_wavelet_type="none",
    shaping_frequency=1.0,
):
    model_length_line = ""
    if model_length is not None:
        model_length_line = f"model_length {model_length}\n"
    pf = tmp_path / "time_domain_ls.pf"
    pf.write_text(
        f"""
target_sample_interval 1.0
operator_nfft {nfft}
deconvolution_data_window_start {window_start:.12f}
deconvolution_data_window_end {window_end:.12f}
damping_factor {damping:.12f}
{model_length_line}shaping_wavelet_dt 1.0
shaping_wavelet_type {shaping_wavelet_type}
shaping_wavelet_frequency {shaping_frequency:.12f}
"""
    )
    return pfread(str(pf))


def _write_multitaper_pf(
    tmp_path,
    *,
    window_start=-10.0,
    window_end=0.0,
    nfft=512,
    damping=0.1,
    shaping_wavelet_type="none",
    shaping_frequency=1.0,
):
    pf = tmp_path / "multitaper_decon.pf"
    pf.write_text(
        f"""
target_sample_interval 0.05
operator_nfft {nfft}
deconvolution_data_window_start {window_start:.12f}
deconvolution_data_window_end {window_end:.12f}
damping_factor {damping:.12f}
time_bandwidth_product 2.5
number_tapers 4
shaping_wavelet_dt 0.05
shaping_wavelet_type {shaping_wavelet_type}
shaping_wavelet_frequency {shaping_frequency:.12f}
"""
    )
    return pfread(str(pf))


def _write_noise_stable_pf(tmp_path, *, window_start=0.0, window_end=63.0, nfft=128):
    pf = tmp_path / "noise_stable_decon.pf"
    pf.write_text(
        f"""
target_sample_interval 1.0
operator_nfft {nfft}
deconvolution_data_window_start {window_start:.12f}
deconvolution_data_window_end {window_end:.12f}
ns_gid_mu_min 1.0e-4
ns_gid_alpha 1.0
ns_gid_noise_floor 1.0e-12
ns_gid_gain_max 1.0e3
ns_gid_use_reliability_taper false
ns_gid_snr_taper_low 1.0
ns_gid_snr_taper_high 3.0
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


def test_multitaper_power_stabilized_aliases_are_exported():
    assert MultiTaperPowerXcorDecon is MultiTaperXcorDecon
    assert MultiTaperPowerSpecDivDecon is MultiTaperSpecDivDecon


def test_deconvolution_binding_imports_diagnostic_return_types():
    code = r"""
from mspasspy.ccore.algorithms.deconvolution import DoubleVector, LeastSquareDecon
from mspasspy.ccore.utility import Metadata
md = Metadata()
md["target_sample_interval"] = 1.0
md["operator_nfft"] = 8
md["deconvolution_data_window_start"] = 0.0
md["deconvolution_data_window_end"] = 3.0
md["damping_factor"] = 1.0e-3
md["shaping_wavelet_dt"] = 1.0
md["shaping_wavelet_type"] = "none"
op = LeastSquareDecon(md)
op.load(DoubleVector([1.0, 0.0, 0.0, 0.0]), DoubleVector([1.0, 0.0, 0.0, 0.0]))
op.process()
assert op.actual_output().npts > 0
assert op.inverse_wavelet().npts > 0
"""
    subprocess.run([sys.executable, "-c", code], check=True)


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


def test_fft_window_sample_count_uses_rounding_near_integer_boundary(tmp_path):
    dt = 0.1
    n = 33
    # One ulp below 3.2 gives duration/dt just below 32.  Truncating that
    # value yields 32 samples, while rounding preserves the intended 33.
    window_end = np.nextafter((n - 1) * dt, 0.0)
    pf_file = tmp_path / "rounding_fft_length.pf"
    pf_file.write_text(
        f"""
target_sample_interval {dt:.17g}
operator_nfft {n}
deconvolution_data_window_start 0.0
deconvolution_data_window_end {window_end:.17g}
damping_factor 1.0e-12
water_level 1.0e-12
shaping_wavelet_dt {dt:.17g}
shaping_wavelet_type none
"""
    )
    pf = pfread(str(pf_file))
    engine = LeastSquareDecon(pf)
    wavelet = np.zeros(n)
    wavelet[0] = 1.0
    data = np.zeros(n)
    data[-1] = 1.0

    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()

    assert engine.actual_output().npts == 128
    assert len(engine.getresult()) == n


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


@pytest.mark.parametrize("engine_class", [LeastSquareDecon, WaterLevelDecon])
def test_scalar_fft_changeparameter_recomputes_sample_shift(tmp_path, engine_class):
    base_pf = _write_scalar_pf(
        tmp_path,
        damping=1.0e-12,
        water_level=1.0e-12,
        nfft=64,
        window_start=0.0,
        window_end=63.0,
    )
    shifted_pf = _write_scalar_pf(
        tmp_path,
        damping=1.0e-12,
        water_level=1.0e-12,
        nfft=128,
        window_start=-8.0,
        window_end=63.0,
    )
    wavelet = np.zeros(64)
    wavelet[0] = 1.0
    data = np.zeros(64)
    data[0] = 1.0

    engine = engine_class(base_pf)
    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()
    assert np.argmax(np.abs(np.asarray(engine.getresult(), dtype=np.float64))) == 0

    engine.changeparameter(shifted_pf)
    engine.process()
    shifted = np.asarray(engine.getresult(), dtype=np.float64)

    assert np.argmax(np.abs(shifted)) == 8


@pytest.mark.parametrize(
    "engine_class,pf_factory",
    [
        (LeastSquareDecon, _write_scalar_pf),
        (WaterLevelDecon, _write_scalar_pf),
        (NoiseStableDecon, _write_noise_stable_pf),
        (TimeDomainLeastSquareDecon, _write_time_domain_ls_pf),
        (MultiTaperXcorDecon, _write_multitaper_pf),
        (MultiTaperSpecDivDecon, _write_multitaper_pf),
    ],
)
def test_decon_operators_reject_reversed_deconvolution_windows(
    tmp_path, engine_class, pf_factory
):
    pf = pf_factory(tmp_path, window_start=10.0, window_end=0.0)

    with pytest.raises(MsPASSError, match="window") as excinfo:
        engine_class(pf)
    assert excinfo.value.severity == ErrorSeverity.Fatal


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


def test_time_domain_ls_diagnostics_use_model_lag_window(tmp_path):
    ndata = 37
    zero_lag_sample = 8
    pf = _write_time_domain_ls_pf(
        tmp_path,
        nfft=128,
        window_start=-float(zero_lag_sample),
        window_end=float(ndata - zero_lag_sample - 1),
        shaping_wavelet_type="ricker",
        shaping_frequency=0.08,
    )
    wavelet = np.zeros(ndata)
    wavelet[zero_lag_sample] = 1.0
    data = np.zeros(ndata)
    data[zero_lag_sample] = 1.0

    engine = TimeDomainLeastSquareDecon(pf)
    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()

    actual = engine.actual_output()
    shaping = engine.output_shaping_wavelet()

    assert actual.npts == ndata
    assert shaping.npts == ndata
    assert actual.dt == shaping.dt
    assert actual.t0 == shaping.t0
    assert actual.sample_number(0.0) == shaping.sample_number(0.0)
    prediction_error = np.asarray(actual.data) - np.asarray(shaping.data)
    assert np.isfinite(prediction_error).all()


def _processed_scalar_engine_for_diagnostics(tmp_path, engine_class):
    ndata = 37
    wavelet = np.zeros(ndata)
    wavelet[5] = 1.0
    data = np.zeros(ndata)
    data[8] = 1.0
    if engine_class is TimeDomainLeastSquareDecon:
        pf = _write_time_domain_ls_pf(
            tmp_path,
            nfft=128,
            window_start=-5.0,
            window_end=float(ndata - 6),
            shaping_wavelet_type="ricker",
            shaping_frequency=0.08,
        )
        engine = engine_class(pf)
        engine.load(_double_vector(wavelet), _double_vector(data))
    elif engine_class in (MultiTaperXcorDecon, MultiTaperSpecDivDecon):
        pf = _write_multitaper_pf(
            tmp_path,
            window_start=-5.0 * 0.05,
            window_end=float(ndata - 6) * 0.05,
            shaping_wavelet_type="ricker",
            shaping_frequency=1.0,
        )
        noise = 0.001 * np.ones(ndata)
        engine = engine_class(pf)
        engine.load(
            _double_vector(wavelet), _double_vector(data), _double_vector(noise)
        )
    elif engine_class is NoiseStableDecon:
        pf = _write_noise_stable_pf(
            tmp_path,
            window_start=-5.0,
            window_end=float(ndata - 6),
            nfft=128,
        )
        engine = engine_class(pf)
        engine.load(_double_vector(wavelet), _double_vector(data))
    else:
        pf = _write_scalar_pf(
            tmp_path,
            window_start=-5.0,
            window_end=float(ndata - 6),
        )
        engine = engine_class(pf)
        engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()
    return engine


@pytest.mark.parametrize(
    "engine_class",
    [
        LeastSquareDecon,
        WaterLevelDecon,
        TimeDomainLeastSquareDecon,
        MultiTaperXcorDecon,
        MultiTaperSpecDivDecon,
        NoiseStableDecon,
    ],
)
def test_scalar_decon_diagnostic_windows_are_consistent(tmp_path, engine_class):
    engine = _processed_scalar_engine_for_diagnostics(tmp_path, engine_class)

    actual = engine.actual_output()
    shaping = engine.output_shaping_wavelet()

    assert actual.npts == shaping.npts
    assert actual.dt == shaping.dt
    assert actual.t0 == shaping.t0
    assert actual.sample_number(0.0) == shaping.sample_number(0.0)
    assert np.isfinite(np.asarray(actual.data)).all()
    assert np.isfinite(np.asarray(shaping.data)).all()


def test_noise_stable_changeparameter_recomputes_lag_shift(tmp_path):
    base_pf = _write_noise_stable_pf(
        tmp_path,
        window_start=0.0,
        window_end=15.0,
        nfft=64,
    )
    shifted_pf = _write_noise_stable_pf(
        tmp_path,
        window_start=-4.0,
        window_end=11.0,
        nfft=64,
    )
    wavelet = np.zeros(16)
    wavelet[4] = 1.0
    data = np.zeros(16)
    data[4] = 1.0

    engine = NoiseStableDecon(base_pf)
    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()

    engine.changeparameter(shifted_pf)
    engine.process()
    recovered = np.asarray(engine.getresult(), dtype=np.float64)

    assert np.argmax(np.abs(recovered)) == 4


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


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_rejects_zero_lag_outside_output_window(tmp_path, engine_class):
    pf = _write_multitaper_pf(tmp_path, window_start=-11.0, window_end=-1.0)
    wavelet = np.zeros(201)
    wavelet[0] = 1.0
    data = np.zeros(201)
    data[5] = 1.0
    noise = 0.01 * np.ones(201)

    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))

    with pytest.raises(MsPASSError, match="zero-lag sample"):
        engine.process()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_rejects_empty_noise_vector(tmp_path, engine_class):
    n = 64
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=256,
    )
    engine = engine_class(pf)

    with pytest.raises(MsPASSError, match="noise vector cannot be empty"):
        engine.loadnoise(_double_vector([]))


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_actual_output_requires_process(tmp_path, engine_class):
    n = 64
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=256,
    )
    engine = engine_class(pf)

    with pytest.raises(MsPASSError, match="process must be called"):
        engine.actual_output()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_failed_process_does_not_leave_processed_state(
    tmp_path, engine_class
):
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=-5.0,
        window_end=5.0,
        nfft=256,
    )
    n = 24
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.4) / 0.08) ** 2)
    data = wavelet.copy()
    noise = 0.001 * np.sin(2.0 * np.pi * 3.0 * t)
    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))

    with pytest.raises(MsPASSError, match="zero-lag sample"):
        engine.process()

    qc = dict(engine.QCMetrics())
    assert qc["multitaper_processed"] is False
    assert qc["multitaper_number_outputs"] == 0
    with pytest.raises(MsPASSError, match="process must be called"):
        engine.actual_output()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_safely_pads_vectors_shorter_than_taper_length(
    tmp_path, engine_class
):
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=5.0,
        nfft=256,
    )
    wavelet = np.ones(16)
    data = np.ones(16)
    noise = np.ones(101)
    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()
    recovered = np.asarray(engine.getresult(), dtype=np.float64)

    assert recovered.size == wavelet.size
    assert np.isfinite(recovered).all()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
@pytest.mark.parametrize("amplitude", [0.5, -1.25, 2.0])
def test_multitaper_direct_ratio_matches_scalar_amplitude(
    tmp_path, engine_class, amplitude
):
    n = 128
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=512,
        damping=1.0e-6,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 1.0) / 0.18) ** 2)
    wavelet += 0.35 * np.exp(-0.5 * ((t - 1.55) / 0.25) ** 2)
    data = amplitude * wavelet
    noise = 1.0e-4 * np.sin(2.0 * np.pi * 4.5 * t)

    recovered = _run_multitaper_engine(engine_class, pf, wavelet, data, noise)

    assert recovered[0] == pytest.approx(amplitude, abs=2.0e-3)
    assert np.sign(recovered[0]) == np.sign(amplitude)
    assert np.max(np.abs(recovered)) == pytest.approx(
        abs(amplitude), rel=1.0e-2
    )


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_process_and_actual_output_are_idempotent(tmp_path, engine_class):
    n = 160
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=512,
        damping=0.05,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.8) / 0.15) ** 2)
    model = np.zeros(n)
    model[[0, 37, 113]] = [1.0, -0.35, 0.22]
    data = np.convolve(wavelet, model, mode="full")[:n]
    noise = 0.01 * np.sin(2.0 * np.pi * 3.7 * t)

    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()
    first_result = np.asarray(engine.getresult(), dtype=np.float64)
    first_actual = np.asarray(engine.actual_output().data, dtype=np.float64)
    second_actual = np.asarray(engine.actual_output().data, dtype=np.float64)
    engine.process()
    second_result = np.asarray(engine.getresult(), dtype=np.float64)

    assert np.allclose(first_actual, second_actual, atol=1.0e-12)
    assert np.allclose(first_result, second_result, atol=1.0e-12)


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_changeparameter_refreshes_shaping_wavelet_only(
    tmp_path, engine_class
):
    n = 160
    base_pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=512,
        shaping_wavelet_type="none",
    )
    ricker_pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=512,
        shaping_wavelet_type="ricker",
        shaping_frequency=2.0,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.8) / 0.15) ** 2)
    data = 1.2 * wavelet
    noise = 0.001 * np.sin(2.0 * np.pi * 3.0 * t)

    engine = engine_class(base_pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()
    actual_none = np.asarray(engine.actual_output().data, dtype=np.float64)
    engine.changeparameter(ricker_pf)
    assert dict(engine.QCMetrics())["multitaper_processed"] is False
    with pytest.raises(MsPASSError, match="process must be called"):
        engine.actual_output()
    with pytest.raises(MsPASSError, match="process must be called"):
        engine.inverse_wavelet()
    engine.process()
    actual_ricker = np.asarray(engine.actual_output().data, dtype=np.float64)

    assert not np.allclose(actual_none, actual_ricker)
    assert np.isfinite(actual_ricker).all()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_changeparameter_refreshes_lag_window(
    tmp_path, engine_class
):
    n = 160
    dt = 0.05
    base_pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * dt,
        nfft=512,
        damping=1.0e-6,
    )
    shifted_pf = _write_multitaper_pf(
        tmp_path,
        window_start=-2.0,
        window_end=float(n - 1) * dt,
        nfft=512,
        damping=1.0e-6,
    )
    t = np.arange(n) * dt
    wavelet = np.exp(-0.5 * ((t - 0.8) / 0.15) ** 2)
    data = 1.5 * wavelet
    noise = 1.0e-4 * np.sin(2.0 * np.pi * 3.2 * t)
    engine = engine_class(base_pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()
    result_base = np.asarray(engine.getresult(), dtype=np.float64)
    assert np.argmax(np.abs(result_base)) <= 1

    engine.changeparameter(shifted_pf)
    assert dict(engine.QCMetrics())["multitaper_processed"] is False
    engine.process()
    result_shifted = np.asarray(engine.getresult(), dtype=np.float64)
    zero_lag_index = int(round(2.0 / dt))

    assert np.argmax(np.abs(result_shifted)) == zero_lag_index
    assert result_shifted[zero_lag_index] == pytest.approx(1.5, abs=2.0e-3)


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_rejects_inputs_longer_than_taperlen(tmp_path, engine_class):
    n = 80
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=256,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.8) / 0.15) ** 2)
    data = 1.2 * wavelet
    noise = 0.001 * np.sin(2.0 * np.pi * 3.0 * t)
    overlong = np.r_[data, 0.0]

    engine = engine_class(pf)
    with pytest.raises(MsPASSError, match="taper"):
        engine.load(_double_vector(wavelet), _double_vector(overlong), _double_vector(noise))

    engine = engine_class(pf)
    with pytest.raises(MsPASSError, match="taper"):
        engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(overlong))

    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.loadwavelet(_double_vector(np.r_[wavelet, 0.0]))
    with pytest.raises(MsPASSError, match="taper"):
        engine.process()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_accepts_short_noise_by_right_padding(tmp_path, engine_class):
    n = 80
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=256,
        damping=0.05,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.8) / 0.15) ** 2)
    data = 1.2 * wavelet
    short_noise = 0.001 * np.ones(16)

    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(short_noise))
    engine.process()
    result = np.asarray(engine.getresult(), dtype=np.float64)

    assert np.isfinite(result).all()
    assert np.linalg.norm(result) > 0.0


@pytest.mark.parametrize(
    "engine_class,operator_type",
    [
        (MultiTaperXcorDecon, "xcor_power_stabilized"),
        (MultiTaperSpecDivDecon, "specdiv_power_stabilized"),
    ],
)
def test_multitaper_qc_reports_regularization_metadata(
    tmp_path, engine_class, operator_type
):
    n = 96
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=256,
        damping=0.04,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.8) / 0.15) ** 2)
    data = wavelet.copy()
    noise = 0.001 * np.cos(2.0 * np.pi * 2.2 * t)
    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()
    qc = dict(engine.QCMetrics())

    assert qc["multitaper_operator_type"] == operator_type
    assert qc["multitaper_operator_nfft"] == 256
    assert qc["multitaper_number_tapers"] == 4
    assert qc["multitaper_damping_factor"] == pytest.approx(0.04)
    assert qc["multitaper_processed"]
    assert qc["multitaper_number_outputs"] >= 1


def test_noise_stable_rejects_invalid_power_spectrum(tmp_path):
    pf = _write_noise_stable_pf(tmp_path)
    engine = NoiseStableDecon(pf)
    dead_spectrum = PowerSpectrum(
        Metadata(), _double_vector([1.0]), 1.0, "dead", 0.0, 1.0, 1
    )
    dead_spectrum.kill()
    live_empty_spectrum = PowerSpectrum(
        Metadata(), _double_vector([]), 1.0, "empty", 0.0, 1.0, 0
    )
    one_bin_spectrum = PowerSpectrum(
        Metadata(), _double_vector([1.0]), 1.0, "one_bin", 0.0, 1.0, 1
    )
    missing_dc_spectrum = PowerSpectrum(
        Metadata(), _double_vector([1.0, 1.0]), 1.0, "missing_dc", 1.0, 1.0, 2
    )
    below_dc_spectrum = PowerSpectrum(
        Metadata(), _double_vector([1.0, 1.0]), 1.0, "below_dc", -10.0, 1.0, 2
    )
    dc_at_last_bin_spectrum = PowerSpectrum(
        Metadata(), _double_vector([1.0, 1.0]), 1.0, "dc_at_last", -1.0, 1.0, 2
    )

    with pytest.raises(MsPASSError, match="noise vector cannot be empty"):
        engine.loadnoise(_double_vector([]))
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


def test_noise_stable_metadata_constructor_can_process_without_explicit_noise(
    tmp_path,
):
    pf = _write_noise_stable_pf(tmp_path, window_start=0.0, window_end=15.0, nfft=64)
    engine = NoiseStableDecon(pf)
    wavelet = np.zeros(16)
    wavelet[0] = 1.0
    data = np.zeros(16)
    data[3] = 2.0

    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()

    result = np.asarray(engine.getresult(), dtype=np.float64)
    assert result.size == data.size
    assert np.isfinite(result).all()


def test_noise_stable_changeparameter_invalidates_processed_state(tmp_path):
    pf = _write_noise_stable_pf(tmp_path, window_start=0.0, window_end=15.0, nfft=64)
    engine = NoiseStableDecon(pf)
    wavelet = np.zeros(16)
    wavelet[0] = 1.0
    data = np.zeros(16)
    data[4] = 1.0

    engine.load(_double_vector(wavelet), _double_vector(data))
    engine.process()
    assert len(engine.getresult()) == data.size

    pf_changed = _write_noise_stable_pf(
        tmp_path, window_start=0.0, window_end=15.0, nfft=128
    )
    engine.changeparameter(pf_changed)

    assert len(engine.getresult()) == 0
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.actual_output()
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.inverse_wavelet()


def test_noise_stable_diagnostics_require_process_first(tmp_path):
    pf = _write_noise_stable_pf(tmp_path, window_start=0.0, window_end=15.0, nfft=64)
    engine = NoiseStableDecon(pf)

    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.actual_output()
    with pytest.raises(MsPASSError, match="process must be called first"):
        engine.inverse_wavelet()


def _run_multitaper_engine(engine_class, pf, wavelet, data, noise):
    engine = engine_class(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()
    return np.asarray(engine.getresult(), dtype=np.float64)


def _extract_lag_window(buffer, output_length, sample_shift):
    return np.concatenate(
        [buffer[-sample_shift:] if sample_shift else np.array([]), buffer]
    )[:output_length]


def _multitaper_reference(
    method, wavelet, data, noise, *, nfft, nw=2.5, nseq=4, damping=0.1, dt=0.05, t0=0.0
):
    tapers = windows.dpss(len(wavelet), nw, Kmax=nseq, sym=False, norm=2)
    W0 = np.fft.fft(np.pad(wavelet, (0, nfft - len(wavelet))))
    D0 = np.fft.fft(np.pad(data, (0, nfft - len(data))))
    Wk = np.asarray(
        [np.fft.fft(np.pad(taper * wavelet, (0, nfft - len(wavelet)))) for taper in tapers]
    )
    Nk = np.asarray(
        [np.fft.fft(np.pad(taper * noise, (0, nfft - len(noise)))) for taper in tapers]
    )
    source_power = np.abs(Wk) ** 2
    noise_power = np.abs(Nk) ** 2
    if method == "xcor":
        mean_source_power = np.mean(source_power, axis=0)
        relative_floor = max(
            np.finfo(float).eps, 1.0e-12 * float(np.max(mean_source_power))
        )
        den = mean_source_power + damping * np.mean(noise_power, axis=0)
        den += relative_floor
    elif method == "specdiv":
        relative_floor = max(
            np.finfo(float).eps, 1.0e-12 * float(np.max(source_power))
        )
        den = np.mean(
            np.maximum.reduce(
                [
                    source_power,
                    damping * noise_power,
                    np.full_like(source_power, relative_floor),
                ]
            ),
            axis=0,
        )
    else:
        raise ValueError(method)
    G = np.conj(W0) / den
    ao = np.fft.ifft(G * W0).real
    sample_shift = int(round(-t0 / dt))
    ao_lag = _extract_lag_window(ao, len(data), sample_shift)
    peak = ao_lag[sample_shift]
    rf = np.fft.ifft((G / peak) * D0).real
    return _extract_lag_window(rf, len(data), sample_shift)


@pytest.mark.parametrize(
    "engine_class,method",
    [(MultiTaperXcorDecon, "xcor"), (MultiTaperSpecDivDecon, "specdiv")],
)
def test_multitaper_matches_closed_form_hybrid_reference(tmp_path, engine_class, method):
    n = 192
    nfft = 512
    damping = 0.07
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=nfft,
        damping=damping,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 1.0) / 0.16) ** 2)
    wavelet -= 0.25 * np.exp(-0.5 * ((t - 1.45) / 0.24) ** 2)
    model = np.zeros(n)
    model[[0, 41, 129]] = [1.0, -0.35, 0.22]
    data = np.convolve(wavelet, model, mode="full")[:n]
    noise = 0.01 * np.sin(2.0 * np.pi * 1.7 * t) + 0.004 * np.cos(2.0 * np.pi * 5.2 * t)

    cpp = _run_multitaper_engine(engine_class, pf, wavelet, data, noise)
    ref = _multitaper_reference(
        method, wavelet, data, noise, nfft=nfft, damping=damping, dt=0.05, t0=0.0
    )

    assert np.corrcoef(cpp, ref)[0, 1] > 0.995
    assert np.max(np.abs(cpp - ref)) < 0.08 * np.max(np.abs(ref))


def test_multitaper_specdiv_legacy_all_methods_are_bound(tmp_path):
    n = 128
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=512,
        damping=0.05,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 0.9) / 0.16) ** 2)
    data = 1.3 * wavelet
    noise = 0.001 * np.sin(2.0 * np.pi * 2.0 * t)

    engine = MultiTaperSpecDivDecon(pf)
    engine.load(_double_vector(wavelet), _double_vector(data), _double_vector(noise))
    engine.process()

    assert len(engine.all_inverse_wavelets()) == engine.get_number_outputs()
    assert len(engine.all_rfestimates()) == engine.get_number_outputs()
    assert len(engine.all_actual_outputs()) == engine.get_number_outputs()


@pytest.mark.parametrize(
    "engine_class", [MultiTaperXcorDecon, MultiTaperSpecDivDecon]
)
def test_multitaper_recovers_late_sparse_arrivals_without_taper_phase_bias(
    tmp_path, engine_class
):
    n = 260
    pf = _write_multitaper_pf(
        tmp_path,
        window_start=0.0,
        window_end=float(n - 1) * 0.05,
        nfft=1024,
        damping=0.02,
    )
    t = np.arange(n) * 0.05
    wavelet = np.exp(-0.5 * ((t - 1.2) / 0.18) ** 2)
    wavelet += 0.2 * np.exp(-0.5 * ((t - 1.85) / 0.25) ** 2)
    model = np.zeros(n)
    model[[0, 80, 210]] = [1.0, -0.4, 0.25]
    data = np.convolve(wavelet, model, mode="full")[:n]
    noise = 0.002 * np.sin(2.0 * np.pi * 2.1 * t)

    recovered = _run_multitaper_engine(engine_class, pf, wavelet, data, noise)

    for idx, amp in zip([0, 80, 210], [1.0, -0.4, 0.25]):
        local = recovered[max(0, idx - 2) : min(n, idx + 3)]
        peak = local[np.argmax(np.abs(local))]
        assert np.sign(peak) == np.sign(amp)
        assert abs(peak) > 0.4 * abs(amp)
