import os
import subprocess
from importlib.metadata import distribution, version
from pathlib import Path

import numpy as np
import pytest

import mspasspy.algorithms.snr as snr_module
import mspasspy.ccore.seismic as seismic_binding
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum
from mspasspy.ccore.utility import Metadata


def _spectrum(values, df=1.0, dt=0.05):
    return PowerSpectrum(
        Metadata(),
        DoubleVector(values),
        df,
        "test spectrum",
        0.0,
        dt,
        20,
    )


def _signal_and_noise():
    noise = _spectrum([1.0] * 11)
    signal = _spectrum([0.25] + [4.0] * 8 + [0.25, 0.25])
    return signal, noise


def _assert_module_from_selected_build(module, relative_path):
    source_root = os.environ.get("MSPASS_TEST_SOURCE_ROOT")
    if source_root:
        expected_module = Path(source_root) / relative_path
    else:
        expected_module = distribution("mspasspy").locate_file(relative_path)
        installed_version = version("mspasspy")
        installed_commit = installed_version.partition("+g")[2].partition(".")[0]
        assert installed_commit, "installed mspasspy version lacks a source commit"
        repository_root = next(
            parent
            for parent in Path(__file__).resolve().parents
            if (parent / ".git").exists()
        )
        checkout_commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repository_root, text=True
        ).strip()
        assert checkout_commit.startswith(installed_commit)
    assert Path(module.__file__).resolve() == Path(expected_module).resolve()


def test_contract_suite_uses_selected_build_and_real_binding():
    _assert_module_from_selected_build(snr_module, "mspasspy/algorithms/snr.py")
    assert Path(seismic_binding.__file__).suffix == ".so"


@pytest.mark.parametrize(
    "f_max,expected_high_edge",
    [(4.0, 4.0), (4.6, 4.0), (8.0, 8.0), (20.0, 8.0), (None, 8.0)],
)
def test_fmax_is_bounded_by_eighty_percent_of_nyquist(f_max, expected_high_edge):
    signal, noise = _signal_and_noise()

    result = snr_module.EstimateBandwidth(
        signal, noise, snr_threshold=1.5, f0=1.0, f_max=f_max
    )

    assert result.high_edge_f == expected_high_edge
    assert result.f_range == 10.0


@pytest.mark.parametrize(
    "f_max", [0.0, -1.0, np.nan, np.inf, -np.inf, "8.0", True, np.bool_(True)]
)
def test_invalid_fmax_raises_value_error_before_spectrum_access(f_max):
    signal, noise = _signal_and_noise()
    signal.spectrum = DoubleVector()

    with pytest.raises(ValueError, match="f_max must be a finite positive number"):
        snr_module.EstimateBandwidth(signal, noise, f0=0.0, f_max=f_max)


@pytest.mark.parametrize("f0", [0.0, 8.0])
def test_f0_accepts_both_inclusive_frequency_bounds(f0):
    signal, noise = _signal_and_noise()

    result = snr_module.EstimateBandwidth(signal, noise, f0=f0)

    assert result.f_range == 10.0


def test_numpy_real_scalars_are_valid_numeric_arguments():
    signal, noise = _signal_and_noise()

    result = snr_module.EstimateBandwidth(
        signal,
        noise,
        f0=np.int64(1),
        f_max=np.float32(8.0),
        df_smoother=np.float64(0.1),
    )

    assert result.high_edge_f == 8.0


@pytest.mark.parametrize(
    "f0",
    [
        -1.0,
        np.nextafter(8.0, np.inf),
        np.nan,
        np.inf,
        -np.inf,
        "1.0",
        True,
        np.bool_(True),
    ],
)
def test_invalid_f0_raises_value_error_instead_of_index_error(f0):
    signal, noise = _signal_and_noise()
    signal.spectrum = DoubleVector()

    with pytest.raises(
        ValueError, match=r"f0 must be finite and in the range \[0, high_f_ceiling\]"
    ):
        snr_module.EstimateBandwidth(signal, noise, f0=f0)


@pytest.mark.parametrize("npts", [1, 3, 4, 11])
def test_smoothing_uses_only_available_bins_and_renormalizes(npts):
    values = np.arange(1.0, 12.0)
    expected = []
    left = npts // 2
    right = (npts - 1) // 2
    for i in range(len(values)):
        expected.append(
            np.mean(values[max(0, i - left) : min(len(values), i + right + 1)])
        )

    actual = snr_module._smooth_snr_curve(values, npts)

    np.testing.assert_allclose(actual, expected)


@pytest.mark.parametrize("npts", [1, 3, 4, 11])
def test_smoothing_does_not_depress_a_constant_spectrum_at_edges(npts):
    values = np.full(11, 2.75)

    np.testing.assert_allclose(
        snr_module._smooth_snr_curve(values, npts), values, rtol=0.0, atol=0.0
    )


def test_convolution_return_value_drives_the_bandwidth_result(monkeypatch):
    signal, noise = _signal_and_noise()
    calls = []

    def replace_with_below_threshold(values, npts):
        calls.append((np.array(values), npts))
        return np.zeros_like(values)

    monkeypatch.setattr(
        snr_module, "_smooth_snr_curve", replace_with_below_threshold
    )

    result = snr_module.EstimateBandwidth(signal, noise, f0=1.0, df_smoother=3.0)

    assert len(calls) == 1
    assert result.low_edge_f == 0.0
    assert result.high_edge_f == 0.0
    assert result.low_edge_snr == 0.0
    assert result.high_edge_snr == 0.0
    assert result.f_range == 10.0


@pytest.mark.parametrize(
    "width",
    [0.0, -1.0, np.nan, np.inf, -np.inf, "1.0", True, np.bool_(True)],
)
def test_invalid_smoothing_width_raises_before_convolution(monkeypatch, width):
    signal, noise = _signal_and_noise()
    convolve = pytest.fail
    monkeypatch.setattr(snr_module.np, "convolve", convolve)

    with pytest.raises(
        ValueError, match="df_smoother must be a finite positive number"
    ):
        snr_module.EstimateBandwidth(signal, noise, f0=1.0, df_smoother=width)


def test_no_smoothed_value_at_threshold_returns_null_bandwidth():
    signal = _spectrum([1.0] * 11)
    noise = _spectrum([1.0] * 11)

    result = snr_module.EstimateBandwidth(
        signal,
        noise,
        snr_threshold=1.5,
        f0=0.0,
        df_smoother=100.0,
    )

    assert result.low_edge_f == 0.0
    assert result.high_edge_f == 0.0
    assert result.low_edge_snr == 0.0
    assert result.high_edge_snr == 0.0
    assert result.f_range == 10.0


def test_value_equal_to_threshold_is_not_inside_the_passband():
    signal = _spectrum([1.0, 1.0, 2.25, 1.0, 1.0])
    noise = _spectrum([1.0] * 5)

    result = snr_module.EstimateBandwidth(signal, noise, snr_threshold=1.5, f0=2.0)

    assert result.low_edge_f == 0.0
    assert result.high_edge_f == 0.0
    assert result.low_edge_snr == 0.0
    assert result.high_edge_snr == 0.0
    assert result.f_range == 4.0


def test_threshold_equality_marks_the_first_outside_band_edge():
    signal = _spectrum([1.0, 4.0, 2.25, 1.0, 1.0])
    noise = _spectrum([1.0] * 5)

    result = snr_module.EstimateBandwidth(signal, noise, snr_threshold=1.5, f0=1.0)

    assert result.low_edge_f == 0.0
    assert result.low_edge_snr == 1.0
    assert result.high_edge_f == 2.0
    assert result.high_edge_snr == 1.5


def test_searches_do_not_index_below_zero_or_past_a_truncated_spectrum():
    noise = _spectrum([1.0] * 5)
    signal = _spectrum([0.25, 4.0, 4.0, 4.0, 4.0])

    below_start_result = snr_module.EstimateBandwidth(signal, noise, f0=0.0)
    terminal_result = snr_module.EstimateBandwidth(signal, noise, f0=8.0)

    assert below_start_result.low_edge_f == 0.0
    assert below_start_result.high_edge_f == 0.0
    assert terminal_result.high_edge_f == 4.0


def test_truncated_noise_grid_is_checked_before_sample_number():
    signal = _spectrum([4.0] * 5)
    noise = _spectrum([1.0] * 2)

    result = snr_module.EstimateBandwidth(signal, noise, f0=0.0)

    assert result.low_edge_f == 0.0
    assert result.low_edge_snr == 2.0
    assert result.high_edge_f == 2.0
    assert result.high_edge_snr == 1.0
    assert result.f_range == 4.0
