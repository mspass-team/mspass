import os
from pathlib import Path

import pytest

import mspasspy.ccore.algorithms.amplitudes as amplitudes_module
from mspasspy.ccore.algorithms.amplitudes import (
    BandwidthData,
    BandwidthStatistics,
    EstimateBandwidth,
)
from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum
from mspasspy.ccore.utility import Metadata


def _constant_spectrum(length):
    return PowerSpectrum(
        Metadata(),
        DoubleVector([4.0] * length),
        1.0,
        "constant normalized PSD",
        0.0,
        0.05,
        20,
    )


def test_contract_suite_loads_selected_native_binding():
    binding_path = Path(amplitudes_module.__file__).resolve()
    assert binding_path.suffix == ".so"

    expected_root = os.environ.get("MSPASS_TEST_CCORE_ROOT")
    if expected_root is not None:
        assert binding_path.is_relative_to(Path(expected_root).resolve())


def test_normalized_snr_is_independent_of_spectrum_storage_length():
    signal = _constant_spectrum(11)
    noise = _constant_spectrum(6)
    passband = EstimateBandwidth(1.0, signal, noise, 0.5, 0.5, 4.0, True)

    assert passband.low_edge_snr == 1.0
    assert passband.high_edge_snr == 1.0

    stats = BandwidthStatistics(signal, noise, passband)
    assert stats["stats_are_valid"] is True
    for key in (
        "median_snr",
        "maximum_snr",
        "minimum_snr",
        "q1_4_snr",
        "q3_4_snr",
        "mean_snr",
    ):
        assert stats[key] == 1.0


def test_storage_length_does_not_create_false_passband():
    signal = _constant_spectrum(11)
    noise = _constant_spectrum(6)
    result = EstimateBandwidth(1.0, signal, noise, 1.1, 0.5, 4.0, True)

    assert result.low_edge_f == 0.0
    assert result.high_edge_f == 0.0
    assert result.f_range == 0.0


def test_bandwidth_rejects_nonpositive_low_edge():
    bandwidth = BandwidthData()
    bandwidth.f_range = 10.0
    bandwidth.high_edge_f = 10.0

    bandwidth.low_edge_f = -1.0
    assert bandwidth.bandwidth() == 0.0

    bandwidth.low_edge_f = 0.0
    assert bandwidth.bandwidth() == 0.0

    bandwidth.low_edge_f = 1.0
    assert bandwidth.bandwidth() == pytest.approx(20.0)
