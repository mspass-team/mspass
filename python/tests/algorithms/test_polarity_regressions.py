"""Physics-based regressions for free-surface rotation and scalar water level.

Unlike transform/inverse-transform round trips, these tests use the
traction-free boundary conditions of an independent elastic half-space
forward model.  Geographic input axes are East, North, Up; MsPASS
free-surface output axes are SH, SV, P.

Conventions and corrected P/SV matrix:
Svenningsen & Jacobsen (2004), doi:10.1029/2004GL021413, eq. (1)
(corrected right-hand side).
"""

import numpy as np
import pytest

from mspasspy.ccore.algorithms.deconvolution import WaterLevelDecon
from mspasspy.ccore.seismic import Seismogram, SlownessVector
from mspasspy.ccore.utility import Metadata, MsPASSError


def _free_surface_observations(vp, vs, p):
    """Return columns [incident SH, SV, P] as surface [T, R, Z] samples.

    Derive the total surface displacement by solving traction(RZ) = 0
    for reflected P/SV amplitudes, not by inverting the transformation
    under test.  The common factors density and i*omega cancel.
    """
    qa = np.sqrt(1.0 / vp**2 - p**2)
    qb = np.sqrt(1.0 / vs**2 - p**2)
    mu = vs**2
    lam = vp**2 - 2.0 * vs**2

    def traction(k, e):
        k_r, k_z = k
        e_r, e_z = e
        return np.array(
            [mu * (k_z * e_r + k_r * e_z),
             lam * (k_r * e_r + k_z * e_z) + 2.0 * mu * k_z * e_z]
        )

    reflected = [
        (np.array([p, -qa]), vp * np.array([p, -qa])),
        (np.array([p, -qb]), vs * np.array([qb, p])),
    ]
    stresses = np.column_stack([traction(k, e) for k, e in reflected])
    displacements = np.column_stack([e for _, e in reflected])

    incident = [
        (np.array([p, qb]), vs * np.array([qb, -p])),
        (np.array([p, qa]), vp * np.array([p, qa])),
    ]
    surface = np.zeros((3, 3))
    surface[0, 0] = 2.0  # Incident plus reflected SH at a traction-free surface.
    for mode, (k, e) in enumerate(incident, start=1):
        reflection_amplitudes = np.linalg.solve(stresses, -traction(k, e))
        np.testing.assert_allclose(
            stresses @ reflection_amplitudes + traction(k, e),
            np.zeros(2),
            atol=1e-13,
        )
        surface[1:, mode] = e + displacements @ reflection_amplitudes
    return surface


def _seismogram_with_modes(vp, vs, p, azimuth_deg):
    """Create three samples: independent surface SH, SV, and P responses."""
    az = np.deg2rad(azimuth_deg)
    # C++ rotates [E,N,Z] to [T,R,Z] using this convention.
    c, s = np.cos(az), np.sin(az)
    rotation = np.array([[c, -s, 0.0], [s, c, 0.0], [0.0, 0.0, 1.0]])
    enz = rotation.T @ _free_surface_observations(vp, vs, p)

    data = Seismogram(3)
    data.dt = 0.1
    data.t0 = 0.0
    data.set_live()
    for row in range(3):
        for sample in range(3):
            data.data[row, sample] = float(enz[row, sample])

    # az0 is meaningful for normal incidence, where ux=uy=0.
    slow = SlownessVector(p * s, p * c, az)
    return data, slow


@pytest.mark.parametrize("vp,vs", [(5.0, 3.0), (6.0, 3.5), (8.0, 4.5)])
@pytest.mark.parametrize("critical_fraction", [0.0, 0.2, 0.7, 0.9])
@pytest.mark.parametrize("azimuth_deg", [0.0, 37.0, 90.0, 183.0])
def test_free_surface_separates_independently_generated_modes(
    vp, vs, critical_fraction, azimuth_deg
):
    p = critical_fraction / vp
    data, slow = _seismogram_with_modes(vp, vs, p, azimuth_deg)
    data.free_surface_transformation(slow, vp, vs)
    assert data.live
    actual = np.array(
        [[data.data[i, j] for j in range(3)] for i in range(3)]
    )
    # Columns were generated as pure SH, SV, and P, with unit incidence.
    np.testing.assert_allclose(actual, np.eye(3), rtol=0.0, atol=3e-10)


@pytest.mark.parametrize(
    "vp,vs,p",
    [
        (5.0, 3.0, 1.0 / 5.0),  # P critical angle: qP=0, singular.
        (5.0, 3.0, 1.0 / 3.0),  # Beyond P critical and S critical.
        (0.0, 3.0, 0.05),       # Invalid surface speed.
        (5.0, 0.0, 0.05),
        (np.nan, 3.0, 0.05),
        (5.0, 3.0, np.nan),
    ],
)
def test_free_surface_rejects_invalid_inputs_without_mutating_data(vp, vs, p):
    seis = Seismogram(3)
    seis.dt, seis.t0 = 0.1, 0.0
    seis.set_live()
    for i in range(3):
        for j in range(3):
            seis.data[i, j] = 1.0 + 3 * i + j
    original = np.array(
        [[seis.data[i, j] for j in range(3)] for i in range(3)]
    )
    slow = SlownessVector(p, 0.0)
    with pytest.raises(MsPASSError):
        seis.free_surface_transformation(slow, vp, vs)
    np.testing.assert_array_equal(
        np.array([[seis.data[i, j] for j in range(3)] for i in range(3)]),
        original,
    )


@pytest.mark.parametrize("second_sample", [-0.95, -1.0])
def test_water_level_preserves_nonzero_phase_and_zero_bin_symmetry(second_sample):
    # The "none" shaping filter is unity in the frequency domain.
    # Large water level intentionally exercises regularization of many bins.
    md = Metadata(
        {
            "water_level": 12.0,
            "operator_nfft": 128,
            "target_sample_interval": 0.1,
            "shaping_wavelet_dt": 0.1,
            "shaping_wavelet_type": "none",
            "deconvolution_data_window_start": 0.0,
            "deconvolution_data_window_end": 3.1,
        }
    )
    wavelet = np.zeros(32)
    wavelet[0] = 1.0
    wavelet[1] = second_sample

    op = WaterLevelDecon(md)
    op.load(wavelet.tolist(), wavelet.tolist())
    op.process()
    observed = np.asarray(op.actual_output().data, dtype=float)

    nfft = len(observed)
    W = np.fft.fft(wavelet, nfft)
    # ComplexArray::rms() is sqrt(sum |W|^2) / nfft, not np.std(W).
    rms = np.linalg.norm(W) / nfft
    floor = md["water_level"] * rms
    B = W.copy()
    magnitudes = np.abs(B)
    to_floor = magnitudes < floor
    zero_phase_undefined = to_floor & (magnitudes / rms < np.finfo(float).eps)
    nonzero = to_floor & ~zero_phase_undefined
    B[zero_phase_undefined] = floor + 0j
    B[nonzero] *= floor / magnitudes[nonzero]
    assert np.any(nonzero)

    expected_raw = np.fft.ifft(W / B)
    np.testing.assert_allclose(expected_raw.imag, 0.0, atol=2e-12)
    expected = np.roll(expected_raw.real, nfft // 2)
    expected /= np.linalg.norm(expected)
    np.testing.assert_allclose(observed, expected, rtol=1e-9, atol=1e-10)

    if second_sample == -1.0:
        # A true DC spectral null requires a real-valued floor.  The old
        # (floor + i*floor) branch gave only half the correct real DC gain.
        assert W[0] == 0.0
        inverse = np.asarray(op.inverse_wavelet().data, dtype=float)
        np.testing.assert_allclose(
            np.sum(inverse), 1.0 / floor, rtol=1e-9, atol=1e-10
        )
