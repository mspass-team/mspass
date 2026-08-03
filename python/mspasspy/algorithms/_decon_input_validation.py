"""Shared validation for metadata-aware deconvolution inputs."""

import numpy as np

from mspasspy.ccore.seismic import TimeReferenceType, TimeSeries
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def validate_gid_rf_lag_domain(datum, caller):
    """Require the P-relative lag coordinates used by the GID RF API."""
    if datum.tref == TimeReferenceType.UTC:
        raise MsPASSError(
            f"{caller}: GID receiver-function processing requires P-relative "
            "lag coordinates; convert UTC data first with "
            "ator(P-arrival epoch)",
            ErrorSeverity.Invalid,
        )
    return datum


def validate_external_wavelet_timeseries(
    wavelet,
    caller,
    *,
    expected_dt=None,
    expected_dt_name="target_sample_interval",
    dt_policy="gid",
):
    """Validate a TimeSeries wavelet without changing it or an engine.

    This helper deliberately performs every check before callers update a
    Python cache or invoke a stateful C++ ``loadwavelet`` method.
    """
    if not isinstance(wavelet, TimeSeries):
        raise TypeError(f"{caller}: wavelet must be a TimeSeries")
    if wavelet.dead():
        raise MsPASSError(
            f"{caller}: external wavelet is marked dead", ErrorSeverity.Invalid
        )
    if wavelet.npts <= 0:
        raise MsPASSError(
            f"{caller}: external wavelet is empty", ErrorSeverity.Invalid
        )
    if not np.isfinite(wavelet.dt) or wavelet.dt <= 0.0:
        raise MsPASSError(
            f"{caller}: external TimeSeries dt must be finite and positive",
            ErrorSeverity.Invalid,
        )
    if not np.isfinite(wavelet.t0) or not np.isfinite(wavelet.endtime()):
        raise MsPASSError(
            f"{caller}: external TimeSeries t0 and endtime must be finite",
            ErrorSeverity.Invalid,
        )
    samples = np.asarray(wavelet.data, dtype=float)
    if samples.ndim != 1 or samples.size != wavelet.npts:
        raise MsPASSError(
            f"{caller}: external wavelet sample vector is inconsistent with npts",
            ErrorSeverity.Invalid,
        )
    if not np.isfinite(samples).all():
        raise MsPASSError(
            f"{caller}: external wavelet contains nonfinite samples",
            ErrorSeverity.Invalid,
        )
    if expected_dt is not None:
        if not np.isfinite(expected_dt) or expected_dt <= 0.0:
            raise MsPASSError(
                f"{caller}: {expected_dt_name} must be finite and positive",
                ErrorSeverity.Invalid,
            )
        if dt_policy == "gid":
            matches = abs(float(wavelet.dt) - float(expected_dt)) <= (
                1.0e-6
                * max(1.0, abs(float(wavelet.dt)), abs(float(expected_dt)))
            )
        elif dt_policy == "scalar":
            matches = np.isclose(
                wavelet.dt, expected_dt, rtol=1.0e-7, atol=1.0e-10
            )
        else:
            raise ValueError(f"{caller}: unknown dt tolerance policy={dt_policy}")
        if not matches:
            raise MsPASSError(
                f"{caller}: external TimeSeries dt does not match "
                f"{expected_dt_name}",
                ErrorSeverity.Invalid,
            )
    return wavelet


def validate_external_wavelet_analysis_context(
    wavelet, datum, analysis_t0, caller
):
    """Validate datum-dependent GID compatibility without changing an engine.

    Overlap is intentionally not required.  The C++ common-grid builder
    supports disjoint, sample-aligned records when their union is representable.
    """
    validate_gid_rf_lag_domain(datum, caller)
    if wavelet.tref != datum.tref:
        raise MsPASSError(
            f"{caller}: external wavelet TimeReferenceType does not match "
            "the input datum",
            ErrorSeverity.Invalid,
        )
    if not np.isfinite(datum.dt) or datum.dt <= 0.0:
        raise MsPASSError(
            f"{caller}: input datum dt must be finite and positive",
            ErrorSeverity.Invalid,
        )
    gid_dt_tolerance = 1.0e-6 * max(
        1.0, abs(float(wavelet.dt)), abs(float(datum.dt))
    )
    if abs(float(wavelet.dt) - float(datum.dt)) > gid_dt_tolerance:
        raise MsPASSError(
            f"{caller}: external wavelet dt does not match the input datum",
            ErrorSeverity.Invalid,
        )
    if not np.isfinite(analysis_t0):
        raise MsPASSError(
            f"{caller}: analysis time origin must be finite",
            ErrorSeverity.Invalid,
        )

    def checked_grid_coordinate(time, label):
        q = (float(time) - float(analysis_t0)) / float(datum.dt)
        int_limit = np.iinfo(np.int32).max
        if not np.isfinite(q) or abs(q) > int_limit:
            raise MsPASSError(
                f"{caller}: {label} offset exceeds the supported "
                "signed-int grid limit",
                ErrorSeverity.Invalid,
            )
        nearest = round(q)
        tolerance = min(1.0e-3, 1.0e-6 * max(1.0, abs(q)))
        if abs(q - nearest) > tolerance:
            raise MsPASSError(
                f"{caller}: {label} is not aligned to the analysis sample grid",
                ErrorSeverity.Invalid,
            )

    checked_grid_coordinate(wavelet.t0, "wavelet start")
    checked_grid_coordinate(wavelet.endtime(), "wavelet endpoint")
    datum_grid_offset = (
        float(analysis_t0) - float(datum.t0)
    ) / float(datum.dt)
    if not np.isfinite(datum_grid_offset) or not np.isclose(
        datum_grid_offset,
        round(datum_grid_offset),
        rtol=0.0,
        atol=min(1.0e-3, 1.0e-6 * max(1.0, abs(datum_grid_offset))),
    ):
        raise MsPASSError(
            f"{caller}: analysis origin is not aligned to the input datum grid",
            ErrorSeverity.Invalid,
        )
    return wavelet
