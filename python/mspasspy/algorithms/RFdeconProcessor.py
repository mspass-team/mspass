#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains a class definition for a wrapper for
the suite of scalar deconvolution methods supported by mspass.
It demonstrates the concept of a processing object created
by wrapping C code.  It also contains a top-level function
that is a pythonic interface that meshes with MsPASS schedulers
for parallel processing called `RFdecon`.   `RFdecon` is a
wrapper for all single-station methods.   It cannot be used for
array methods.

Created on Fri Jul 31 06:24:10 2020

@author: Gary Pavlis
"""

import numpy as np
import os
import re
import tempfile
import warnings
from importlib import resources

from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum, Seismogram, TimeSeries
from mspasspy.ccore.utility import AntelopePf, Metadata, MsPASSError, ErrorSeverity
from mspasspy.util.converter import Metadata2dict
from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.algorithms.basic import TimeWindow, _ExtractComponent
from mspasspy.ccore.algorithms.deconvolution import (
    LeastSquareDecon,
    TimeDomainLeastSquareDecon,
    WaterLevelDecon,
    MultiTaperXcorDecon,
    MultiTaperSpecDivDecon,
    TimeDomainGIDDecon,
    FrequencyDomainGIDDecon,
    _antelope_pf_to_text,
)
from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.algorithms._decon_input_validation import (
    validate_external_wavelet_analysis_context,
    validate_external_wavelet_timeseries,
    validate_gid_rf_lag_domain,
)

# These presets describe the output wavelet and time scales used by the
# downstream imaging objective.  The selected no-argument reference targets
# robust, low-frequency plane-wave/Kirchhoff MTZ imaging; it is not a universal
# RF default.  The historical test/example parameter file remains available by
# explicitly passing ``pf="RFdeconProcessor.pf"``.
DEFAULT_RF_PRESET = "mtz_plane_wave_lowfreq"
RFDECON_PRESET_FILES = {
    "mtz_plane_wave_lowfreq": "RFdeconMTZ.pf",
    "mtz_plane_wave_highres": "RFdeconMTZHighRes.pf",
    "crustal_plane_wave": "RFdeconCrustal.pf",
    "raw_decon_diagnostic": "RFdeconRaw.pf",
}


def _resolve_scalar_preset(preset):
    try:
        return RFDECON_PRESET_FILES[preset]
    except KeyError as err:
        choices = ", ".join(sorted(RFDECON_PRESET_FILES))
        raise ValueError(
            f"unknown RF preset {preset!r}; choices are {choices}"
        ) from err


def _packaged_rf_preset_path(pf_name):
    """Return a filesystem path for an internally selected RF preset.

    Explicit ``pf=`` arguments intentionally retain Antelope's normal PF-path
    resolution.  Only no-argument RF presets use this package resource so a
    processor created by an installed MsPASS distribution does not depend on
    the caller's working directory or ``PFPATH``.
    """
    preset = resources.files("mspasspy").joinpath("data", "pf", pf_name)
    if not preset.is_file():
        raise FileNotFoundError(f"packaged RF preset is missing: {pf_name}")
    return os.fspath(preset)


def _align_scalar_wavelet_to_analysis_grid(wavelet, analysis_window, target_dt):
    """Embed a time-aware scalar source wavelet on the analysis grid.

    Scalar C++ engines accept vectors only.  A short source window therefore
    has to be zero-embedded at its physical time rather than treated as sample
    zero of the analysis vector.  This is the scalar counterpart of the
    time-aware GID wavelet loading policy.
    """
    validate_external_wavelet_timeseries(
        wavelet,
        "RFdeconProcessor.loadwavelet",
        expected_dt=target_dt,
        dt_policy="scalar",
    )
    npts = int(round((analysis_window.end - analysis_window.start) / target_dt)) + 1
    if npts <= 0:
        raise ValueError("RFdeconProcessor.loadwavelet: invalid analysis window")
    offset_float = (wavelet.t0 - analysis_window.start) / target_dt
    offset = int(round(offset_float))
    if not np.isclose(offset_float, offset, rtol=0.0, atol=1.0e-6):
        raise MsPASSError(
            "RFdeconProcessor.loadwavelet: wavelet t0 is not sample-aligned "
            "with deconvolution_data_window_start",
            ErrorSeverity.Invalid,
        )
    result = np.zeros(npts, dtype=float)
    source_start = max(0, -offset)
    destination_start = max(0, offset)
    count = min(wavelet.npts - source_start, npts - destination_start)
    if count > 0:
        source_values = np.asarray(wavelet.data, dtype=float)
        result[destination_start : destination_start + count] = np.asarray(
            source_values[source_start : source_start + count], dtype=float
        )
    if not np.any(result):
        raise MsPASSError(
            "RFdeconProcessor.loadwavelet: configured wavelet has no nonzero "
            "overlap with the deconvolution analysis window",
            ErrorSeverity.Invalid,
        )
    return result


def _checked_time_origin(value, caller, parameter="wavelet_t0"):
    """Validate and normalize a time origin supplied for a bare vector."""
    if isinstance(value, (bool, np.bool_)):
        raise TypeError(f"{caller}: {parameter} must be a finite real number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as err:
        raise TypeError(f"{caller}: {parameter} must be a finite real number") from err
    if not np.isfinite(result):
        raise ValueError(f"{caller}: {parameter} must be finite")
    return result


def _legacy_vector_wavelet_t0(analysis_t0, caller):
    """Return the historical vector origin while making that choice visible."""
    warnings.warn(
        "{}: a bare vector wavelet has no time base; interpreting its first "
        "sample at the legacy deconvolution analysis-window origin {}.  Pass "
        "wavelet_t0 explicitly (or pass a TimeSeries) to preserve the physical "
        "source-wavelet time origin.".format(caller, analysis_t0),
        UserWarning,
        stacklevel=3,
    )
    return analysis_t0


def _as_gid_timeseries(x, dt, t0, argname, tref=None):
    if isinstance(x, TimeSeries):
        return x
    if not np.isfinite(dt) or dt <= 0.0:
        raise ValueError(
            "RFdecon: for GID algorithms, target_sample_interval must be "
            "finite and positive"
        )
    t0 = _checked_time_origin(t0, "RFdecon", f"{argname}_t0")
    if isinstance(x, DoubleVector):
        values = np.asarray(x, dtype=float)
    else:
        try:
            values = np.asarray(x, dtype=float)
        except Exception as err:
            raise TypeError(
                "RFdecon: for GID algorithms, {} must be a TimeSeries "
                "or one-dimensional numeric vector".format(argname)
            ) from err
    if values.ndim != 1:
        raise TypeError(
            "RFdecon: for GID algorithms, {} must be a TimeSeries "
            "or one-dimensional numeric vector".format(argname)
        )
    if values.size <= 0:
        raise ValueError(
            "RFdecon: for GID algorithms, {} must not be empty".format(argname)
        )
    if not np.isfinite(values).all():
        raise ValueError(
            "RFdecon: for GID algorithms, {} contains nonfinite samples".format(argname)
        )
    ts = TimeSeries(len(values))
    ts.set_t0(t0)
    ts.set_dt(dt)
    if tref is not None:
        ts.tref = tref
    ts.set_live()
    for i, value in enumerate(values):
        ts.data[i] = float(value)
    return ts


def _get_pf_branch_with_legacy_fallback(pfhandle, preferred, legacy):
    if preferred in pfhandle.arr_keys():
        return Metadata(pfhandle.get_branch(preferred))
    if preferred in pfhandle.tbl_keys() or pfhandle.is_defined(preferred):
        raise MsPASSError(
            f"{preferred} is defined but is not an &Arr parameter branch",
            ErrorSeverity.Fatal,
        )
    return Metadata(pfhandle.get_branch(legacy))


def _write_pickled_pf_text(pf, text):
    suffix = "_" + os.path.basename(pf) if pf else ".pf"
    fp = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", suffix=suffix, delete=False
    )
    try:
        fp.write(text)
        return fp.name
    finally:
        fp.close()


_TIME_DOMAIN_GID_ALIASES = {"GeneralizedIterative", "TimeDomainGID", "time", "td"}
_FREQUENCY_DOMAIN_GID_ALIASES = {"FrequencyDomainGID", "frequency", "fd"}
_SUPPORTED_GID_DECONVOLUTION_TYPES = {
    "least_square",
    "water_level",
    "multi_taper",
    "cnr",
    "cnr3c",
    "ns_gid",
    "noise_stable",
    "noise_aware_stable",
    "group_sparse",
    "group_lasso",
    "sparse_group_lasso",
}
_SUPPORTED_GID_PENALTY_FUNCTIONS = {
    "none",
    "boxcar",
    "cosine_taper",
    "shaping_wavelet",
    "resolution_kernel",
    "adaptive_memory",
}


def _canonical_gid_algorithm(alg):
    if alg in _TIME_DOMAIN_GID_ALIASES:
        return "TimeDomainGID"
    if alg in _FREQUENCY_DOMAIN_GID_ALIASES:
        return "FrequencyDomainGID"
    raise ValueError(
        "GID configuration helpers require alg='TimeDomainGID', "
        "'GeneralizedIterative', or 'FrequencyDomainGID'"
    )


def _gid_default_pf(alg):
    return (
        "TimeDomainGIDDecon.pf"
        if _canonical_gid_algorithm(alg) == "TimeDomainGID"
        else "FrequencyDomainGIDDecon.pf"
    )


def _gid_branch_name(alg):
    return (
        "time_domain_gid_deconvolution"
        if _canonical_gid_algorithm(alg) == "TimeDomainGID"
        else "frequency_domain_gid_deconvolution"
    )


def _format_pf_value(value):
    if isinstance(value, (bool, np.bool_)):
        return "true" if bool(value) else "false"
    if isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.15g}"
    if isinstance(value, str):
        if value.strip() != value or re.search(r"\s", value):
            raise ValueError(
                f"pf scalar string values cannot contain whitespace: {value!r}"
            )
        return value
    raise TypeError(
        "GID parameter values must be bool, int, float, numpy scalar, "
        "or whitespace-free string"
    )


def _normalize_metadata_scalar_value(value, key, context):
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            return value.item()
        raise TypeError(f"{context}: metadata parameter {key!r} must be scalar")
    if isinstance(value, (list, tuple, dict, Metadata)):
        raise TypeError(f"{context}: metadata parameter {key!r} must be scalar")
    return value


def _normalize_metadata_scalars(md, context):
    result = Metadata()
    for key in md.keys():
        result[key] = _normalize_metadata_scalar_value(md[key], key, context)
    return result


def _validate_component_index(value, name, context):
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{context}: {name} must be integer 0, 1, or 2")
    if isinstance(value, np.integer):
        value = int(value)
    elif not isinstance(value, int):
        raise ValueError(f"{context}: {name} must be integer 0, 1, or 2")
    if value not in (0, 1, 2):
        raise ValueError(f"{context}: {name} must be integer 0, 1, or 2")
    return value


def _find_pf_branch_line_range(lines, branch_name, context):
    branch_pattern = re.compile(r"^\s*" + re.escape(branch_name) + r"\s+&Arr\{\s*$")
    starts = [i for i, line in enumerate(lines) if branch_pattern.match(line)]
    if len(starts) != 1:
        raise MsPASSError(
            f"{context}: expected one {branch_name!r} branch, found {len(starts)}",
            ErrorSeverity.Fatal,
        )
    start = starts[0]
    depth = 0
    for i in range(start, len(lines)):
        depth += lines[i].count("{")
        depth -= lines[i].count("}")
        if i > start and depth == 0:
            return start, i
    raise MsPASSError(
        f"{context}: unterminated {branch_name!r} branch",
        ErrorSeverity.Fatal,
    )


def _replace_pf_scalar_in_branch(text, branch_name, key, value, context):
    lines = text.splitlines()
    start, end = _find_pf_branch_line_range(lines, branch_name, context)
    key_pattern = re.compile(r"^(\s*)" + re.escape(key) + r"\s+\S+(\s*(#.*)?)$")
    matches = []
    depth = 1
    for i in range(start + 1, end):
        match = key_pattern.match(lines[i])
        if depth == 1 and match:
            matches.append((i, match))
        depth += lines[i].count("{")
        depth -= lines[i].count("}")
    if len(matches) != 1:
        raise MsPASSError(
            f"{context}: expected one scalar key {key!r} in {branch_name!r}, "
            f"found {len(matches)}",
            ErrorSeverity.Fatal,
        )
    line_index, match = matches[0]
    indent = match.group(1)
    suffix = match.group(2) or ""
    lines[line_index] = f"{indent}{key} {_format_pf_value(value)}{suffix}"
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def _normalize_gid_option_aliases(
    deconvolution_type=None,
    gid_mode=None,
    lag_weight_penalty_function=None,
    gid_penalty_function=None,
    gid_parameters=None,
):
    gid_parameters = dict(gid_parameters or {})

    parameter_mode = gid_parameters.pop("deconvolution_type", None)
    modes = [x for x in (deconvolution_type, gid_mode, parameter_mode) if x is not None]
    if len(set(modes)) > 1:
        raise ValueError("conflicting GID deconvolution_type/gid_mode values")
    mode = modes[0] if modes else None
    if mode is not None and mode not in _SUPPORTED_GID_DECONVOLUTION_TYPES:
        raise ValueError(f"unsupported GID deconvolution_type={mode!r}")

    parameter_penalty = gid_parameters.pop("lag_weight_penalty_function", None)
    penalties = [
        x
        for x in (
            lag_weight_penalty_function,
            gid_penalty_function,
            parameter_penalty,
        )
        if x is not None
    ]
    if len(set(penalties)) > 1:
        raise ValueError("conflicting GID lag_weight_penalty_function values")
    penalty = penalties[0] if penalties else None
    if penalty is not None and penalty not in _SUPPORTED_GID_PENALTY_FUNCTIONS:
        raise ValueError(f"unsupported GID lag_weight_penalty_function={penalty!r}")

    if mode is not None:
        gid_parameters["deconvolution_type"] = mode
    if penalty is not None:
        gid_parameters["lag_weight_penalty_function"] = penalty
    return gid_parameters


def _gid_overrides_requested(
    deconvolution_type=None,
    gid_mode=None,
    lag_weight_penalty_function=None,
    gid_penalty_function=None,
    gid_parameters=None,
):
    return any(
        x is not None
        for x in (
            deconvolution_type,
            gid_mode,
            lag_weight_penalty_function,
            gid_penalty_function,
        )
    ) or bool(gid_parameters)


def make_gid_pf_text(
    alg="TimeDomainGID",
    pf=None,
    *,
    deconvolution_type=None,
    gid_mode=None,
    lag_weight_penalty_function=None,
    gid_penalty_function=None,
    gid_parameters=None,
    _pf_text=None,
):
    """
    Return GID parameter-file text with validated GID-branch overrides applied.

    This helper is the supported Python path for comparing GID inverse or solver
    modes and lag-penalty settings without hand-editing Antelope pf text.  It
    only changes keys in the top-level GID branch; leaf inverse-operator
    parameters should still be changed through a custom pf file or the leaf
    ``changeparameter`` path.  Custom pf files should follow the shipped GID
    pf style: the GID branch opener on its own line and scalar keys stored as
    single-token values.
    """
    canonical_alg = _canonical_gid_algorithm(alg)
    pf = _gid_default_pf(canonical_alg) if pf in (None, "RFdeconProcessor.pf") else pf
    text = _pf_text if _pf_text is not None else _antelope_pf_to_text(AntelopePf(pf))
    overrides = _normalize_gid_option_aliases(
        deconvolution_type=deconvolution_type,
        gid_mode=gid_mode,
        lag_weight_penalty_function=lag_weight_penalty_function,
        gid_penalty_function=gid_penalty_function,
        gid_parameters=gid_parameters,
    )
    branch_name = _gid_branch_name(canonical_alg)
    context = f"make_gid_pf_text({canonical_alg})"
    for key, value in overrides.items():
        text = _replace_pf_scalar_in_branch(text, branch_name, key, value, context)
    return text


def make_gid_pf(
    alg="TimeDomainGID",
    pf=None,
    *,
    deconvolution_type=None,
    gid_mode=None,
    lag_weight_penalty_function=None,
    gid_penalty_function=None,
    gid_parameters=None,
):
    """
    Build an ``AntelopePf`` for a GID engine with selected GID options.

    Examples
    --------
    ``make_gid_pf(alg="TimeDomainGID", deconvolution_type="group_sparse")``
    returns a parameter object suitable for ``TimeDomainGIDDecon``.
    """
    canonical_alg = _canonical_gid_algorithm(alg)
    pf = _gid_default_pf(canonical_alg) if pf in (None, "RFdeconProcessor.pf") else pf
    text = make_gid_pf_text(
        canonical_alg,
        pf,
        deconvolution_type=deconvolution_type,
        gid_mode=gid_mode,
        lag_weight_penalty_function=lag_weight_penalty_function,
        gid_penalty_function=gid_penalty_function,
        gid_parameters=gid_parameters,
    )
    pf_to_load = _write_pickled_pf_text(pf, text)
    try:
        return AntelopePf(pf_to_load)
    finally:
        try:
            os.unlink(pf_to_load)
        except OSError:
            pass


def make_gid_engine(
    alg="TimeDomainGID",
    pf=None,
    *,
    deconvolution_type=None,
    gid_mode=None,
    lag_weight_penalty_function=None,
    gid_penalty_function=None,
    gid_parameters=None,
):
    """
    Build a configured low-level time- or frequency-domain GID engine.

    The returned object is passed to ``TimeDomainGIDRFDecon`` or
    ``FrequencyDomainGIDRFDecon`` directly.  For the high-level ``RFdecon``
    wrapper, use ``RFdeconProcessor`` or pass the same GID keywords directly
    to ``RFdecon``.
    """
    canonical_alg = _canonical_gid_algorithm(alg)
    pfhandle = make_gid_pf(
        canonical_alg,
        pf,
        deconvolution_type=deconvolution_type,
        gid_mode=gid_mode,
        lag_weight_penalty_function=lag_weight_penalty_function,
        gid_penalty_function=gid_penalty_function,
        gid_parameters=gid_parameters,
    )
    if canonical_alg == "TimeDomainGID":
        return TimeDomainGIDDecon(pfhandle)
    return FrequencyDomainGIDDecon(pfhandle)


class RFdeconProcessor:
    """
    This class is a wrapper for the suite of receiver function deconvolution
    methods we call scalar methods.  That is, the operation is reducible to
    two time series:   wavelet signal and the data (TimeSeries) signal.
    That is in contrast to three component methods that always treat the
    data as vector samples.  The class should be created as a global
    processor object to be used in a spark job.  The design assumes the
    processor object will be passed as an argument to the RFdecon
    function that should appear as a function in a spark map call.

    Supported algorithm names are ``LeastSquares``, ``WaterLevel``,
    ``MultiTaperPowerXcor``, ``MultiTaperPowerSpecDiv``,
    ``TimeDomainLeastSquares``, ``GeneralizedIterative``/``TimeDomainGID``,
    and ``FrequencyDomainGID``.  ``MultiTaperXcor`` and
    ``MultiTaperSpecDiv`` are retained as deprecated compatibility aliases for
    the power-stabilized multitaper operators.
    The default scalar operators solve linear convolution problems and return
    the configured lag window rather than a wrapped circular-convolution
    result.  Frequency-domain operators use padding before FFT processing;
    ``TimeDomainLeastSquares`` builds a Toeplitz linear-convolution matrix.
    The GID variants iterate on a sparse impulse response and return the shaped
    receiver function for the configured output window.
    """

    def __repr__(self) -> str:
        repr_str = "{type}(alg='{alg}', md='{md}')".format(
            type=str(self.__class__), alg=self.algorithm, md=self.md
        )
        return repr_str

    def __str__(self) -> str:
        md_str = str(Metadata2dict(self.md))
        processor_str = "{type}(alg='{alg}', md='{md}')".format(
            type=str(self.__class__), alg=self.algorithm, md=self.md
        )
        return processor_str

    def _load_gid_cached_wavelet_to_engine(self):
        """
        Move a Python-level cached GID external wavelet into the C++ engine.
        """
        if not self.__is_3c_engine:
            return
        target_dt = self.md.get_double("target_sample_interval")
        if hasattr(self, "wvector"):
            if hasattr(self, "wtimeseries"):
                self.processor.loadwavelet(self.wtimeseries)
            else:
                self.processor.loadwavelet(
                    _as_gid_timeseries(
                        self.wvector, target_dt, self.dwin.start, "wavelet"
                    )
                )

    def _load_gid_cached_noise_to_engine(self):
        """
        Move Python-level cached GID external noise into the C++ engine.
        """
        if not self.__is_3c_engine:
            return
        target_dt = self.md.get_double("target_sample_interval")
        if self.__uses_noise and hasattr(self, "nvector"):
            if hasattr(self, "ntimeseries"):
                self.processor.loadnoise(self.ntimeseries)
            else:
                self.processor.loadnoise(
                    _as_gid_timeseries(
                        self.nvector, target_dt, self.nwin.start, "noisedata"
                    )
                )
        elif hasattr(self, "external_noise_spectrum"):
            self.processor.loadnoise(self.external_noise_spectrum)

    def _sync_gid_external_state_to_engine(self):
        """
        Move Python-level GID external wavelet/noise caches into the C++ engine.
        """
        self._load_gid_cached_wavelet_to_engine()
        self._load_gid_cached_noise_to_engine()

    def _gid_signal_window(self):
        """Return the input window required by a GID engine load.

        The loaded signal must cover output and sparse-analysis windows, plus
        the automatic source-wavelet window unless a prepared external
        wavelet is already cached on this processor or engine.
        """
        windows = [self.full_dwin, self.dwin]
        has_external_wavelet = hasattr(self, "wvector") or (
            hasattr(self.processor, "external_wavelet_is_loaded")
            and self.processor.external_wavelet_is_loaded()
        )
        if not has_external_wavelet:
            if self.md.is_defined("wavelet_window_start"):
                windows.append(
                    TimeWindow(
                        self.md.get_double("wavelet_window_start"),
                        self.md.get_double("wavelet_window_end"),
                    )
                )
            else:
                windows.append(self.dwin)
        return TimeWindow(
            min(window.start for window in windows),
            max(window.end for window in windows),
        )

    def clear_external_wavelet(self):
        """
        Clear a preconfigured external GID wavelet from this wrapper and engine.

        This is only valid for GID processors.  Scalar processors do not expose
        a consistent external-state clear operation because their wrapped C++
        engines may already hold the last loaded wavelet.
        """
        if not self.__is_3c_engine:
            raise RuntimeError(
                "clear_external_wavelet is only valid for GID processors"
            )
        self.processor.clear_external_wavelet()
        for attr in ("wvector", "wtimeseries"):
            if hasattr(self, attr):
                delattr(self, attr)

    def clear_external_noise(self):
        """
        Clear preconfigured external GID noise from this wrapper and engine.

        This is only valid for GID processors.  Scalar processors do not expose
        a consistent external-state clear operation because their wrapped C++
        engines may already hold the last loaded noise estimate.
        """
        if not self.__is_3c_engine:
            raise RuntimeError("clear_external_noise is only valid for GID processors")
        self.processor.clear_external_noise()
        for attr in ("nvector", "ntimeseries", "external_noise_spectrum"):
            if hasattr(self, attr):
                delattr(self, attr)

    def __init__(
        self,
        alg="LeastSquares",
        pf=None,
        _pf_text=None,
        *,
        preset=DEFAULT_RF_PRESET,
        deconvolution_type=None,
        gid_mode=None,
        lag_weight_penalty_function=None,
        gid_penalty_function=None,
        gid_parameters=None,
    ):
        self.algorithm = alg
        self.__is_3c_engine = False
        if pf is None:
            if alg not in (
                "GeneralizedIterative",
                "TimeDomainGID",
                "FrequencyDomainGID",
            ):
                pf = _packaged_rf_preset_path(_resolve_scalar_preset(preset))
            else:
                if preset != DEFAULT_RF_PRESET:
                    raise ValueError(
                        "GID high-resolution/crustal/raw sensitivity profiles "
                        "require an explicit parameter file"
                    )
                # GID has its own nested parameter format.  This profile uses
                # the same MTZ windows/Ricker output as the scalar default but
                # labels the result as an experimental sensitivity product.
                pf = _packaged_rf_preset_path("RFdeconGIDMTZ.pf")
        elif preset != DEFAULT_RF_PRESET:
            raise ValueError("specify either pf or a non-default preset, not both")
        if pf == "RFdeconProcessor.pf":
            if alg in ("GeneralizedIterative", "TimeDomainGID"):
                pf = "TimeDomainGIDDecon.pf"
            elif alg == "FrequencyDomainGID":
                pf = "FrequencyDomainGIDDecon.pf"
        if _gid_overrides_requested(
            deconvolution_type=deconvolution_type,
            gid_mode=gid_mode,
            lag_weight_penalty_function=lag_weight_penalty_function,
            gid_penalty_function=gid_penalty_function,
            gid_parameters=gid_parameters,
        ):
            if alg not in (
                "GeneralizedIterative",
                "TimeDomainGID",
                "FrequencyDomainGID",
            ):
                raise ValueError("GID configuration keywords require a GID algorithm")
            _pf_text = make_gid_pf_text(
                alg,
                pf,
                deconvolution_type=deconvolution_type,
                gid_mode=gid_mode,
                lag_weight_penalty_function=lag_weight_penalty_function,
                gid_penalty_function=gid_penalty_function,
                gid_parameters=gid_parameters,
                _pf_text=_pf_text,
            )
        self.pf = pf
        pf_to_load = pf
        if _pf_text is not None:
            pf_to_load = _write_pickled_pf_text(pf, _pf_text)
        # use a copy in what is more or less a switch-case block
        # to be robust - I don't think any of the constructors below
        # alter pfhandle but the cost is tiny for this stability
        try:
            pfhandle = AntelopePf(pf_to_load)
        finally:
            if _pf_text is not None:
                try:
                    os.unlink(pf_to_load)
                except OSError:
                    pass
        self._pf_text = (
            _pf_text if _pf_text is not None else _antelope_pf_to_text(pfhandle)
        )
        if self.algorithm == "LeastSquares":
            # In this and elif blocks below we convert
            # return of get_branch to a Metadata container
            # that is necessary because get_branch returns the
            # AntelopePf subclass and we want this to be a clean
            # Metadata object.  Further, at present a pf will not
            # serialize
            self.md = Metadata(pfhandle.get_branch("LeastSquare"))
            self.processor = LeastSquareDecon(self.md)
            self.__uses_noise = False
        elif self.algorithm == "TimeDomainLeastSquares":
            self.md = Metadata(pfhandle.get_branch("TimeDomainLeastSquare"))
            self.processor = TimeDomainLeastSquareDecon(self.md)
            self.__uses_noise = False
        elif alg == "WaterLevel":
            self.md = Metadata(pfhandle.get_branch("WaterLevel"))
            self.processor = WaterLevelDecon(self.md)
            self.__uses_noise = False
        elif alg in ("MultiTaperPowerXcor", "MultiTaperXcor"):
            if alg == "MultiTaperPowerXcor":
                self.md = _get_pf_branch_with_legacy_fallback(
                    pfhandle, "MultiTaperPowerXcor", "MultiTaperXcor"
                )
            else:
                self.md = Metadata(pfhandle.get_branch("MultiTaperXcor"))
            if alg == "MultiTaperXcor":
                warnings.warn(
                    "MultiTaperXcor is a deprecated compatibility name for "
                    "the multitaper power-stabilized untapered-phase operator. "
                    "Use MultiTaperPowerXcor for new code.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            self.processor = MultiTaperXcorDecon(self.md)
            self.__uses_noise = True
        elif alg in ("MultiTaperPowerSpecDiv", "MultiTaperSpecDiv"):
            if alg == "MultiTaperPowerSpecDiv":
                self.md = _get_pf_branch_with_legacy_fallback(
                    pfhandle, "MultiTaperPowerSpecDiv", "MultiTaperSpecDiv"
                )
            else:
                self.md = Metadata(pfhandle.get_branch("MultiTaperSpecDiv"))
            if alg == "MultiTaperSpecDiv":
                warnings.warn(
                    "MultiTaperSpecDiv is a deprecated compatibility name for "
                    "the multitaper power-stabilized untapered-phase operator. "
                    "Use MultiTaperPowerSpecDiv for new code.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            self.processor = MultiTaperSpecDivDecon(self.md)
            self.__uses_noise = True
        elif alg in ("GeneralizedIterative", "TimeDomainGID"):
            mdtop = pfhandle.get_branch("deconvolution_operator_type")
            self.md = Metadata(mdtop.get_branch("time_domain_gid_deconvolution"))
            self.processor = TimeDomainGIDDecon(pfhandle)
            self.__uses_noise = True
            self.__is_3c_engine = True
        elif alg == "FrequencyDomainGID":
            mdtop = pfhandle.get_branch("deconvolution_operator_type")
            self.md = Metadata(mdtop.get_branch("frequency_domain_gid_deconvolution"))
            self.processor = FrequencyDomainGIDDecon(pfhandle)
            self.__uses_noise = True
            self.__is_3c_engine = True
        else:
            raise RuntimeError("Illegal value for alg=" + alg)

    def __getstate__(self):
        state = {
            "algorithm": self.algorithm,
            "pf": self.pf,
            "md": Metadata(self.md),
        }
        if self.__is_3c_engine:
            state["processor"] = self.processor
            attrs = []
        else:
            attrs = ["dvector", "wvector", "nvector"]
        for attr in attrs:
            if hasattr(self, attr):
                state[attr] = getattr(self, attr)
        return state

    def __setstate__(self, state):
        alg = state["algorithm"]
        if alg not in ("GeneralizedIterative", "TimeDomainGID", "FrequencyDomainGID"):
            self.algorithm = alg
            self.pf = state["pf"]
            self._pf_text = state.get("_pf_text")
            self.md = Metadata(state["md"])
            self.__is_3c_engine = False
            if alg == "LeastSquares":
                self.processor = LeastSquareDecon(self.md)
                self.__uses_noise = False
            elif alg == "TimeDomainLeastSquares":
                self.processor = TimeDomainLeastSquareDecon(self.md)
                self.__uses_noise = False
            elif alg == "WaterLevel":
                self.processor = WaterLevelDecon(self.md)
                self.__uses_noise = False
            elif alg in ("MultiTaperPowerXcor", "MultiTaperXcor"):
                self.processor = MultiTaperXcorDecon(self.md)
                self.__uses_noise = True
            elif alg in ("MultiTaperPowerSpecDiv", "MultiTaperSpecDiv"):
                self.processor = MultiTaperSpecDivDecon(self.md)
                self.__uses_noise = True
            else:
                raise RuntimeError("Illegal value for alg=" + alg)
            for attr, value in state.items():
                if attr not in ("algorithm", "pf", "_pf_text", "md"):
                    setattr(self, attr, value)
            return
        self.algorithm = alg
        self.pf = state["pf"]
        self._pf_text = state.get("_pf_text")
        self.md = Metadata(state["md"])
        self.processor = state.get("processor")
        if self.processor is None:
            self.__init__(
                state["algorithm"], state["pf"], _pf_text=state.get("_pf_text")
            )
        else:
            self.__uses_noise = True
            self.__is_3c_engine = True
        for attr, value in state.items():
            if attr not in ("algorithm", "pf", "_pf_text", "md", "processor"):
                setattr(self, attr, value)

    def loaddata(self, d, dtype="Seismogram", component=0, window=False):
        """
        Loads data for processing.  When window is set true
        use the internal pf definition of data time window
        and window the data.  The dtype parameter changes the
        behavior of this algorithm significantly depending on
        the setting.   It can be one of the following:
        Seismogram, TimeSeries, or raw_vector.   For the first
        two the data to process will be extracted in a
        pf specified window if window is True.  If window is
        False TimeSeries data will be passed directly and
        Seismogram data will have the data defined by the
        component parameter copied to the internal data
        vector workspace.   If dtype is set to raw_vector
        d is assumed to be a raw numpy vector of doubles or
        the aliased std::vector used in ccore, for example,
        in the TimeSeries object s vector.  Setting dtype
        to raw_vector and window True will result in this
        method throwing a RuntimeError exception as the
        combination is not possible since raw_vector data
        have no time base.

        :param d: input data (contents expected depend upon
            value of dtype parameter).
        :param dtype: string defining the form d is expected
            to be (see details above)
        :param component: component of Seismogram data to
            load as data vector.  Ignored if dtype is raw_vector
            or TimeSeries.
        :param window: boolean controlling internally
            defined windowing.  (see details above)

        :return:  Nothing is returned
        """
        # First basic sanity checks
        if dtype == "raw_vector" and window:
            raise RuntimeError(
                "RFdeconProcessor.loaddata:  "
                + "Illegal argument combination\nwindow cannot be true with raw_vector input"
            )
        if not (
            dtype == "Seismogram" or dtype == "TimeSeries" or dtype == "raw_vector"
        ):
            raise RuntimeError(
                "RFdeconProcessor.loaddata:  " + " Illegal dtype parameter=" + dtype
            )
        dvector = []
        if window:
            if dtype == "Seismogram":
                ts = _ExtractComponent(d, component)
                ts = WindowData(ts, self.dwin.start, self.dwin.end)
                dvector = ts.data
            elif dtype == "TimeSeries":
                ts = WindowData(d, self.dwin.start, self.dwin.end)
                dvector = ts.data
            else:
                dvector = d
        else:
            if dtype == "Seismogram":
                ts = _ExtractComponent(d, component)
                dvector = ts.data
            elif dtype == "TimeSeries":
                dvector = d.data
            else:
                dvector = d
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.dvector = np.array(dvector)

    def loadwavelet(
        self,
        w,
        dtype="Seismogram",
        component=2,
        window=False,
        wavelet_t0=None,
    ):
        """
        Load the source wavelet used by the wrapped deconvolution engine.

        :param w: wavelet data to load.
        :param dtype: input representation; one of ``"Seismogram"``,
            ``"TimeSeries"``, or ``"raw_vector"``.
        :param component: component index used when ``dtype`` is
            ``"Seismogram"``.
        :param window: when True extract the configured deconvolution window
            before loading the wavelet.
        :param wavelet_t0: physical time of sample zero when ``dtype`` is
            ``"raw_vector"``.  Bare vectors have no time base.  When omitted,
            the historical interpretation (sample zero at
            ``deconvolution_data_window_start``) is retained with a warning.
            Do not specify this argument for a ``TimeSeries`` or
            ``Seismogram``; their own time coordinates are authoritative.

        For GID processors this datum-independent method validates only the
        wavelet's intrinsic state and configured target sample interval.
        Datum-dependent time-reference and common-grid compatibility are
        checked by :func:`RFdecon` before it mutates a reused engine, and by
        the C++ processor when processing directly loaded data.

        :raises RuntimeError: if ``dtype`` or the ``dtype``/``window``
            combination is invalid.
        """
        # This code is painfully similar to loaddata. To reduce errors
        # only the names have been changed to protect the innocent
        if dtype == "raw_vector" and window:
            raise RuntimeError(
                "RFdeconProcessor.loadwavelet:  "
                + "Illegal argument combination\nwindow cannot be true with raw_vector input"
            )
        if not (
            dtype == "Seismogram" or dtype == "TimeSeries" or dtype == "raw_vector"
        ):
            raise RuntimeError(
                "RFdeconProcessor.loadwavelet:  " + " Illegal dtype parameter=" + dtype
            )
        if dtype != "raw_vector" and wavelet_t0 is not None:
            raise ValueError(
                "RFdeconProcessor.loadwavelet: wavelet_t0 is only valid for "
                "raw_vector input; TimeSeries and Seismogram inputs retain "
                "their own time base"
            )
        wvector = []
        wtimeseries = None
        if window:
            if dtype == "Seismogram":
                ts = _ExtractComponent(w, component)
                ts = WindowData(ts, self.waveletwin.start, self.waveletwin.end)
                wvector = ts.data
                wtimeseries = ts
            elif dtype == "TimeSeries":
                ts = WindowData(w, self.waveletwin.start, self.waveletwin.end)
                wvector = ts.data
                wtimeseries = ts
            else:
                wvector = w
        if wtimeseries is not None:
            # Validate the object that will actually be loaded.  In
            # particular, window=True deliberately ignores invalid samples
            # outside the extracted source interval.
            validate_external_wavelet_timeseries(
                wtimeseries,
                "RFdeconProcessor.loadwavelet",
                expected_dt=self.md.get_double("target_sample_interval"),
                dt_policy="gid" if self.__is_3c_engine else "scalar",
            )
        else:
            if dtype == "Seismogram":
                ts = _ExtractComponent(w, component)
                wvector = ts.data
                wtimeseries = ts
            elif dtype == "TimeSeries":
                wvector = w.data
                wtimeseries = TimeSeries(w)
            else:
                wvector = w
        if dtype == "raw_vector":
            if wavelet_t0 is None:
                vector_t0 = _legacy_vector_wavelet_t0(
                    self.dwin.start, "RFdeconProcessor.loadwavelet"
                )
                # Scalar engines historically received this vector unchanged.
                # Preserve that exact representation when no new time origin
                # was requested.  GID still needs a TimeSeries for its C++ API.
                if self.__is_3c_engine:
                    wtimeseries = _as_gid_timeseries(
                        wvector,
                        self.md.get_double("target_sample_interval"),
                        vector_t0,
                        "wavelet",
                    )
            else:
                vector_t0 = _checked_time_origin(
                    wavelet_t0, "RFdeconProcessor.loadwavelet"
                )
                wtimeseries = _as_gid_timeseries(
                    wvector,
                    self.md.get_double("target_sample_interval"),
                    vector_t0,
                    "wavelet",
                )
        if not self.__is_3c_engine and wtimeseries is not None:
            new_wvector = _align_scalar_wavelet_to_analysis_grid(
                wtimeseries,
                self.dwin,
                self.md.get_double("target_sample_interval"),
            )
        else:
            new_wvector = np.array(wvector)
        new_wtimeseries = None
        if self.__is_3c_engine and wtimeseries is not None:
            new_wtimeseries = TimeSeries(wtimeseries)
        if self.__is_3c_engine:
            if new_wtimeseries is not None:
                self.processor.loadwavelet(new_wtimeseries)
            else:
                target_dt = self.md.get_double("target_sample_interval")
                self.processor.loadwavelet(
                    _as_gid_timeseries(
                        new_wvector, target_dt, self.dwin.start, "wavelet"
                    )
                )
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.wvector = new_wvector
        if self.__is_3c_engine:
            if new_wtimeseries is not None:
                self.wtimeseries = new_wtimeseries
            elif hasattr(self, "wtimeseries"):
                del self.wtimeseries

    def loadnoise(self, n, dtype="Seismogram", component=2, window=False):
        """
        Load noise data used by deconvolution methods that require it.

        :param n: noise data to load.
        :param dtype: input representation; one of ``"Seismogram"``,
            ``"TimeSeries"``, or ``"raw_vector"``.
        :param component: component index used when ``dtype`` is
            ``"Seismogram"``.
        :param window: when True extract the configured noise window before
            loading the noise vector.
        :raises RuntimeError: if ``dtype`` or the ``dtype``/``window``
            combination is invalid.
        """
        # First basic sanity checks
        # Return immediately for methods that ignore noise.
        # Note we do this silently assuming the function wrapper below
        if not self.__uses_noise:
            return
        if dtype == "raw_vector" and window:
            raise RuntimeError(
                "RFdeconProcessor.loadnoise:  "
                + "Illegal argument combination\nwindow cannot be true with raw_vector input"
            )
        if not (
            dtype == "Seismogram" or dtype == "TimeSeries" or dtype == "raw_vector"
        ):
            raise RuntimeError(
                "RFdeconProcessor.loadnoise:  " + " Illegal dtype parameter=" + dtype
            )
        nvector = []
        # IMPORTANT  these two parameters are not required by the
        # ScalarDecon C code but need to be inserted in pf for any algorithm
        # that requires noise data (i.e. multitaper) and the window
        # options is desired
        if window:
            tws = self.md.get_double("noise_window_start")
            twe = self.md.get_double("noise_window_end")
            if dtype == "Seismogram":
                ts = _ExtractComponent(n, component)
                ts = WindowData(ts, tws, twe)
                nvector = ts.data
            elif dtype == "TimeSeries":
                ts = WindowData(n, tws, twe)
                nvector = ts.data
            else:
                nvector = n
        else:
            if dtype == "Seismogram":
                ts = _ExtractComponent(n, component)
                nvector = ts.data
            elif dtype == "TimeSeries":
                nvector = n.data
            else:
                nvector = n
        new_nvector = np.array(nvector)
        new_ntimeseries = None
        if self.__is_3c_engine and dtype == "TimeSeries" and not window:
            new_ntimeseries = TimeSeries(n)
        if self.__is_3c_engine:
            if new_ntimeseries is not None:
                self.processor.loadnoise(new_ntimeseries)
            else:
                target_dt = self.md.get_double("target_sample_interval")
                self.processor.loadnoise(
                    _as_gid_timeseries(
                        new_nvector, target_dt, self.nwin.start, "noisedata"
                    )
                )
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.nvector = new_nvector
        if self.__is_3c_engine:
            if new_ntimeseries is not None:
                self.ntimeseries = new_ntimeseries
            elif hasattr(self, "ntimeseries"):
                del self.ntimeseries
            if hasattr(self, "external_noise_spectrum"):
                del self.external_noise_spectrum

    def apply(self):
        """
        Compute the RF estimate using the algorithm defined internally.

        :return: vector of data that are the RF estimate computed from previously loaded data.
        """
        if self.__is_3c_engine:
            raise RuntimeError(
                "RFdeconProcessor.apply cannot process a three-component "
                "iterative engine component by component; use apply_3c instead"
            )
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.getresult()

    def apply_3c(self, d):
        """
        Compute a three-component generalized iterative deconvolution result.

        The GID engines operate on a full `Seismogram` because the iteration
        picks vector spikes from all three components at once.  This method is
        intentionally separate from `apply`, which remains the scalar
        component-wise interface for conventional RF methods.
        """
        if not self.__is_3c_engine:
            raise RuntimeError("apply_3c is only valid for GID algorithms")
        try:
            load_status = self.processor.load(d, self._gid_signal_window(), self.nwin)
        except MsPASSError as err:
            if err.severity == ErrorSeverity.Fatal:
                raise
            raise MsPASSError(
                "RFdeconProcessor.apply_3c: configured signal/noise windows "
                "could not be loaded from input data: {}".format(str(err)),
                ErrorSeverity.Invalid,
            )
        if load_status:
            raise MsPASSError(
                "RFdeconProcessor.apply_3c: configured signal/noise windows "
                "could not be loaded from input data",
                ErrorSeverity.Invalid,
            )
        self._sync_gid_external_state_to_engine()
        self.processor.process()
        return Seismogram(self.processor.getresult())

    def actual_output(self):
        """
        The actual output of a decon operator is the inverse filter applied to
        the wavelet.  By design it is an approximation of the shaping wavelet
        defined for this operator.

        :return: Actual output as a full :class:`~mspasspy.ccore.seismic.TimeSeries`.
            The C++ deconvolution API returns a TimeSeries suitable for
            higher-level algorithms such as WindowData.  Metadata are
            intentionally minimal.  Because actual-output
            waveforms are normally zero-phase, their time origin is centered
            on zero lag.
        """
        if self.__is_3c_engine:
            return self.processor.actual_output()
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.actual_output()

    def output_shaping_wavelet(self):
        """
        Return the output shaping wavelet, ws(t) in Wang and Pavlis (2016),
        as a full :class:`~mspasspy.ccore.seismic.TimeSeries`.

        For GID this is the configured wavelet used to convolve the sparse
        impulse response to form the finite-bandwidth receiver function.  For
        scalar operators it is the optional post-deconvolution
        shaping/bandlimiting wavelet.  The C++ API returns a TimeSeries
        suitable for WindowData and other metadata-aware TimeSeries APIs.
        """
        if self.__is_3c_engine:
            return self.processor.output_shaping_wavelet()
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.output_shaping_wavelet()

    def ideal_output(self):
        """
        Legacy alias for output_shaping_wavelet.

        New code should use output_shaping_wavelet to avoid confusing this
        diagnostic with the actual output/resolution kernel.
        """
        return self.output_shaping_wavelet()

    def inverse_filter(self):
        """
        This method returns the actual inverse filter that if convolved with
        the original data will produce the RF estimate.  Note the filter is
        meaningful only if the source wavelet is minimum phase.  A standard
        theorem from time series analysis shows that the inverse of mixed
        phase wavelet is usually unstable and a maximum phase wavelet is always
        unstable.   Fourier-based methods can still compute a stable solution
        even with a mixed phase wavelet because of the implied circular
        convolution.

        The result is returned as  TimeSeries object.
        """

        if self.__is_3c_engine:
            return self.processor.inverse_wavelet()
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.inverse_wavelet()

    def QCMetrics(self, prediction_error_key="prediction_error") -> dict:
        """
        All decon algorithms compute a set of algorithm dependent quality
        control metrics.  The return is a Metadata container.
        All this wrapper really does is translate that return into
        a python dictionary that can be used as the base of a subdocument
        posting to outputs.  This method MUST ONLY BE CALLED after
        calling the process method of the C++ engine.
        """
        # the base of what is returned is an echo of the input parameter set
        qcmd = dict(self.md)
        # merge in an output of the implementations QCMetrics method
        qcmeth_output = dict(self.processor.QCMetrics())
        qcmd.update(qcmeth_output)
        decon_type = qcmeth_output.get(
            "deconvolution_type", qcmd.get("deconvolution_type")
        )
        if decon_type != "group_sparse":
            qcmd = {
                key: value
                for key, value in qcmd.items()
                if not key.startswith("group_sparse")
            }
        else:
            qcmd = {
                key: value
                for key, value in qcmd.items()
                if not key.startswith("gid_penalty")
                and not key.startswith("lag_weight")
            }
        if decon_type != "ns_gid":
            qcmd = {
                key: value
                for key, value in qcmd.items()
                if not key.startswith("ns_gid")
            }
        if self.__is_3c_engine:
            return dict(qcmd)
        # always compute the prediction error
        perr = self._prediction_error()
        qcmd[prediction_error_key] = perr
        return dict(qcmd)

    def change_parameters(self, md):
        """
        Use this method to change the internal parameter setting of the
        processor.  It can only change the parameters for a particular
        algorithm.   A new instance of this class needs to be created if
        you need to switch to a different algorithm.   It does little
        more than call the change-parameter method of the already loaded
        processor.  A new processor should be constructed for GID-level window
        or iteration-control changes that are intentionally rejected by the
        GID engines.

        :param md: is a mspass.Metadata object containing required parameters
            for the alternative algorithm.
        """
        parameter_md = _normalize_metadata_scalars(
            md, "RFdeconProcessor.change_parameters"
        )
        if hasattr(self.processor, "changeparameter"):
            self.processor.changeparameter(parameter_md)
        elif hasattr(self.processor, "change_parameter"):
            self.processor.change_parameter(parameter_md)
        else:
            raise AttributeError(
                "wrapped deconvolution processor does not expose a parameter "
                "change method"
            )
        if not self.__is_3c_engine:
            self.md = parameter_md

    @property
    def uses_noise(self):
        """
        Return True when the configured deconvolution engine requires noise data.

        :rtype: bool
        """
        return self.__uses_noise

    @property
    def is_3c_engine(self):
        """
        Return True when the wrapped engine processes three-component data.

        :rtype: bool
        """
        return self.__is_3c_engine

    @property
    def dwin(self):
        """
        Return the configured deconvolution data window.

        :rtype: :class:`mspasspy.ccore.utility.TimeWindow`
        """
        tws = self.md.get_double("deconvolution_data_window_start")
        twe = self.md.get_double("deconvolution_data_window_end")
        return TimeWindow(tws, twe)

    @property
    def full_dwin(self):
        """
        Return the full data window used by three-component GID engines.

        If no explicit full-data window is configured, this property falls back
        to :attr:`dwin`.

        :rtype: :class:`mspasspy.ccore.utility.TimeWindow`
        """
        if self.md.is_defined("full_data_window_start"):
            tws = self.md.get_double("full_data_window_start")
            twe = self.md.get_double("full_data_window_end")
            return TimeWindow(tws, twe)
        return self.dwin

    @property
    def waveletwin(self):
        """Return the configured source-wavelet window.

        Legacy parameter files without these keys retain the historical
        behavior of using the analysis window for the source estimate.
        """
        if self.md.is_defined("wavelet_window_start"):
            return TimeWindow(
                self.md.get_double("wavelet_window_start"),
                self.md.get_double("wavelet_window_end"),
            )
        return self.dwin

    @property
    def nwin(self):
        """
        Return the configured noise window.

        Engines that do not use noise data return the default, broad
        :class:`mspasspy.ccore.utility.TimeWindow` instance.

        :rtype: :class:`mspasspy.ccore.utility.TimeWindow`
        """
        if self.__uses_noise:
            tws = self.md.get_double("noise_window_start")
            twe = self.md.get_double("noise_window_end")
            return TimeWindow(tws, twe)
        else:
            return TimeWindow()  # always initialize even if not used

    def _prediction_error(self) -> float:
        """
        Small internal function used to compute prediction error of
        deconvolution operator defined as norm(ao-io)/norm(io) where
        norm is L2.
        """
        ao = self.actual_output()
        io = self.output_shaping_wavelet()
        # with internal use can assume ao and io are the same length
        err = ao - io
        return np.linalg.norm(err.data) / np.linalg.norm(io.data)


@mspass_func_wrapper
def RFdecon(
    d,
    *args,
    engine=None,
    alg="LeastSquares",
    pf=None,
    preset=DEFAULT_RF_PRESET,
    deconvolution_type=None,
    gid_mode=None,
    lag_weight_penalty_function=None,
    gid_penalty_function=None,
    gid_parameters=None,
    wavelet=None,
    wavelet_t0=None,
    noisedata=None,
    wcomp=2,
    ncomp=2,
    QCdocument_key="RFdecon_properties",
    object_history=False,
    alg_name="RFdecon",
    alg_id=None,
    dryrun=False,
    handles_ensembles=False,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
):
    """
    Use this function to compute conventional receiver functions
    from a single three component seismogram. In this function,
    an instance of wrapper class RFdeconProcessor will be built and
    initialized with alg and pf.

    Default assumes d contains all data sections required to do
    the deconvolution with the wavelet in component 2 (3 for matlab
    and FORTRAN people).  By default the data and noise
    (if required by the algorithm) sections will be extracted from
    the (assumed larger) data section using time windows defined
    internally in the processor pf definition.   For variations (e.g.
    adding tapering to one or more of the time series inputs)
    use the d, wavelet, and (if required) noise arguments to load
    each component separately.  Note d is dogmatically required
    to be three component data.  Conventional scalar methods accept
    optional wavelet and noisedata as plain numeric vectors.  GID methods
    accept prepared TimeSeries inputs or one-dimensional numeric vectors;
    raw wavelet vectors are converted to TimeSeries using the processor target
    sample interval and ``wavelet_t0``.  Omitting ``wavelet_t0`` retains the
    legacy analysis-window-start origin with an explicit warning.  Raw noise
    vectors use the configured noise-window start.
    GID receiver-function processing uses P-relative lag coordinates; convert
    UTC input first with ``ator(P-arrival epoch)``.  This restriction belongs
    to the GID RF workflow and is not a general TimeSeries restriction.
    When a reused GID engine already has a preconfigured external wavelet
    or noise estimate, omitting wavelet or noisedata preserves that state.
    Call RFdeconProcessor.clear_external_wavelet() or
    clear_external_noise() before RFdecon to force component/window-derived
    input again.
    If any configured data, wavelet, or noise window cannot be extracted, the
    function returns a killed datum and does not attach the QC subdocument.

    To make use of the extended outputs from RFdeconProcessor
    algorithms (e.g. actual output of the computed operator)
    call those methods after this function returns successfully
    with a three-component seismogram output.  That is possible
    because the processor object caches the most recent wavelet
    and inverse used for the deconvolution.   An exception is
    that all algorithms call their QCmetrics method of processor
    and push them to the headers of the deconvolved output.
    QCmetric attributes are algorithm dependent.

    The ProcessingHistory feature can optionally be enabled by
    setting the save_history argument to True.   When enabled one should
    normally set a unique id for the algid argument.

    :param d:  Seismogram input data.  See notes above about the required time span.
    :type d:  Must be a Seismogram object or the function will throw a TypeError exception.
    :param engine:   optional instance of a RFdeconProcessor
        object.   By default the function instantiates an instance of
        a processor for each call to the function.   For algorithms
        like the multitaper based algorithms with a high initialization
        cost performance will improve by sending an instance to the
        function via this argument.
    :type engine:  None or an instance of RFdeconProcessor.
        When None (default) an instance of an RFdeconProcessor is
        created on entry based on the keyword defined by the alg
        argument.   The algorithm built into the instance of
        RFdeconProcessor is used if engine is not null.  A reused GID engine
        preserves any external wavelet/noise loaded on the processor unless
        the corresponding clear_external_* method is called.
    :param alg: The algorithm to be applied, used for initializing
        a RFdeconProcessor object.  Ignored if engine is used.  Conventional
        scalar methods are applied component by component.  Generalized
        iterative methods (`GeneralizedIterative`, `TimeDomainGID`, and
        `FrequencyDomainGID`) operate on the full three-component seismogram
        because spike selection is vector-valued.  By default they preserve the
        receiver-function convention of deriving the source wavelet from
        component 2, but callers may pass a prepared TimeSeries wavelet or a
        one-dimensional numeric vector to use the same external wavelet for all
        components.
    :param pf: Optional explicit PF file.  When omitted, conventional scalar
        methods use the ``mtz_plane_wave_lowfreq`` preset and GID uses its
        explicitly labelled MTZ sensitivity PF.  Ignored if engine is used.
    :type pf:  string defining an absolute path for the file name, a path
        relative to a directory defined by PFPATH, or ``None``.
    :param preset: Named scalar objective preset used only when ``pf`` is
        omitted.  Choices are ``mtz_plane_wave_lowfreq`` (default),
        ``mtz_plane_wave_highres``, ``crustal_plane_wave``, and
        ``raw_decon_diagnostic``.  GID non-default profiles require an
        explicit parameter file.
    :param deconvolution_type: optional GID inverse or solver mode override
        used only when ``alg`` selects ``GeneralizedIterative``,
        ``TimeDomainGID``, or ``FrequencyDomainGID``.  Common values are ``ns_gid``,
        ``least_square``, ``cnr``, and ``group_sparse``.  This builds a fresh
        configured GID processor instead of requiring callers to edit a pf
        file.
    :param gid_mode: compatibility alias for ``deconvolution_type``.
    :param lag_weight_penalty_function: optional GID lag-weight penalty
        override, e.g. ``adaptive_memory`` or ``none``.
    :param gid_penalty_function: compatibility alias for
        ``lag_weight_penalty_function``.
    :param gid_parameters: optional dictionary of additional scalar keys in
        the top-level GID branch to override, such as
        ``{"group_sparse_lambda_scale": 0.8}``.
    :param wavelet:   vector of doubles (numpy array or the
        std::vector container internal to TimeSeries object) or TimeSeries
        defining the wavelet to use to compute deconvolution operator.
        Default is None.  For a fresh processor this uses the configured
        component/window-derived wavelet estimate.  For a reused GID
        processor it preserves any previously loaded external wavelet.
        For scalar and GID algorithms, use ``wavelet_t0`` to define the first
        sample time of a raw vector.  A TimeSeries always retains its own
        ``t0`` and ``dt``.
    :type wavelet:  None, TimeSeries, or an iterable vector container
        (in MsPASS that means a python array, a numpy array, or a DoubleVector)
    :param wavelet_t0: physical time of sample zero for a bare vector supplied
        with ``wavelet``.  It is invalid with a TimeSeries wavelet or when no
        wavelet is supplied.  ``None`` preserves the historical vector
        interpretation—sample zero at the deconvolution analysis-window
        start—and emits a warning so that origin is not silently assumed.
    :type wavelet_t0: finite float or None
    :param noisedata:  vector of doubles (numpy array or the
        std::vector container internal to TimeSeries object), TimeSeries, or
        PowerSpectrum defining noise data to use for computing regularization.
        Not all RF
        estimation algorithms use noise estimators so this parameter
        is optional.   It can also be extracted from d depending on
        parameter file options.
        For GID algorithms, raw vectors are converted to a TimeSeries using
        target_sample_interval and noise_window_start.  If noisedata is None,
        a reused GID processor preserves any previously loaded external noise.
        External PowerSpectrum noise is supported by the NS-GID inverse mode
        and by ``group_sparse`` GID, which uses the NS-GID inverse internally.
        It regularizes only the inverse operator; residual-domain stopping and
        sparse support decisions still use the configured noise window or
        loaded TimeSeries/vector noise where applicable.
    :type noisedata:  None, TimeSeries, PowerSpectrum, or an iterable vector container
        (in MsPASS that means a python array, a numpy array, or a DoubleVector)
    :param wcomp:  When defined from Seismogram d the wavelet
        estimate in conventional RFs is one of the components that
        are most P wave dominated. That is always one of three
        things:  Z, L of LQT, or the L component from the output of
        Kennett's free surface transformation operator.  The
        default is 2, which for ccore.Seismogram is always one of
        the above.   This parameter would be changed only if the
        data has undergone some novel transformation not yet invented
        and the best wavelet estimate was on in 2 (3 with FORTRAN
        and matlab numbering).
    :type wcomp:  int (must 0, 1, or 2)
    :param ncomp: component number to use to compute noise.  This is used
        only if the algorithm in processor requires a noise estimate.
        Normally it should be the same as wcomp and is by default (2).
    :type ncomp:  int (must be 0, 1, or 2)
    :param QCdocument_key:   A summary of the parameters defining the
        deconvolution operator (really a dump of the pf content used for
        creating the engine) and computed QC attributes are posted to a
        python dictionary.   That content is posted to the outputs
        Metadata container with the key defined by this argument.
        In MongoDB lingo that means when saved to the database the
        dictionary content associated with this key becomes a "subdocument".
    :type QCdocument_key:  string (default is "RFdecon_properties")
    :param object_history: boolean to enable or disable saving object
        level history.  Default is False.  Note this functionality is
        implemented via the mspass_func_wrapper decorator.
    :param alg_name:   When history is enabled this is the algorithm name
        assigned to the stamp for applying this algorithm.
        Default ("RFdecon") should normally be used.
        Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param alg_id:  algorithm id to assign to history record (used only if
        object_history is set True.)
        Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param dryrun:  When true only the arguments are checked for validity.
        When true nothing is calculated and the original data are returned.
        Note this functionality is implemented via the mspass_func_wrapper decorator.

    :return:  Returns a Seismogram object containing the RF estimates.
        The orientations are always the same as the input.
    """

    if not isinstance(d, Seismogram):
        message = "RFdecon:  arg0 is of type={}.  Must be a Seismogram object".format(
            str(type(d))
        )
        raise TypeError(message)
    if d.dead():
        return d
    wcomp = _validate_component_index(wcomp, "wcomp", "RFdecon")
    ncomp = _validate_component_index(ncomp, "ncomp", "RFdecon")
    if wavelet is None and wavelet_t0 is not None:
        raise ValueError("RFdecon: wavelet_t0 requires a wavelet argument")
    if isinstance(wavelet, TimeSeries) and wavelet_t0 is not None:
        raise ValueError(
            "RFdecon: wavelet_t0 is only valid for a bare vector; a "
            "TimeSeries wavelet retains its own t0"
        )
    if engine:
        if _gid_overrides_requested(
            deconvolution_type=deconvolution_type,
            gid_mode=gid_mode,
            lag_weight_penalty_function=lag_weight_penalty_function,
            gid_penalty_function=gid_penalty_function,
            gid_parameters=gid_parameters,
        ):
            raise ValueError(
                "GID configuration keywords cannot be used with an already "
                "constructed RFdeconProcessor engine"
            )
        if isinstance(engine, RFdeconProcessor):
            processor = engine
        else:
            message = (
                "RFdecon:   illegal type for defined engine argument = {}\n".format(
                    type(engine)
                )
            )
            message += "If defined must be an instance of RFdeconProcessor"
            raise TypeError(message)
    else:
        processor = RFdeconProcessor(
            alg,
            pf,
            preset=preset,
            deconvolution_type=deconvolution_type,
            gid_mode=gid_mode,
            lag_weight_penalty_function=lag_weight_penalty_function,
            gid_penalty_function=gid_penalty_function,
            gid_parameters=gid_parameters,
        )

    if processor.is_3c_engine:
        try:
            validate_gid_rf_lag_domain(d, "RFdecon")
            target_dt = processor.md.get_double("target_sample_interval")
            if wavelet is not None:
                vector_wavelet_t0 = wavelet_t0
                if not isinstance(wavelet, TimeSeries) and vector_wavelet_t0 is None:
                    vector_wavelet_t0 = _legacy_vector_wavelet_t0(
                        processor.dwin.start, "RFdecon"
                    )
                wts = _as_gid_timeseries(
                    wavelet,
                    target_dt,
                    (
                        wavelet.t0
                        if isinstance(wavelet, TimeSeries)
                        else _checked_time_origin(vector_wavelet_t0, "RFdecon")
                    ),
                    "wavelet",
                    tref=d.tref,
                )
                validate_external_wavelet_timeseries(
                    wts,
                    "RFdecon",
                    expected_dt=target_dt,
                    dt_policy="gid",
                )
                validate_external_wavelet_analysis_context(
                    wts, d, processor.dwin.start, "RFdecon"
                )
                processor.loadwavelet(wts, dtype="TimeSeries")
            if noisedata is not None:
                if isinstance(noisedata, PowerSpectrum):
                    processor.processor.loadnoise(noisedata)
                    processor.external_noise_spectrum = noisedata
                    for attr in ("nvector", "ntimeseries"):
                        if hasattr(processor, attr):
                            delattr(processor, attr)
                else:
                    nts = _as_gid_timeseries(
                        noisedata,
                        target_dt,
                        processor.nwin.start,
                        "noisedata",
                    )
                    processor.loadnoise(nts, dtype="TimeSeries")
            result = processor.apply_3c(d)
            subdoc = processor.QCMetrics()
            subdoc["algorithm"] = processor.algorithm
            result[QCdocument_key] = subdoc
            return result
        except MsPASSError as err:
            if err.severity == ErrorSeverity.Fatal:
                raise
            d.kill()
            d.elog.log_error(err)
            return d
        except Exception as err:
            d.kill()
            d.elog.log_error("RFdecon", str(err), ErrorSeverity.Invalid)
            return d

    try:
        if wavelet is not None:
            if isinstance(wavelet, TimeSeries):
                processor.loadwavelet(wavelet, dtype="TimeSeries")
            else:
                processor.loadwavelet(
                    wavelet, dtype="raw_vector", wavelet_t0=wavelet_t0
                )
        else:
            # processor.loadwavelet(d,dtype='Seismogram',window=True,component=wcomp)
            processor.loadwavelet(d, window=True, component=wcomp)
        if processor.uses_noise:
            if noisedata is not None:
                processor.loadnoise(noisedata, dtype="raw_vector")
            else:
                processor.loadnoise(d, window=True, component=ncomp)
    except MsPASSError as err:
        if err.severity == ErrorSeverity.Fatal:
            raise
        d.kill()
        d.elog.log_error(err)
        return d
    # We window data before computing RF estimates for efficiency
    # Otherwise we would call the window operator 3 times below
    # WindowData will kill the output if the window doesn't match,
    # which is reason for the test immediately after this call
    result = WindowData(d, processor.dwin.start, processor.dwin.end)
    if result.dead():
        return result
    npts = result.npts
    try:
        for k in range(3):
            processor.loaddata(result, component=k)
            x = processor.apply()
            # overwrite this component's data in the result Seismogram
            # Use some caution handling any size mismatch
            nx = len(x)
            if nx >= npts:
                for i in range(npts):
                    result.data[k, i] = x[i]
            else:
                # this may not be the fastest way to do this but it is simple and clean
                # matters little since this is an error condition and should be rare
                for i in range(npts):
                    if i < nx:
                        result.data[k, i] = x[i]
                    else:
                        result.data[k, i] = 0.0
                # This is actually an error condition so we log it
                message = (
                    "Windowing size mismatch.\nData window length = %d which is less than operator length= %d"
                    % (nx, npts)
                )
                result.elog.log_error("RFdecon", message, ErrorSeverity.Complaint)
    except MsPASSError as err:
        if err.severity == ErrorSeverity.Fatal:
            raise
        result.kill()
        result.elog.log_error(err)
    except Exception as err:
        result.kill()
        result.elog.log_error(
            "RFdecon",
            "Unexpected exception caught: {}".format(str(err)),
            ErrorSeverity.Invalid,
        )
    finally:
        if result.dead():
            return result
        # assume this method creates the dictionary we use as base for the
        # QC subdocument.  Note that always includes the prediction error
        subdoc = processor.QCMetrics()
        result[QCdocument_key] = subdoc
        return result
