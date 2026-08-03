#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Python wrapper logic for generalized iterative deconvolution engines.
"""

import warnings

import numpy as np

from mspasspy.ccore.seismic import Seismogram, TimeSeries
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.algorithms._decon_input_validation import (
    validate_external_wavelet_analysis_context,
    validate_external_wavelet_timeseries,
    validate_gid_rf_lag_domain,
)


def _external_wavelet_timeseries(
    external_wavelet,
    wavelet_t0,
    sample_interval,
    legacy_t0,
    alg,
    time_reference,
):
    """Give a bare external GID wavelet an explicit physical time base."""
    if isinstance(external_wavelet, TimeSeries):
        if wavelet_t0 is not None:
            raise ValueError(
                f"{alg}: wavelet_t0 is only valid for a bare vector; a "
                "TimeSeries wavelet retains its own t0"
            )
        return validate_external_wavelet_timeseries(
            external_wavelet,
            alg,
        )
    try:
        values = np.asarray(external_wavelet, dtype=float)
    except Exception as err:
        raise TypeError(
            f"{alg}: external_wavelet must be a TimeSeries or a "
            "one-dimensional numeric vector"
        ) from err
    if values.ndim != 1:
        raise TypeError(
            f"{alg}: external_wavelet must be a TimeSeries or a "
            "one-dimensional numeric vector"
        )
    if values.size == 0:
        raise ValueError(f"{alg}: external_wavelet must not be empty")
    if not np.isfinite(values).all():
        raise ValueError(f"{alg}: external_wavelet contains nonfinite samples")
    if wavelet_t0 is None:
        wavelet_t0 = legacy_t0
        warnings.warn(
            f"{alg}: a bare vector external_wavelet has no time base; "
            f"interpreting its first sample at the legacy deconvolution "
            f"analysis-window origin {legacy_t0}.  Pass wavelet_t0 explicitly "
            "(or pass a TimeSeries) to preserve the physical wavelet origin.",
            UserWarning,
            stacklevel=3,
        )
    if isinstance(wavelet_t0, (bool, np.bool_)):
        raise TypeError(f"{alg}: wavelet_t0 must be a finite real number")
    try:
        wavelet_t0 = float(wavelet_t0)
    except (TypeError, ValueError, OverflowError) as err:
        raise TypeError(f"{alg}: wavelet_t0 must be a finite real number") from err
    if not np.isfinite(wavelet_t0):
        raise ValueError(f"{alg}: wavelet_t0 must be finite")
    if not np.isfinite(sample_interval) or sample_interval <= 0.0:
        raise ValueError(f"{alg}: input sample interval must be finite and positive")
    result = TimeSeries(values.size)
    result.set_t0(wavelet_t0)
    result.set_dt(sample_interval)
    result.tref = time_reference
    result.set_live()
    for i, value in enumerate(values):
        result.data[i] = float(value)
    return result


def _run_gid_rf_decon(
    seis,
    engine,
    engine_type,
    alg,
    *,
    signal_window=None,
    noise_window=None,
    external_wavelet=None,
    wavelet_t0=None,
    external_noise=None,
    QCdata_key=None,
    return_wavelet=False,
):
    """
    Common implementation for time- and frequency-domain GID RF wrappers.
    """
    if not isinstance(seis, Seismogram):
        message = alg
        message += ": illegal type={} for arg0\n".format(str(type(seis)))
        message += "arg0 must be a Seismogram object"
        raise TypeError(message)
    if seis.dead():
        if return_wavelet:
            return [seis, None, None]
        return seis
    if external_wavelet is None and wavelet_t0 is not None:
        raise ValueError(f"{alg}: wavelet_t0 requires external_wavelet")
    if not isinstance(engine, engine_type):
        message = alg
        message += ": required arg1 (engine) is invalid type={}\n".format(
            str(type(engine))
        )
        message += "Must be an instance of {}".format(engine_type.__name__)
        raise TypeError(message)
    d = Seismogram(seis)
    try:
        # GID RF windows are P-relative lag coordinates.  Reject UTC before
        # deriving a default signal window or reading any configured window,
        # and before a reusable engine can receive external state.
        validate_gid_rf_lag_domain(seis, alg)
    except MsPASSError as err:
        if err.severity == ErrorSeverity.Fatal:
            raise
        d.elog.log_error(err)
        d.kill()
        if return_wavelet:
            return [d, None, None]
        return d
    if signal_window is None:
        signal_window = TimeWindow(seis.t0, seis.endtime())
    elif not isinstance(signal_window, TimeWindow):
        raise TypeError("signal_window must be a TimeWindow or None")
    if noise_window is not None and not isinstance(noise_window, TimeWindow):
        raise TypeError("noise_window must be a TimeWindow or None")
    if noise_window is None:
        noise_window = TimeWindow(
            engine.noise_window_start(), engine.noise_window_end()
        )

    if (
        signal_window.start > engine.deconvolution_window_start()
        or signal_window.end < engine.deconvolution_window_end()
        or signal_window.start > engine.output_window_start()
        or signal_window.end < engine.output_window_end()
    ):
        message = (
            "signal_window does not contain the engine output and analysis "
            "windows: output=[{}, {}], analysis=[{}, {}]".format(
                engine.output_window_start(),
                engine.output_window_end(),
                engine.deconvolution_window_start(),
                engine.deconvolution_window_end(),
            )
        )
        d.elog.log_error(alg, message, ErrorSeverity.Invalid)
        d.kill()
        if return_wavelet:
            return [d, None, None]
        return d

    try:
        # Loading is guarded so invalid external wavelets retain the wrapper's
        # normal killed-datum/error-log behavior.  It is deliberately after
        # lag-domain and output/analysis validation so a rejected request
        # cannot mutate a reusable engine's external-wavelet state.
        if external_wavelet is not None:
            candidate_wavelet = _external_wavelet_timeseries(
                external_wavelet,
                wavelet_t0,
                seis.dt,
                engine.deconvolution_window_start(),
                alg,
                seis.tref,
            )
            validate_external_wavelet_analysis_context(
                candidate_wavelet,
                seis,
                engine.deconvolution_window_start(),
                alg,
            )
            engine.loadwavelet(candidate_wavelet)
        if not engine.external_wavelet_is_loaded() and (
            signal_window.start > engine.wavelet_window_start()
            or signal_window.end < engine.wavelet_window_end()
        ):
            message = (
                "signal_window does not contain the engine automatic wavelet "
                "window [{}, {}]".format(
                    engine.wavelet_window_start(), engine.wavelet_window_end()
                )
            )
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
            if return_wavelet:
                return [d, None, None]
            return d
        load_status = engine.load(d, signal_window, noise_window)
        if load_status:
            d.elog.log_error(
                alg,
                "engine.load failed for the configured signal/noise windows",
                ErrorSeverity.Invalid,
            )
            d.kill()
            if return_wavelet:
                return [d, None, None]
            return d
        if external_noise is not None:
            engine.loadnoise(external_noise)
        engine.process()
        rf = Seismogram(engine.getresult())
        qcmd = engine.QCMetrics()
        if QCdata_key:
            qcmd = dict(qcmd)
            qcmd["algorithm"] = alg
            rf[QCdata_key] = qcmd
        if return_wavelet:
            # Fetch diagnostics inside the recoverable error boundary so a
            # failed accessor has the same contract as a failed process call.
            diagnostics = [
                engine.actual_output(),
                engine.output_shaping_wavelet(),
            ]
    except MsPASSError as err:
        if err.severity == ErrorSeverity.Fatal:
            raise
        d.elog.log_error(err)
        d.kill()
        if return_wavelet:
            return [d, None, None]
        return d
    except Exception as err:
        d.elog.log_error(alg, str(err), ErrorSeverity.Invalid)
        d.kill()
        if return_wavelet:
            return [d, None, None]
        return d

    if return_wavelet:
        return [rf, *diagnostics]
    return rf
