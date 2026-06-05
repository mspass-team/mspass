#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Python wrapper logic for generalized iterative deconvolution engines.
"""

from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError


def _run_gid_rf_decon(
    seis,
    engine,
    engine_type,
    alg,
    *,
    signal_window=None,
    noise_window=None,
    external_wavelet=None,
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
    if not isinstance(engine, engine_type):
        message = alg
        message += ": required arg1 (engine) is invalid type={}\n".format(
            str(type(engine))
        )
        message += "Must be an instance of {}".format(engine_type.__name__)
        raise TypeError(message)
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

    d = Seismogram(seis)
    if (
        signal_window.start > engine.deconvolution_window_start()
        or signal_window.end < engine.deconvolution_window_end()
    ):
        message = (
            "signal_window does not contain the engine deconvolution window "
            "[{}, {}]".format(
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
        if external_wavelet is not None:
            engine.loadwavelet(external_wavelet)
        if external_noise is not None:
            engine.loadnoise(external_noise)
        engine.process()
        rf = Seismogram(engine.getresult())
        qcmd = engine.QCMetrics()
        if QCdata_key:
            qcmd = dict(qcmd)
            qcmd["algorithm"] = alg
            rf[QCdata_key] = qcmd
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
        return [rf, engine.actual_output(), engine.output_shaping_wavelet()]
    return rf
