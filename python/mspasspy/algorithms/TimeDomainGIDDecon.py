#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python wrappers for the time-domain generalized iterative deconvolution
engine.

The underlying operator follows the generalized iterative deconvolution
interpretation of Wang and Pavlis (2016).  Iteration is performed on a sparse
three-component spike train.  The returned receiver function is that sparse
train convolved with the configured shaping wavelet for the requested lag
window; it is not the raw spike train itself.
"""

from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.ccore.algorithms.deconvolution import TimeDomainGIDDecon


@mspass_func_wrapper
def TimeDomainGIDRFDecon(
    seis,
    engine,
    *args,
    signal_window=None,
    noise_window=None,
    QCdata_key="TimeDomainGIDDecon_properties",
    return_wavelet=False,
    object_history=False,
    alg_name="TimeDomainGIDRFDecon",
    alg_id=None,
    dryrun=False,
    inplace_return=False,
    handles_ensembles=False,
    function_return_key=None,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
):
    """
    Estimate a three-component receiver function with generalized iterative
    deconvolution.

    The engine first builds a configurable inverse operator for the source
    wavelet, applies it to the current residual to form the detection function,
    picks the largest vector spike, subtracts the corresponding shaped source
    pulse from the residual, and repeats until the residual-improvement or
    residual-energy convergence criteria are met.

    :param seis: input `Seismogram` containing signal and noise windows.
    :param engine: configured `TimeDomainGIDDecon` instance.
    :param signal_window: optional `TimeWindow` defining the full output and
        iterative analysis window.  When omitted the input datum time range is
        used.
    :param noise_window: optional `TimeWindow` defining pre-event noise.  When
        omitted the engine's parameter-file noise window is used.
    :param QCdata_key: metadata key used to store the engine's QC metrics.
    :param return_wavelet: when True return `[rf, actual_output, ideal_output]`.
    :return: deconvolved `Seismogram`, or the tuple described above.
    """
    alg = "TimeDomainGIDRFDecon"
    if not isinstance(seis, Seismogram):
        message = alg
        message += ": illegal type={} for arg0\n".format(str(type(seis)))
        message += "arg0 must be a Seismogram object"
        raise TypeError(message)
    if seis.dead():
        if return_wavelet:
            return [seis, None, None]
        return seis
    if not isinstance(engine, TimeDomainGIDDecon):
        message = alg
        message += ": required arg1 (engine) is invalid type={}\n".format(
            str(type(engine))
        )
        message += "Must be an instance of TimeDomainGIDDecon"
        raise TypeError(message)
    if signal_window is None:
        signal_window = TimeWindow(seis.t0, seis.endtime())
    elif not isinstance(signal_window, TimeWindow):
        raise TypeError("signal_window must be a TimeWindow or None")
    if noise_window is not None and not isinstance(noise_window, TimeWindow):
        raise TypeError("noise_window must be a TimeWindow or None")

    d = Seismogram(seis)
    try:
        if noise_window is None:
            engine.load(d, signal_window)
        else:
            engine.load(d, signal_window, noise_window)
        engine.process()
        rf = Seismogram(engine.getresult())
        qcmd = engine.QCMetrics()
        if QCdata_key:
            qcmd = dict(qcmd)
            qcmd["algorithm"] = alg
            rf[QCdata_key] = qcmd
    except MsPASSError as err:
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
        return [rf, engine.actual_output(), engine.ideal_output()]
    return rf
