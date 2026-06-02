#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python wrappers for the frequency-domain generalized iterative deconvolution
engine.

The operator uses the same sparse-spike iterative model as
``TimeDomainGIDDecon`` but evaluates the configurable inverse operator in the
frequency domain with padded FFTs.  The returned receiver function is the
shaped sparse-spike estimate for the requested lag window, not a circularly
wrapped inverse-filter response.
"""

from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.seismic import Seismogram
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.ccore.algorithms.deconvolution import FrequencyDomainGIDDecon


@mspass_func_wrapper
def FrequencyDomainGIDRFDecon(
    seis,
    engine,
    *args,
    signal_window=None,
    noise_window=None,
    QCdata_key="FrequencyDomainGIDDecon_properties",
    return_wavelet=False,
    object_history=False,
    alg_name="FrequencyDomainGIDRFDecon",
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
    Estimate a three-component receiver function with the frequency-domain
    generalized iterative deconvolution algorithm.

    The inverse operator mode is selected in the engine parameter file.  The
    supported modes include damped least squares, water level, multitaper, and
    CNR.  Damping or water-level protection is required for stable behavior
    near source-wavelet spectral zeros.
    """
    alg = "FrequencyDomainGIDRFDecon"
    if not isinstance(seis, Seismogram):
        message = alg
        message += ": illegal type={} for arg0\n".format(str(type(seis)))
        message += "arg0 must be a Seismogram object"
        raise TypeError(message)
    if seis.dead():
        if return_wavelet:
            return [seis, None, None]
        return seis
    if not isinstance(engine, FrequencyDomainGIDDecon):
        message = alg
        message += ": required arg1 (engine) is invalid type={}\n".format(
            str(type(engine))
        )
        message += "Must be an instance of FrequencyDomainGIDDecon"
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
