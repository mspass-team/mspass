#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python wrappers for the time-domain generalized iterative deconvolution
engine.

The underlying operator follows the generalized iterative deconvolution
interpretation of Wang and Pavlis (2016).  Iteration is performed on a sparse
three-component impulse response.  The returned receiver function is that
sparse impulse response convolved with the configured output shaping wavelet
for the requested lag window; it is not the raw sparse result itself.
"""

from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.algorithms.deconvolution import TimeDomainGIDDecon
from mspasspy.algorithms._gid_decon_wrapper import _run_gid_rf_decon


@mspass_func_wrapper
def TimeDomainGIDRFDecon(
    seis,
    engine,
    *args,
    signal_window=None,
    noise_window=None,
    external_wavelet=None,
    external_noise=None,
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
        used.  The window must contain the engine's configured deconvolution
        window.
    :param noise_window: optional `TimeWindow` defining pre-event noise.  When
        omitted the engine's parameter-file noise window is used.
    :param external_wavelet: optional prepared wavelet passed directly to the
        GID engine.  If omitted, any external wavelet already loaded into
        ``engine`` is preserved; otherwise the engine preserves RF
        compatibility and derives the wavelet from component 2 of the input
        seismogram.  Use ``engine.clear_external_wavelet()`` to force
        component-derived wavelets after loading an external one.
    :param external_noise: optional scalar noise `TimeSeries`, `CoreTimeSeries`,
        or `PowerSpectrum` passed to NS-GID inverse stabilization.
        If omitted, any external noise already loaded into ``engine`` is
        preserved.  Use ``engine.clear_external_noise()`` to force the
        configured noise window only.
    :param QCdata_key: metadata key used to store the engine's QC metrics.
    :param return_wavelet: when True return
        `[rf, actual_output, output_shaping_wavelet]`.
    :return: deconvolved `Seismogram`, or the tuple described above.
    """
    return _run_gid_rf_decon(
        seis,
        engine,
        TimeDomainGIDDecon,
        alg_name,
        signal_window=signal_window,
        noise_window=noise_window,
        external_wavelet=external_wavelet,
        external_noise=external_noise,
        QCdata_key=QCdata_key,
        return_wavelet=return_wavelet,
    )
