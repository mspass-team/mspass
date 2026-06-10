#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python wrappers for the frequency-domain generalized iterative deconvolution
engine.

The operator uses the same sparse-spike iterative model as
``TimeDomainGIDDecon`` but evaluates the configurable inverse operator in the
frequency domain with padded FFTs.  The returned receiver function is the
shaped sparse impulse response for the requested lag window, not a circularly
wrapped inverse-filter response.
"""

from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.algorithms.deconvolution import FrequencyDomainGIDDecon
from mspasspy.algorithms._gid_decon_wrapper import _run_gid_rf_decon


@mspass_func_wrapper
def FrequencyDomainGIDRFDecon(
    seis,
    engine,
    *args,
    signal_window=None,
    noise_window=None,
    external_wavelet=None,
    external_noise=None,
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

    When ``noise_window`` is omitted, the engine's configured parameter-file
    noise window is used.  ``signal_window`` must contain the configured
    deconvolution window used to build the inverse operator.  Omitted
    ``external_wavelet`` or ``external_noise`` arguments leave any external
    state already loaded into ``engine`` unchanged; call the engine's
    ``clear_external_*`` methods to force internal wavelet/noise selection.
    """
    return _run_gid_rf_decon(
        seis,
        engine,
        FrequencyDomainGIDDecon,
        alg_name,
        signal_window=signal_window,
        noise_window=noise_window,
        external_wavelet=external_wavelet,
        external_noise=external_noise,
        QCdata_key=QCdata_key,
        return_wavelet=return_wavelet,
    )
