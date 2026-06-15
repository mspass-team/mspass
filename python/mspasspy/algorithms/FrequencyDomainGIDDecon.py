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

    :param seis: input `Seismogram` containing signal and noise windows.
    :param engine: configured `FrequencyDomainGIDDecon` instance.  The engine
        parameter file controls the inverse-operator mode, deconvolution
        window, convergence limits, and output shaping wavelet.
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
        or `PowerSpectrum` passed to inverse-operator stabilization.  If
        omitted, any external noise already loaded into ``engine`` is preserved.
        Use ``engine.clear_external_noise()`` to force the configured noise
        window only.
    :param QCdata_key: metadata key used to store the engine's QC metrics on
        the returned receiver function.
    :param return_wavelet: when True return
        `[rf, actual_output, output_shaping_wavelet]`.
    :return: deconvolved `Seismogram`, or the list described above when
        ``return_wavelet`` is True.  Dead inputs and recoverable processing
        failures return a dead `Seismogram`; with ``return_wavelet`` True the
        auxiliary outputs are returned as ``None``.
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
