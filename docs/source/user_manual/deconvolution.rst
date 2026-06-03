Deconvolution Operators
=======================

This page summarizes the mathematical conventions used by the receiver
function deconvolution operators in MsPASS.  The defaults are intended for
seismic time series and solve linear convolution problems.  Circular
convolution is used only internally as an FFT implementation detail after
zero padding, or in explicitly diagnostic quantities.

Common convention
-----------------

The deconvolution operators model a signal as

.. math::

   d(t) = s(t) * m(t) + n(t),

where ``d`` is the observed component, ``s`` is the source or vertical
component wavelet, ``m`` is the receiver-function estimate, and ``n`` is
noise.  For discrete data, the implementation treats this as a linear
convolution problem.  FFT-based scalar operators pad the working arrays
before transforming, then extract the requested lag window from the padded
linear-convolution result.  The default result is therefore not the wrapped
output of a circular convolution.  For a requested output window of ``N``
samples, scalar FFT operators use an FFT length of at least ``2*N - 1``,
rounded up to the next power of two.  That is the linear
convolution/correlation length for the loaded source and data windows and
avoids the unnecessary computation and diagnostic spectral over-sampling of a
larger guard buffer.

The receiver-function output window is controlled by the operator parameter
file, normally through ``deconvolution_data_window_start`` and
``deconvolution_data_window_end``.  The zero-lag convention is the sample of
the output window whose time is 0.0 relative to the source-wavelet reference
time.  If an operator reports ``actual_output`` or ``inverse_wavelet``, that
diagnostic object may be the full padded operator response; use the returned
receiver function for the cropped seismic result.

Terminology
-----------

This page follows the terminology of Wang & Pavlis (2016) for generalized
iterative deconvolution:

``sparse impulse response``
    The finite spike series :math:`\hat{i}(t)` selected by the iterative loop.
    In code this is returned by ``sparse_output``.

``output shaping wavelet``
    The wavelet :math:`w_s(t)` convolved with :math:`\hat{i}(t)` to form the
    finite-duration receiver-function representation
    :math:`\hat{m}_{id}(t)=w_s(t)*\hat{i}(t)`.  In code the preferred method
    name is ``output_shaping_wavelet``.  The older method name
    ``ideal_output`` is retained only as a legacy alias.

``actual output`` or ``resolution kernel``
    The inverse operator applied to the estimated source wavelet,
    :math:`\hat{R}(\omega)=\hat{s}^{-g}(\omega)\hat{s}(\omega)`.  This is the
    paper's actual output of the inverse filter and is returned by
    ``actual_output`` or the alias ``resolution_kernel``.

``inverse operator``
    The operator :math:`\hat{s}^{-g}` used to transform the residual into the
    GID detection function.  It is not the output shaping wavelet.

Wrapper Data Handling
---------------------

The low-level scalar C++ operators load raw vectors and do not carry absolute
time windows.  ``RFdeconProcessor`` and ``RFdecon`` own scalar data handling:
they extract the configured deconvolution, wavelet, and noise windows before
calling the scalar engines.  If a configured window is not present in the
input datum, the wrapper returns a killed datum and does not attach a QC
subdocument.

``CNRRFDecon`` and ``CNRArrayDecon`` have a stricter explicit contract.  A
caller must either provide both ``signal_window`` and ``noise_window`` or
provide a precomputed ``PowerSpectrum`` noise estimate.  If a signal window is
provided it is applied to the output data.  If an external wavelet is provided,
that wavelet is used for all components; otherwise the configured component of
the signal window is used as the source wavelet.

The GID wrappers share a common convention.  If ``signal_window`` is omitted,
the input datum's full time range is used as the analysis/output window.  If
``noise_window`` is omitted, the engine's configured parameter-file noise
window is used.  The analysis window must contain the configured
deconvolution/inverse-operator window; otherwise the wrapper returns a killed
datum instead of processing a partial window.

Scalar inverse operators
------------------------

Scalar operators load one source wavelet and one data trace at a time.  They
return a finite-bandwidth receiver-function trace, not a sparse impulse
response.
The historical FFT scalar operators in MsPASS also multiply the inverse result
by a configured output shaping wavelet before returning ``getresult``.  In
scalar operators this wavelet is a target-pulse/bandlimiting convention.  It
is distinct from the GID sparse impulse response, where
``output_shaping_wavelet`` is applied only after iterative spike picking.

``LeastSquareDecon``
    Frequency-domain damped least-squares deconvolution.  The inverse
    operator has the form

    .. math::

       S_g^{-1}(\omega) =
       \frac{\overline{S(\omega)}}{|S(\omega)|^2 + \mu},

    where ``mu`` is the damping term.  Smaller damping improves resolution
    for clean data but amplifies noise and spectral notches.  Larger damping
    produces a smoother, more stable estimate.  In the implementation
    ``mu`` is ``(rms(S) * damping_factor)^2`` in the normal-equation
    denominator.  The data and wavelet are zero padded to the linear
    convolution length before the FFT, ``winv`` stores the stabilized inverse,
    and ``getresult`` returns the cropped lag window of
    ``W_out(omega) S_g^{-1}(omega) D(omega)``.

``WaterLevelDecon``
    Frequency-domain deconvolution with a water-level floor on the source
    power spectrum.  The water level protects the result from division by
    near-zero spectral amplitudes.  The implementation raises
    ``|S(omega)|`` to at least ``water_level * rms(S)`` before division, then
    applies the configured scalar output shaping wavelet and extracts the
    linear lag window.  Raising the floor improves stability at the cost of
    reduced resolution.

``MultiTaperXcorDecon`` and ``MultiTaperSpecDivDecon``
    Multitaper frequency-domain operators.  Both estimate source and noise
    spectra with DPSS tapers before forming the inverse operator.  The final
    receiver-function estimate is produced by applying the multitaper inverse
    to the untapered, zero-padded data window.  This avoids biasing delayed
    converted phases by the taper value at their arrival time.

    ``MultiTaperXcorDecon`` forms a single stabilized cross-spectral inverse
    from the sum of tapered source cross spectra.  ``MultiTaperSpecDivDecon``
    forms a separate regularized spectral-division inverse for each taper and
    then averages the per-taper RF estimates.  The two methods should be
    similar on clean data, but they are not expected to be identical.  The
    default behavior still returns a linear-convolution lag window; tapering
    controls variance and leakage in the spectral estimates.

``NoiseStableDecon``
    Noise-aware stable scalar inverse used by the ``ns_gid`` GID mode and also
    exposed as a standalone scalar operator for validation.  It applies one
    inverse operator,

    .. math::

       G(f)=B(f)\frac{\overline{S(f)}}{|S(f)|^2+\mu(f)},

    directly to the data spectrum and returns the finite-bandwidth inverse
    result.  It does not run spike picking, does not expose a sparse output,
    and does not apply the GID output shaping wavelet.  ``mu(f)`` is the
    maximum of a relative minimum floor scaled by peak ``|S|^2``, a
    noise-spectrum damping term, and a term required to enforce
    ``|G(f)| <= ns_gid_gain_max``.  An optional SNR reliability taper can
    reduce or zero low-SNR bands before inverse filtering.

``TimeDomainLeastSquareDecon``
    Time-domain least-squares deconvolution.  This operator builds the
    Toeplitz linear-convolution matrix for the requested output lag window
    and solves the regularized normal equations with a stable linear solver.
    It does not build a circular convolution matrix.  The damping parameter
    controls the same resolution/noise trade-off as the frequency-domain
    least-squares operator.  The implementation forms ``S^T S`` and
    ``S^T d`` directly from the cropped linear convolution operator, adds
    ``damping_factor * max(diag(S^T S))`` to the diagonal, and solves with
    LAPACK Cholesky.  If Cholesky fails, it falls back to LAPACK LU and raises
    an error if the regularized system is still singular.  No explicit matrix
    inverse is constructed.  ``getresult`` applies the configured scalar output
    shaping wavelet to the solved model; set ``shaping_wavelet_type none`` only
    when the raw high-resolution Toeplitz model is desired.

Three-component and iterative operators
---------------------------------------

``CNRDeconEngine`` and ``CNR3CDecon``
    Three-component correlation-noise-ratio receiver-function deconvolution.
    These operators estimate a noise spectrum from a configured noise window
    and regularize the source spectrum as a function of colored noise level
    and SNR.  ``colored_noise_damping`` adds a frequency-dependent damping term
    to the normal-equation denominator.  ``generalized_water_level`` raises
    low-SNR source amplitudes before division.  Both produce scalar
    finite-bandwidth component traces after the configured output shaping
    wavelet is applied.  ``CNRDeconEngine`` is the current engine used by the
    Python wrappers and GID inverse mode.  ``CNR3CDecon`` is the older 3C
    prototype kept for compatibility.

``TimeDomainGIDDecon`` and ``FrequencyDomainGIDDecon``
    Generalized iterative deconvolution following Wang and Pavlis (2016).
    The method maintains a sparse impulse response during iteration.  At each
    step it computes a detection function

    .. math::

       a(t) = s_g^{-1}(t) * r(t),

    where ``r`` is the current residual and ``s_g^{-1}`` is a configurable
    inverse operator.  Supported inverse modes include damped least squares,
    water level, multitaper, and the three-component CNR inverse where
    available.  The represented receiver function is the sparse impulse
    response convolved with the output shaping wavelet; the raw sparse impulse
    response is not the finite-bandwidth receiver function.

    The GID engines maintain residuals in the original data domain.  The
    inverse operator is used only to form the detection function for candidate
    spike selection.  Residual subtraction uses the inverse operator's actual
    output/resolution kernel, and optional joint refitting of accepted spike
    amplitudes solves a small dense system with LAPACK Cholesky and LU
    fallback.  The sparse impulse response is exposed by ``sparse_output``.

    Both GID Python wrappers use the same window convention.  If
    ``signal_window`` is omitted, the input datum's full time range is used as
    the analysis/output window.  If ``noise_window`` is omitted, the engine's
    configured parameter-file noise window is used.  The analysis window must
    contain the configured deconvolution/inverse-operator window; otherwise
    the wrappers return a killed datum instead of processing a partial window.
    Internally, the loaded noise window is transformed into the same domain as
    the sparse iteration before legacy residual-noise stopping thresholds are
    evaluated.

    The ``multi_taper`` inverse mode in both GID engines currently uses
    ``MultiTaperXcorDecon`` as the core inverse operator.  In
    ``TimeDomainGIDDecon`` the term "time domain" describes the iterative
    residual subtraction and sparse-spike update.  The multitaper inverse
    operator itself is still estimated in the frequency domain.

    The iterative operators expose separate convergence controls for
    fractional residual improvement and normalized residual energy.  The
    residual-energy threshold is a noise-level stopping rule: increasing it
    suppresses noise-generated spikes, while decreasing it can recover weak
    arrivals at the risk of fitting noise.

``NS-GID`` inverse mode
    Noise-aware stable generalized iterative deconvolution is selected with
    ``deconvolution_type ns_gid`` in either GID engine.  It is a stable
    inverse-operator variant of GID, not a separate receiver-function
    preprocessing workflow.  The core inverse operator is

    .. math::

       G(f) = B(f)\frac{\overline{S(f)}}{|S(f)|^2 + \mu(f)},

    with frequency-dependent damping from the wavelet and noise spectra, an
    explicit maximum gain cap, and an optional smooth SNR reliability taper.
    The implementation enforces ``|G(f)| <= ns_gid_gain_max`` within floating
    point tolerance.  ``ns_gid_mu_min`` is interpreted as a relative floor
    scaled by the peak wavelet spectral power, so the same parameter remains
    meaningful for externally supplied wavelets with different amplitudes.
    Stability comes from ``G(f)``, ``mu(f)``, the gain cap, and noise-aware
    stopping; it does not come from the output shaping wavelet.

    NS-GID supports externally supplied wavelets through the GID
    ``loadwavelet`` APIs and the Python wrappers' ``external_wavelet``
    argument.  This is the intended path when the source wavelet is a prepared
    P-wave stack from a network, subarray, or other caller-managed procedure.
    Constructing that stack, selecting events, rotating components, and array
    stacking are outside this operator.  If no external wavelet is loaded, the
    engines preserve the existing receiver-function compatibility behavior and
    estimate the wavelet from the configured component/window.

    The sparse impulse response is kept conceptually separate from the shaped
    receiver-function representation.  The inverse operator is used only for
    candidate spike detection and stabilization.  The residual update remains
    in the GID data domain, and ``getresult`` returns the accepted sparse
    support convolved with the configured output shaping wavelet.  The sparse
    impulse response is available through ``sparse_output`` for QC and
    validation.
    The underlying ``NoiseStableDecon`` scalar operator is a single stable
    inverse operation.  It is not a sparse picker and does not apply the GID
    output shaping wavelet; finite bandwidth comes from the stable inverse
    operator itself.
    Candidate acceptance can use an empirical inverse-filtered noise threshold,
    a sigma threshold, residual-to-noise stopping, maximum spike count, and
    the existing fractional-improvement limit.  Higher thresholds suppress
    noise-generated spikes; lower thresholds may recover weaker arrivals but
    can overfit noise.

``RFdeconProcessor``
    High-level scalar receiver-function wrapper.  It exposes the scalar
    frequency-domain methods, time-domain least squares, conventional
    iterative deconvolution, and both generalized iterative variants through
    a consistent ``alg`` selector.  The processor loads the wavelet, noise,
    and data windows once and returns the cropped receiver-function estimate
    for the configured output lag window.

Validation plots
----------------

The deconvolution validation tests can optionally write diagnostic plots for
manual inspection.  Normal test runs do not create figures.  To plot the
colored three-component synthetic validation case, run for example:

.. code-block:: bash

   PYTHONPATH=/tmp/mspass-cxx-import:/home/iwang/code/mspass/python \
   /home/iwang/code/.venvs/mspass-dev/bin/python -m pytest \
       python/tests/algorithms/test_decon_algorithm_validation.py::test_scalar_methods_are_consistent_for_complex_colored_3c_synthetic \
       --decon-validation-plots \
       --decon-validation-plot-dir /tmp/mspass-decon-validation-plots \
       --decon-validation-noise-scale 0.03

The output directory will contain:

``complex_colored_scalar_methods.png``
    Overlay of the known sparse receiver function and the recovered scalar and
    CNR deconvolution results for each component.

``complex_colored_validation_wavelet.png``
    Source wavelet used in the synthetic validation, including the imposed
    spectral notches.

``TimeDomainGIDDecon_inverse_modes.png``
    Overlay of time-domain generalized iterative deconvolution results for
    each configured inverse-operator mode: least squares, water level,
    multitaper, CNR, and NS-GID.

``TimeDomainGIDDecon_inverse_modes_sparse_results.png``
    Raw sparse impulse responses selected by the time-domain GID loop for the
    same inverse-operator modes.  This is the plot to compare directly against
    the known sparse truth.

``FrequencyDomainGIDDecon_inverse_modes.png``
    Overlay of frequency-domain generalized iterative deconvolution results
    for each configured inverse-operator mode: least squares, water level,
    multitaper, CNR, and NS-GID.

``FrequencyDomainGIDDecon_inverse_modes_sparse_results.png``
    Raw sparse impulse responses selected by the frequency-domain GID loop for
    the same inverse-operator modes.

``external_wavelet_all_methods.png``
    Overlay of scalar, CNR, and NS-GID results when every method is driven by
    the same externally supplied prepared wavelet.  Scalar operators load the
    external wavelet as a raw vector, so this plot shifts the truth sparse
    impulse response to the measured direct-arrival lag of the least-squares
    result for visual comparison.

``scalar_noise_<scale>_stress_spike_results.png``
    Scalar-method and CNR results for a denser synthetic with multiple close,
    opposite-polarity spike pairs.  The ``<scale>`` value is controlled by
    ``--decon-validation-noise-scale``.

``TimeDomainGIDDecon_noise_<scale>_stress_spike_results.png`` and
``FrequencyDomainGIDDecon_noise_<scale>_stress_spike_results.png``
    GID inverse-mode overlays for the same dense stress synthetic and selected
    plot noise scale.

``TimeDomainGIDDecon_noise_<scale>_stress_sparse_results.png`` and
``FrequencyDomainGIDDecon_noise_<scale>_stress_sparse_results.png``
    Raw GID sparse impulse responses for the dense stress synthetic.  These
    plots intentionally show ``sparse_output`` rather than the shaped
    ``getresult`` representation.

These figures are intended as an audit aid.  The numerical assertions in the
test remain the source of pass/fail behavior and use a fixed reproducible
noise level.  The command-line noise scale is for the optional stress plots so
you can visually compare behavior as noise increases.
