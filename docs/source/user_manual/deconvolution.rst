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

Regularized scalar operators
----------------------------

``LeastSquareDecon``
    Frequency-domain damped least-squares deconvolution.  The inverse
    operator has the form

    .. math::

       S_g^{-1}(\omega) =
       \frac{\overline{S(\omega)}}{|S(\omega)|^2 + \mu},

    where ``mu`` is the damping term.  Smaller damping improves resolution
    for clean data but amplifies noise and spectral notches.  Larger damping
    produces a smoother, more stable estimate.

``WaterLevelDecon``
    Frequency-domain deconvolution with a water-level floor on the source
    power spectrum.  The water level protects the result from division by
    near-zero spectral amplitudes.  Raising the floor improves stability at
    the cost of reduced resolution.

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

``TimeDomainLeastSquareDecon``
    Time-domain least-squares deconvolution.  This operator builds the
    Toeplitz linear-convolution matrix for the requested output lag window
    and solves the regularized normal equations with a stable linear solver.
    It does not build a circular convolution matrix.  The damping parameter
    controls the same resolution/noise trade-off as the frequency-domain
    least-squares operator.

Three-component and iterative operators
---------------------------------------

``CNRDeconEngine`` and ``CNR3CDecon``
    Three-component correlation-noise-ratio receiver-function deconvolution.
    These operators use the vertical component as the source wavelet and
    estimate the transverse and radial responses with a three-component
    noise model.

``TimeDomainGIDDecon`` and ``FrequencyDomainGIDDecon``
    Generalized iterative deconvolution following Wang and Pavlis (2016).
    The method maintains a sparse spike train during iteration.  At each
    step it computes a detection function

    .. math::

       a(t) = s_g^{-1}(t) * r(t),

    where ``r`` is the current residual and ``s_g^{-1}`` is a configurable
    inverse operator.  Supported inverse modes include damped least squares,
    water level, multitaper, and the three-component CNR inverse where
    available.  The displayed receiver function is the sparse spike train
    convolved with the output shaping wavelet; the raw sparse train is not
    the finite-bandwidth receiver function.

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
    multitaper, and CNR3C.

``FrequencyDomainGIDDecon_inverse_modes.png``
    Overlay of frequency-domain generalized iterative deconvolution results
    for each configured inverse-operator mode: least squares, water level,
    multitaper, and CNR.

``scalar_noise_<scale>_stress_spike_results.png``
    Scalar-method and CNR results for a denser synthetic with multiple close,
    opposite-polarity spike pairs.  The ``<scale>`` value is controlled by
    ``--decon-validation-noise-scale``.

``TimeDomainGIDDecon_noise_<scale>_stress_spike_results.png`` and
``FrequencyDomainGIDDecon_noise_<scale>_stress_spike_results.png``
    GID inverse-mode overlays for the same dense stress synthetic and selected
    plot noise scale.

These figures are intended as an audit aid.  The numerical assertions in the
test remain the source of pass/fail behavior and use a fixed reproducible
noise level.  The command-line noise scale is for the optional stress plots so
you can visually compare behavior as noise increases.
