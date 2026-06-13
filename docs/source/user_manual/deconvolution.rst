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

Noise Semantics
---------------

The word "noise" appears in several deconvolution APIs, but it does not always
mean the same mathematical object.  Keeping these roles separate is important
when building reusable processors for Dask or Spark jobs:

``source/wavelet noise``
    Noise used to stabilize the inverse operator denominator.  Multitaper,
    CNR, and NS-GID use this information to avoid excessive inverse gain in
    weak or noisy source-wavelet bands.  This noise controls the inverse
    operator, not the target trace itself.

``target/data noise``
    Noise already present in the component being deconvolved.  Any inverse
    filter can amplify this noise.  Stable scalar operators reduce that
    amplification indirectly by regularizing the source-wavelet inverse, but
    they do not explicitly separate every target-trace noise component.

``residual-domain noise``
    Noise used by iterative methods to decide whether another sparse spike is
    significant and when iteration should stop.  In GID/NS-GID this is a
    stopping and candidate-acceptance quantity.  It is conceptually separate
    from the frequency-domain noise spectrum used to build the inverse
    operator.

For NS-GID, an external ``TimeSeries`` noise estimate can serve both as the
inverse-operator noise estimate and, when explicitly loaded as residual noise,
the residual-domain stopping estimate.  An external ``PowerSpectrum`` noise
estimate can only regularize the inverse operator; a residual-domain noise
window is still required for spike significance and residual-noise stopping.

Scalar inverse operators
------------------------

Scalar operators load one source wavelet and one data trace at a time.  They
return a regularized inverse-filter receiver-function trace, not a sparse
impulse response.  The scalar parameter files default to
``shaping_wavelet_type none``.  Scalar operators can still apply an optional
legacy target-pulse filter if a shaping wavelet is configured, but that is a
post-deconvolution display/bandlimiting filter and is not the GID shaping
wavelet used to represent sparse spikes.

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
    ``S_g^{-1}(omega) D(omega)`` unless an optional scalar output filter is
    configured.

``WaterLevelDecon``
    Frequency-domain deconvolution with a water-level floor on the source
    power spectrum.  The water level protects the result from division by
    near-zero spectral amplitudes.  The implementation raises
    ``|S(omega)|`` to at least ``water_level * rms(S)`` before division and
    extracts the linear lag window.  Raising the floor improves stability at
    the cost of reduced resolution.

``MultiTaperPowerXcor`` and ``MultiTaperPowerSpecDiv``
    Multitaper frequency-domain operators.  Both use DPSS tapers to stabilize
    spectral estimates, but the final receiver-function estimate is produced
    by applying the inverse operator to the untapered, zero-padded data
    window.  This avoids biasing delayed converted phases by the taper value
    at their arrival time.

    These MsPASS operators are not paper-faithful Park and Levin (2000)
    multitaper correlation estimators.  Park and Levin use tapered data/source
    cross spectra in both numerator and denominator.  The current operators
    instead use the untapered wavelet phase and untapered data spectrum in the
    final inverse, while DPSS spectra stabilize the source-power denominator:

    .. math::

       G(f) = \frac{\overline{W_0(f)}}{\operatorname{Den}_{mt}(f)},\quad
       RF(f)=G(f)D_0(f),\quad AO(f)=G(f)W_0(f).

    In RF workflows the multitaper noise vector should normally come from the
    source/wavelet component, commonly the vertical or P component or the
    uncertainty estimate of an external stacked wavelet, not from the radial or
    transverse target component being deconvolved.

    ``MultiTaperPowerXcor`` uses the mean multitaper source power plus a
    damped mean multitaper noise power as ``Den_mt``.
    ``MultiTaperPowerSpecDiv`` is the multitaper power-stabilized
    spectral-division variant: it regularizes each tapered source power by the
    corresponding noise power and a small relative floor, combines those
    powers, and then forms the same untapered-phase inverse.  The two methods
    should be close on clean data; large differences normally indicate
    instability or a configuration problem.  Both return a linear-convolution
    lag window.  The C++/pybind classes are still registered as
    ``MultiTaperXcorDecon`` and ``MultiTaperSpecDivDecon`` for ABI and pickle
    compatibility; ``MultiTaperPowerXcorDecon`` and
    ``MultiTaperPowerSpecDivDecon`` are Python module aliases to those classes,
    not distinct runtime types.  The old RF processor algorithm names
    ``MultiTaperXcor`` and ``MultiTaperSpecDiv`` remain available as
    deprecated compatibility aliases for the power-stabilized operators.
    ``MultiTaperSpecDivDecon`` still exposes legacy ``all_inverse_wavelets``,
    ``all_rfestimates``, and ``all_actual_outputs`` methods, but in the current
    implementation those compatibility methods normally contain one combined
    multitaper estimate rather than one independent product per taper.

    .. warning::

       This is a migration point.  New code should use the explicit
       ``MultiTaperPowerXcor`` and ``MultiTaperPowerSpecDiv`` RF algorithm
       names.  ``damping_factor`` is now interpreted in the power-domain
       denominator, and parameter files
       tuned against older tapered-numerator behavior should be retuned and
       revalidated.  Values should not be assumed numerically equivalent to the
       historical Park-Levin-style implementation.

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
    inverse is constructed.  ``getresult`` returns the solved Toeplitz model by
    default.  If a scalar output filter is explicitly configured it is applied
    after solving, but this is not part of the least-squares inverse itself.

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
    spike selection.  Residual subtraction uses a compact copy of the inverse
    operator's actual output/resolution kernel.  That internal residual-update
    kernel is trimmed, may be shortened to a small window around zero lag, and
    is peak-normalized before use.  By contrast, public ``actual_output()``
    returns the underlying inverse operator's full resolution kernel.  QC fields
    ``gid_actual_o_fir_npts``, ``gid_actual_o_fir_zero_lag_index``, and
    ``gid_actual_o_fir_peak_normalized`` describe the compact internal kernel.
    Optional joint refitting of accepted spike amplitudes solves a small dense
    system with LAPACK Cholesky and LU fallback.  The sparse impulse response is
    exposed by ``sparse_output``.

    Candidate spike selection can be biased by a lag-weight penalty function.
    The penalty is configured with ``lag_weight_penalty_function`` (``none``,
    ``boxcar``, ``cosine_taper``, ``shaping_wavelet``, or
    ``resolution_kernel``), ``lag_weight_penalty_scale_factor``, and
    ``lag_weight_function_width``.  After a spike is accepted, the selected lag
    neighborhood is multiplied by that penalty, which suppresses repeated picks
    of the same large arrival and encourages later iterations to search for
    weaker arrivals.  ``boxcar`` and ``cosine_taper`` use the configured fixed
    width.  ``shaping_wavelet`` and ``resolution_kernel`` derive their applied
    width from a kernel autocorrelation and preserve the configured width only
    as QC context.  ``QCMetrics`` reports ``lag_weight_Linf_final`` and
    ``lag_weight_L2_final`` and both engines expose the full final weight curve
    with ``lag_weight_vector()`` for diagnostic plotting.  The metadata fields
    ``gid_penalty_scale_factor`` and ``gid_penalty_width`` preserve configured
    values, while ``gid_penalty_effective_width`` records the helper kernel
    length actually applied (for example, ``none`` is a one-sample no-op
    kernel).  The Python wrappers store scalar QC fields in their QC metadata
    subdocuments, so they are saved with normal database records.

    The five helper modes map directly to the final lag-weight plots produced
    by the validation tests.  The theoretical interpretation and recommended
    default are summarized in `Lag-weight penalty framework`_.

    The time- and frequency-domain GID engines now use the same iteration-cap
    behavior.  Reaching ``maximum_iterations`` stops the iteration, returns the
    best accepted sparse model after the final amplitude refit, and records
    ``gid_stop_reason="max_iterations"`` with ``gid_converged=false``.  Other
    common QC fields include ``decon_operator``, ``decon_processed``,
    ``gid_iterations``, ``gid_number_spikes``, ``gid_penalty_function``,
    ``gid_penalty_scale_factor``, ``gid_penalty_width``, and
    ``gid_penalty_effective_width``.

    Both GID Python wrappers use the same window convention.  If
    ``signal_window`` is omitted, the input datum's full time range is used as
    the analysis/output window.  If ``noise_window`` is omitted, the engine's
    configured parameter-file noise window is used.  The analysis window must
    contain the configured deconvolution/inverse-operator window; otherwise
    the wrappers return a killed datum instead of processing a partial window.
    Internally, the loaded noise window is transformed into the same domain as
    the sparse iteration before legacy residual-noise stopping thresholds are
    evaluated.

    For ``ns_gid``, externally supplied noise and the configured noise window
    serve different roles.  External TimeSeries noise passed to ``loadnoise`` or
    the Python wrappers is used by the stable inverse operator to estimate
    frequency-dependent regularization and gain limits.  When a windowed noise
    segment has already been loaded, that window remains the residual-domain
    noise estimate for spike significance and residual-noise stopping.  External
    PowerSpectrum noise is only an inverse-operator regularization estimate; a
    direct engine call sequence such as ``loadnoise(power_spectrum); load(d,
    dwin); process()`` is incomplete because no residual-domain noise window has
    been loaded.  Use ``load(d, dwin, nwin)`` or ``loadnoise(d, nwin)`` as well.

    Direct ``TimeDomainGIDDecon`` and ``FrequencyDomainGIDDecon`` engine
    objects are pickleable for distributed wrapper use.  Their pickle state
    preserves the parameter-file configuration, successful leaf-operator
    changes made through ``changeparameter()``, and any externally loaded
    wavelet, TimeSeries noise, or PowerSpectrum noise.  It does not preserve
    loaded seismograms, processed receiver functions, residuals, sparse spike
    trains, or runtime QC state; a restored engine is a reusable configured
    operator, not a copy of the last processed datum.

    For Dask or Spark jobs, build and configure the reusable deconvolution
    processor on the driver, then scatter or close over that configured object.
    Avoid loading per-datum scalar data into an ``RFdeconProcessor`` before
    scattering it, because scalar compatibility mode intentionally preserves
    cached input vectors for post-processing diagnostics.  For GID processors,
    externally loaded reusable wavelets/noise are synchronized into the C++
    engine before serialization and are not duplicated as separate Python
    arrays in the pickle payload.

    The ``multi_taper`` inverse mode in both GID engines currently uses
    ``MultiTaperXcorDecon`` as the core C++ inverse operator, with the
    power-stabilized untapered-phase semantics described above.  In
    ``TimeDomainGIDDecon`` the term "time domain" describes the iterative
    residual subtraction and sparse-spike update.  The multitaper inverse
    operator itself is still estimated in the frequency domain.

    The iterative operators expose separate convergence controls for
    fractional residual improvement and normalized residual energy.  The
    residual-energy threshold is a noise-level stopping rule: increasing it
    suppresses noise-generated spikes, while decreasing it can recover weak
    arrivals at the risk of fitting noise.

Lag-weight penalty framework
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The GID penalty is best viewed as an adaptive prior on future spike locations,
not as a shrinkage applied to amplitudes that have already been accepted.  At
iteration :math:`k`, let :math:`\mathbf{a}^{(k)}_j` be the three-component
detection-function vector at candidate lag sample :math:`j`, and let
:math:`\ell^{(k)}_j \in [0, 1]` be the accumulated lag weight.  Ignoring
candidate lags where the residual-update kernel cannot fit inside the working
window, the implemented selection statistic is

.. math::

   q^{(k)}_j = \left(\ell^{(k)}_j\right)^2
              \left\|\mathbf{a}^{(k)}_j\right\|_2^2,
   \qquad
   j_* = \operatorname*{arg\,max}_j q^{(k)}_j .

The candidate spike amplitude is then rescaled against the compact resolution
kernel and accepted only if the data-domain residual :math:`L_2` norm
decreases.  Consequently, the penalty influences which lag is tried next, but
the residual test still protects the physical data-domain fit.

After accepting a spike at :math:`j_*`, the engine multiplies the lag weights
near :math:`j_*` by a compact penalty kernel :math:`p_m`:

.. math::

   \ell^{(k+1)}_j =
       \operatorname{clip}\left(\ell^{(k)}_j p_{j-j_*}, 0, 1\right).

Outside the penalty support, :math:`p_m=1`.  Repeated hits near the same
arrival therefore multiply down the same neighborhood, making the penalty a
soft exclusion process.  Equivalently, the spike picker maximizes the
unweighted detection energy with an adaptive location cost
:math:`-2\log \ell^{(k)}_j`; already explained neighborhoods become more
expensive while untouched lags retain cost zero.

Let :math:`\alpha` be ``lag_weight_penalty_scale_factor`` and let
:math:`W=2h+1` be the odd fixed penalty width in samples.  If an even width is
configured, the implementation increases it by one so the kernel has a center
sample.  The fixed-width helper kernels are:

.. math::

   p_m =
   \begin{cases}
   1, & \text{``none''}, \\
   1-\alpha, & \text{``boxcar'' and } |m|\le h, \\
   1-\frac{\alpha}{2}\left[1+\cos\left(\frac{\pi m}{h+1}\right)\right],
      & \text{``cosine_taper'' and } |m|\le h, \\
   1, & |m| > h .
   \end{cases}

``none`` leaves the greedy selector unchanged.  It is useful as a diagnostic
baseline and can be appropriate for very clean data, but it provides no
mechanism to discourage repeated picks of a strong arrival.  ``boxcar`` is a
hard local suppression.  It is easy to interpret, but its discontinuous edges
can move false picks to the first unpenalized samples outside the window.
``cosine_taper`` is a smooth compact suppression: the accepted sample receives
the strongest penalty and nearby samples are downweighted gradually.  With the
current default :math:`\alpha=0.5` and :math:`W=5`, the center sample is
multiplied by ``0.5``, adjacent samples by ``0.625``, and edge samples by
``0.875``.

The kernel-derived modes replace the user-selected width with a resolution
measure.  Let :math:`h_i` be either the requested output shaping wavelet
(``shaping_wavelet``) or the compact actual residual-update kernel
(``resolution_kernel``).  The engines compute the normalized absolute kernel
coherence

.. math::

   c_m =
     \frac{\left|\sum_i h_i h_{i+m}\right|}
          {\sum_i h_i^2},
   \qquad c_0 = 1,

and keep the contiguous full-width-at-half-maximum support around zero lag,
where :math:`c_m \ge 0.5`.  Because the GID picker ranks squared
three-component amplitudes, the applied penalty uses coherence energy:

.. math::

   p_m = 1-\alpha c_m^2,

with :math:`p_m=1` outside the support.  This gives the accepted spike the same
center suppression as the fixed helpers, but sets both the width and the
shoulder shape from the resolving power of the wavelet/operator while keeping
the shoulders gentler for close arrivals than a raw-coherence penalty.  The
``shaping_wavelet`` mode is useful for controlled comparisons and for cases
where the requested output pulse is the desired resolution reference.
``resolution_kernel`` is more adaptive because it uses the actual kernel after
the inverse operator, trimming, and peak normalization.  For ``ns_gid`` that
kernel also reflects noise-dependent regularization.

The optional validation figures demonstrate this framework directly.  The
``*_least_square_penalty_lag_weights.png`` plots show the final
:math:`\ell^{(K)}_j` curve above an aligned true sparse impulse response.  A
dip in that curve is the accumulated adaptive cost for selecting another
candidate near an already accepted spike.  The companion shaped and sparse
penalty plots show the resulting receiver functions and raw spike support.
The stress validation intentionally uses the less stable ``least_square``
inverse mode in those figures so the penalty behavior is visible; with
``ns_gid`` the inverse operator is often stable enough that all penalty helpers
select nearly the same support.

The recommended production default remains ``cosine_taper`` with
``lag_weight_penalty_scale_factor=0.5`` and ``lag_weight_function_width=5``.
The validation sweeps show that it is the most conservative choice across the
current close-arrival tests: it suppresses repeated picks without changing the
historical fixed-width behavior.  ``resolution_kernel`` is the preferred
adaptive option when users want to remove manual width tuning, but its QC
fields and validation plots should be checked on data sets with closely spaced
opposite-polarity arrivals before making it a project default.  ``boxcar``
should be reserved for tests or cases where a deliberate hard exclusion is
desired.  In noisy production workflows, this lag penalty should be combined
with the ``ns_gid`` inverse mode; the least-square validation plots use
``least_square`` only because its weaker noise stability makes the differences
among penalty kernels easier to see.  For backward compatibility, the
distributed parameter files still default to ``deconvolution_type
least_square``.  Users should switch the selected GID branch to
``deconvolution_type ns_gid`` for noisy production deconvolution.

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
    can overfit noise.  ``ns_gid_refit_interval`` controls how often accepted
    spike amplitudes are jointly refit during iteration; the engines always
    run a final refit, so values larger than one reduce repeated dense solves
    without disabling final amplitude correction.

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
manual inspection.  Normal test runs do not create figures.  To write the full
set of scalar, GID, sparse-output, and external-wavelet validation plots, run:

.. code-block:: bash

   python -m pytest \
       python/tests/algorithms/test_decon_algorithm_validation.py \
       --decon-validation-plots \
       --decon-validation-plot-dir /tmp/mspass-decon-validation-plots \
       --decon-validation-noise-scale 0.03

The output directory contains overlays for scalar/CNR methods, shaped GID
receiver functions, raw GID sparse impulse responses, external-wavelet runs,
and the source-wavelet validation setups used in the synthetic cases.  The
setup figures include the notched source wavelet, noisy convolved data, true
three-component impulse response, and the requested manual plot noise scale.
``--decon-validation-noise-scale`` is passed directly as the Gaussian noise
amplitude before the synthetic bandpass coloring filter; it is not a target
SNR or percent-noise value.  The setup figure normalizes the displayed data
panel, so large changes in absolute noise amplitude are easiest to judge by
the waveform shape and the noise-scale label.  The sparse GID plots are the
most direct visual comparison against the known sparse truth; shaped GID and
CNR plots intentionally have more limited bandwidth.  The external-wavelet
overlay uses raw scalar inverse results and raw GID sparse outputs so spike
support can be checked visually.  The same plot run also writes a
``external_wavelet_all_methods_display_filtered.png`` overlay with a common
plotting-only Ricker filter so different methods can be compared at a similar
visual bandwidth.  That display filter is not part of the scalar algorithms
and is not used by the pass/fail assertions.

These figures are intended as an audit aid.  The numerical assertions in the
test remain the source of pass/fail behavior and use a fixed reproducible
noise level.  The command-line noise scale is for the optional stress plots so
you can visually compare behavior as noise increases.
