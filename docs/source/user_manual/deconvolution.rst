Deconvolution Operators
=======================

This page describes the receiver-function deconvolution operators available in
MsPASS.  Deconvolution estimates an Earth response from an observed component
and an estimated source wavelet.  In simplified form,

.. math::

   d(t) = s(t) * m(t) + n(t),

where ``d`` is the observed target component, ``s`` is the source or reference
wavelet, ``m`` is the receiver-function estimate, and ``n`` is noise.  Real
source spectra contain notches and real data contain noise, so MsPASS uses
regularized inverse filters or sparse iterative methods rather than direct
spectral division.

Most users should read this page in four passes:

1. Skim `Essential terms`_ so the method descriptions use familiar words.
2. Read `Wrappers and engines`_, `Common conventions`_, and
   `Choosing an operator`_ to understand the processing workflow.
3. Read the method section for the operator you plan to use.
4. Use `Validation and QC workflow`_ to inspect plots and saved metadata.

Essential terms
---------------

``target component`` or ``target trace``
    The observed component being deconvolved.

``source wavelet`` or ``reference wavelet``
    The estimated source pulse used to build an inverse operator.  In many
    receiver-function workflows this is the vertical component, the P component
    aligned with the incoming P-wave direction, or an externally prepared stack.

``signal_window``
    The target interval to analyze and usually return.

``noise_window``
    A time interval used to estimate noise for inverse stabilization or
    iterative stopping.

``TimeSeries`` and ``PowerSpectrum``
    MsPASS data objects used by some deconvolution APIs.  A ``TimeSeries``
    stores a sampled time-domain signal.  A ``PowerSpectrum`` stores a
    frequency-domain power estimate, commonly used as a reusable noise
    spectrum.

``deconvolution window`` or ``receiver-function lag window``
    The output lag interval configured by parameters such as
    ``deconvolution_data_window_start`` and
    ``deconvolution_data_window_end``.  For the GID wrappers, the
    ``signal_window`` must contain this lag window.

``receiver function``
    The estimated Earth response.  Scalar inverse operators return a
    regularized receiver-function trace directly.  GID operators first estimate
    a sparse impulse response and then shape it into a receiver-function trace.

``inverse operator``
    The regularized operator applied to data or residuals.  In GID it is used
    to form a detection function for choosing candidate spikes.  It is not the
    output shaping wavelet.

``generalized iterative deconvolution`` or ``GID``
    A three-component sparse deconvolution family based on Wang and Pavlis
    (2016).  This page uses ``greedy GID`` for the classic one-spike-at-a-time
    iteration and ``group_sparse`` for the regularized solver that estimates
    all lags together.

``NS-GID``
    Noise-stable GID.  It is selected with ``deconvolution_type ns_gid`` and
    changes the inverse operator used inside GID.

``sparse impulse response``
    The finite spike series selected by GID.  In code this is returned by
    ``sparse_output``.

``shaped receiver function``
    The sparse impulse response convolved with the output shaping wavelet.  This
    is the finite-bandwidth receiver function returned by ``getresult`` for GID.

``output shaping wavelet``
    The wavelet convolved with the sparse impulse response to form the shaped
    receiver function.  In code the preferred method name is
    ``output_shaping_wavelet``.  The older name ``ideal_output`` is retained
    only as a legacy alias.

``actual output`` or ``resolution kernel``
    The inverse operator applied to the estimated source wavelet.  It describes
    the resolution of the inverse filter and is returned by ``actual_output``
    or ``resolution_kernel``.

``inverse_wavelet``
    A diagnostic representation of the inverse filter itself.  It can be longer
    than the returned receiver-function lag window because FFT-based operators
    work on padded arrays internally.

``sparse support``
    The lag samples retained as nonzero arrivals in the sparse impulse
    response.  A ``lag group`` is the three-component coefficient vector at one
    lag sample.

``support threshold``
    A cutoff used after a sparse solve to decide which lag groups are retained
    in ``sparse_output`` and in the final refit model.

``lag-weight penalty``
    A greedy-GID mechanism that downweights candidate lags after a spike has
    already been accepted.  It changes future candidate selection; it is not an
    amplitude shrinkage penalty.

``group-sparse regularization``
    A separate GID mode selected with ``deconvolution_type group_sparse``.  It
    estimates the full sparse impulse response with a grouped sparsity penalty
    instead of using the greedy GID spike picker.

``QC metadata``
    Quality-control metadata stored by wrappers and engines.  These scalar
    fields record processing status, convergence, residual norms, penalty
    settings, support thresholds, and inverse-stability diagnostics.

``CNR``
    Colored-noise-ratio deconvolution.

``SNR``
    Signal-to-noise ratio.

Wrappers and engines
--------------------

MsPASS separates data handling from numerical deconvolution.

``RFdeconProcessor`` and ``RFdecon``
    General receiver-function wrappers.  They operate on MsPASS data objects,
    read parameter files, extract wavelet, data, and noise windows, call the
    selected engine, and save QC metadata in a subdocument.  Conventional scalar
    methods are applied component by component.  Generalized iterative
    deconvolution methods operate on the full three-component seismogram because
    their spike selection is vector-valued.  If a required window is absent, the
    wrapper returns a killed datum and does not attach the QC subdocument.

``CNRRFDecon`` and ``CNRArrayDecon``
    Wrappers for colored-noise-ratio deconvolution.  A caller must provide both
    ``signal_window`` and ``noise_window`` or provide a precomputed
    ``PowerSpectrum`` noise estimate.  If an external wavelet is supplied, it
    is used for all components; otherwise the configured component of the
    signal window is used as the source wavelet.

``TimeDomainGIDRFDecon`` and ``FrequencyDomainGIDRFDecon``
    Direct wrappers around the GID engines.  If ``signal_window`` is omitted,
    the full input time range is used as the interval to analyze and return.  If
    ``noise_window`` is omitted, the engine's parameter-file noise window is
    used.  The analysis interval must contain the configured receiver-function
    lag window.

The lower-level C++ engines do the numerical work.  They are useful for tests,
diagnostics, and specialized processing tools, but they expect the caller to
load the correct data, wavelet, and noise estimates.

Common conventions
------------------

MsPASS treats receiver-function deconvolution as a linear convolution problem.
FFT-based operators use zero padding internally, but the returned receiver
function is the requested linear lag window, not a wrapped circular-convolution
result.  For a requested scalar FFT output window of ``N`` samples, the working
FFT length is at least ``2*N - 1`` samples, rounded up to the next power of two.

The receiver-function output window is normally configured with
``deconvolution_data_window_start`` and ``deconvolution_data_window_end``.  Zero
lag is the sample whose time is 0.0 relative to the source-wavelet reference
time.  Diagnostic objects can describe the full padded inverse-operator
response: ``actual_output`` or ``resolution_kernel`` is the inverse operator
applied to the source wavelet, while ``inverse_wavelet`` is the inverse filter
itself.  The returned receiver function is the cropped seismic result.

Windows and noise
-----------------

Three window concepts appear throughout the deconvolution APIs:

``signal_window``
    The target time interval to analyze and normally the output interval to
    return.

``wavelet`` or ``wavelet window``
    The source estimate used to build the inverse operator.  In receiver
    function workflows this is commonly the vertical or P component, or an
    external stacked source estimate.

``noise_window``
    A time interval or spectrum used to stabilize the inverse or to decide when
    an iterative method should stop.

The word "noise" has several meanings:

``source/wavelet noise``
    Noise used to stabilize the inverse operator denominator.  Multitaper,
    CNR, and NS-GID use this information to avoid excessive inverse gain in
    weak or noisy source-wavelet bands.

``target/data noise``
    Noise already present in the component being deconvolved.  Stable inverse
    operators reduce amplification of this noise indirectly by regularizing the
    source-wavelet inverse.

``residual-domain noise``
    Noise used by iterative methods to decide whether another sparse spike is
    significant and when iteration should stop.

For ``ns_gid``, an external ``TimeSeries`` noise estimate can be used for both
inverse-operator regularization and, when loaded as residual noise,
residual-domain stopping.  An external ``PowerSpectrum`` can regularize only
the inverse operator; a residual-domain noise window is still needed for sparse
iteration stopping.

Choosing an operator
--------------------

No deconvolution operator is best for every receiver-function problem.  Start
with the failure mode you need to control:

.. list-table::
   :header-rows: 1

   * - Goal or data condition
     - Recommended starting point
   * - Simple scalar receiver functions with moderate noise
     - ``LeastSquareDecon`` or ``WaterLevelDecon`` through ``RFdecon``
   * - Source spectrum is noisy or has strong notches
     - ``NoiseStableDecon`` for scalar tests, or ``ns_gid`` for GID
   * - Need colored-noise regularization on three components
     - ``CNRDeconEngine`` through ``CNRRFDecon`` or ``CNRArrayDecon``
   * - Need sparse three-component arrivals and per-spike diagnostics
     - ``TimeDomainGIDDecon`` or ``FrequencyDomainGIDDecon``
   * - Noisy GID repeatedly picks the same strong arrival
     - ``deconvolution_type ns_gid`` with ``adaptive_memory`` lag penalty
   * - Want shared sparse support across components
     - ``deconvolution_type group_sparse``

The distributed GID parameter files use ``deconvolution_type ns_gid`` with the
``adaptive_memory`` lag penalty.  Validation sweeps selected that combination
because it is more resistant to noise-driven false picks than the legacy
``least_square`` inverse.  Use ``group_sparse`` when a shared-support sparse
prior matches the problem, and reserve ``least_square`` for explicit legacy
comparisons or diagnostics.

Switching GID methods safely
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The shipped GID defaults changed from ``deconvolution_type least_square`` to
``deconvolution_type ns_gid`` with ``lag_weight_penalty_function
adaptive_memory``.  New code should switch GID modes through the public Python
API instead of editing the installed parameter files.  The examples below assume
normal parameter-file lookup, meaning ``PFPATH`` includes the shipped MsPASS
``pf`` directory.

For one-off receiver-function calls, pass the GID options directly to
``RFdecon``:

.. code-block:: python

   from mspasspy.algorithms.RFdeconProcessor import RFdecon

   # Current shipped default: noise-stable GID plus adaptive-memory lag weights.
   rf_default = RFdecon(seis, alg="TimeDomainGID")

   # Explicit legacy comparison.  Keep this as a diagnostic, not the default.
   rf_legacy = RFdecon(
       seis,
       alg="TimeDomainGID",
       deconvolution_type="least_square",
   )

   # Group-sparse GID: one regularized solve with shared lag support.
   rf_group_sparse = RFdecon(
       seis,
       alg="TimeDomainGID",
       deconvolution_type="group_sparse",
   )

   # Make the greedy lag-weight penalty stronger or weaker.
   rf_weaker_penalty = RFdecon(
       seis,
       alg="TimeDomainGID",
       deconvolution_type="ns_gid",
       lag_weight_penalty_function="adaptive_memory",
       gid_parameters={"lag_weight_penalty_scale_factor": 0.2},
   )

For repeated processing, build a configured processor once and reuse it:

.. code-block:: python

   from mspasspy.algorithms.RFdeconProcessor import RFdecon, RFdeconProcessor

   processor = RFdeconProcessor(
       alg="TimeDomainGID",
       deconvolution_type="group_sparse",
       gid_parameters={"group_sparse_active_threshold": 0.03},
   )

   rf = RFdecon(seis, alg="TimeDomainGID", engine=processor)

For lower-level workflows that use the GID engines directly, use
``make_gid_engine`` or the corresponding parameter helpers:

.. code-block:: python

   from mspasspy.algorithms.RFdeconProcessor import (
       make_gid_engine,
       make_gid_pf,
       make_gid_pf_text,
   )

   engine = make_gid_engine(
       alg="FrequencyDomainGID",
       gid_mode="ns_gid",
       gid_penalty_function="adaptive_memory",
   )

   pf = make_gid_pf(
       alg="TimeDomainGID",
       deconvolution_type="group_sparse",
       gid_parameters={"group_sparse_active_threshold": 0.03},
   )
   pf_text = make_gid_pf_text(
       alg="TimeDomainGID",
       deconvolution_type="least_square",
   )

These settings are separate layers.  The ``deconvolution_type`` method layer and
its alias ``gid_mode`` choose the GID inverse or solver mode, such as ``ns_gid``,
``least_square``, or ``group_sparse``.  ``lag_weight_penalty_function`` and its
alias ``gid_penalty_function`` change only the greedy GID lag-selection penalty;
they do not enable grouped sparsity.  The group-sparse support threshold keys,
such as ``group_sparse_active_threshold``,
``group_sparse_active_threshold_scale``, and
``group_sparse_active_threshold_quantile``, are applied after the group-sparse
solve to decide which lag groups remain in the sparse output.

Scalar inverse operators
------------------------

Scalar operators load one source wavelet and one target trace.  They return a
regularized inverse-filter receiver function, not a sparse impulse response.
Their default parameter files do not apply a GID-style output shaping wavelet
(``shaping_wavelet_type none``).  If an older scalar parameter file configures
an output filter, treat it as a legacy display or bandlimiting filter.

``LeastSquareDecon``
    Frequency-domain damped least-squares deconvolution.  The inverse operator
    has the form

    .. math::

       S_g^{-1}(\omega) =
       \frac{\overline{S(\omega)}}{|S(\omega)|^2 + \mu},

    where ``mu`` is the damping term.  Smaller damping improves resolution for
    clean data but amplifies noise and spectral notches.  Larger damping
    produces a smoother, more stable estimate.

``WaterLevelDecon``
    Frequency-domain deconvolution with a water-level floor on the source
    spectrum.  Raising the water level improves stability but reduces
    resolution.

``MultiTaperPowerXcor`` and ``MultiTaperPowerSpecDiv``
    Multitaper frequency-domain operators that use discrete prolate spheroidal
    sequence (DPSS) tapers to stabilize source-power estimates.  The final
    receiver-function estimate is formed with the untapered data window so
    delayed converted phases are not weighted by the taper value at their
    arrival time.  In receiver-function workflows, the noise vector should
    normally describe the source/wavelet component, not the radial or
    transverse target component.

    These MsPASS operators are not paper-faithful Park and Levin (2000)
    multitaper correlation estimators.  They use DPSS spectra to stabilize the
    source-power denominator, but apply the final inverse operator to the
    untapered wavelet phase and untapered data spectrum.

``NoiseStableDecon``
    Noise-aware scalar inverse used by ``ns_gid`` and exposed as a standalone
    validation operator.  It applies gain limits and frequency-dependent
    regularization from the source/noise estimate:

    .. math::

       G(f)=B(f)\frac{\overline{S(f)}}{|S(f)|^2+\mu(f)}.

    The operator does not pick sparse spikes and does not apply the GID output
    shaping wavelet.

``TimeDomainLeastSquareDecon``
    Time-domain least-squares deconvolution.  This operator builds a linear
    convolution system for the requested lag window and solves the regularized
    normal equations.  It does not build a circular convolution matrix.

Three-component CNR operators
-----------------------------

``CNRDeconEngine`` and ``CNR3CDecon`` implement colored-noise-ratio
receiver-function deconvolution.  They estimate or load a noise spectrum and
regularize the source spectrum as a function of colored noise level and SNR.
``colored_noise_damping`` adds a frequency-dependent damping term to the
normal-equation denominator.  ``generalized_water_level`` raises low-SNR source
amplitudes before division.

``CNRDeconEngine`` is the current engine used by the Python wrappers and by the
CNR inverse mode inside GID.  ``CNR3CDecon`` is the older three-component
prototype kept for compatibility.

Generalized iterative deconvolution (GID)
-----------------------------------------

``TimeDomainGIDDecon`` and ``FrequencyDomainGIDDecon`` estimate a sparse
three-component impulse response.  Accepted spikes are convolved with the
configured output shaping wavelet to produce the shaped receiver function
returned by ``getresult``.  The raw sparse impulse response is available
through ``sparse_output`` for diagnostics and validation.

Greedy GID workflow
~~~~~~~~~~~~~~~~~~~

Greedy GID is the classic one-spike-at-a-time sparse method.  At each
iteration the engine applies a configured inverse operator to the current
residual to form a detection function,

.. math::

   a(t) = g(t) * r(t),

where ``r`` is the current residual and ``g`` is the configured inverse
operator for the selected GID mode.  The largest acceptable three-component
detection-function peak becomes a candidate spike.  The engine subtracts that
candidate's predicted contribution from the data-domain residual and keeps the
spike only if the residual decreases.

The inverse operator is therefore used to choose candidate spike locations and
amplitudes.  It is not the final receiver-function representation.  The
reported GID receiver function is the sparse impulse response convolved with
the output shaping wavelet.

Time-domain and frequency-domain engines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The two GID engines implement the same sparse model with different internal
machinery.  The time-domain engine performs the iterative residual update in
the time domain.  The frequency-domain engine builds inverse and resolution
quantities with frequency-domain operations.  Both engines update residuals
with a compact, peak-normalized residual-update kernel derived from the inverse
operator's actual output.  The public ``actual_output()`` method returns the
full inverse-operator resolution diagnostic, not the compact residual-update
kernel.

The ``multi_taper`` inverse mode is still a frequency-domain inverse operator,
even when selected inside ``TimeDomainGIDDecon``.  In that case "time domain"
describes the sparse iteration and residual update, not the spectral method
used by the inverse operator.

``NS-GID`` inverse mode
~~~~~~~~~~~~~~~~~~~~~~~

``NS-GID`` is selected with ``deconvolution_type ns_gid`` in either GID engine.
It is a noise-stable inverse mode used inside the GID iteration, not a separate
preprocessing workflow.  The inverse operator is

.. math::

   G(f) = B(f)\frac{\overline{S(f)}}{|S(f)|^2 + \mu(f)},

where ``S`` is the source wavelet spectrum, ``B`` is the optional band-limiting
or reliability taper, and ``mu(f)`` is a frequency-dependent damping term.  The
damping combines a relative spectral floor, noise-spectrum information, and an
explicit maximum-gain constraint controlled by ``ns_gid_gain_max``.

Use ``ns_gid`` when noisy or spectrally weak source wavelets make greedy GID
with a less noise-stable inverse pick noise-generated spikes.  External
wavelets can be supplied through the GID ``loadwavelet`` APIs or wrapper
``external_wavelet`` argument.  External ``TimeSeries`` noise can support both
inverse regularization and residual-domain stopping when loaded as residual
noise.  External ``PowerSpectrum`` noise only regularizes the inverse operator;
a residual-domain noise window is still needed for spike significance and
stopping.

Convergence and stopping
~~~~~~~~~~~~~~~~~~~~~~~~

GID stops when the configured iteration limit is reached or when a stopping
test says another spike is not useful.  Common stopping tests include small
fractional residual improvement, residual energy reaching a noise floor, no
acceptable candidate lag, a lag-weight penalty exhausting valid candidates, and,
when ``ns_gid`` is selected, optional ``ns_gid_max_spikes`` and
noise-significance thresholds.

Time-domain and frequency-domain GID use the same iteration-cap behavior.
Reaching ``maximum_iterations`` stops the iteration, returns the best accepted
sparse model after final amplitude refit, and records
``gid_stop_reason="max_iterations"`` with ``gid_converged=false``.  Stops caused
by residual or lag-weight floors are reported as converged because the engine
found a configured reason not to add more spikes.

QC metadata
~~~~~~~~~~~

``QCMetrics`` records both processing status and diagnostic context.  Useful
first-pass GID fields include ``decon_operator``, ``decon_processed``,
``gid_converged``, ``gid_stop_reason``, ``gid_iterations``,
``gid_number_spikes``, ``residual_L2_initial``, and ``residual_L2_final``.

Greedy lag-weight penalty runs also record ``gid_penalty_function``,
``gid_penalty_scale_factor``, ``gid_penalty_width``,
``gid_penalty_effective_width``, ``lag_weight_Linf_final``, and
``lag_weight_L2_final``.  Adaptive-memory runs add fields such as
``gid_adaptive_penalty_enabled``, ``gid_penalty_noise_amplitude``,
``gid_penalty_last_confidence``, ``gid_penalty_last_decay_factor``,
``gid_penalty_memory_Linf_final``, and ``gid_penalty_memory_L2_final``.

When ``deconvolution_type ns_gid`` is active, additional ``ns_gid_*`` fields
record inverse stability and stopping diagnostics, including
``ns_gid_stop_reason``, ``ns_gid_converged``, ``ns_gid_peak_threshold``,
``ns_gid_noise_amplitude_rms``, ``ns_gid_gain_max_actual``, and
``ns_gid_effective_bandwidth_fraction``.  The Python wrappers store these QC
values in receiver-function metadata subdocuments, so they are saved with
normal database records.

For a beginner-oriented guide to reading these fields after a plot run or
database save, see `Validation and QC workflow`_.

Lag-weight penalty framework
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lag-weight penalties apply only to greedy GID.  They modify the score used to
choose the next candidate lag after a spike has
already been accepted.  They do not shrink accepted amplitudes, and they are
separate from the grouped-sparsity solve used by ``deconvolution_type
group_sparse`` (see `Group-sparse regularized GID`_).  In all greedy modes the
data-domain residual decrease test remains the final acceptance rule.

Recommended use
^^^^^^^^^^^^^^^

The easiest way to choose a penalty is by failure mode:

.. list-table::
   :header-rows: 1

   * - Situation
     - Suggested setting
   * - Noisy receiver functions repeatedly pick one strong arrival
     - ``deconvolution_type ns_gid`` with
       ``lag_weight_penalty_function adaptive_memory``
   * - Closely spaced true arrivals are the scientific target
     - ``lag_weight_penalty_function none``
   * - Visual diagnostics
     - compare ``none`` and the active penalty
   * - Fixed-width controlled tests
     - ``cosine_taper`` or ``boxcar``
   * - Resolution-footprint studies
     - ``resolution_kernel`` or ``shaping_wavelet``

The shipped GID parameter files set ``deconvolution_type ns_gid``,
``lag_weight_penalty_function adaptive_memory``, and
``lag_weight_penalty_scale_factor=0.35``.  Validation sweeps across moderate and
large noise levels selected this as the default because the legacy
``least_square`` inverse can generate noise-driven false picks even with the
same lag penalty.  Treat it as a robust starting point, not a universal optimum.

How the greedy penalty enters GID
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

At iteration :math:`k`, let :math:`\mathbf{a}^{(k)}_j` be the
three-component detection-function vector at lag sample :math:`j`, and let
:math:`\ell^{(k)}_j \in [0, 1]` be the accumulated lag weight.  The implemented
selection statistic is

.. math::

   q^{(k)}_j = \left(\ell^{(k)}_j\right)^2
              \left\|\mathbf{a}^{(k)}_j\right\|_2^2,
   \qquad
   j_* = \operatorname*{arg\,max}_j q^{(k)}_j .

After accepting a spike at :math:`j_*`, the engine multiplies nearby lag
weights by a compact penalty kernel :math:`p_m`:

.. math::

   \ell^{(k+1)}_j =
      \operatorname{clip}\left(\ell^{(k)}_j p_{j-j_*}, 0, 1\right).

Outside the penalty support, :math:`p_m=1`.  Repeated hits near the same
arrival multiply down the same neighborhood, making the penalty a soft
anti-cycling cost.  The penalty changes which lag is tried next; the
data-domain residual decrease test still decides whether the candidate is
accepted.

Current lag-weight penalty methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``none``
    Leaves the greedy selector unchanged.  It is the safest support-recovery
    choice when real arrivals may be tightly clustered.

``boxcar``
    Applies fixed-width hard local suppression.  It is easy to interpret but
    can move false picks to the first unpenalized sample outside the window.

``cosine_taper``
    Applies fixed-width smooth compact suppression.  The accepted sample
    receives the strongest penalty and nearby samples are downweighted
    gradually.

``shaping_wavelet``
    Derives the applied penalty footprint from the requested output shaping
    wavelet.  This is useful for controlled comparisons where the requested
    output pulse is the desired resolution reference.

``resolution_kernel``
    Derives the footprint from the compact actual residual-update kernel.  This
    reflects inverse-operator regularization, trimming, and peak normalization.

``adaptive_memory``
    Derives the footprint from the resolution kernel and changes memory
    strength and retention from event to event.  Strong, well-localized
    arrivals create stronger local memory.  Broad or low-confidence arrivals
    create weaker memory that decays quickly.

Adaptive memory theory
^^^^^^^^^^^^^^^^^^^^^^

``adaptive_memory`` implements a finite-memory local penalty.  After each
accepted spike it asks three questions:

1. Which lags are in the same ambiguous state as the accepted spike?
2. How reliable is the accepted spike as an explanation of the residual?
3. How long should that local penalty persist?

Let :math:`M^{(k)}_j=-\log \ell^{(k)}_j` be the local penalty cost.  The picker
can be written as

.. math::

   q^{(k)}_j =
      \exp\left[-2M^{(k)}_j\right]
      \left\|\mathbf{a}^{(k)}_j\right\|_2^2 .

An accepted spike adds local memory shaped by the resolution-kernel coherence,
scaled by a noise-normalized confidence, and retained according to the
specificity of the footprint.  In the practical extremes, high-SNR sharp
arrivals create durable local memory, high-SNR broad arrivals create strong
but quickly forgotten memory, low-SNR sharp arrivals create cautious memory,
and low-SNR broad arrivals create little durable memory.  This discourages
immediate cycling without permanently forbidding nearby lags.

The algorithm estimates confidence from a search-adjusted three-component
detection statistic.  If :math:`\Sigma_s` is the local
detection-function noise covariance at the accepted sample :math:`s`, a natural
single-lag statistic is

.. math::

   Z_s^2 = \mathbf{a}_s^T \Sigma_s^{-1}\mathbf{a}_s .

Because GID picks the maximum over all currently valid lags, a pure-noise
maximum also grows with search size.  With :math:`N_\mathrm{valid}` valid lag
samples, the helper normalizes by a three-component upper-tail bound,

.. math::

   E_\mathrm{search} =
      1 + 2\sqrt{\frac{2\log N_\mathrm{valid}}{3}}
        + \frac{4\log N_\mathrm{valid}}{3},

using :math:`E_\mathrm{search}=1` when there is only one valid lag.  The
bounded confidence is then

.. math::

   B_s =
      \begin{cases}
        0, & Z_{*,s} \le 1, \\
        1 - Z_{*,s}^{-2}, & Z_{*,s} > 1 ,
      \end{cases}
   \qquad
   Z_{*,s}^2 = Z_s^2/E_\mathrm{search}.

The ambiguity footprint comes from the coherence of shifted resolution-kernel
atoms.  A broad footprint has low information concentration and should be
forgotten faster than a sharp footprint.  The implemented specificity is

.. math::

   S_s =
      1 - \frac{\log N_{\mathrm{eff},s}}
               {\log N_\mathrm{valid}},
   \qquad
   N_{\mathrm{eff},s} =
      \frac{\left(\sum_j A_s(j)\right)^2}{\sum_j A_s(j)^2},

where :math:`A_s(j)` is the applied footprint weight.  The retention strength
is :math:`\gamma_s=B_sS_s`.  Conceptually, the memory update is

.. math::

   M^{(k+1)}_j =
      R^{(k)}_j M^{(k)}_j
      - \log\left[1-\alpha B_s A_s(j)\right],

where :math:`\alpha` is ``lag_weight_penalty_scale_factor`` and
:math:`R^{(k)}_j` is the retention already stored at lag :math:`j`.  This is a
temporary local penalty, not a permanent exclusion rule.

Benchmark interpretation
^^^^^^^^^^^^^^^^^^^^^^^^

The validation results should be read as evidence for guarded anti-cycling
behavior, not as proof that ``adaptive_memory`` is universally best.  In the
moderate-noise synthetic sweep, ``adaptive_memory`` reduced some repeated-pick
failures and false positives.  In dense close-arrival cases, ``none`` can
remain better because any local anti-cycling memory can suppress the next true
arrival.
In high-noise sweeps, search-adjusted confidence can make ``adaptive_memory``
collapse toward ``none`` once selected peaks are no longer strong relative to
the full lag search.

Use the penalty plots and QC fields to decide whether the penalty helped the
specific data and noise setting being processed.

Serialization and distributed use
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Direct ``TimeDomainGIDDecon`` and ``FrequencyDomainGIDDecon`` engine objects
are pickleable for distributed wrapper use.  Their pickle state preserves the
parameter-file configuration, successful ``changeparameter()`` updates to the
underlying inverse operator, and any externally loaded wavelet, ``TimeSeries``
noise, or ``PowerSpectrum`` noise.  It does not preserve loaded seismograms,
processed receiver functions, residuals, sparse spike trains, or runtime QC
state.

For Dask or Spark jobs, build and configure the reusable deconvolution
processor on the driver, then scatter or close over that configured object.
Avoid loading per-datum scalar data into an ``RFdeconProcessor`` before
scattering it, because scalar compatibility mode intentionally preserves cached
input vectors for post-processing diagnostics.

Group-sparse regularized GID
----------------------------

``group_sparse`` is a separate GID mode from the greedy lag-weight penalties
described above.  Lag-weight penalties modify which candidate lag the greedy
iteration tries next.  ``group_sparse`` instead solves one regularized model
for the full sparse impulse response, the lag-indexed spike train returned by
``sparse_output``.  Its group-lasso penalty favors shared support, meaning the
same nonzero lag samples, across the three components while still allowing
different component amplitudes.

Relation to classic greedy GID
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``group_sparse`` estimates the full sparse impulse response by solving an
explicit regularized objective instead of selecting one spike per iteration.
This makes it a different estimator, not a strict upgrade.  Classic greedy GID
remains useful for diagnostics, direct per-spike stopping behavior, and cases
where close arrivals need to remain separate.

Both time-domain and frequency-domain engines still build a GID inverse
operator and compact actual-output/resolution kernel.  In the current
implementation ``deconvolution_type group_sparse`` uses the ``ns_gid`` inverse
branch internally, so the inverse-filtered data and resolution kernel inherit
NS-GID's gain cap, noise-spectrum damping, and optional reliability taper.
After those quantities are built, the two engines use the same group-sparse
objective.

Model and solver
~~~~~~~~~~~~~~~~

Let :math:`\mathbf{y}_c` be component :math:`c` of the inverse-filtered data,
and let :math:`R` be the compact, peak-normalized resolution-kernel convolution
operator used by GID residual updates.  The unknown sparse impulse response is
a matrix :math:`H`, where each lag sample :math:`j` has a three-component group
and each :math:`\mathbf{h}_c` denotes the lag series for component
:math:`c`.

.. math::

   \mathbf{h}_j = \left(h_{E,j}, h_{N,j}, h_{Z,j}\right)^T .

The implemented model is

.. math::

   \hat{H}
   = \operatorname*{arg\,min}_{H}
     \frac{1}{2}\sum_{c \in \{E,N,Z\}}
       \left\|R \mathbf{h}_c - \mathbf{y}_c\right\|_2^2
     + \lambda \sum_j \left\|\mathbf{h}_j\right\|_2 .

The first term asks the sparse impulse response, convolved with the actual GID
resolution kernel, to explain the inverse-filtered data.  The second term is an
:math:`\ell_{2,1}` group-lasso penalty.  It shrinks all three component
amplitudes at a lag as one group, favoring shared arrival support.

The solver uses proximal gradient iterations.  Each iteration takes a gradient
step on the data-misfit term and then applies group soft thresholding,

.. math::

   \mathbf{h}^{(k+1)}_j =
   \max\left(0, 1 - \frac{\tau \lambda}{\|\mathbf{z}_j\|_2}\right)
   \mathbf{z}_j .

The multiplier is interpreted as zero when
:math:`\|\mathbf{z}_j\|_2 = 0`.

The implementation uses a conservative step bound derived from the finite
impulse response (FIR) resolution kernel and stops when the objective's
relative change is below ``group_sparse_tolerance`` or when
``group_sparse_max_iterations`` is reached.
After support is selected, the engine runs the same final amplitude refit used
by classic greedy GID and recomputes the reported residual metrics.

At convergence, the group-lasso Karush-Kuhn-Tucker conditions clarify the role
of ``group_sparse_lambda``.  For inactive lags,

.. math::

   \left\|R_j^T \mathbf{r}\right\|_2 \le \lambda ,

where :math:`\mathbf{r}=R\hat{H}-\mathbf{y}` stacks the residuals for all three
components and :math:`R_j` denotes the resolution-kernel operator block
associated with lag :math:`j`.  For active lags,

.. math::

   R_j^T \mathbf{r}
      = -\lambda
        \frac{\hat{\mathbf{h}}_j}
             {\left\|\hat{\mathbf{h}}_j\right\|_2}.

Thus ``group_sparse_lambda`` is the evidence threshold inside the regularized
objective.  It is not, by itself, the threshold used to decide which nonzero
coefficients are exported as sparse arrivals.

Regularization and support reporting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``group_sparse_lambda`` controls shrinkage inside the objective.  A positive
value is used directly.  The default value ``0.0`` selects an automatic
noise-scaled value:

.. math::

   \lambda =
   \texttt{group\_sparse\_lambda\_scale}
   \times
   \begin{cases}
      \text{NS-GID inverse-filtered noise threshold},
         & \text{if available}, \\
      0.02\,\|\mathbf{y}\|_\infty,
         & \text{otherwise}.
   \end{cases}

The exported sparse support has a second adaptive decision rule.  The
regularized coefficient field can contain tiny numerical coefficients or small
clustered sidelobe coefficients.  The default support threshold is

.. math::

   \tau_\text{support}
      =
      \max\left(
        \texttt{group\_sparse\_active\_threshold},
        \texttt{group\_sparse\_active\_threshold\_scale}
        \;Q_q\left(\left\{\|\hat{\mathbf{h}}_j\|_2\right\}\right)
      \right),

where :math:`q=\texttt{group\_sparse\_active\_threshold\_quantile}`.  The
distributed defaults are ``group_sparse_active_threshold=0.02``,
``group_sparse_active_threshold_scale=1.0``, and
``group_sparse_active_threshold_quantile=0.90``.

This threshold is the group-sparse analogue of the adaptive part of
``adaptive_memory``.  The adaptation is global and distributional rather than a
per-spike memory update, but it lets the support decision follow the solved
coefficient field instead of a hand-picked constant.  ``group_sparse_lambda``
is therefore the objective shrinkage strength, while
``group_sparse_active_threshold_used`` is the actual cutoff applied to
``sparse_output`` and the final refit model.

If the proximal solve is already sparse, most lag groups are zero and the
absolute floor controls the exported support.  If coherent leakage fills many
lag groups, the upper-tail quantile rises and prunes the leaked coefficient
field.  Benchmark threshold sweeps are therefore important: disabling the
adaptive upper-tail cutoff can leave many retained coefficients, while an
overly large fixed floor can remove weak converted phases.

QC and interpretation
~~~~~~~~~~~~~~~~~~~~~

``QCMetrics`` records ``group_sparse_lambda_requested``,
``group_sparse_lambda_scale``, ``group_sparse_lambda_used``,
``group_sparse_active_threshold``, ``group_sparse_active_threshold_scale``,
``group_sparse_active_threshold_quantile``,
``group_sparse_active_threshold_quantile_value``,
``group_sparse_active_threshold_used``, and
``group_sparse_active_groups``.  It also records ``group_sparse_iterations``
and ``group_sparse_converged`` for the proximal solve.  These fields let
downstream QC distinguish the regularized solve from the support-reporting
layer.

``group_sparse_active_groups`` is a count of retained coefficient groups, not a
resolved geologic arrival count.  Validation plots and tests use separate
detection metrics for arrival-level comparisons.

Because this mode uses the NS-GID inverse branch internally, it also records
group-sparse-prefixed inverse QC fields such as
``group_sparse_inverse_gain_max_actual``,
``group_sparse_inverse_noise_amplification``, and
``group_sparse_inverse_effective_bandwidth_fraction``.

The careful benchmark claim is narrow: in the supplied synthetic validation
fixtures, group-sparse solving with the adaptive support rule can improve
sparse-support recovery and false-positive control.  It should not be presented
as universally better than classic greedy GID, including ``adaptive_memory``
penalty runs.  Adaptive-memory GID may still produce a lower final residual
norm or preserve dense close arrivals better.

Validation and QC workflow
--------------------------

The validation tests can write optional figures that are useful for learning
how the deconvolution operators behave on controlled synthetic examples.  These
figures are an audit aid, not a field-data benchmark.  The pytest assertions
remain the source of pass/fail behavior.

Run the optional validation plots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To write the default diagnostic figures, run:

.. code-block:: bash

   python -m pytest \
       python/tests/algorithms/test_decon_algorithm_validation.py \
       --decon-validation-plots \
       --decon-validation-plot-dir /tmp/mspass-decon-validation-plots

Use ``--decon-validation-noise-scale`` when you want extra stress plots at a
different synthetic noise amplitude:

.. code-block:: bash

   python -m pytest \
       python/tests/algorithms/test_decon_algorithm_validation.py \
       --decon-validation-plots \
       --decon-validation-plot-dir /tmp/mspass-decon-validation-plots-noise-003 \
       --decon-validation-noise-scale 0.03

``--decon-validation-noise-scale`` is the Gaussian noise amplitude before the
synthetic coloring filter.  It is not an SNR value and it is not a percent-noise
setting.  Changing it changes the optional figures used for visual inspection;
the core numerical checks still use fixed reproducible validation cases.

What the figures show
~~~~~~~~~~~~~~~~~~~~~

``complex_colored_validation_wavelet.png``
    Shows the notched source wavelet, the noisy convolved three-component data,
    and the known sparse impulse response used as synthetic truth.

``complex_colored_scalar_methods.png`` and
``scalar_noise_<scale>_stress_spike_results.png``
    Compare scalar and CNR receiver-function outputs.  These traces are shaped
    receiver functions, not sparse impulse responses.

``TimeDomainGIDDecon_*`` and ``FrequencyDomainGIDDecon_*`` overlays
    Compare GID outputs for the time-domain and frequency-domain engines.  Files
    ending in ``_sparse_results.png`` show raw sparse impulse responses from
    ``sparse_output`` and are the most direct visual comparison to the known
    sparse truth.

``external_wavelet_all_methods.png``
    Compares methods when a prepared external wavelet is supplied.  The
    companion ``external_wavelet_all_methods_display_filtered.png`` applies a
    common plotting-only display filter; that filter is not part of the
    algorithms or the pass/fail assertions.

Reading noise-scale plots
~~~~~~~~~~~~~~~~~~~~~~~~~

Start with the setup figure.  The data panel is normalized for display, so use
the noise-scale label, pre-event RMS, signal RMS, and waveform shape together.
A larger noise scale should make pre-event and inter-arrival fluctuations more
visible, but it should not be read as a calibrated prediction of field-data
performance.

If a high-noise diagnostic run marks a method as failed or omits that method's
output trace, treat that as stress-run context.  It is a reason to inspect the
matching QC metadata, not by itself a benchmark result.

Reading sparse-support plots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For GID methods, compare the raw sparse plots to the true impulse response.  The
main questions are whether expected arrivals are present near the correct lag,
whether signs are plausible, and whether the result contains many extra
isolated or clustered spikes.

For ``group_sparse`` labels, ``lam`` is the regularization value used, ``thr``
is the support threshold used after the solve, ``k`` is the number of retained
coefficient groups, one three-component lag group per lag sample, and ``it`` is
the number of proximal iterations.  ``k`` is not automatically a geologic
arrival count.

Reading penalty comparison plots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The penalty comparison figures should be read as a three-part diagnostic:

* The shaped-result plot shows the receiver-function traces users would
  normally inspect.
* The sparse-result plot shows how each penalty changed the accepted sparse
  support.
* The lag-weight plot shows the final penalty state.  Values near 1.0 are
  effectively unpenalized; lower values mark lag neighborhoods that were
  downweighted after earlier accepted spikes.

The labels report the penalty function, scale factor, effective width, and, for
``adaptive_memory``, the last confidence and decay values.  These plots can show
whether a penalty reduced repeated picking near the same arrival, but they
should not be summarized as one penalty universally beating another.  Prefer
wording such as "in this synthetic stress case" or "for this configured noise
level."

Inspecting QC metadata after deconvolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After running deconvolution, inspect the QC subdocument saved on the output
datum.  ``RFdecon`` uses ``RFdecon_properties`` by default.  The direct GID
wrappers use ``TimeDomainGIDDecon_properties`` and
``FrequencyDomainGIDDecon_properties`` by default.  If you set a custom
``QCdocument_key`` or ``QCdata_key``, inspect that key instead.

Useful first-pass fields are:

``decon_operator`` and ``decon_processed``
    Confirm which engine ran and whether it processed the datum.

``decon_window_start``, ``decon_window_end``, ``noise_window_start``, and
``noise_window_end``
    Confirm that the analysis and noise windows match the intended workflow.

``residual_L2_initial`` and ``residual_L2_final``
    Check whether the fitted model reduced residual energy.  Smaller is not
    automatically better if the sparse support becomes physically implausible.

``gid_converged``, ``gid_stop_reason``, ``gid_iterations``, and
``gid_number_spikes``
    Summarize the GID iteration outcome.

``gid_penalty_function``, ``gid_penalty_effective_width``,
``lag_weight_L2_final``, and the ``gid_penalty_*`` adaptive-memory fields
    Explain how the lag-weight penalty affected candidate selection.

``group_sparse_enabled``, ``group_sparse_converged``,
``group_sparse_iterations``, ``group_sparse_lambda_used``,
``group_sparse_active_threshold_used``, ``group_sparse_active_groups``,
``group_sparse_objective_initial``, and ``group_sparse_objective_final``
    Summarize the regularized group-sparse solve and the exported sparse
    support decision.

``ns_gid_gain_max_actual``, ``ns_gid_noise_amplification``, and
``ns_gid_effective_bandwidth_fraction``
    Help audit inverse-operator stability for ``deconvolution_type ns_gid``.

``group_sparse_inverse_gain_max_actual``,
``group_sparse_inverse_noise_amplification``, and
``group_sparse_inverse_effective_bandwidth_fraction``
    Help audit the NS-GID inverse branch used internally by
    ``deconvolution_type group_sparse``.

Implementation and compatibility notes
--------------------------------------

The C++/pybind multitaper classes are still registered as
``MultiTaperXcorDecon`` and ``MultiTaperSpecDivDecon`` for ABI and pickle
compatibility.  ``MultiTaperPowerXcorDecon`` and
``MultiTaperPowerSpecDivDecon`` are Python-level class aliases to those
classes, not distinct runtime types.  The old RF processor algorithm names
``MultiTaperXcor`` and ``MultiTaperSpecDiv`` remain available as deprecated
compatibility aliases for the power-stabilized operators.

``MultiTaperSpecDivDecon`` still exposes legacy ``all_inverse_wavelets``,
``all_rfestimates``, and ``all_actual_outputs`` methods.  In the current
implementation those compatibility methods normally contain one combined
multitaper estimate rather than one independent product per taper.

Parameter files tuned against older tapered-numerator multitaper behavior
should be retuned and revalidated.  ``damping_factor`` is now interpreted in
the power-domain denominator, so values should not be assumed numerically
equivalent to historical Park-Levin-style implementations.
