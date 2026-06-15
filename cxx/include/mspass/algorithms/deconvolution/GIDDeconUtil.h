#ifndef __GID_DECON_UTIL_H__
#define __GID_DECON_UTIL_H__
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/ThreeCSpike.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include <list>
#include <string>
#include <vector>

namespace mspass::algorithms::deconvolution {
/*! \brief Result bundle returned by group-sparse GID deconvolution.

The fields summarize the selected spikes, residual signal, convergence state,
and objective-function diagnostics from the solver.
*/
struct GroupSparseDeconResult {
  std::list<ThreeCSpike> spikes; /*!< Selected three-component spikes. */
  mspass::seismic::CoreSeismogram residual; /*!< Final residual signal. */
  int iterations = 0; /*!< Number of solver iterations completed. */
  int active_groups = 0; /*!< Number of active spike groups in the solution. */
  bool converged = false; /*!< True when the solver met the stop tolerance. */
  double lambda = 0.0; /*!< Group-sparse penalty weight. */
  double active_threshold_floor = 0.0; /*!< Absolute lower bound on active threshold. */
  double active_threshold_scale = 1.0; /*!< Scale factor applied to the active threshold. */
  double active_threshold_quantile = 0.0; /*!< Quantile used for adaptive thresholding. */
  double active_threshold_quantile_value = 0.0; /*!< Value at the selected quantile. */
  double active_threshold_used = 0.0; /*!< Final threshold used by the solver. */
  double objective_initial = 0.0; /*!< Objective value before iteration. */
  double objective_final = 0.0; /*!< Objective value after iteration. */
  double fractional_improvement_final = 0.0; /*!< Final fractional objective improvement. */
};

/*! Parse the configured GID leaf inverse-operator type.
 *
 * \param md metadata containing the deconvolution_type key.
 * \param caller name added to exception messages.
 * \return enum value used to select the inverse operator.
 * \exception MsPASSError if deconvolution_type is unknown.
 */
IterDeconType ParseGIDDeconType(const mspass::utility::Metadata &md,
                                const std::string &caller);
/*! Return the canonical parameter-file name for a GID inverse-operator type. */
std::string GIDDeconTypeName(const IterDeconType type);
/*! Return a double parameter or a caller-supplied default when absent. */
double GetDoubleDefault(const mspass::utility::Metadata &md,
                        const std::string &key, const double default_value);
/*! Return a required numeric parameter as a double.
 *
 * Adds GID-specific context to type mismatch errors.
 */
double GetDoubleRequired(const mspass::utility::Metadata &md,
                         const std::string &key);
/*! Return an int parameter or a caller-supplied default when absent. */
int GetIntDefault(const mspass::utility::Metadata &md, const std::string &key,
                  const int default_value);
/*! Return a required integer parameter with contextual errors. */
int GetIntRequired(const mspass::utility::Metadata &md,
                   const std::string &key);
/*! Return a required long integer parameter with contextual errors. */
long GetLongRequired(const mspass::utility::Metadata &md,
                     const std::string &key);
/*! Return a bool parameter or a caller-supplied default when absent. */
bool GetBoolDefault(const mspass::utility::Metadata &md,
                    const std::string &key, const bool default_value);
/*! Validate that a probability is finite and lies in the closed interval [0, 1]. */
void ValidateProbability(const double p, const std::string &key,
                         const std::string &caller);
/*! Validate that a floating-point parameter is finite and greater than zero. */
void ValidatePositive(const double x, const std::string &key,
                      const std::string &caller);
/*! Validate that a floating-point parameter is finite and nonnegative. */
void ValidateNonnegative(const double x, const std::string &key,
                         const std::string &caller);
/*! Validate that an integer parameter is greater than zero. */
void ValidatePositiveInteger(const int x, const std::string &key,
                             const std::string &caller);
/*! Validate a three-component index.
 *
 * Component indices must be 0, 1, or 2.
 */
void ValidateThreeComponentIndex(const int component, const std::string &key,
                                 const std::string &caller);
/*! Copy all metadata values from source into target with prefixed keys.
 *
 * Unsupported boost::any payload types raise MsPASSError.
 */
void PutPrefixedMetadata(mspass::utility::Metadata &target,
                         const mspass::utility::Metadata &source,
                         const std::string &prefix);
/*! Render an AntelopePf tree as deterministic parameter-file text.
 *
 * \param pf parameter tree to serialize.
 * \param indent number of spaces to prefix each generated line.
 */
std::string AntelopePfToText(const mspass::utility::AntelopePf &pf,
                             const int indent = 0);
/*! Compute Euclidean three-component amplitudes for each matrix column. */
std::vector<double> ThreeCAmplitudes(const mspass::utility::dmatrix &d);
/*! Evaluate the group-sparse objective for a residual and spike list.
 *
 * The returned value is 0.5 times residual sum-of-squares plus lambda times
 * the sum of spike vector norms.
 */
double GroupSparseObjective(const mspass::seismic::CoreSeismogram &residual,
                            const std::list<ThreeCSpike> &spikes,
                            const double lambda);
/*! Validate that a leaf inverse operator uses the enclosing GID window. */
void ValidateGIDLeafWindow(const mspass::utility::AntelopePf &mdleaf,
                           const mspass::algorithms::TimeWindow &fftwin,
                           const std::string &leaf_name,
                           const std::string &base_error);
/*! Validate metadata accepted by changeparameter for a leaf inverse operator.
 *
 * GID-level parameters are rejected because they require constructing a new
 * GID engine rather than retuning the current leaf operator.
 */
void ValidateGIDLeafOperatorMetadata(
    const mspass::utility::Metadata &md,
    const mspass::algorithms::TimeWindow &fftwin, const double target_dt,
    const std::string &caller, const bool allow_noise_window_keys = false);
/*! Validate that an externally loaded TimeSeries has the target sample interval. */
void ValidateExternalTimeSeriesSampleInterval(
    const mspass::seismic::TimeSeries &d, const double target_dt,
    const std::string &caller);
/*! Clip a requested window to the live time span of a CoreTimeSeries.
 *
 * \exception MsPASSError if the input is dead, empty, invalid, or nonoverlapping.
 */
mspass::algorithms::TimeWindow ClipTimeWindowToSeries(
    const mspass::seismic::CoreTimeSeries &d,
    const mspass::algorithms::TimeWindow &requested,
    const std::string &caller);
/*! Build a static lag-weight penalty vector from GID metadata. */
std::vector<double> BuildGIDLagWeightPenaltyFunction(
    const mspass::utility::Metadata &md, const std::string &caller);
/*! Return true when a penalty type must be derived from a wavelet or kernel. */
bool GIDLagWeightPenaltyUsesDynamicKernel(const std::string &penalty_type);
/*! Return true when a penalty type uses adaptive accumulated memory. */
bool GIDLagWeightPenaltyUsesAdaptiveMemory(const std::string &penalty_type);
/*! \brief Diagnostics from adaptive lag-penalty memory updates. */
struct GIDAdaptivePenaltyMetrics {
  double confidence = 0.0; /*!< Confidence assigned to the current candidate. */
  double immediate_strength = 0.0; /*!< Immediate penalty contribution. */
  double specificity = 0.0; /*!< Concentration of the selected kernel. */
  double decay_factor = 0.0; /*!< Memory-retention decay factor. */
  double memory_linf = 0.0; /*!< L-infinity norm of accumulated memory. */
  double memory_l2 = 0.0; /*!< L2 norm of accumulated memory. */
  double noise_amplitude = 0.0; /*!< Noise amplitude used for scaling. */
  int effective_width = 1; /*!< Effective width of the adaptive kernel. */
};
/*! Build a lag penalty from a supplied resolution or shaping kernel. */
std::vector<double> BuildGIDLagWeightPenaltyFunctionFromKernel(
    const std::string &penalty_type, const double penalty_scale,
    const std::vector<double> &kernel, const int zero_lag_sample,
    const std::string &caller);
/*! Apply a precomputed lag penalty centered on a candidate spike column. */
void ApplyGIDLagWeightPenalty(std::vector<double> &lag_weights,
                              const std::vector<double> &penalty,
                              const int center_col);
/*! Estimate the RMS column vector amplitude of a three-component seismogram. */
double EstimateThreeCColumnAmplitudeRMS(
    const mspass::seismic::CoreSeismogram &d);
/*! Apply the adaptive-memory lag penalty and return diagnostic metrics.
 *
 * The memory and retention vectors are resized as needed to match lag_weights.
 */
GIDAdaptivePenaltyMetrics ApplyGIDAdaptiveMemoryPenalty(
    std::vector<double> &lag_weights, std::vector<double> &memory,
    std::vector<double> &retention, const std::vector<double> &kernel,
    const int zero_lag_sample, const int center_col,
    const double penalty_scale, const double candidate_amplitude,
    const double noise_amplitude, const std::string &caller);
/*! Compute overlap between two shifted copies of an FIR kernel. */
double FIRSelfOverlap(const std::vector<double> &fir, const int col0_i,
                      const int col0_j, const int ncols);
/*! Compute overlap between an FIR kernel and one target component. */
double FIRDataOverlap(const std::vector<double> &fir,
                      const mspass::seismic::CoreSeismogram &target,
                      const int component, const int col0);
/*! Solve a dense linear system used by spike-amplitude refitting.
 *
 * Cholesky factorization is attempted first; a general solve is used as a
 * fallback for non-positive-definite systems.
 */
std::vector<double> SolveDenseSystem(const std::vector<std::vector<double>> &a,
                                     const std::vector<double> &b,
                                     const std::string &caller);
/*! Refit spike amplitudes by solving the dense least-squares normal equations. */
void RefitSpikeAmplitudes(std::list<ThreeCSpike> &spikes,
                          const mspass::seismic::CoreSeismogram &target,
                          const std::vector<double> &actual_o_fir,
                          const int actual_o_0,
                          const double ridge_beta = 1.0e-10);
/*! Solve the group-sparse GID sparse-spike inverse problem.
 *
 * \return residual, selected spikes, convergence state, and QC diagnostics.
 */
GroupSparseDeconResult SolveGroupSparseDecon(
    const mspass::seismic::CoreSeismogram &target,
    const std::vector<double> &actual_o_fir, const int actual_o_0,
    const double lambda, const int max_iterations, const double tolerance,
    const double active_threshold, const double active_threshold_scale,
    const double active_threshold_quantile, const std::string &caller);
} // namespace mspass::algorithms::deconvolution
#endif
