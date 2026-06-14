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
struct GroupSparseDeconResult {
  std::list<ThreeCSpike> spikes;
  mspass::seismic::CoreSeismogram residual;
  int iterations = 0;
  int active_groups = 0;
  bool converged = false;
  double lambda = 0.0;
  double active_threshold_floor = 0.0;
  double active_threshold_scale = 1.0;
  double active_threshold_quantile = 0.0;
  double active_threshold_quantile_value = 0.0;
  double active_threshold_used = 0.0;
  double objective_initial = 0.0;
  double objective_final = 0.0;
  double fractional_improvement_final = 0.0;
};

IterDeconType ParseGIDDeconType(const mspass::utility::Metadata &md,
                                const std::string &caller);
std::string GIDDeconTypeName(const IterDeconType type);
double GetDoubleDefault(const mspass::utility::Metadata &md,
                        const std::string &key, const double default_value);
int GetIntDefault(const mspass::utility::Metadata &md, const std::string &key,
                  const int default_value);
bool GetBoolDefault(const mspass::utility::Metadata &md,
                    const std::string &key, const bool default_value);
void ValidateProbability(const double p, const std::string &key,
                         const std::string &caller);
void ValidatePositive(const double x, const std::string &key,
                      const std::string &caller);
void ValidateNonnegative(const double x, const std::string &key,
                         const std::string &caller);
void ValidatePositiveInteger(const int x, const std::string &key,
                             const std::string &caller);
void ValidateThreeComponentIndex(const int component, const std::string &key,
                                 const std::string &caller);
void PutPrefixedMetadata(mspass::utility::Metadata &target,
                         const mspass::utility::Metadata &source,
                         const std::string &prefix);
std::string AntelopePfToText(const mspass::utility::AntelopePf &pf,
                             const int indent = 0);
std::vector<double> ThreeCAmplitudes(const mspass::utility::dmatrix &d);
double GroupSparseObjective(const mspass::seismic::CoreSeismogram &residual,
                            const std::list<ThreeCSpike> &spikes,
                            const double lambda);
void ValidateGIDLeafWindow(const mspass::utility::AntelopePf &mdleaf,
                           const mspass::algorithms::TimeWindow &fftwin,
                           const std::string &leaf_name,
                           const std::string &base_error);
void ValidateGIDLeafOperatorMetadata(
    const mspass::utility::Metadata &md,
    const mspass::algorithms::TimeWindow &fftwin, const double target_dt,
    const std::string &caller, const bool allow_noise_window_keys = false);
void ValidateExternalTimeSeriesSampleInterval(
    const mspass::seismic::TimeSeries &d, const double target_dt,
    const std::string &caller);
mspass::algorithms::TimeWindow ClipTimeWindowToSeries(
    const mspass::seismic::CoreTimeSeries &d,
    const mspass::algorithms::TimeWindow &requested,
    const std::string &caller);
std::vector<double> BuildGIDLagWeightPenaltyFunction(
    const mspass::utility::Metadata &md, const std::string &caller);
bool GIDLagWeightPenaltyUsesDynamicKernel(const std::string &penalty_type);
bool GIDLagWeightPenaltyUsesAdaptiveMemory(const std::string &penalty_type);
struct GIDAdaptivePenaltyMetrics {
  double confidence = 0.0;
  double immediate_strength = 0.0;
  double specificity = 0.0;
  double decay_factor = 0.0;
  double memory_linf = 0.0;
  double memory_l2 = 0.0;
  double noise_amplitude = 0.0;
  int effective_width = 1;
};
std::vector<double> BuildGIDLagWeightPenaltyFunctionFromKernel(
    const std::string &penalty_type, const double penalty_scale,
    const std::vector<double> &kernel, const int zero_lag_sample,
    const std::string &caller);
void ApplyGIDLagWeightPenalty(std::vector<double> &lag_weights,
                              const std::vector<double> &penalty,
                              const int center_col);
double EstimateThreeCColumnAmplitudeRMS(
    const mspass::seismic::CoreSeismogram &d);
GIDAdaptivePenaltyMetrics ApplyGIDAdaptiveMemoryPenalty(
    std::vector<double> &lag_weights, std::vector<double> &memory,
    std::vector<double> &retention, const std::vector<double> &kernel,
    const int zero_lag_sample, const int center_col,
    const double penalty_scale, const double candidate_amplitude,
    const double noise_amplitude, const std::string &caller);
double FIRSelfOverlap(const std::vector<double> &fir, const int col0_i,
                      const int col0_j, const int ncols);
double FIRDataOverlap(const std::vector<double> &fir,
                      const mspass::seismic::CoreSeismogram &target,
                      const int component, const int col0);
std::vector<double> SolveDenseSystem(const std::vector<std::vector<double>> &a,
                                     const std::vector<double> &b,
                                     const std::string &caller);
void RefitSpikeAmplitudes(std::list<ThreeCSpike> &spikes,
                          const mspass::seismic::CoreSeismogram &target,
                          const std::vector<double> &actual_o_fir,
                          const int actual_o_0,
                          const double ridge_beta = 1.0e-10);
GroupSparseDeconResult SolveGroupSparseDecon(
    const mspass::seismic::CoreSeismogram &target,
    const std::vector<double> &actual_o_fir, const int actual_o_0,
    const double lambda, const int max_iterations, const double tolerance,
    const double active_threshold, const double active_threshold_scale,
    const double active_threshold_quantile, const std::string &caller);
} // namespace mspass::algorithms::deconvolution
#endif
