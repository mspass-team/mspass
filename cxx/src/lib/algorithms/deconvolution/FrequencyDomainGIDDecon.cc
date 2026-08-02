#include "mspass/algorithms/deconvolution/FrequencyDomainGIDDecon.h"
#include "gsl/gsl_cblas.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
#include "mspass/algorithms/deconvolution/LeastSquareDecon.h"
#include "mspass/algorithms/deconvolution/MultiTaperXcorDecon.h"
#include "mspass/algorithms/deconvolution/NoiseStableDecon.h"
#include "mspass/algorithms/deconvolution/WaterLevelDecon.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <limits>
#include <sstream>

namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::algorithms;
using namespace mspass::seismic;
using namespace mspass::utility;

namespace {
double matrix_l2(dmatrix &d) {
  int nd = d.rows() * d.columns();
  return cblas_dnrm2(nd, d.get_address(0, 0), 1);
}

double matrix_linf(dmatrix &d) {
  double dmax(0.0);
  for (int i = 0; i < d.rows(); ++i) {
    for (int j = 0; j < d.columns(); ++j)
      dmax = max(dmax, fabs(d(i, j)));
  }
  return dmax;
}

bool residual_matrix_is_finite(dmatrix &d) {
  for (int i = 0; i < d.rows(); ++i)
    for (int j = 0; j < d.columns(); ++j)
      if (!isfinite(d(i, j)))
        return false;
  return true;
}

bool time_series_samples_are_finite(const TimeSeries &d) {
  return all_of(d.s.begin(), d.s.end(),
                [](const double sample) { return isfinite(sample); });
}

/* Evaluate ||r - A spike||_2 without copying or mutating the whole residual.
 * A single FIR spike affects only its clipped kernel support. */
double trial_residual_l2(const dmatrix &residual, const double current_l2,
                         const ThreeCSpike &spike,
                         const vector<double> &kernel, const int kernel_zero) {
  const int col0 = spike.col - kernel_zero;
  const int p_start = max(0, -col0);
  const int p_end = min(static_cast<int>(kernel.size()),
                        static_cast<int>(residual.columns()) - col0);
  if (!isfinite(current_l2) || current_l2 < 0.0)
    return numeric_limits<double>::quiet_NaN();
  long double correction(0.0L), correction_magnitude(0.0L);
  for (int k = 0; k < 3; ++k) {
    for (int p = p_start; p < p_end; ++p) {
      const long double predicted =
          static_cast<long double>(spike.u[k]) * kernel[p];
      const long double old_value = residual(k, col0 + p);
      if (!isfinite(static_cast<double>(predicted)) ||
          !isfinite(static_cast<double>(old_value)))
        return numeric_limits<double>::quiet_NaN();
      const long double term = predicted * predicted - 2.0L * old_value * predicted;
      correction += term;
      correction_magnitude += fabsl(predicted * predicted) +
                              fabsl(2.0L * old_value * predicted);
    }
  }
  const long double baseline = static_cast<long double>(current_l2) * current_l2;
  const long double squared = baseline + correction;
  if (!isfinite(static_cast<double>(squared)))
    return numeric_limits<double>::quiet_NaN();
  const long double roundoff = 64.0L * numeric_limits<double>::epsilon() *
                               max(1.0L, fabsl(baseline) + correction_magnitude);
  if (squared < -roundoff)
    return numeric_limits<double>::quiet_NaN();
  return sqrt(static_cast<double>(max(0.0L, squared)));
}

double inverse_wavelet_noise_rms(const CoreTimeSeries &winv,
                                 const CoreSeismogram &noise,
                                 const double inverse_domain_scale) {
  if (noise.dead() || noise.npts() <= 0 || winv.npts() <= 0)
    return 0.0;
  CoreSeismogram nwork(sparse_convolve(winv, noise));
  TimeWindow trimwin;
  trimwin.start = nwork.t0() + nwork.dt() * static_cast<double>(winv.npts());
  trimwin.end = nwork.endtime() - nwork.dt() * static_cast<double>(winv.npts());
  if (trimwin.end > trimwin.start)
    nwork = WindowData(nwork, trimwin);
  return inverse_domain_scale * EstimateThreeCColumnAmplitudeRMS(nwork);
}

double cnr_processed_noise_rms(CNRDeconEngine &op,
                               const TimeSeries &noise_wavelet,
                               const CoreSeismogram &noise,
                               const int transient_npts) {
  if (noise.dead() || noise.npts() <= 0)
    return 0.0;
  PowerSpectrum psnoise(op.compute_noise_spectrum(noise_wavelet));
  Seismogram nwork(noise);
  nwork = op.process(nwork, psnoise, 0.02, 2.0);
  TimeWindow trimwin;
  trimwin.start =
      nwork.t0() + nwork.dt() * static_cast<double>(transient_npts);
  trimwin.end =
      nwork.endtime() - nwork.dt() * static_cast<double>(transient_npts);
  if (trimwin.end > trimwin.start)
    nwork = WindowData(nwork, trimwin);
  return EstimateThreeCColumnAmplitudeRMS(nwork);
}

} // namespace

FrequencyDomainGIDDecon::FrequencyDomainGIDDecon(const AntelopePf &mdtoplevel)
    : ScalarDecon(), preprocessor(nullptr), cnrprocessor(nullptr) {
  const string base_error("FrequencyDomainGIDDecon constructor: ");
  try {
    config_pf_text = AntelopePfToText(mdtoplevel);
    AntelopePf md = mdtoplevel.get_branch("deconvolution_operator_type");
    AntelopePf mdgid = md.get_branch("frequency_domain_gid_deconvolution");
    decon_type = ParseGIDDeconType(mdgid, "FrequencyDomainGIDDecon");
    dwin = TimeWindow(GetDoubleRequired(mdgid, "full_data_window_start"),
                      GetDoubleRequired(mdgid, "full_data_window_end"));
    outputwin = dwin;
    fftwin =
        TimeWindow(GetDoubleRequired(mdgid, "deconvolution_data_window_start"),
                   GetDoubleRequired(mdgid, "deconvolution_data_window_end"));
    if (mdgid.is_defined("wavelet_window_start") ||
        mdgid.is_defined("wavelet_window_end")) {
      waveletwin = TimeWindow(GetDoubleRequired(mdgid, "wavelet_window_start"),
                              GetDoubleRequired(mdgid, "wavelet_window_end"));
    } else {
      /* Preserve legacy files that used the analysis window as the wavelet. */
      waveletwin = fftwin;
    }
    nwin = TimeWindow(GetDoubleRequired(mdgid, "noise_window_start"),
                      GetDoubleRequired(mdgid, "noise_window_end"));
    ValidateWindowDuration(dwin, "full_data_window", base_error);
    ValidateWindowDuration(fftwin, "deconvolution_data_window", base_error);
    ValidateWindowDuration(waveletwin, "wavelet_window", base_error);
    ValidateWindowDuration(nwin, "noise_window", base_error);
    if (fftwin.start < dwin.start || fftwin.end > dwin.end)
      throw MsPASSError(base_error +
                            "deconvolution window must be inside full window",
                        ErrorSeverity::Invalid);
    noise_component = GetIntRequired(mdgid, "noise_component");
    ValidateThreeComponentIndex(noise_component, "noise_component", base_error);
    target_dt = GetDoubleRequired(mdgid, "target_sample_interval");
    ValidatePositive(target_dt, "target_sample_interval", base_error);
    if (mdgid.is_defined("shaping_wavelet_dt")) {
      const double shaping_dt = GetDoubleRequired(mdgid, "shaping_wavelet_dt");
      if (fabs(shaping_dt - target_dt) >
          1.0e-6 * max(1.0, max(fabs(shaping_dt), fabs(target_dt))))
        throw MsPASSError(base_error +
                              "shaping_wavelet_dt must match "
                              "target_sample_interval",
                          ErrorSeverity::Fatal);
    }
    const int analysis_npts = CheckedGIDWindowSampleCount(
        fftwin, target_dt, base_error + "deconvolution_data_window");
    const int wavelet_npts = CheckedGIDWindowSampleCount(
        waveletwin, target_dt, base_error + "wavelet_window");
    const int noise_npts = CheckedGIDWindowSampleCount(
        nwin, target_dt, base_error + "noise_window");
    const int nfft = CheckedGIDLinearConvolutionNFFT(
        analysis_npts, wavelet_npts, noise_npts,
        decon_type == NS_GID || decon_type == GROUP_SPARSE, base_error);
    inverse_operator_nfft = nfft;
    const int shaping_nfft = nextPowerOf2(analysis_npts);
    mdgid.put("operator_nfft", shaping_nfft);
    this->ScalarDecon::changeparameter(mdgid);
    this->shapingwavelet = ShapingWavelet(mdgid, shaping_nfft);
    iter_max = GetIntRequired(mdgid, "maximum_iterations");
    ValidatePositiveInteger(iter_max, "maximum_iterations", base_error);
    residual_ratio_floor = GetDoubleRequired(mdgid, "residual_ratio_floor");
    ValidateNonnegative(residual_ratio_floor, "residual_ratio_floor",
                        base_error);
    residual_improvement_floor =
        GetDoubleRequired(mdgid, "residual_fractional_improvement_floor");
    ValidateNonnegative(residual_improvement_floor,
                        "residual_fractional_improvement_floor", base_error);
    lag_weight_penalty_function =
        mdgid.is_defined("lag_weight_penalty_function")
            ? mdgid.get_string("lag_weight_penalty_function")
            : "none";
    lag_weight_penalty_scale_factor =
        mdgid.is_defined("lag_weight_penalty_scale_factor")
            ? GetDoubleRequired(mdgid, "lag_weight_penalty_scale_factor")
            : 1.0;
    if (!isfinite(lag_weight_penalty_scale_factor) ||
        lag_weight_penalty_scale_factor <= 0.0 ||
        lag_weight_penalty_scale_factor > 1.0)
      throw MsPASSError(base_error +
                            "lag_weight_penalty_scale_factor must be in (0, 1]",
                        ErrorSeverity::Fatal);
    lag_weight_function_width =
        mdgid.is_defined("lag_weight_function_width")
            ? GetIntRequired(mdgid, "lag_weight_function_width")
            : 0;
    if (mdgid.is_defined("lag_weight_function_width"))
      ValidatePositiveInteger(lag_weight_function_width,
                              "lag_weight_function_width", base_error);
    if (lag_weight_penalty_function == "none") {
      lag_weight_penalty = vector<double>{1.0};
    } else if (lag_weight_penalty_function == "shaping_wavelet") {
      CoreTimeSeries shaping(this->output_shaping_wavelet());
      lag_weight_penalty = BuildGIDLagWeightPenaltyFunctionFromKernel(
          lag_weight_penalty_function, lag_weight_penalty_scale_factor,
          shaping.s, shaping.sample_number(0.0), "FrequencyDomainGIDDecon");
    } else if (lag_weight_penalty_function == "resolution_kernel" ||
               GIDLagWeightPenaltyUsesAdaptiveMemory(
                   lag_weight_penalty_function)) {
      lag_weight_penalty = vector<double>{1.0};
    } else {
      lag_weight_penalty =
          BuildGIDLagWeightPenaltyFunction(mdgid, "FrequencyDomainGIDDecon");
    }

    AntelopePf mdleaf;
    switch (decon_type) {
    case WATER_LEVEL:
      mdleaf = md.get_branch("water_level");
      ValidateGIDLeafWindow(mdleaf, fftwin, "water level", base_error);
      ValidateGIDLeafOperatorMetadata(mdleaf, fftwin, target_dt, base_error);
      mdleaf.put("operator_nfft", nfft);
      preprocessor = std::make_unique<WaterLevelDecon>(mdleaf);
      break;
    case LEAST_SQ:
      mdleaf = md.get_branch("least_square");
      ValidateGIDLeafWindow(mdleaf, fftwin, "least square", base_error);
      ValidateGIDLeafOperatorMetadata(mdleaf, fftwin, target_dt, base_error);
      mdleaf.put("operator_nfft", nfft);
      preprocessor = std::make_unique<LeastSquareDecon>(mdleaf);
      break;
    case MULTI_TAPER:
      mdleaf = md.get_branch("multi_taper");
      ValidateGIDLeafWindow(mdleaf, fftwin, "multi taper", base_error);
      ValidateGIDLeafOperatorMetadata(mdleaf, fftwin, target_dt, base_error);
      mdleaf.put("operator_nfft", nfft);
      preprocessor = std::make_unique<MultiTaperXcorDecon>(mdleaf);
      break;
    case CNR:
      mdleaf = md.get_branch("cnr");
      ValidateGIDLeafWindow(mdleaf, fftwin, "CNR", base_error);
      ValidateGIDLeafOperatorMetadata(mdleaf, fftwin, target_dt, base_error,
                                      true);
      mdleaf.put("operator_nfft", nfft);
      cnrprocessor = std::make_unique<CNRDeconEngine>(mdleaf);
      break;
    case GROUP_SPARSE:
      mdleaf = md.get_branch("ns_gid");
      ValidateGIDLeafWindow(mdleaf, fftwin, "group sparse NS-GID inverse",
                            base_error);
      ValidateGIDLeafOperatorMetadata(mdleaf, fftwin, target_dt, base_error);
      mdleaf.put("operator_nfft", nfft);
      preprocessor = std::make_unique<NoiseStableDecon>(mdleaf);
      break;
    case NS_GID:
    default:
      mdleaf = md.get_branch("ns_gid");
      ValidateGIDLeafWindow(mdleaf, fftwin, "NS-GID", base_error);
      ValidateGIDLeafOperatorMetadata(mdleaf, fftwin, target_dt, base_error);
      mdleaf.put("operator_nfft", nfft);
      preprocessor = std::make_unique<NoiseStableDecon>(mdleaf);
      break;
    };
    changed_leaf_metadata = Metadata(mdleaf);
    leaf_operator_metadata = Metadata(mdleaf);
    external_wavelet_loaded = false;
    external_noise_loaded = false;
    external_noise_spectrum_loaded = false;
    residual_noise_from_external = false;
    leaf_parameters_changed = false;
    external_wavelet_allowed = GetBoolDefault(
        mdgid, "ns_gid_external_wavelet_allowed", true);
    // This multiplier applies to the robust scalar-component sigma RMS, not
    // the 3C vector-amplitude RMS used by the empirical threshold.
    ns_peak_sigma_threshold =
        GetDoubleDefault(mdgid, "ns_gid_peak_sigma_threshold", 3.0);
    ValidatePositive(ns_peak_sigma_threshold, "ns_gid_peak_sigma_threshold",
                     base_error);
    ns_peak_probability_threshold = GetDoubleDefault(
        mdgid, "ns_gid_peak_probability_threshold", 0.995);
    ValidateProbability(ns_peak_probability_threshold,
                        "ns_gid_peak_probability_threshold", base_error);
    ns_use_empirical_noise_threshold = GetBoolDefault(
        mdgid, "ns_gid_use_empirical_noise_threshold", true);
    ns_residual_noise_ratio_floor = GetDoubleDefault(
        mdgid, "ns_gid_residual_noise_ratio_floor", 1.0);
    ValidateNonnegative(ns_residual_noise_ratio_floor,
                        "ns_gid_residual_noise_ratio_floor", base_error);
    ns_max_spikes = GetIntDefault(mdgid, "ns_gid_max_spikes", 0);
    if (ns_max_spikes < 0)
      throw MsPASSError(base_error + "ns_gid_max_spikes must be nonnegative",
                        ErrorSeverity::Fatal);
    ns_refit_interval = GetIntDefault(mdgid, "ns_gid_refit_interval", 5);
    if (ns_refit_interval < 1)
      throw MsPASSError(base_error + "ns_gid_refit_interval must be positive",
                        ErrorSeverity::Fatal);
    ns_ridge_beta =
        GetDoubleDefault(mdgid, "ns_gid_ridge_beta", 1.0e-10);
    ValidateNonnegative(ns_ridge_beta, "ns_gid_ridge_beta", base_error);
    group_sparse_lambda = GetDoubleDefault(mdgid, "group_sparse_lambda", 0.0);
    ValidateNonnegative(group_sparse_lambda, "group_sparse_lambda",
                        base_error);
    group_sparse_lambda_scale =
        GetDoubleDefault(mdgid, "group_sparse_lambda_scale", 1.0);
    ValidateNonnegative(group_sparse_lambda_scale,
                        "group_sparse_lambda_scale", base_error);
    group_sparse_tolerance =
        GetDoubleDefault(mdgid, "group_sparse_tolerance", 1.0e-4);
    ValidatePositive(group_sparse_tolerance, "group_sparse_tolerance",
                     base_error);
    group_sparse_max_iterations =
        GetIntDefault(mdgid, "group_sparse_max_iterations", iter_max);
    ValidatePositiveInteger(group_sparse_max_iterations,
                            "group_sparse_max_iterations", base_error);
    group_sparse_active_threshold =
        GetDoubleDefault(mdgid, "group_sparse_active_threshold", 2.0e-2);
    ValidateNonnegative(group_sparse_active_threshold,
                        "group_sparse_active_threshold", base_error);
    group_sparse_active_threshold_scale =
        GetDoubleDefault(mdgid, "group_sparse_active_threshold_scale", 1.0);
    ValidateNonnegative(group_sparse_active_threshold_scale,
                        "group_sparse_active_threshold_scale", base_error);
    group_sparse_active_threshold_quantile =
        GetDoubleDefault(mdgid, "group_sparse_active_threshold_quantile", 0.90);
    ValidateProbability(group_sparse_active_threshold_quantile,
                        "group_sparse_active_threshold_quantile", base_error);
    this->invalidate_processing_state();
  } catch (...) {
    throw;
  };
}

FrequencyDomainGIDDecon::~FrequencyDomainGIDDecon() {}

void FrequencyDomainGIDDecon::invalidate_processing_state() {
  result.clear();
  spikes.clear();
  actual_o_fir.clear();
  lag_weights.clear();
  adaptive_penalty_memory.clear();
  adaptive_penalty_retention.clear();
  iter_count = 0;
  resid_l2_initial = 0.0;
  resid_l2_prev = 0.0;
  resid_l2_final = 0.0;
  resid_linf_initial = 0.0;
  resid_linf_final = 0.0;
  lag_weight_linf_final = 0.0;
  lag_weight_l2_final = 0.0;
  actual_o_0 = 0;
  gid_leaf_raw_zero_lag_gain = 0.0;
  gid_inverse_domain_amplitude_scale = 0.0;
  adaptive_penalty_last_confidence = 0.0;
  adaptive_penalty_last_immediate_strength = 0.0;
  adaptive_penalty_last_specificity = 0.0;
  adaptive_penalty_last_decay_factor = 0.0;
  adaptive_penalty_noise_amplitude = 0.0;
  adaptive_penalty_memory_linf = 0.0;
  adaptive_penalty_memory_l2 = 0.0;
  ns_fractional_improvement_final = 0.0;
  ns_fractional_improvement_state_final = 0.0;
  ns_final_refit_applied = false;
  ns_refit_epochs = 0;
  ns_refit_resume_count = 0;
  ns_last_peak_significance = 0.0;
  ns_peak_threshold = 0.0;
  ns_noise_l2 = 0.0;
  ns_noise_amplitude_rms = 0.0;
  ns_noise_component_sigma_rms = 0.0;
  ns_noise_component_sigma_rms_robust = 0.0;
  ns_noise_component_rms_aggregate = 0.0;
  ns_noise_component_sigma_rms_fallback_used = false;
  ns_residual_rms_initial = 0.0;
  ns_residual_rms_final = 0.0;
  ns_peak_threshold_empirical = 0.0;
  ns_peak_threshold_sigma = 0.0;
  ns_noise_amplitude_robust = 0.0;
  ns_last_candidate_amplitude = 0.0;
  ns_noise_samples_at_or_above_peak_threshold = 0;
  ns_noise_amplitude_sample_count = 0;
  ns_initial_stationary_null_search_lag_count = 0;
  ns_initial_stationary_null_expected_noise_exceedances = 0.0;
  ns_last_selected_candidate_lag = -1;
  ns_last_selected_candidate_lag_weight = 0.0;
  ns_last_selected_candidate_weighted_amplitude = 0.0;
  ns_max_raw_candidate_amplitude = 0.0;
  ns_max_raw_candidate_significance = 0.0;
  ns_max_raw_candidate_lag = -1;
  ns_last_scan_raw_significant_candidate_remaining = false;
  ns_final_scan_max_raw_candidate_amplitude = 0.0;
  ns_final_scan_max_raw_candidate_significance = 0.0;
  ns_final_scan_max_raw_candidate_lag = -1;
  ns_final_scan_raw_significant_candidate_remaining = false;
  ns_final_scan_existing_support_max_raw_amplitude = 0.0;
  ns_final_scan_existing_support_max_raw_significance = 0.0;
  ns_final_scan_existing_support_max_raw_lag = -1;
  ns_final_scan_significant_candidate_count = 0;
  ns_final_scan_best_trial_lag = -1;
  ns_final_scan_best_trial_residual_l2 = 0.0;
  ns_final_scan_best_trial_fractional_improvement = 0.0;
  ns_final_scan_decision_candidate_lag = -1;
  ns_final_scan_global_acceptable_candidate_count = 0;
  ns_final_scan_decision_trial_residual_l2 = 0.0;
  ns_final_scan_decision_trial_fractional_improvement = 0.0;
  ns_final_scan_decision = "not_evaluated";
  ns_final_scan_acceptable_candidate_remaining = false;
  ns_noise_component_rms.clear();
  ns_candidate_lag_history.clear();
  ns_candidate_accepted_history.clear();
  ns_candidate_lag_time_history.clear();
  ns_candidate_amplitude_history.clear();
  ns_candidate_threshold_history.clear();
  ns_candidate_significance_history.clear();
  ns_candidate_post_residual_rms_ratio_history.clear();
  ns_candidate_residual_l2_before_history.clear();
  ns_candidate_trial_residual_l2_history.clear();
  ns_candidate_post_refit_residual_l2_history.clear();
  ns_candidate_fractional_improvement_history.clear();
  ns_candidate_state_fractional_improvement_history.clear();
  ns_candidate_periodic_refit_applied_history.clear();
  ns_candidate_final_refit_applied_history.clear();
  ns_candidate_trial_evaluated_history.clear();
  ns_candidate_metric_available_history.clear();
  ns_candidate_stop_history.clear();
  legacy_eq15_candidates_tested = 0;
  legacy_eq15_candidates_rejected = 0;
  legacy_eq15_post_acceptance_state_tests = 0;
  legacy_eq15_post_acceptance_floor_stops = 0;
  legacy_eq15_candidates_below_floor = 0;
  legacy_eq15_candidates_non_decreasing = 0;
  legacy_eq15_candidates_nonfinite = 0;
  legacy_eq15_rejected_lag_samples_truncated = 0;
  legacy_eq15_rejected_iteration_samples_truncated = 0;
  legacy_eq15_last_trial_fractional_improvement = 0.0;
  legacy_eq15_stop_detail.clear();
  legacy_eq15_rejected_lag_times.clear();
  legacy_eq15_rejected_candidates_per_iteration.clear();
  gid_analysis_samples = 0;
  gid_wavelet_samples = 0;
  gid_alignment_offset_samples = 0;
  gid_analysis_t0 = 0.0;
  gid_wavelet_t0 = 0.0;
  ns_converged = false;
  ns_stop_reason = "not_started";
  ns_provisional_stop_reason_before_final_refit = "not_started";
  gid_converged = false;
  gid_stop_reason = "not_started";
  group_sparse_lambda_used = 0.0;
  group_sparse_objective_initial = 0.0;
  group_sparse_objective_final = 0.0;
  group_sparse_fractional_improvement_final = 0.0;
  group_sparse_debiased_objective_final = 0.0;
  group_sparse_debiased_fractional_improvement_final = 0.0;
  group_sparse_refit_gram_condition_number = 0.0;
  group_sparse_refit_relative_ridge_beta = 0.0;
  group_sparse_refit_residual_l2_pre = 0.0;
  group_sparse_refit_residual_l2_post = 0.0;
  group_sparse_refit_maximum_amplitude_pre = 0.0;
  group_sparse_refit_maximum_amplitude_post = 0.0;
  group_sparse_refit_condition_guard_applied = false;
  group_sparse_refit_fallback_to_pre_debias = false;
  group_sparse_refit_fallback_reason = "not_run";
  group_sparse_active_threshold_quantile_value = 0.0;
  group_sparse_active_threshold_used = 0.0;
  group_sparse_iterations = 0;
  group_sparse_active_groups = 0;
  group_sparse_converged = false;
  gid_noise_samples_loaded = 0;
  gid_noise_samples_used = 0;
  gid_noise_truncated = false;
  processed = false;
}

void FrequencyDomainGIDDecon::changeparameter(const Metadata &md) {
  const bool cnr_mode(decon_type == CNR);
  ValidateGIDLeafOperatorMetadata(
      md, fftwin, target_dt, "FrequencyDomainGIDDecon::changeparameter",
      cnr_mode);
  this->invalidate_processing_state();
  if (cnr_mode)
    cnrprocessor->changeparameter(md);
  else
    preprocessor->changeparameter(md);
  changed_leaf_metadata = Metadata(md);
  leaf_operator_metadata = Metadata(md);
  leaf_parameters_changed = true;
}

int FrequencyDomainGIDDecon::actual_inverse_operator_size() const {
  if (decon_type == CNR)
    return cnrprocessor->operator_size();
  auto *fft = dynamic_cast<FFTDeconOperator *>(preprocessor.get());
  return fft ? fft->operator_size() : inverse_operator_nfft;
}

void FrequencyDomainGIDDecon::ensure_inverse_operator_size(
    const int data_npts, const int wavelet_npts, const int noise_npts) {
  const int needed = CheckedGIDLinearConvolutionNFFT(
      data_npts, wavelet_npts, noise_npts,
      decon_type == NS_GID || decon_type == GROUP_SPARSE,
      "FrequencyDomainGIDDecon::ensure_inverse_operator_size");
  if (needed <= this->actual_inverse_operator_size()) {
    inverse_operator_nfft = this->actual_inverse_operator_size();
    return;
  }
  Metadata md(leaf_operator_metadata);
  md.put("operator_nfft", needed);
  if (decon_type == CNR)
    cnrprocessor->changeparameter(md);
  else
    preprocessor->changeparameter(md);
  leaf_operator_metadata = md;
  inverse_operator_nfft = this->actual_inverse_operator_size();
}

int FrequencyDomainGIDDecon::load(const CoreSeismogram &draw,
                                  TimeWindow dwin_in) {
  this->invalidate_processing_state();
  d_all.kill();
  ndwin = 0;
  ValidateWindowDuration(dwin_in, "signal_window",
                         "FrequencyDomainGIDDecon::load");
  if (!isfinite(draw.dt()) ||
      fabs(draw.dt() - target_dt) > 1.0e-8 * max(1.0, target_dt))
    throw MsPASSError("FrequencyDomainGIDDecon::load: input sample interval "
                      "does not match target_sample_interval",
                      ErrorSeverity::Invalid);
  if ((dwin_in.start > fftwin.start) || (dwin_in.end < fftwin.end) ||
      ((!external_wavelet_loaded) &&
       ((dwin_in.start > waveletwin.start) ||
        (dwin_in.end < waveletwin.end))) ||
      (dwin_in.start > outputwin.start) || (dwin_in.end < outputwin.end))
    return 1;
  dwin = dwin_in;
  d_all = WindowData(draw, dwin);
  if (d_all.dead() || d_all.npts() <= 0)
    return 1;
  ndwin = d_all.npts();
  return 0;
}

int FrequencyDomainGIDDecon::loadnoise(const CoreSeismogram &draw,
                                       TimeWindow nwin_in) {
  this->invalidate_processing_state();
  n.kill();
  nnwin = 0;
  residual_noise_from_external = false;
  ValidateWindowDuration(nwin_in, "noise_window",
                         "FrequencyDomainGIDDecon::loadnoise");
  if (!isfinite(draw.dt()) ||
      fabs(draw.dt() - target_dt) > 1.0e-8 * max(1.0, target_dt))
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: input sample "
                      "interval does not match target_sample_interval",
                      ErrorSeverity::Invalid);
  nwin = nwin_in;
  CoreSeismogram candidate_noise(WindowData(draw, nwin));
  if (candidate_noise.dead() || candidate_noise.npts() <= 0)
    return 1;
  if (!residual_matrix_is_finite(candidate_noise.u))
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: noise window "
                      "contains nonfinite samples",
                      ErrorSeverity::Invalid);
  n = candidate_noise;
  nnwin = n.npts();
  return 0;
}

int FrequencyDomainGIDDecon::loadwavelet(const TimeSeries &wavelet) {
  if (!external_wavelet_allowed)
    throw MsPASSError("FrequencyDomainGIDDecon::loadwavelet: external "
                      "wavelets are disabled by "
                      "ns_gid_external_wavelet_allowed",
                      ErrorSeverity::Invalid);
  if (wavelet.dead())
    throw MsPASSError("FrequencyDomainGIDDecon::loadwavelet: external wavelet "
                      "is marked dead",
                      ErrorSeverity::Invalid);
  if (wavelet.npts() <= 0)
    throw MsPASSError("FrequencyDomainGIDDecon::loadwavelet: external wavelet "
                      "is empty",
                      ErrorSeverity::Invalid);
  ValidateExternalTimeSeriesSampleInterval(
      wavelet, target_dt, "FrequencyDomainGIDDecon::loadwavelet");
  this->invalidate_processing_state();
  external_wavelet_loaded = false;
  external_wavelet = wavelet;
  external_wavelet_loaded = true;
  return 0;
}

int FrequencyDomainGIDDecon::loadwavelet(const CoreTimeSeries &wavelet) {
  TimeSeries ts(wavelet, "FrequencyDomainGIDDecon");
  return this->loadwavelet(ts);
}

int FrequencyDomainGIDDecon::loadnoise(const TimeSeries &noise_in) {
  this->invalidate_processing_state();
  const auto clear_rejected_noise_state = [this]() {
    n.kill();
    nnwin = 0;
    residual_noise_from_external = false;
    external_noise_loaded = false;
    external_noise_spectrum_loaded = false;
    external_noise = TimeSeries();
    external_noise_spectrum = PowerSpectrum();
  };
  if (noise_in.dead())
  {
    clear_rejected_noise_state();
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external noise is "
                      "marked dead",
                      ErrorSeverity::Invalid);
  }
  if (noise_in.npts() <= 0)
  {
    clear_rejected_noise_state();
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external noise is "
                      "empty",
                      ErrorSeverity::Invalid);
  }
  if (!time_series_samples_are_finite(noise_in))
  {
    clear_rejected_noise_state();
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external noise "
                      "contains nonfinite samples",
                      ErrorSeverity::Invalid);
  }
  try {
    ValidateExternalTimeSeriesSampleInterval(
        noise_in, target_dt, "FrequencyDomainGIDDecon::loadnoise");
  } catch (...) {
    clear_rejected_noise_state();
    throw;
  }
  const bool keep_residual_noise =
      n.live() && n.npts() > 0 && !residual_noise_from_external;
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  external_noise_spectrum = PowerSpectrum();
  external_noise = noise_in;
  external_noise_loaded = true;
  external_noise_spectrum_loaded = false;
  if (!keep_residual_noise) {
    n = CoreSeismogram(noise_in.npts());
    n.set_t0(noise_in.t0());
    n.set_dt(noise_in.dt());
    n.set_live();
    n.set_tref(noise_in.timetype());
    for (int k = 0; k < 3; ++k)
      cblas_dcopy(noise_in.npts(), &(noise_in.s[0]), 1, n.u.get_address(k, 0),
                  3);
    nnwin = n.npts();
    residual_noise_from_external = true;
  }
  return 0;
}

int FrequencyDomainGIDDecon::loadnoise(const CoreTimeSeries &noise_in) {
  TimeSeries ts(noise_in, "FrequencyDomainGIDDecon");
  return this->loadnoise(ts);
}

int FrequencyDomainGIDDecon::loadnoise(const PowerSpectrum &noise_spectrum_in) {
  this->invalidate_processing_state();
  const auto clear_rejected_noise_state = [this]() {
    n.kill();
    nnwin = 0;
    residual_noise_from_external = false;
    external_noise_loaded = false;
    external_noise_spectrum_loaded = false;
    external_noise = TimeSeries();
    external_noise_spectrum = PowerSpectrum();
  };
  if (decon_type != NS_GID && decon_type != GROUP_SPARSE)
  {
    clear_rejected_noise_state();
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external "
                      "PowerSpectrum noise is only supported for ns_gid and "
                      "group_sparse; pass a TimeSeries noise estimate for "
                      "multi_taper or use the configured noise window for "
                      "other GID modes",
                      ErrorSeverity::Invalid);
  }
  try {
    ValidatePowerSpectrumCoversDC(noise_spectrum_in,
                                  "FrequencyDomainGIDDecon::loadnoise");
  } catch (...) {
    clear_rejected_noise_state();
    throw;
  }
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  external_noise = TimeSeries();
  external_noise_spectrum = noise_spectrum_in;
  external_noise_spectrum_loaded = true;
  external_noise_loaded = false;
  if (residual_noise_from_external) {
    n.kill();
    nnwin = 0;
    residual_noise_from_external = false;
  }
  return 0;
}
void FrequencyDomainGIDDecon::clear_external_wavelet() {
  external_wavelet_loaded = false;
  external_wavelet = TimeSeries();
  this->invalidate_processing_state();
}
void FrequencyDomainGIDDecon::clear_external_noise() {
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  external_noise = TimeSeries();
  external_noise_spectrum = PowerSpectrum();
  if (residual_noise_from_external) {
    n.kill();
    nnwin = 0;
    residual_noise_from_external = false;
  }
  this->invalidate_processing_state();
}

int FrequencyDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                                  TimeWindow nwin) {
  this->invalidate_processing_state();
  d_all.kill();
  n.kill();
  ndwin = 0;
  nnwin = 0;
  ValidateWindowDuration(dwin, "signal_window",
                         "FrequencyDomainGIDDecon::load");
  ValidateWindowDuration(nwin, "noise_window",
                         "FrequencyDomainGIDDecon::load");
  if ((dwin.start > fftwin.start) || (dwin.end < fftwin.end) ||
      ((!external_wavelet_loaded) &&
       ((dwin.start > waveletwin.start) || (dwin.end < waveletwin.end))) ||
      (dwin.start > outputwin.start) || (dwin.end < outputwin.end))
    return 1;
  int iretn = this->loadnoise(draw, nwin);
  int iretd = this->load(draw, dwin);
  return iretn + iretd;
}

void FrequencyDomainGIDDecon::initialize_inverse_operator() {
  d_decon = WindowData(d_all, fftwin);
  dmatrix uwork(d_decon.u);
  uwork.zero();
  CoreTimeSeries srcwavelet;
  if (external_wavelet_loaded) {
    ValidateExternalTimeSeriesTimeReference(
        external_wavelet, d_decon.timetype(),
        "FrequencyDomainGIDDecon::initialize_inverse_operator");
    srcwavelet = CoreTimeSeries(external_wavelet);
  } else {
    CoreTimeSeries source_component(ExtractComponent(d_all, 2));
    srcwavelet = WindowData(source_component, waveletwin);
    if (srcwavelet.dead() || srcwavelet.npts() <= 0)
      throw MsPASSError("FrequencyDomainGIDDecon::initialize_inverse_operator: "
                        "input data do not contain the requested wavelet window",
                        ErrorSeverity::Invalid);
  }
  if (external_noise_loaded)
    ValidateExternalTimeSeriesTimeReference(
        external_noise, d_decon.timetype(),
        "FrequencyDomainGIDDecon::initialize_inverse_operator");
  /* Scalar leaf operators consume vectors without a time reference.  Build a
   * common physical grid that contains the full source and analysis windows. */
  const double analysis_t0 = d_decon.t0();
  const auto leaf_grid = BuildGIDCommonTimeGrid(
      d_decon, srcwavelet,
      "FrequencyDomainGIDDecon::initialize_inverse_operator");
  const int analysis_npts = static_cast<int>(d_decon.npts());
  CoreSeismogram leaf_data(leaf_grid.npts);
  leaf_data.set_t0(leaf_grid.t0);
  leaf_data.set_dt(d_decon.dt());
  leaf_data.set_tref(d_decon.timetype());
  leaf_data.set_live();
  leaf_data.u.zero();
  const int data_offset = leaf_grid.analysis_offset;
  for (int k = 0; k < 3; ++k)
    cblas_dcopy(d_decon.npts(), d_decon.u.get_address(k, 0), 3,
                leaf_data.u.get_address(k, data_offset), 3);
  CoreTimeSeries leafwavelet(leaf_data.npts());
  leafwavelet.s.assign(leaf_data.npts(), 0.0);
  leafwavelet.set_t0(leaf_data.t0());
  leafwavelet.set_dt(leaf_data.dt());
  leafwavelet.set_tref(leaf_data.timetype());
  leafwavelet.set_live();
  const int leafwavelet_start = leaf_grid.wavelet_offset;
  copy(srcwavelet.s.begin(), srcwavelet.s.end(),
       leafwavelet.s.begin() + leafwavelet_start);
  current_wavelet = TimeSeries(leafwavelet, "FrequencyDomainGIDDecon");
  gid_analysis_samples = analysis_npts;
  gid_wavelet_samples = srcwavelet.npts();
  gid_alignment_offset_samples =
      static_cast<int>(round((srcwavelet.t0() - analysis_t0) / d_decon.dt()));
  gid_analysis_t0 = analysis_t0;
  gid_wavelet_t0 = srcwavelet.t0();
  if (decon_type != CNR) {
    Metadata md(leaf_operator_metadata);
    md.put("deconvolution_data_window_start", leaf_data.t0());
    md.put("deconvolution_data_window_end", leaf_data.endtime());
    preprocessor->changeparameter(md);
    leaf_operator_metadata = md;
  }
  int runtime_noise_npts = n.npts();
  if (external_noise_loaded)
    runtime_noise_npts = max(runtime_noise_npts,
                             static_cast<int>(external_noise.npts()));
  this->ensure_inverse_operator_size(leaf_data.npts(), leafwavelet.npts(),
                                     runtime_noise_npts);
  double raw_zero_lag_gain = 0.0;
  const int leaf_zero_lag_index = leafwavelet.sample_number(0.0);
  if (decon_type == CNR) {
    TimeSeries nwavelet;
    if (external_noise_loaded)
      nwavelet = TimeSeries(external_noise);
    else
      nwavelet =
          TimeSeries(ExtractComponent(n, noise_component),
                     "FrequencyDomainGIDDecon");
    cnrprocessor->initialize_inverse_operator(current_wavelet, nwavelet);
    PowerSpectrum psnoise(cnrprocessor->compute_noise_spectrum(nwavelet));
    /* actual_output() intentionally normalizes its result for its public
     * diagnostic contract.  Probe the same source-through-inverse response
     * before that normalization so the GID residual and its unit-peak kernel
     * remain in one amplitude domain. */
    Seismogram response_input(leaf_data);
    for (int k = 0; k < 3; ++k)
      for (int j = 0; j < response_input.npts(); ++j)
        response_input.u(k, j) = leafwavelet.s[j];
    Seismogram raw_response(
        cnrprocessor->process(response_input, psnoise, 0.02, 2.0));
    const int response_zero_lag_index = raw_response.sample_number(0.0);
    if (response_zero_lag_index < 0 ||
        response_zero_lag_index >= raw_response.npts())
      throw MsPASSError("FrequencyDomainGIDDecon: CNR raw actual output "
                        "does not contain zero lag",
                        ErrorSeverity::Invalid);
    raw_zero_lag_gain = fabs(raw_response.u(0, response_zero_lag_index));
    Seismogram dwork(leaf_data);
    dwork = cnrprocessor->process(dwork, psnoise, 0.02, 2.0);
    for (int k = 0; k < 3; ++k)
      cblas_dcopy(d_decon.npts(), dwork.u.get_address(k, data_offset), 3,
                  uwork.get_address(k, 0), 3);
  } else {
    if (decon_type == MULTI_TAPER) {
      MultiTaperXcorDecon *mtop =
          dynamic_cast<MultiTaperXcorDecon *>(preprocessor.get());
      vector<double> mt_noise;
      if (external_noise_loaded) {
        mt_noise = external_noise.s;
      } else {
        CoreTimeSeries nts(ExtractComponent(n, noise_component));
        mt_noise = nts.s;
      }
      if (mt_noise.size() > static_cast<size_t>(mtop->get_taperlen()))
        mt_noise.resize(mtop->get_taperlen());
      mtop->loadnoise(mt_noise);
    } else if (decon_type == NS_GID || decon_type == GROUP_SPARSE) {
      NoiseStableDecon *nsop = dynamic_cast<NoiseStableDecon *>(preprocessor.get());
      if (external_noise_spectrum_loaded)
        nsop->loadnoise(external_noise_spectrum);
      else if (external_noise_loaded)
        nsop->loadnoise(external_noise);
      else {
        CoreTimeSeries nts(ExtractComponent(n, noise_component));
        nsop->loadnoise(nts);
      }
    }
    preprocessor->ScalarDecon::load(leafwavelet.s, leafwavelet.s);
    preprocessor->process();
    vector<double> raw_response(preprocessor->getresult());
    if (leaf_zero_lag_index < 0 ||
        leaf_zero_lag_index >= static_cast<int>(raw_response.size()))
      throw MsPASSError("FrequencyDomainGIDDecon: scalar raw actual output "
                        "does not contain zero lag",
                        ErrorSeverity::Invalid);
    raw_zero_lag_gain = fabs(raw_response[leaf_zero_lag_index]);
    for (int k = 0; k < 3; ++k) {
      CoreTimeSeries dcomp(ExtractComponent(leaf_data, k));
      preprocessor->ScalarDecon::load(leafwavelet.s, dcomp.s);
      preprocessor->process();
      vector<double> deconout(preprocessor->getresult());
      cblas_dcopy(d_decon.npts(), &(deconout[data_offset]), 1,
                  uwork.get_address(k, 0), 3);
    }
  }
  d_decon.u = uwork;

  /* Keep the reciprocal safely below DBL_MAX**0.5.  The later finite check
   * catches data-dependent overflow without imposing an arbitrary damping
   * cutoff on legitimate, strongly damped leaves. */
  const double min_raw_gain =
      1.0 / sqrt(numeric_limits<double>::max());
  if (!isfinite(raw_zero_lag_gain) || raw_zero_lag_gain <= min_raw_gain)
    throw MsPASSError("FrequencyDomainGIDDecon: raw zero-lag inverse gain is "
                      "too small, nonpositive, or nonfinite",
                      ErrorSeverity::Invalid);
  gid_leaf_raw_zero_lag_gain = raw_zero_lag_gain;
  gid_inverse_domain_amplitude_scale = 1.0 / raw_zero_lag_gain;
  if (!isfinite(gid_inverse_domain_amplitude_scale) ||
      gid_inverse_domain_amplitude_scale <= 0.0)
    throw MsPASSError("FrequencyDomainGIDDecon: inverse-domain amplitude "
                      "scale is nonfinite or nonpositive",
                      ErrorSeverity::Invalid);
  cblas_dscal(3 * d_decon.npts(), gid_inverse_domain_amplitude_scale,
              d_decon.u.get_address(0, 0), 1);
  if (!residual_matrix_is_finite(d_decon.u))
    throw MsPASSError("FrequencyDomainGIDDecon: inverse-domain scaling made "
                      "the deconvolved data nonfinite",
                      ErrorSeverity::Invalid);

  CoreTimeSeries actual_out;
  if (decon_type == CNR)
    actual_out = cnrprocessor->actual_output(current_wavelet);
  else
    actual_out = preprocessor->actual_output();
  int prezero_available =
      static_cast<int>(round((-fftwin.start) / d_decon.dt()));
  int postzero_available = d_decon.npts() - prezero_available - 1;
  int actual_zero = actual_out.sample_number(0.0);
  int actual_postzero = actual_out.npts() - actual_zero - 1;
  if ((actual_out.npts() > d_decon.npts() / 2) ||
      (actual_zero > prezero_available) ||
      (actual_postzero > postzero_available)) {
    TimeWindow compact_kernel(
        max(-2.0, -static_cast<double>(prezero_available) * d_decon.dt()),
        min(2.0, static_cast<double>(postzero_available) * d_decon.dt()));
    actual_out = WindowData(
        actual_out,
        ClipTimeWindowToSeries(actual_out, compact_kernel,
                               "FrequencyDomainGIDDecon"));
  }
  actual_o_fir = actual_out.s;
  actual_o_0 = actual_out.sample_number(0.0);
  if (actual_o_0 < 0 || actual_o_0 >= static_cast<int>(actual_o_fir.size()))
    throw MsPASSError("FrequencyDomainGIDDecon: actual output zero-lag "
                      "sample is outside kernel",
                      ErrorSeverity::Invalid);
  double peak_scale = fabs(actual_o_fir[actual_o_0]);
  if (peak_scale <= 0.0)
    throw MsPASSError("FrequencyDomainGIDDecon: actual output has zero peak",
                      ErrorSeverity::Invalid);
  for (auto &x : actual_o_fir)
    x /= peak_scale;
  if (lag_weight_penalty_function == "resolution_kernel" ||
      GIDLagWeightPenaltyUsesAdaptiveMemory(lag_weight_penalty_function)) {
    lag_weight_penalty = BuildGIDLagWeightPenaltyFunctionFromKernel(
        lag_weight_penalty_function, lag_weight_penalty_scale_factor,
        actual_o_fir, actual_o_0, "FrequencyDomainGIDDecon");
  }
}

TimeSeries FrequencyDomainGIDDecon::ideal_output() {
  return this->ScalarDecon::output_shaping_wavelet();
}

TimeSeries FrequencyDomainGIDDecon::actual_output() {
  if (!processed)
    throw MsPASSError(
        "FrequencyDomainGIDDecon::actual_output: process must be called first",
        ErrorSeverity::Invalid);
  if (decon_type == CNR)
    return cnrprocessor->actual_output(current_wavelet);
  return preprocessor->actual_output();
}

CoreTimeSeries FrequencyDomainGIDDecon::inverse_wavelet() {
  return this->inverse_wavelet(0.0);
}

CoreTimeSeries FrequencyDomainGIDDecon::inverse_wavelet(double t0parent) {
  if (!processed)
    throw MsPASSError(
        "FrequencyDomainGIDDecon::inverse_wavelet: process must be called first",
        ErrorSeverity::Invalid);
  if (decon_type == CNR)
    return cnrprocessor->inverse_wavelet(current_wavelet, t0parent);
  return preprocessor->inverse_wavelet(t0parent);
}

double FrequencyDomainGIDDecon::compute_ns_peak_threshold() {
  if (decon_type != NS_GID && decon_type != GROUP_SPARSE)
    return 0.0;
  CoreSeismogram nwork(n);
  /* The leaf is constructed with enough FFT padding for the whole noise
   * window and the selected wavelet.  Keep the threshold policy identical to
   * the time-domain GID engine: use every loaded sample. */
  dmatrix uwork(nwork.u);
  uwork.zero();
  NoiseStableDecon *nsop = dynamic_cast<NoiseStableDecon *>(preprocessor.get());
  if (nsop == nullptr)
    return 0.0;
  for (int k = 0; k < 3; ++k) {
    CoreTimeSeries ncomp(ExtractComponent(nwork, k));
    preprocessor->ScalarDecon::load(current_wavelet.s, ncomp.s);
    preprocessor->process();
    vector<double> deconout(preprocessor->getresult());
    int copysize = min(static_cast<int>(deconout.size()),
                       static_cast<int>(nwork.npts()));
    if (copysize > 0)
      cblas_dcopy(copysize, &(deconout[0]), 1, uwork.get_address(k, 0), 3);
  }
  preprocessor->ScalarDecon::load(current_wavelet.s, current_wavelet.s);
  preprocessor->process();
  nwork.u = uwork;
  cblas_dscal(3 * nwork.npts(), gid_inverse_domain_amplitude_scale,
              nwork.u.get_address(0, 0), 1);
  vector<double> noise_amps(ThreeCAmplitudes(nwork.u));
  sort(noise_amps.begin(), noise_amps.end());
  if (noise_amps.empty())
    return 0.0;
  int ip = static_cast<int>(ns_peak_probability_threshold *
                            static_cast<double>(noise_amps.size()));
  if (ip < 0)
    ip = 0;
  if (ip >= noise_amps.size())
    ip = noise_amps.size() - 1;
  double empirical = noise_amps[ip];
  double sumsq(0.0);
  for (auto x : noise_amps)
    sumsq += x * x;
  ns_noise_amplitude_rms = sqrt(sumsq / static_cast<double>(noise_amps.size()));
  /* Robust scale is derived from pooled-in-time signed components, not the
   * non-Gaussian distribution of 3C vector amplitudes.  Its component RMS is
   * used by the multiplier; the quadrature sum remains a vector diagnostic. */
  double robust_vector_sumsq(0.0);
  for (int k = 0; k < 3; ++k) {
    vector<double> signed_component(nwork.npts());
    for (int j = 0; j < nwork.npts(); ++j)
      signed_component[j] = nwork.u(k, j);
    sort(signed_component.begin(), signed_component.end());
    const double median = signed_component[signed_component.size() / 2];
    vector<double> absdev(signed_component.size());
    for (size_t i = 0; i < signed_component.size(); ++i)
      absdev[i] = fabs(signed_component[i] - median);
    sort(absdev.begin(), absdev.end());
    const double component_sigma = 1.4826 * absdev[absdev.size() / 2];
    robust_vector_sumsq += component_sigma * component_sigma;
  }
  ns_noise_component_rms.assign(3, 0.0);
  double ordinary_component_sumsq(0.0);
  for (int k = 0; k < 3; ++k) {
    double component_sumsq(0.0);
    for (int j = 0; j < nwork.npts(); ++j)
      component_sumsq += nwork.u(k, j) * nwork.u(k, j);
    ns_noise_component_rms[k] =
        sqrt(component_sumsq / static_cast<double>(nwork.npts()));
    ordinary_component_sumsq +=
        ns_noise_component_rms[k] * ns_noise_component_rms[k];
  }
  ns_noise_component_sigma_rms_robust = sqrt(robust_vector_sumsq / 3.0);
  ns_noise_component_rms_aggregate = sqrt(ordinary_component_sumsq / 3.0);
  ns_noise_component_sigma_rms_fallback_used = false;
  if (isfinite(ns_noise_component_sigma_rms_robust) &&
      ns_noise_component_sigma_rms_robust >
          1.0e-12 * ns_noise_component_rms_aggregate) {
    ns_noise_component_sigma_rms = ns_noise_component_sigma_rms_robust;
  } else if (isfinite(ns_noise_component_rms_aggregate) &&
             ns_noise_component_rms_aggregate > 0.0) {
    /* A low-entropy/quantized noise window can have zero MAD even when it has
     * real nonzero energy.  Use ordinary signed-component RMS as a
     * conservative scalar reference. */
    ns_noise_component_sigma_rms = ns_noise_component_rms_aggregate;
    ns_noise_component_sigma_rms_fallback_used = true;
  } else {
    throw MsPASSError("FrequencyDomainGIDDecon::compute_ns_peak_threshold: "
                      "noise scale is nonpositive or nonfinite",
                      ErrorSeverity::Invalid);
  }
  ns_noise_amplitude_robust = sqrt(robust_vector_sumsq);
  ns_peak_threshold_empirical = empirical;
  ns_peak_threshold_sigma =
      ns_peak_sigma_threshold * ns_noise_component_sigma_rms;
  if (!isfinite(ns_peak_threshold_sigma) || ns_peak_threshold_sigma <= 0.0)
    throw MsPASSError("FrequencyDomainGIDDecon::compute_ns_peak_threshold: "
                      "NS-GID sigma peak threshold is nonpositive or "
                      "nonfinite",
                      ErrorSeverity::Invalid);
  ns_peak_threshold =
      ns_use_empirical_noise_threshold
          ? max(ns_peak_threshold_empirical, ns_peak_threshold_sigma)
          : ns_peak_threshold_sigma;
  if (!isfinite(ns_peak_threshold) || ns_peak_threshold <= 0.0)
    throw MsPASSError("FrequencyDomainGIDDecon::compute_ns_peak_threshold: "
                      "NS-GID peak threshold is nonpositive or nonfinite",
                      ErrorSeverity::Invalid);
  ns_noise_samples_at_or_above_peak_threshold = 0;
  for (const auto amplitude : noise_amps) {
    if (amplitude >= ns_peak_threshold)
      ++ns_noise_samples_at_or_above_peak_threshold;
  }
  ns_noise_amplitude_sample_count = static_cast<int>(noise_amps.size());
  ns_noise_l2 = matrix_l2(nwork.u);
  return ns_peak_threshold;
}

void FrequencyDomainGIDDecon::rescale_spike(ThreeCSpike &spk) {
  int col0 = spk.col - actual_o_0;
  double denom =
      cblas_ddot(actual_o_fir.size(), &(actual_o_fir[0]), 1,
                 &(actual_o_fir[0]), 1);
  if (denom <= 0.0)
    return;
  for (int k = 0; k < 3; ++k) {
    double num = FIRDataOverlap(actual_o_fir, r, k, col0);
    spk.u[k] = num / denom;
  }
  spk.amp =
      sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
}

void FrequencyDomainGIDDecon::update_residual_matrix(const ThreeCSpike &spk) {
  int col0 = spk.col - actual_o_0;
  const int p_start = max(0, -col0);
  const int p_end =
      min(static_cast<int>(actual_o_fir.size()), static_cast<int>(r.npts()) - col0);
  const int n = p_end - p_start;
  if (n <= 0)
    return;
  for (int k = 0; k < 3; ++k) {
    cblas_daxpy(n, -spk.u[k], &(actual_o_fir[p_start]), 1,
                r.u.get_address(k, col0 + p_start), 3);
  }
  if (!residual_matrix_is_finite(r.u))
    throw MsPASSError("FrequencyDomainGIDDecon::update_residual_matrix: "
                      "residual is nonfinite after spike subtraction",
                      ErrorSeverity::Invalid);
}

void FrequencyDomainGIDDecon::update_lag_weights(
    const int col, const double candidate_amplitude) {
  if (GIDLagWeightPenaltyUsesAdaptiveMemory(lag_weight_penalty_function)) {
    GIDAdaptivePenaltyMetrics metrics(ApplyGIDAdaptiveMemoryPenalty(
        lag_weights, adaptive_penalty_memory, adaptive_penalty_retention,
        actual_o_fir, actual_o_0, col, lag_weight_penalty_scale_factor,
        candidate_amplitude, adaptive_penalty_noise_amplitude,
        "FrequencyDomainGIDDecon::update_lag_weights"));
    adaptive_penalty_last_confidence = metrics.confidence;
    adaptive_penalty_last_immediate_strength = metrics.immediate_strength;
    adaptive_penalty_last_specificity = metrics.specificity;
    adaptive_penalty_last_decay_factor = metrics.decay_factor;
    adaptive_penalty_noise_amplitude = metrics.noise_amplitude;
    adaptive_penalty_memory_linf = metrics.memory_linf;
    adaptive_penalty_memory_l2 = metrics.memory_l2;
    lag_weight_penalty.assign(metrics.effective_width, 1.0);
  } else {
    ApplyGIDLagWeightPenalty(lag_weights, lag_weight_penalty, col);
  }
}

void FrequencyDomainGIDDecon::process() {
  const string base_error("FrequencyDomainGIDDecon::process: ");
  string process_stage("initialize inverse operator");
  this->invalidate_processing_state();
  ns_stop_reason = (decon_type == NS_GID) ? "running" : "not_enabled";
  gid_stop_reason = "running";
  gid_converged = false;
  try {
    if (d_all.dead() || d_all.npts() <= 0)
      throw MsPASSError(base_error + "valid data window has not been loaded",
                        ErrorSeverity::Invalid);
    if (!residual_matrix_is_finite(d_all.u))
      throw MsPASSError(base_error + "input data contains nonfinite samples",
                        ErrorSeverity::Invalid);
    if (n.dead() || n.npts() <= 0)
      throw MsPASSError(base_error + "valid noise window has not been loaded",
                        ErrorSeverity::Invalid);
    if (!residual_matrix_is_finite(n.u))
      throw MsPASSError(base_error + "noise window contains nonfinite samples",
                        ErrorSeverity::Invalid);
    /* Keep these GID-level QC fields tied to the residual/stopping noise
     * record rather than to optional leaf regularization inputs. */
    gid_noise_samples_loaded = n.npts();
    gid_noise_samples_used = gid_noise_samples_loaded;
    gid_noise_truncated = false;
    this->initialize_inverse_operator();
    if (decon_type == NS_GID || decon_type == GROUP_SPARSE) {
      process_stage = "compute NS-GID peak threshold";
      ns_peak_threshold = this->compute_ns_peak_threshold();
      adaptive_penalty_noise_amplitude =
          (ns_noise_amplitude_rms > 0.0) ? ns_noise_amplitude_rms
                                         : ns_peak_threshold;
    } else if (decon_type == CNR) {
      TimeSeries nwavelet =
          external_noise_loaded
              ? TimeSeries(external_noise)
              : TimeSeries(ExtractComponent(n, noise_component),
                           "FrequencyDomainGIDDecon");
      adaptive_penalty_noise_amplitude =
          cnr_processed_noise_rms(*cnrprocessor, nwavelet, n,
                                  static_cast<int>(actual_o_fir.size())) *
          gid_inverse_domain_amplitude_scale;
    } else {
      process_stage = "estimate inverse-wavelet noise amplitude";
      CoreTimeSeries winv(preprocessor->inverse_wavelet(d_decon.t0()));
      adaptive_penalty_noise_amplitude =
          inverse_wavelet_noise_rms(winv, n,
                                    gid_inverse_domain_amplitude_scale);
    }
    process_stage = "initialize residual";
    r = d_decon;
    spikes.clear();
    iter_count = 0;
    lag_weights.clear();
    lag_weights.reserve(r.npts());
    for (int i = 0; i < r.npts(); ++i)
      lag_weights.push_back(1.0);
    for (int i = 0; i < r.npts(); ++i) {
      int col0 = i - actual_o_0;
      if ((col0 < 0) || ((col0 + actual_o_fir.size()) > r.npts()))
        lag_weights[i] = 0.0;
    }
    if (decon_type == NS_GID) {
      ns_initial_stationary_null_search_lag_count =
          static_cast<int>(count_if(lag_weights.begin(), lag_weights.end(),
                                    [](const double weight) {
                                      return weight > 0.0;
                                    }));
      ns_initial_stationary_null_expected_noise_exceedances =
          (ns_noise_amplitude_sample_count > 0)
              ? static_cast<double>(ns_initial_stationary_null_search_lag_count) *
                    static_cast<double>(
                        ns_noise_samples_at_or_above_peak_threshold) /
                    static_cast<double>(ns_noise_amplitude_sample_count)
              : 0.0;
    }
    adaptive_penalty_memory.assign(lag_weights.size(), 0.0);
    adaptive_penalty_retention.assign(lag_weights.size(), 0.0);
    ns_converged = false;
    ns_stop_reason = (decon_type == NS_GID) ? "running" : "not_enabled";
    gid_stop_reason = "running";
    gid_converged = false;
    ns_last_peak_significance = 0.0;
    resid_l2_initial = matrix_l2(r.u);
    resid_linf_initial = matrix_linf(r.u);
    if (!residual_matrix_is_finite(r.u) || !isfinite(resid_l2_initial))
      throw MsPASSError(base_error + "input data residual is nonfinite",
                        ErrorSeverity::Invalid);
    ns_residual_rms_initial = EstimateThreeCColumnAmplitudeRMS(r);
    ns_residual_rms_final = ns_residual_rms_initial;
    resid_l2_prev = resid_l2_initial;
    if (resid_l2_initial <= 0.0)
      throw MsPASSError(base_error + "input data residual is zero",
                        ErrorSeverity::Invalid);
    if (decon_type == GROUP_SPARSE) {
      process_stage = "group-sparse regularized solve";
      if (ns_peak_threshold <= 0.0)
        ns_peak_threshold = this->compute_ns_peak_threshold();
      group_sparse_lambda_used = group_sparse_lambda;
      if (group_sparse_lambda_used <= 0.0) {
        const double lambda_base =
            (ns_peak_threshold > 0.0) ? ns_peak_threshold
                                      : 0.02 * resid_linf_initial;
        group_sparse_lambda_used = group_sparse_lambda_scale * lambda_base;
      }
      GroupSparseDeconResult gs = SolveGroupSparseDecon(
          d_decon, actual_o_fir, actual_o_0, group_sparse_lambda_used,
          group_sparse_max_iterations, group_sparse_tolerance,
          group_sparse_active_threshold, group_sparse_active_threshold_scale,
          group_sparse_active_threshold_quantile, "FrequencyDomainGIDDecon");
      spikes = gs.spikes;
      iter_count = gs.iterations;
      group_sparse_iterations = gs.iterations;
      group_sparse_active_groups = static_cast<int>(spikes.size());
      group_sparse_converged = gs.converged;
      group_sparse_objective_initial = gs.objective_initial;
      group_sparse_objective_final = gs.objective_final;
      group_sparse_fractional_improvement_final =
          gs.fractional_improvement_final;
      group_sparse_active_threshold_quantile_value =
          gs.active_threshold_quantile_value;
      group_sparse_active_threshold_used = gs.active_threshold_used;
      process_stage = "refit group-sparse amplitudes";
      SpikeRefitDiagnostics refit;
      RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                           ns_ridge_beta, &refit, 1.0e5, 1.0e-2);
      group_sparse_refit_gram_condition_number = refit.gram_condition_number;
      group_sparse_refit_relative_ridge_beta = refit.relative_ridge_beta;
      group_sparse_refit_residual_l2_pre = refit.residual_l2_pre;
      group_sparse_refit_residual_l2_post = refit.residual_l2_post;
      group_sparse_refit_maximum_amplitude_pre = refit.maximum_amplitude_pre;
      group_sparse_refit_maximum_amplitude_post = refit.maximum_amplitude_post;
      group_sparse_refit_condition_guard_applied = refit.condition_guard_applied;
      group_sparse_refit_fallback_to_pre_debias = refit.fallback_to_pre_debias;
      group_sparse_refit_fallback_reason = refit.fallback_reason;
      spikes.remove_if([this](const ThreeCSpike &spk) {
        return spk.amp <= group_sparse_active_threshold_used;
      });
      group_sparse_active_groups = static_cast<int>(spikes.size());
      process_stage = "recompute group-sparse final residual";
      r = d_decon;
      for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
        this->update_residual_matrix(*sptr);
      resid_l2_final = matrix_l2(r.u);
      resid_linf_final = matrix_linf(r.u);
      ns_residual_rms_final = EstimateThreeCColumnAmplitudeRMS(r);
      group_sparse_debiased_objective_final =
          GroupSparseObjective(r, spikes, group_sparse_lambda_used);
      group_sparse_debiased_fractional_improvement_final =
          (group_sparse_objective_initial -
           group_sparse_debiased_objective_final) /
          max(1.0, group_sparse_objective_initial);
      if (!lag_weights.empty()) {
        auto lwmax = max_element(lag_weights.begin(), lag_weights.end());
        lag_weight_linf_final = (lwmax != lag_weights.end()) ? *lwmax : 0.0;
        lag_weight_l2_final =
            cblas_dnrm2(lag_weights.size(), &(lag_weights[0]), 1);
      }
      gid_stop_reason = group_sparse_converged
                            ? "group_sparse_converged"
                            : "group_sparse_max_iterations";
      gid_converged = group_sparse_converged;
      ns_stop_reason = "not_enabled";
      ns_converged = false;
      processed = true;
      return;
    }
    vector<double> amps;
    amps.reserve(r.npts());
    vector<double> raw_candidate_amplitudes;
    raw_candidate_amplitudes.reserve(r.npts());
    /* NS-GID support is a set, not a multiset.  Existing columns are kept in
     * the residual diagnostic but excluded from all new candidate scans. */
    vector<bool> ns_active_support(r.npts(), false);
    process_stage = "sparse iteration";
    /* A ridge amplitude refit changes the residual used for candidate
     * selection.  Continue with a new epoch only after an off-support,
     * strictly improving candidate has been audited on that residual. */
    bool ns_support_dirty(false);
    for (;;) {
    for (int iiter = iter_count; iiter < iter_max; ++iiter) {
      amps.clear();
      raw_candidate_amplitudes.clear();
      if (decon_type == NS_GID) {
        ns_max_raw_candidate_amplitude = 0.0;
        ns_max_raw_candidate_significance = 0.0;
        ns_max_raw_candidate_lag = -1;
        ns_last_scan_raw_significant_candidate_remaining = false;
      }
      for (int i = 0; i < r.npts(); ++i) {
        int col0 = i - actual_o_0;
        if ((col0 < 0) || ((col0 + actual_o_fir.size()) > r.npts())) {
          raw_candidate_amplitudes.push_back(0.0);
          amps.push_back(0.0);
        } else {
          double amp2(0.0);
          for (int k = 0; k < 3; ++k)
            amp2 += r.u(k, i) * r.u(k, i);
          const double raw_amplitude = sqrt(max(0.0, amp2));
          raw_candidate_amplitudes.push_back(
              (decon_type == NS_GID && ns_active_support[i]) ? 0.0
                                                              : raw_amplitude);
          if (decon_type == NS_GID && ns_active_support[i]) {
            amp2 = 0.0;
          } else if (decon_type == NS_GID && lag_weights[i] > 0.0) {
            if (raw_amplitude > ns_max_raw_candidate_amplitude) {
              ns_max_raw_candidate_amplitude = raw_amplitude;
              ns_max_raw_candidate_lag = i;
            }
            /* Significance is defined in raw 3C-amplitude units.  Restrict
             * the penalized selection objective to significant candidates. */
            if (raw_amplitude < ns_peak_threshold)
              amp2 = 0.0;
          }
          amps.push_back(amp2 * lag_weights[i] * lag_weights[i]);
        }
      }
      auto amax = max_element(amps.begin(), amps.end());
      if (decon_type == NS_GID) {
        const int selected_significant_candidate =
            SelectNoiseSignificantGIDCandidateIndex(raw_candidate_amplitudes,
                                                     lag_weights,
                                                     ns_peak_threshold);
        if (selected_significant_candidate >= 0)
          amax = amps.begin() + selected_significant_candidate;
        ns_max_raw_candidate_significance =
            (ns_peak_threshold > 0.0)
                ? ns_max_raw_candidate_amplitude / ns_peak_threshold
                : 0.0;
        ns_last_scan_raw_significant_candidate_remaining =
            ns_max_raw_candidate_significance >= 1.0;
      }
      if ((amax == amps.end()) || (*amax <= 0.0)) {
        if (decon_type == NS_GID && ns_max_raw_candidate_lag >= 0 &&
            !ns_last_scan_raw_significant_candidate_remaining) {
          const int imax = ns_max_raw_candidate_lag;
          const double candidate_amp = ns_max_raw_candidate_amplitude;
          ns_last_candidate_amplitude = candidate_amp;
          ns_last_selected_candidate_lag = imax;
          ns_last_selected_candidate_lag_weight = lag_weights[imax];
          ns_last_selected_candidate_weighted_amplitude =
              candidate_amp * ns_last_selected_candidate_lag_weight;
          ns_last_peak_significance = ns_max_raw_candidate_significance;
          ns_candidate_lag_history.push_back(imax);
          ns_candidate_lag_time_history.push_back(r.time(imax));
          ns_candidate_amplitude_history.push_back(candidate_amp);
          ns_candidate_threshold_history.push_back(ns_peak_threshold);
          ns_candidate_significance_history.push_back(ns_last_peak_significance);
          ns_candidate_accepted_history.push_back(0);
          ns_candidate_post_residual_rms_ratio_history.push_back(
              (ns_noise_amplitude_rms > 0.0)
                  ? EstimateThreeCColumnAmplitudeRMS(r) / ns_noise_amplitude_rms
                  : 0.0);
          ns_candidate_residual_l2_before_history.push_back(resid_l2_prev);
          ns_candidate_trial_residual_l2_history.push_back(0.0);
          ns_candidate_post_refit_residual_l2_history.push_back(0.0);
          ns_candidate_fractional_improvement_history.push_back(0.0);
          ns_candidate_state_fractional_improvement_history.push_back(0.0);
          ns_candidate_periodic_refit_applied_history.push_back(0);
          ns_candidate_final_refit_applied_history.push_back(0);
          ns_candidate_trial_evaluated_history.push_back(0);
          ns_candidate_metric_available_history.push_back(0);
          ns_candidate_stop_history.push_back("candidate_not_significant");
          ns_stop_reason = "candidate_not_significant";
          ns_converged = true;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
          break;
        }
        if (decon_type == NS_GID) {
          /* An all-zero eligible score means every off-support candidate is
           * below threshold (or there is no off-support lag), not that an
           * active support column should be retried. */
          ns_stop_reason = "candidate_not_significant";
          ns_converged = true;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
        } else {
          gid_stop_reason = "no_valid_candidate";
          gid_converged = true;
        }
        break;
      }
      if (decon_type == NS_GID) {
        int imax = distance(amps.begin(), amax);
        double candidate_amp2(0.0);
        for (int k = 0; k < 3; ++k)
          candidate_amp2 += r.u(k, imax) * r.u(k, imax);
        double candidate_amp = sqrt(max(0.0, candidate_amp2));
        ns_last_candidate_amplitude = candidate_amp;
        ns_last_selected_candidate_lag = imax;
        ns_last_selected_candidate_lag_weight = lag_weights[imax];
        ns_last_selected_candidate_weighted_amplitude =
            candidate_amp * ns_last_selected_candidate_lag_weight;
        ns_last_peak_significance =
            (ns_peak_threshold > 0.0) ? candidate_amp / ns_peak_threshold
                                      : 0.0;
        if (ns_peak_threshold > 0.0 && candidate_amp < ns_peak_threshold) {
          ns_candidate_lag_history.push_back(imax);
          ns_candidate_lag_time_history.push_back(r.time(imax));
          ns_candidate_amplitude_history.push_back(candidate_amp);
          ns_candidate_threshold_history.push_back(ns_peak_threshold);
          ns_candidate_significance_history.push_back(ns_last_peak_significance);
          ns_candidate_accepted_history.push_back(0);
          ns_candidate_post_residual_rms_ratio_history.push_back(
              (ns_noise_amplitude_rms > 0.0)
                  ? EstimateThreeCColumnAmplitudeRMS(r) / ns_noise_amplitude_rms
                  : 0.0);
          ns_candidate_residual_l2_before_history.push_back(resid_l2_prev);
          ns_candidate_trial_residual_l2_history.push_back(0.0);
          ns_candidate_post_refit_residual_l2_history.push_back(0.0);
          ns_candidate_fractional_improvement_history.push_back(0.0);
          ns_candidate_state_fractional_improvement_history.push_back(0.0);
          ns_candidate_periodic_refit_applied_history.push_back(0);
          ns_candidate_final_refit_applied_history.push_back(0);
          ns_candidate_trial_evaluated_history.push_back(0);
          ns_candidate_metric_available_history.push_back(0);
          ns_candidate_stop_history.push_back("candidate_not_significant");
          ns_stop_reason = "candidate_not_significant";
          ns_converged = true;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
          break;
        }
        if ((ns_max_spikes > 0) &&
            (static_cast<int>(spikes.size()) >= ns_max_spikes)) {
          ns_candidate_lag_history.push_back(imax);
          ns_candidate_lag_time_history.push_back(r.time(imax));
          ns_candidate_amplitude_history.push_back(candidate_amp);
          ns_candidate_threshold_history.push_back(ns_peak_threshold);
          ns_candidate_significance_history.push_back(ns_last_peak_significance);
          ns_candidate_accepted_history.push_back(0);
          ns_candidate_post_residual_rms_ratio_history.push_back(
              (ns_noise_amplitude_rms > 0.0)
                  ? EstimateThreeCColumnAmplitudeRMS(r) / ns_noise_amplitude_rms
                  : 0.0);
          ns_candidate_residual_l2_before_history.push_back(resid_l2_prev);
          ns_candidate_trial_residual_l2_history.push_back(0.0);
          ns_candidate_post_refit_residual_l2_history.push_back(0.0);
          ns_candidate_fractional_improvement_history.push_back(0.0);
          ns_candidate_state_fractional_improvement_history.push_back(0.0);
          ns_candidate_periodic_refit_applied_history.push_back(0);
          ns_candidate_final_refit_applied_history.push_back(0);
          ns_candidate_trial_evaluated_history.push_back(0);
          ns_candidate_metric_available_history.push_back(0);
          ns_candidate_stop_history.push_back("max_spikes");
          ns_stop_reason = "max_spikes";
          ns_converged = true;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
          break;
        }
      }
      int imax = distance(amps.begin(), amax);
      bool accepted(false);
      int legacy_rejected_this_iteration(0);
      bool legacy_found_decreasing_trial(false);
      bool ns_found_fractional_floor_rejection(false);
      while (!accepted && (*amax > 0.0)) {
        imax = distance(amps.begin(), amax);
        double candidate_amp2(0.0);
        for (int k = 0; k < 3; ++k)
          candidate_amp2 += r.u(k, imax) * r.u(k, imax);
        const double candidate_amp = sqrt(max(0.0, candidate_amp2));
        if (decon_type == NS_GID) {
          ns_candidate_lag_history.push_back(imax);
          ns_candidate_lag_time_history.push_back(r.time(imax));
          ns_candidate_amplitude_history.push_back(candidate_amp);
          ns_candidate_threshold_history.push_back(ns_peak_threshold);
          ns_candidate_significance_history.push_back(
              (ns_peak_threshold > 0.0) ? candidate_amp / ns_peak_threshold : 0.0);
          ns_candidate_accepted_history.push_back(0);
          ns_candidate_post_residual_rms_ratio_history.push_back(0.0);
          ns_candidate_residual_l2_before_history.push_back(resid_l2_prev);
          ns_candidate_trial_residual_l2_history.push_back(0.0);
          ns_candidate_post_refit_residual_l2_history.push_back(0.0);
          ns_candidate_fractional_improvement_history.push_back(0.0);
          ns_candidate_state_fractional_improvement_history.push_back(0.0);
          ns_candidate_periodic_refit_applied_history.push_back(0);
          ns_candidate_final_refit_applied_history.push_back(0);
          ns_candidate_trial_evaluated_history.push_back(1);
          ns_candidate_metric_available_history.push_back(1);
          ns_candidate_stop_history.push_back("rejected_residual");
        }
        ThreeCSpike spk(r.u, imax);
        this->rescale_spike(spk);
        const double resid_l2_before_candidate = resid_l2_prev;
        /* NS-GID needs a pre-commit fractional-improvement gate.  The
         * original GID leaf modes intentionally retain their historical
         * greedy policy: accept the best decreasing candidate, then apply
         * Eq.(15) as the global post-acceptance stopping criterion. */
        const bool candidate_fractional_gate = decon_type == NS_GID;
        const bool legacy_eq15_mode =
            decon_type != NS_GID && decon_type != GROUP_SPARSE;
        CoreSeismogram saved_r;
        double trial_l2(0.0);
        if (candidate_fractional_gate) {
          trial_l2 = trial_residual_l2(r.u, resid_l2_prev, spk,
                                       actual_o_fir, actual_o_0);
        } else {
          saved_r = r;
          this->update_residual_matrix(spk);
          trial_l2 = matrix_l2(r.u);
        }
        const double trial_fractional_improvement =
            (resid_l2_prev - trial_l2) / resid_l2_initial;
        if (legacy_eq15_mode) {
          /* Audit Eq.(15), but never let that audit alter the legacy support.
           * Its stopping decision remains below, after accepting this
           * decreasing candidate and updating the residual state. */
          ++legacy_eq15_candidates_tested;
          legacy_eq15_last_trial_fractional_improvement =
              trial_fractional_improvement;
          if (isfinite(trial_l2) && trial_l2 < resid_l2_prev &&
              trial_fractional_improvement <= residual_improvement_floor)
            ++legacy_eq15_candidates_below_floor;
        }
        if (decon_type == NS_GID) {
          ns_candidate_trial_residual_l2_history.back() = trial_l2;
          ns_candidate_fractional_improvement_history.back() =
              trial_fractional_improvement;
        }
        const bool ns_fractional_significant =
            decon_type != NS_GID ||
            (isfinite(trial_fractional_improvement) &&
             trial_fractional_improvement > residual_improvement_floor);
        if (trial_l2 < resid_l2_prev && ns_fractional_significant) {
          if (candidate_fractional_gate)
            this->update_residual_matrix(spk);
          spikes.push_back(spk);
          if (decon_type == NS_GID)
            ns_support_dirty = true;
          if (decon_type == NS_GID)
            ns_active_support[imax] = true;
          this->update_lag_weights(imax, candidate_amp);
          iter_count = iiter + 1;
          accepted = true;
          if (decon_type == NS_GID) {
            ns_candidate_accepted_history.back() = 1;
            ns_candidate_post_residual_rms_ratio_history.back() =
                (ns_noise_amplitude_rms > 0.0)
                    ? EstimateThreeCColumnAmplitudeRMS(r) / ns_noise_amplitude_rms
                    : 0.0;
            ns_candidate_stop_history.back() = "continue";
            ns_candidate_residual_l2_before_history.back() =
                resid_l2_before_candidate;
            ns_fractional_improvement_final =
                ns_candidate_fractional_improvement_history.back();
          }
          if (decon_type == NS_GID && ns_refit_interval > 0 &&
              (static_cast<int>(spikes.size()) % ns_refit_interval) == 0) {
            RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                                 ns_ridge_beta);
            r = d_decon;
            for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
              this->update_residual_matrix(*sptr);
            ns_candidate_periodic_refit_applied_history.back() = 1;
            ns_support_dirty = false;
          }
          if (decon_type == NS_GID) {
            const double post_refit_l2 = matrix_l2(r.u);
            ns_candidate_post_refit_residual_l2_history.back() = post_refit_l2;
            ns_candidate_state_fractional_improvement_history.back() =
                (resid_l2_before_candidate - post_refit_l2) / resid_l2_initial;
            ns_candidate_post_residual_rms_ratio_history.back() =
                (ns_noise_amplitude_rms > 0.0)
                    ? EstimateThreeCColumnAmplitudeRMS(r) /
                          ns_noise_amplitude_rms
                    : 0.0;
          }
        } else {
          if (!candidate_fractional_gate)
            r = saved_r;
          if (legacy_eq15_mode) {
            ++legacy_eq15_candidates_rejected;
            ++legacy_rejected_this_iteration;
            if (!isfinite(trial_l2)) {
              ++legacy_eq15_candidates_nonfinite;
            } else {
              ++legacy_eq15_candidates_non_decreasing;
            }
            constexpr size_t max_rejected_lag_samples = 64;
            if (legacy_eq15_rejected_lag_times.size() <
                max_rejected_lag_samples)
              legacy_eq15_rejected_lag_times.push_back(r.time(imax));
            else
              ++legacy_eq15_rejected_lag_samples_truncated;
          } else if (decon_type == NS_GID) {
            if (isfinite(trial_l2) && trial_l2 < resid_l2_prev) {
              ns_found_fractional_floor_rejection = true;
              ns_candidate_stop_history.back() =
                  "fractional_improvement_floor_rejected";
            } else {
              ns_candidate_stop_history.back() = "non_decreasing_trial";
            }
          }
          if (decon_type == NS_GID) {
            ns_candidate_post_refit_residual_l2_history.back() =
                resid_l2_before_candidate;
            ns_candidate_state_fractional_improvement_history.back() = 0.0;
            /* A significant best candidate that reduces the residual but
             * fails the fractional floor is the stopping decision.  Do not
             * discard it and let a lower-ranked candidate bypass the floor. */
            if (ns_found_fractional_floor_rejection)
              break;
          }
          amps[imax] = 0.0;
          amax = max_element(amps.begin(), amps.end());
        }
      }
      if (decon_type != NS_GID && decon_type != GROUP_SPARSE) {
        constexpr size_t max_rejected_iteration_samples = 256;
        if (legacy_eq15_rejected_candidates_per_iteration.size() <
            max_rejected_iteration_samples)
          legacy_eq15_rejected_candidates_per_iteration.push_back(
              legacy_rejected_this_iteration);
        else
          ++legacy_eq15_rejected_iteration_samples_truncated;
      }
      if (!accepted) {
        if (decon_type == NS_GID && !ns_candidate_accepted_history.empty() &&
            ns_candidate_accepted_history.back() < 0)
          ns_candidate_accepted_history.back() = 0;
        if (decon_type == NS_GID) {
          ns_stop_reason = "no_acceptable_candidate";
          if (ns_found_fractional_floor_rejection)
            ns_stop_reason = "fractional_improvement_floor";
          /* Exhausting significant candidates because none clears the
           * strictly-positive fractional-improvement floor is convergence,
           * not a failed/no-progress iteration. */
          ns_converged = ns_found_fractional_floor_rejection;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
        } else {
          gid_stop_reason = "no_acceptable_candidate";
          legacy_eq15_stop_detail = legacy_found_decreasing_trial
                                        ? "decreasing_candidate_rejected"
                                        : "no_decreasing_candidate";
          gid_converged = true;
        }
        break;
      }
      resid_l2_final = matrix_l2(r.u);
      resid_linf_final = matrix_linf(r.u);
      ns_residual_rms_final = EstimateThreeCColumnAmplitudeRMS(r);
      double ratio = resid_l2_final / resid_l2_initial;
      const double state_improvement =
          (resid_l2_prev - resid_l2_final) / resid_l2_initial;
      ns_fractional_improvement_state_final = state_improvement;
      resid_l2_prev = resid_l2_final;
      if (decon_type == NS_GID && ns_noise_amplitude_rms > 0.0 &&
          (ns_residual_rms_final / ns_noise_amplitude_rms) <=
              ns_residual_noise_ratio_floor) {
        ns_stop_reason = "residual_reached_noise_floor";
        ns_converged = true;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
        break;
      }
      if (ratio <= residual_ratio_floor) {
        gid_stop_reason = "residual_ratio_floor";
        gid_converged = true;
        if (decon_type == NS_GID) {
          ns_stop_reason = "residual_ratio_floor";
          ns_converged = true;
        }
        break;
      }
      /* Eq.(15) is a post-acceptance state criterion for the historical
       * greedy leaf modes.  NS-GID uses its accepted candidate trial metric. */
      const double improvement = (decon_type == NS_GID)
                                     ? ns_fractional_improvement_final
                                     : state_improvement;
      if (decon_type != NS_GID && decon_type != GROUP_SPARSE)
        ++legacy_eq15_post_acceptance_state_tests;
      if (improvement <= residual_improvement_floor) {
        gid_stop_reason = "fractional_improvement_floor";
        gid_converged = true;
        if (decon_type == NS_GID) {
          ns_stop_reason = "fractional_improvement_floor";
          ns_converged = true;
        } else {
          ++legacy_eq15_post_acceptance_floor_stops;
          legacy_eq15_stop_detail =
              "post_acceptance_fractional_improvement_floor";
        }
        break;
      }
    }
    if (decon_type == NS_GID && ns_stop_reason == "running") {
      if (iter_count >= iter_max) {
        ns_stop_reason = "max_iterations";
        ns_converged = false;
      } else {
        ns_stop_reason = "converged";
        ns_converged = true;
      }
      gid_stop_reason = ns_stop_reason;
      gid_converged = ns_converged;
    }
    if (decon_type != NS_GID && gid_stop_reason == "running") {
      gid_stop_reason = (iter_count >= iter_max) ? "max_iterations"
                                                 : "converged";
      gid_converged = (gid_stop_reason != "max_iterations");
    }
    ns_provisional_stop_reason_before_final_refit = ns_stop_reason;
    process_stage = "refit sparse amplitudes";
    double ridge_beta = (decon_type == NS_GID) ? ns_ridge_beta : 1.0e-10;
    const bool apply_terminal_refit =
        (decon_type != NS_GID) || (ns_support_dirty && !spikes.empty());
    if (apply_terminal_refit)
      RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                           ridge_beta);
    ns_final_refit_applied =
        (decon_type == NS_GID && apply_terminal_refit && !spikes.empty());
    if (ns_final_refit_applied) {
      ++ns_refit_epochs;
      ns_support_dirty = false;
    }
    process_stage = "recompute final residual";
    r = d_decon;
    for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
      this->update_residual_matrix(*sptr);
    resid_l2_final = matrix_l2(r.u);
    resid_linf_final = matrix_linf(r.u);
    ns_residual_rms_final = EstimateThreeCColumnAmplitudeRMS(r);
    if (decon_type == NS_GID) {
      ns_final_scan_max_raw_candidate_amplitude = 0.0;
      ns_final_scan_max_raw_candidate_significance = 0.0;
      ns_final_scan_max_raw_candidate_lag = -1;
      vector<int> active_lags;
      active_lags.reserve(spikes.size());
      for (const auto &spike : spikes)
        active_lags.push_back(spike.col);
      vector<bool> active(r.npts(), false);
      for (const int lag : active_lags)
        if (lag >= 0 && lag < r.npts())
          active[lag] = true;
      ns_final_scan_existing_support_max_raw_amplitude = 0.0;
      ns_final_scan_existing_support_max_raw_significance = 0.0;
      ns_final_scan_existing_support_max_raw_lag = -1;
      ns_final_scan_significant_candidate_count = 0;
      ns_final_scan_best_trial_lag = -1;
      ns_final_scan_best_trial_residual_l2 = 0.0;
      ns_final_scan_best_trial_fractional_improvement = 0.0;
      ns_final_scan_decision_candidate_lag = -1;
      ns_final_scan_global_acceptable_candidate_count = 0;
      ns_final_scan_decision_trial_residual_l2 = 0.0;
      ns_final_scan_decision_trial_fractional_improvement = 0.0;
      ns_final_scan_decision = "not_evaluated";
      ns_final_scan_acceptable_candidate_remaining = false;
      for (int j = 0; j < r.npts(); ++j) {
        if (lag_weights[j] <= 0.0)
          continue;
        double amp2(0.0);
        for (int k = 0; k < 3; ++k)
          amp2 += r.u(k, j) * r.u(k, j);
        const double amp = sqrt(max(0.0, amp2));
        if (active[j]) {
          if (amp > ns_final_scan_existing_support_max_raw_amplitude) {
            ns_final_scan_existing_support_max_raw_amplitude = amp;
            ns_final_scan_existing_support_max_raw_lag = j;
          }
        } else if (amp > ns_final_scan_max_raw_candidate_amplitude) {
          ns_final_scan_max_raw_candidate_amplitude = amp;
          ns_final_scan_max_raw_candidate_lag = j;
        }
      }
      ns_final_scan_existing_support_max_raw_significance =
          (ns_peak_threshold > 0.0)
              ? ns_final_scan_existing_support_max_raw_amplitude /
                    ns_peak_threshold
              : 0.0;
      const vector<int> final_candidates = OrderedNoiseSignificantGIDCandidates(
          r.u, lag_weights, active_lags, ns_peak_threshold);
      ns_final_scan_significant_candidate_count =
          static_cast<int>(final_candidates.size());
      ns_final_scan_max_raw_candidate_significance =
          (ns_peak_threshold > 0.0)
              ? ns_final_scan_max_raw_candidate_amplitude / ns_peak_threshold
              : 0.0;
      ns_final_scan_raw_significant_candidate_remaining =
          !final_candidates.empty();
      if (!final_candidates.empty()) {
        /* Ordered score and raw-amplitude QC are deliberately distinct. */
        (void)final_candidates.front();
      }
      for (size_t i = ns_candidate_accepted_history.size(); i-- > 0;) {
        if (ns_candidate_accepted_history[i] != 0) {
          ns_candidate_post_refit_residual_l2_history[i] = resid_l2_final;
          ns_candidate_final_refit_applied_history[i] =
              ns_final_refit_applied ? 1 : 0;
          ns_candidate_state_fractional_improvement_history[i] =
              (ns_candidate_residual_l2_before_history[i] - resid_l2_final) /
              resid_l2_initial;
          ns_fractional_improvement_state_final =
              ns_candidate_state_fractional_improvement_history[i];
          break;
        }
      }
    }
    const bool ns_final_noise_floor =
        decon_type == NS_GID && ns_noise_amplitude_rms > 0.0 &&
        (ns_residual_rms_final / ns_noise_amplitude_rms) <=
            ns_residual_noise_ratio_floor;
    const bool ns_final_residual_ratio_floor =
        decon_type == NS_GID && resid_l2_initial > 0.0 &&
        (resid_l2_final / resid_l2_initial) <= residual_ratio_floor;
    if (decon_type == NS_GID && ns_final_refit_applied &&
        ns_final_noise_floor) {
      /* Match the main-loop priority: noise floor is tested before the
       * residual-ratio floor after a terminal amplitude refit. */
      ns_stop_reason = "residual_reached_noise_floor";
      ns_converged = true;
      gid_stop_reason = ns_stop_reason;
      gid_converged = true;
    } else if (decon_type == NS_GID && ns_final_refit_applied &&
               ns_final_residual_ratio_floor) {
      ns_stop_reason = "residual_ratio_floor";
      ns_converged = true;
      gid_stop_reason = ns_stop_reason;
      gid_converged = true;
    } else if (decon_type == NS_GID && ns_final_refit_applied &&
               (ns_stop_reason == "residual_reached_noise_floor" ||
                ns_stop_reason == "residual_ratio_floor")) {
      /* The old global-floor decision used pre-refit amplitudes.  Reclassify
       * or resume from the final residual rather than exposing a synthetic
       * post_refit_* terminal state. */
      ns_stop_reason = "running";
      ns_converged = false;
      gid_stop_reason = ns_stop_reason;
      gid_converged = false;
    }
    /* The terminal trace entry is published only after the final ridge refit,
     * so its residual ratio and stop condition describe the same state as the
     * global NS-GID QC fields for every exit path. */
    if (decon_type == NS_GID && !ns_candidate_stop_history.empty()) {
      const size_t terminal = ns_candidate_stop_history.size() - 1;
      ns_candidate_post_residual_rms_ratio_history[terminal] =
          (ns_noise_amplitude_rms > 0.0)
              ? ns_residual_rms_final / ns_noise_amplitude_rms
              : 0.0;
      /* Preserve the historical candidate decision.  The post-refit audit is
       * a separate global state, not a mutation of the candidate trace. */
      gid_stop_reason = ns_stop_reason;
      gid_converged = ns_converged;
    }
    if (!lag_weights.empty()) {
      auto lwmax = max_element(lag_weights.begin(), lag_weights.end());
      lag_weight_linf_final = (lwmax != lag_weights.end()) ? *lwmax : 0.0;
      lag_weight_l2_final =
          cblas_dnrm2(lag_weights.size(), &(lag_weights[0]), 1);
    }
    bool ns_resume_after_refit(false);
    bool ns_audit_has_decreasing_trial(false);
    if (decon_type == NS_GID && ns_final_refit_applied &&
        !ns_final_noise_floor && !ns_final_residual_ratio_floor &&
        iter_count < iter_max &&
        (ns_max_spikes <= 0 || static_cast<int>(spikes.size()) < ns_max_spikes)) {
      vector<int> active_lags;
      active_lags.reserve(spikes.size());
      for (const auto &spike : spikes)
        active_lags.push_back(spike.col);
      const vector<int> candidates = OrderedNoiseSignificantGIDCandidates(
          r.u, lag_weights, active_lags, ns_peak_threshold);
      for (const int lag : candidates) {
        /* Match the main loop: nonfinite and nondecreasing trials can be
         * skipped, but the first decreasing sub-floor candidate is the
         * terminal decision and cannot be bypassed by a lower-ranked lag. */
        ThreeCSpike audit_spike(r.u, lag);
        this->rescale_spike(audit_spike);
        const double trial_l2 = trial_residual_l2(
            r.u, resid_l2_final, audit_spike, actual_o_fir, actual_o_0);
        const double improvement =
            (resid_l2_final - trial_l2) / resid_l2_initial;
        if (isfinite(trial_l2) &&
            (ns_final_scan_best_trial_lag < 0 ||
             trial_l2 < ns_final_scan_best_trial_residual_l2)) {
          ns_final_scan_best_trial_lag = lag;
          ns_final_scan_best_trial_residual_l2 = trial_l2;
          ns_final_scan_best_trial_fractional_improvement = improvement;
        }
        if (!isfinite(trial_l2) || trial_l2 >= resid_l2_final)
          continue;
        if (isfinite(improvement) &&
            improvement > residual_improvement_floor) {
          ++ns_final_scan_global_acceptable_candidate_count;
          ns_final_scan_acceptable_candidate_remaining = true;
        }
        if (!ns_audit_has_decreasing_trial) {
          ns_audit_has_decreasing_trial = true;
          ns_final_scan_decision_candidate_lag = lag;
          ns_final_scan_decision_trial_residual_l2 = trial_l2;
          ns_final_scan_decision_trial_fractional_improvement = improvement;
          if (isfinite(improvement) &&
              improvement > residual_improvement_floor) {
            ns_final_scan_decision = "resume";
            ns_resume_after_refit = true;
          } else {
            ns_final_scan_decision = "fractional_improvement_floor";
          }
        }
      }
      if (!ns_resume_after_refit && candidates.empty()) {
        ns_stop_reason = "candidate_not_significant";
        ns_converged = true;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
      } else if (!ns_resume_after_refit) {
        ns_stop_reason = ns_audit_has_decreasing_trial
                             ? "fractional_improvement_floor"
                             : "no_acceptable_candidate";
        ns_converged = ns_audit_has_decreasing_trial;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
      }
    }
    if (ns_resume_after_refit) {
      ++ns_refit_resume_count;
      ns_stop_reason = "running";
      ns_converged = false;
      gid_stop_reason = ns_stop_reason;
      gid_converged = false;
      continue;
    }
    break;
    } /* refit-aware NS-GID epoch */
    processed = true;
  } catch (const MsPASSError &err) {
    throw MsPASSError(base_error + "failed during " + process_stage + "\n" +
                          string(err.what()),
                      err.severity());
  } catch (...) {
    throw;
  };
}

CoreSeismogram FrequencyDomainGIDDecon::sparse_output() {
  if (!processed)
    throw MsPASSError(
        "FrequencyDomainGIDDecon::sparse_output: process must be called first",
        ErrorSeverity::Invalid);
  CoreSeismogram result(d_all);
  result = WindowData(result, outputwin);
  result.u.zero();
  double dt0 = result.t0() - r.t0();
  int delta_col = round(dt0 / r.dt());
  for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr) {
    int resultcol = (sptr->col) - delta_col;
    if ((resultcol < 0) || (resultcol >= result.npts()))
      continue;
    for (int k = 0; k < 3; ++k)
      result.u(k, resultcol) = sptr->u[k];
  }
  return result;
}

vector<double> FrequencyDomainGIDDecon::lag_weight_vector() const {
  if (!processed)
    throw MsPASSError("FrequencyDomainGIDDecon::lag_weight_vector: process "
                      "must be called first",
                      ErrorSeverity::Invalid);
  return lag_weights;
}

CoreSeismogram FrequencyDomainGIDDecon::getresult() {
  if (!processed)
    throw MsPASSError(
        "FrequencyDomainGIDDecon::getresult: process must be called first",
        ErrorSeverity::Invalid);
  CoreSeismogram sparse(this->sparse_output());
  CoreTimeSeries shaping(this->output_shaping_wavelet());
  CoreSeismogram shaped(sparse_convolve(shaping, sparse));
  return WindowData(shaped, outputwin);
}

Metadata FrequencyDomainGIDDecon::QCMetrics() {
  Metadata md;
  const bool inverse_scale_valid =
      isfinite(gid_inverse_domain_amplitude_scale) &&
      gid_inverse_domain_amplitude_scale > 0.0;
  const double raw_ns_peak_threshold = inverse_scale_valid
      ? ns_peak_threshold / gid_inverse_domain_amplitude_scale
      : numeric_limits<double>::quiet_NaN();
  const double raw_ns_noise_rms = inverse_scale_valid
      ? ns_noise_amplitude_rms / gid_inverse_domain_amplitude_scale
      : numeric_limits<double>::quiet_NaN();
  PutPrefixedMetadata(md, changed_leaf_metadata, "gid_leaf_");
  md.put("decon_operator", string("FrequencyDomainGIDDecon"));
  md.put("deconvolution_type", GIDDeconTypeName(decon_type));
  md.put("decon_processed", processed);
  md.put("decon_sample_interval", target_dt);
  md.put("decon_window_start", outputwin.start);
  md.put("decon_window_end", outputwin.end);
  md.put("deconvolution_window_start", fftwin.start);
  md.put("deconvolution_window_end", fftwin.end);
  md.put("wavelet_window_start", waveletwin.start);
  md.put("wavelet_window_end", waveletwin.end);
  md.put("noise_window_start", nwin.start);
  md.put("noise_window_end", nwin.end);
  md.put("gid_noise_samples_loaded", gid_noise_samples_loaded);
  md.put("gid_noise_samples_used", gid_noise_samples_used);
  md.put("gid_noise_truncated", gid_noise_truncated);
  int leaf_noise_samples_loaded(0);
  int leaf_noise_samples_used(0);
  bool leaf_noise_truncated(false);
  const bool leaf_uses_timeseries_noise =
      (decon_type == CNR || decon_type == MULTI_TAPER ||
       decon_type == NS_GID || decon_type == GROUP_SPARSE);
  const bool leaf_external_noise_used =
      external_noise_loaded && leaf_uses_timeseries_noise;
  if (leaf_uses_timeseries_noise && !external_noise_spectrum_loaded) {
    leaf_noise_samples_loaded = external_noise_loaded
                                    ? static_cast<int>(external_noise.npts())
                                    : static_cast<int>(n.npts());
    leaf_noise_samples_used = leaf_noise_samples_loaded;
    if (decon_type == MULTI_TAPER) {
      auto *mtop = dynamic_cast<MultiTaperXcorDecon *>(preprocessor.get());
      if (mtop != nullptr)
        leaf_noise_samples_used =
            min(leaf_noise_samples_used, mtop->get_taperlen());
      leaf_noise_truncated =
          leaf_noise_samples_used < leaf_noise_samples_loaded;
    }
  }
  md.put("gid_leaf_noise_samples_loaded", leaf_noise_samples_loaded);
  md.put("gid_leaf_noise_samples_used", leaf_noise_samples_used);
  md.put("gid_leaf_noise_truncated", leaf_noise_truncated);
  md.put("gid_leaf_external_noise_used", leaf_external_noise_used);
  md.put("gid_residual_external_noise_used", residual_noise_from_external);
  md.put("gid_inverse_operator_nfft", this->actual_inverse_operator_size());
  md.put("gid_analysis_samples", gid_analysis_samples);
  md.put("gid_wavelet_samples", gid_wavelet_samples);
  md.put("gid_analysis_t0", gid_analysis_t0);
  md.put("gid_wavelet_t0", gid_wavelet_t0);
  md.put("gid_wavelet_alignment_offset_samples", gid_alignment_offset_samples);
  md.put("gid_leaf_parameters_changed", leaf_parameters_changed);
  md.put("gid_processed", processed);
  md.put("gid_converged", gid_converged);
  md.put("gid_stop_reason", gid_stop_reason);
  md.put("gid_iterations", iter_count);
  md.put("gid_number_spikes", static_cast<int>(spikes.size()));
  md.put("gid_leaf_raw_zero_lag_gain", gid_leaf_raw_zero_lag_gain);
  md.put("gid_inverse_domain_amplitude_scale",
         gid_inverse_domain_amplitude_scale);
  md.put("gid_inverse_domain_scaling_policy",
         string("raw_zero_lag_normalized_v2"));
  md.put("gid_inverse_domain_scaling_valid", inverse_scale_valid);
  md.put("gid_inverse_domain_scaling_applied",
         gid_inverse_domain_amplitude_scale > 0.0);
  md.put("gid_inverse_domain_scaling_modes",
         string("scalar_leaf_and_cnr_including_group_sparse"));
  md.put("gid_legacy_eq15_candidate_policy",
         string("post_acceptance_state_improvement_v1;pretrial_scan_compat_v1"));
  md.put("gid_legacy_eq15_policy_valid", true);
  md.put("gid_legacy_eq15_policy_applied",
         decon_type != NS_GID && decon_type != GROUP_SPARSE && processed);
  md.put("gid_legacy_eq15_candidates_tested", legacy_eq15_candidates_tested);
  md.put("gid_legacy_eq15_pretrial_scan_candidates_tested",
         legacy_eq15_candidates_tested);
  md.put("gid_legacy_eq15_candidates_rejected",
         legacy_eq15_candidates_rejected);
  md.put("gid_legacy_eq15_pretrial_scan_candidates_rejected",
         legacy_eq15_candidates_rejected);
  md.put("gid_legacy_eq15_post_acceptance_state_tests",
         legacy_eq15_post_acceptance_state_tests);
  md.put("gid_legacy_eq15_post_acceptance_floor_stops",
         legacy_eq15_post_acceptance_floor_stops);
  md.put("gid_legacy_eq15_candidates_below_floor",
         legacy_eq15_candidates_below_floor);
  md.put("gid_legacy_eq15_candidates_non_decreasing",
         legacy_eq15_candidates_non_decreasing);
  md.put("gid_legacy_eq15_candidates_nonfinite",
         legacy_eq15_candidates_nonfinite);
  md.put("gid_legacy_eq15_rejected_lag_samples_recorded",
         static_cast<int>(legacy_eq15_rejected_lag_times.size()));
  md.put("gid_legacy_eq15_rejected_lag_samples_truncated",
         legacy_eq15_rejected_lag_samples_truncated);
  md.put("gid_legacy_eq15_rejected_iteration_samples_truncated",
         legacy_eq15_rejected_iteration_samples_truncated);
  md.put("gid_legacy_eq15_last_trial_fractional_improvement",
         legacy_eq15_last_trial_fractional_improvement);
  md.put("gid_legacy_eq15_stop_detail", legacy_eq15_stop_detail);
  {
    ostringstream rejected_lags;
    for (size_t i = 0; i < legacy_eq15_rejected_lag_times.size(); ++i) {
      if (i > 0)
        rejected_lags << ',';
      rejected_lags << legacy_eq15_rejected_lag_times[i];
    }
    md.put("gid_legacy_eq15_rejected_lag_seconds", rejected_lags.str());
  }
  {
    ostringstream rejected_per_iteration;
    for (size_t i = 0;
         i < legacy_eq15_rejected_candidates_per_iteration.size(); ++i) {
      if (i > 0)
        rejected_per_iteration << ',';
      rejected_per_iteration << legacy_eq15_rejected_candidates_per_iteration[i];
    }
    md.put("gid_legacy_eq15_rejected_candidates_per_iteration",
           rejected_per_iteration.str());
  }
  {
    constexpr double component_abs_tol = 1.0e-12;
    constexpr double component_rel_tol = 1.0e-8;
    ostringstream accepted_lags;
    ostringstream accepted_components;
    bool first = true;
    for (const auto &spike : spikes) {
      if (!first) {
        accepted_lags << ',';
        accepted_components << ',';
      }
      accepted_lags << (r.t0() + spike.col * r.dt());
      const double max_component = max({fabs(spike.u[0]), fabs(spike.u[1]), fabs(spike.u[2])});
      const double component_tol = max(component_abs_tol, component_rel_tol * max_component);
      bool component_first = true;
      for (int component = 0; component < 3; ++component) {
        if (fabs(spike.u[component]) <= component_tol)
          continue;
        if (!component_first)
          accepted_components << '+';
        accepted_components << component;
        component_first = false;
      }
      if (component_first)
        accepted_components << "none";
      first = false;
    }
    md.put("gid_accepted_spike_lag_seconds", accepted_lags.str());
    md.put("gid_accepted_spike_components", accepted_components.str());
    md.put("gid_accepted_spike_component_abs_tolerance", component_abs_tol);
    md.put("gid_accepted_spike_component_relative_tolerance", component_rel_tol);
  }
  md.put("gid_maximum_iterations",
         (decon_type == GROUP_SPARSE) ? group_sparse_max_iterations : iter_max);
  if (decon_type != GROUP_SPARSE) {
    md.put("gid_penalty_function", lag_weight_penalty_function);
    md.put("gid_penalty_scale_factor", lag_weight_penalty_scale_factor);
    md.put("gid_penalty_width", lag_weight_function_width);
    md.put("gid_penalty_effective_width",
           static_cast<int>(lag_weight_penalty.size()));
    md.put("gid_adaptive_penalty_enabled",
           GIDLagWeightPenaltyUsesAdaptiveMemory(lag_weight_penalty_function));
    md.put("gid_penalty_noise_amplitude", adaptive_penalty_noise_amplitude);
    md.put("gid_penalty_last_confidence", adaptive_penalty_last_confidence);
    md.put("gid_penalty_last_immediate_strength",
           adaptive_penalty_last_immediate_strength);
    md.put("gid_penalty_last_specificity", adaptive_penalty_last_specificity);
    md.put("gid_penalty_last_decay_factor",
           adaptive_penalty_last_decay_factor);
    md.put("gid_penalty_memory_Linf_final", adaptive_penalty_memory_linf);
    md.put("gid_penalty_memory_L2_final", adaptive_penalty_memory_l2);
    int gid_penalty_valid_lags(0);
    for (auto w : lag_weights) {
      if (w > 0.0)
        ++gid_penalty_valid_lags;
    }
    md.put("gid_penalty_valid_lags", gid_penalty_valid_lags);
  }
  md.put("gid_external_wavelet_used", external_wavelet_loaded);
  md.put("gid_external_noise_used",
         residual_noise_from_external || leaf_external_noise_used);
  md.put("gid_external_noise_spectrum_used",
         external_noise_spectrum_loaded);
  md.put("iteration_count", iter_count);
  md.put("residual_Linf_initial", resid_linf_initial);
  md.put("residual_Linf_final", resid_linf_final);
  /* residual_L2_* are retained as legacy, unnormalised Euclidean norms.
   * Derive generic RMS QC directly from those norms so it is valid for every
   * GID mode, not only NS-GID.  RMS removes analysis-window-length scaling;
   * only the final/initial fraction also removes record-amplitude scaling. */
  const int residual_npts = static_cast<int>(d_decon.npts());
  const double residual_rms_initial =
      (residual_npts > 0) ? resid_l2_initial / sqrt(residual_npts) : 0.0;
  const double residual_rms_final =
      (residual_npts > 0) ? resid_l2_final / sqrt(residual_npts) : 0.0;
  const double residual_3c_rms_initial =
      (residual_npts > 0)
          ? resid_l2_initial / sqrt(3.0 * static_cast<double>(residual_npts))
          : 0.0;
  const double residual_3c_rms_final =
      (residual_npts > 0)
          ? resid_l2_final / sqrt(3.0 * static_cast<double>(residual_npts))
          : 0.0;
  const bool residual_rms_fraction_valid =
      processed && residual_npts > 0 && isfinite(resid_l2_initial) &&
      isfinite(resid_l2_final) && resid_l2_initial > 0.0;
  const double residual_rms_fraction =
      residual_rms_fraction_valid
          ? residual_rms_final / residual_rms_initial
          : numeric_limits<double>::quiet_NaN();
  md.put("residual_rms_initial", residual_rms_initial);
  md.put("residual_rms_final", residual_rms_final);
  md.put("residual_3c_rms_initial", residual_3c_rms_initial);
  md.put("residual_3c_rms_final", residual_3c_rms_final);
  md.put("residual_rms_fraction_valid", residual_rms_fraction_valid);
  md.put("residual_rms_final_fraction", residual_rms_fraction);
  md.put("residual_rms_reduction_fraction",
         residual_rms_fraction_valid ? 1.0 - residual_rms_fraction
                                     : numeric_limits<double>::quiet_NaN());
  md.put("residual_energy_final_fraction",
         residual_rms_fraction_valid
             ? residual_rms_fraction * residual_rms_fraction
             : numeric_limits<double>::quiet_NaN());
  md.put("residual_energy_reduction_fraction",
         residual_rms_fraction_valid
             ? 1.0 - residual_rms_fraction * residual_rms_fraction
             : numeric_limits<double>::quiet_NaN());
  md.put("residual_L2_initial", resid_l2_initial);
  md.put("residual_L2_final", resid_l2_final);
  md.put("residual_L2_metric",
         string("legacy_unnormalized_three_component_euclidean_norm_gain_normalized_inverse_domain"));
  md.put("lag_weight_Linf_final", lag_weight_linf_final);
  md.put("lag_weight_L2_final", lag_weight_l2_final);
  md.put("gid_actual_o_fir_npts", static_cast<int>(actual_o_fir.size()));
  md.put("gid_actual_o_fir_zero_lag_index", actual_o_0);
  md.put("gid_actual_o_fir_peak_normalized",
         processed && !actual_o_fir.empty());
  /* Retain the older key as an alias, but make the normalization reference
   * explicit for consumers that compare engines. */
  md.put("gid_actual_o_fir_zero_lag_normalized",
         processed && !actual_o_fir.empty());
  if (decon_type == GROUP_SPARSE) {
    md.put("group_sparse_enabled", true);
    const int valid_start = actual_o_0;
    const int valid_end = static_cast<int>(d_decon.npts()) -
                          static_cast<int>(actual_o_fir.size()) + actual_o_0;
    const int valid_count = max(0, valid_end - valid_start + 1);
    md.put("group_sparse_valid_lag_start_samples", valid_start);
    md.put("group_sparse_valid_lag_end_samples", valid_end);
    md.put("group_sparse_valid_lag_start_time",
           d_decon.t0() + valid_start * d_decon.dt());
    md.put("group_sparse_valid_lag_end_time",
           d_decon.t0() + valid_end * d_decon.dt());
    md.put("group_sparse_min_observed_energy_fraction", 1.0);
    md.put("group_sparse_min_observed_support_samples",
           static_cast<int>(actual_o_fir.size()));
    md.put("group_sparse_boundary_policy",
           string("centered_full_fir_support"));
    md.put("group_sparse_boundary_candidates_rejected",
           max(0, static_cast<int>(d_decon.npts()) - valid_count));
    md.put("group_sparse_inverse_operator", string("ns_gid"));
    md.put("group_sparse_lambda_requested", group_sparse_lambda);
    md.put("group_sparse_lambda_scale", group_sparse_lambda_scale);
    md.put("group_sparse_lambda_used", group_sparse_lambda_used);
    md.put("group_sparse_tolerance", group_sparse_tolerance);
    md.put("group_sparse_max_iterations", group_sparse_max_iterations);
    md.put("group_sparse_iterations", group_sparse_iterations);
    md.put("group_sparse_converged", group_sparse_converged);
    md.put("group_sparse_active_threshold", group_sparse_active_threshold);
    md.put("group_sparse_active_threshold_scale",
           group_sparse_active_threshold_scale);
    md.put("group_sparse_active_threshold_quantile",
           group_sparse_active_threshold_quantile);
    md.put("group_sparse_active_threshold_quantile_value",
           group_sparse_active_threshold_quantile_value);
    md.put("group_sparse_active_threshold_used",
           group_sparse_active_threshold_used);
    md.put("group_sparse_active_groups", group_sparse_active_groups);
    md.put("group_sparse_objective_initial", group_sparse_objective_initial);
    md.put("group_sparse_objective_final", group_sparse_objective_final);
    md.put("group_sparse_fractional_improvement_final",
           group_sparse_fractional_improvement_final);
    md.put("group_sparse_refit_gram_condition_number",
           group_sparse_refit_gram_condition_number);
    md.put("group_sparse_refit_relative_ridge_beta",
           group_sparse_refit_relative_ridge_beta);
    md.put("group_sparse_refit_residual_l2_pre",
           group_sparse_refit_residual_l2_pre);
    md.put("group_sparse_refit_residual_l2_post",
           group_sparse_refit_residual_l2_post);
    md.put("group_sparse_refit_maximum_amplitude_pre",
           group_sparse_refit_maximum_amplitude_pre);
    md.put("group_sparse_refit_maximum_amplitude_post",
           group_sparse_refit_maximum_amplitude_post);
    md.put("group_sparse_refit_condition_guard_applied",
           group_sparse_refit_condition_guard_applied);
    md.put("group_sparse_refit_fallback_to_pre_debias",
           group_sparse_refit_fallback_to_pre_debias);
    md.put("group_sparse_refit_fallback_reason",
           group_sparse_refit_fallback_reason);
    md.put("group_sparse_debiased_objective_final",
           group_sparse_debiased_objective_final);
    md.put("group_sparse_debiased_fractional_improvement_final",
           group_sparse_debiased_fractional_improvement_final);
    md.put("group_sparse_noise_threshold", ns_peak_threshold);
    md.put("group_sparse_noise_threshold_raw_inverse_domain",
           raw_ns_peak_threshold);
    md.put("group_sparse_noise_threshold_normalized_inverse_domain",
           ns_peak_threshold);
    md.put("group_sparse_peak_threshold_empirical",
           ns_peak_threshold_empirical);
    md.put("group_sparse_peak_threshold_sigma", ns_peak_threshold_sigma);
    const double group_threshold_tolerance =
        64.0 * numeric_limits<double>::epsilon() *
        max({1.0, fabs(ns_peak_threshold_empirical), fabs(ns_peak_threshold_sigma)});
    const string group_threshold_controlling_term =
        !ns_use_empirical_noise_threshold
            ? "sigma_empirical_disabled"
            : ((fabs(ns_peak_threshold_empirical - ns_peak_threshold_sigma) <=
                group_threshold_tolerance)
                   ? "tie"
                   : ((ns_peak_threshold_empirical > ns_peak_threshold_sigma)
                          ? "empirical"
                          : "sigma"));
    md.put("group_sparse_use_empirical_noise_threshold",
           ns_use_empirical_noise_threshold);
    md.put("group_sparse_peak_threshold_controlling_term",
           group_threshold_controlling_term);
    md.put("group_sparse_noise_component_sigma_rms",
           ns_noise_component_sigma_rms);
    md.put("group_sparse_noise_component_sigma_rms_robust",
           ns_noise_component_sigma_rms_robust);
    md.put("group_sparse_noise_component_rms_aggregate",
           ns_noise_component_rms_aggregate);
    md.put("group_sparse_noise_component_sigma_rms_fallback_used",
           ns_noise_component_sigma_rms_fallback_used);
    NoiseStableDecon *nsop =
        dynamic_cast<NoiseStableDecon *>(preprocessor.get());
    if (nsop != nullptr) {
      Metadata nsmd(nsop->QCMetrics());
      md.put("group_sparse_inverse_gain_max_requested",
             nsmd.get_double("ns_gid_gain_max_requested"));
      md.put("group_sparse_inverse_gain_max_actual",
             nsmd.get_double("ns_gid_gain_max_actual"));
      md.put("group_sparse_inverse_mu_min", nsmd.get_double("ns_gid_mu_min"));
      md.put("group_sparse_inverse_alpha", nsmd.get_double("ns_gid_alpha"));
      md.put("group_sparse_inverse_noise_amplification",
             nsmd.get_double("ns_gid_noise_amplification"));
      md.put("group_sparse_inverse_effective_bandwidth_fraction",
             nsmd.get_double("ns_gid_effective_bandwidth_fraction"));
      md.put("group_sparse_inverse_operator_nfft",
             nsmd.get_int("ns_gid_operator_nfft"));
      md.put("group_sparse_inverse_use_reliability_taper",
             nsmd.get_bool("ns_gid_use_reliability_taper"));
      md.put("group_sparse_inverse_external_wavelet_used",
             external_wavelet_loaded);
      md.put("group_sparse_inverse_external_noise_used",
             external_noise_loaded);
      md.put("group_sparse_inverse_external_noise_spectrum_used",
             external_noise_spectrum_loaded);
    }
  }
  if (decon_type == NS_GID) {
    md.put("ns_gid_enabled", true);
    md.put("ns_gid_stop_reason", ns_stop_reason);
    md.put("ns_gid_provisional_stop_reason_before_final_refit",
           ns_provisional_stop_reason_before_final_refit);
    md.put("ns_gid_converged", ns_converged);
    md.put("ns_gid_iterations", iter_count);
    md.put("ns_gid_number_spikes", static_cast<int>(spikes.size()));
    md.put("ns_gid_peak_threshold", ns_peak_threshold);
    md.put("ns_gid_peak_threshold_raw_inverse_domain", raw_ns_peak_threshold);
    md.put("ns_gid_peak_threshold_normalized_inverse_domain", ns_peak_threshold);
    md.put("ns_gid_peak_threshold_empirical", ns_peak_threshold_empirical);
    md.put("ns_gid_peak_threshold_sigma", ns_peak_threshold_sigma);
    const double threshold_tolerance =
        64.0 * numeric_limits<double>::epsilon() *
        max({1.0, fabs(ns_peak_threshold_empirical), fabs(ns_peak_threshold_sigma)});
    const string threshold_controlling_term =
        !ns_use_empirical_noise_threshold
            ? "sigma_empirical_disabled"
            : ((fabs(ns_peak_threshold_empirical - ns_peak_threshold_sigma) <=
                threshold_tolerance)
                   ? "tie"
                   : ((ns_peak_threshold_empirical > ns_peak_threshold_sigma)
                          ? "empirical"
                          : "sigma"));
    md.put("ns_gid_peak_threshold_controlling_term", threshold_controlling_term);
    md.put("ns_gid_use_empirical_noise_threshold",
           ns_use_empirical_noise_threshold);
    md.put("ns_gid_peak_threshold_scope", string("pointwise_candidate_lag"));
    md.put("ns_gid_empirical_peak_threshold", ns_peak_threshold_empirical);
    md.put("ns_gid_sigma_peak_threshold", ns_peak_threshold_sigma);
    md.put("ns_gid_noise_amplitude_rms", ns_noise_amplitude_rms);
    md.put("ns_gid_noise_amplitude_rms_raw_inverse_domain", raw_ns_noise_rms);
    md.put("ns_gid_noise_amplitude_rms_normalized_inverse_domain",
           ns_noise_amplitude_rms);
    md.put("ns_gid_noise_component_sigma_rms", ns_noise_component_sigma_rms);
    md.put("ns_gid_noise_component_sigma_rms_robust",
           ns_noise_component_sigma_rms_robust);
    md.put("ns_gid_noise_component_rms_aggregate",
           ns_noise_component_rms_aggregate);
    md.put("ns_gid_noise_component_sigma_rms_fallback_used",
           ns_noise_component_sigma_rms_fallback_used);
    md.put("ns_gid_noise_amplitude_robust", ns_noise_amplitude_robust);
    md.put("ns_gid_last_candidate_amplitude", ns_last_candidate_amplitude);
    md.put("ns_gid_last_selected_candidate_lag_samples",
           ns_last_selected_candidate_lag);
    md.put("ns_gid_last_selected_candidate_lag_weight",
           ns_last_selected_candidate_lag_weight);
    md.put("ns_gid_last_selected_candidate_weighted_amplitude",
           ns_last_selected_candidate_weighted_amplitude);
    md.put("ns_gid_max_raw_candidate_lag_samples", ns_max_raw_candidate_lag);
    md.put("ns_gid_max_raw_candidate_amplitude",
           ns_max_raw_candidate_amplitude);
    md.put("ns_gid_max_raw_candidate_significance",
           ns_max_raw_candidate_significance);
    md.put("ns_gid_last_scan_raw_significant_candidate_remaining",
           ns_last_scan_raw_significant_candidate_remaining);
    md.put("ns_gid_final_scan_max_raw_candidate_lag_samples",
           ns_final_scan_max_raw_candidate_lag);
    md.put("ns_gid_final_scan_max_raw_candidate_amplitude",
           ns_final_scan_max_raw_candidate_amplitude);
    md.put("ns_gid_final_scan_max_raw_candidate_significance",
           ns_final_scan_max_raw_candidate_significance);
    md.put("ns_gid_final_scan_raw_significant_candidate_remaining",
           ns_final_scan_raw_significant_candidate_remaining);
    md.put("ns_gid_final_scan_existing_support_max_raw_lag_samples",
           ns_final_scan_existing_support_max_raw_lag);
    md.put("ns_gid_final_scan_existing_support_max_raw_amplitude",
           ns_final_scan_existing_support_max_raw_amplitude);
    md.put("ns_gid_final_scan_existing_support_max_raw_significance",
           ns_final_scan_existing_support_max_raw_significance);
    md.put("ns_gid_final_scan_significant_candidate_count",
           ns_final_scan_significant_candidate_count);
    md.put("ns_gid_final_scan_best_trial_lag_samples",
           ns_final_scan_best_trial_lag);
    md.put("ns_gid_final_scan_best_trial_residual_l2",
           ns_final_scan_best_trial_residual_l2);
    md.put("ns_gid_final_scan_best_trial_fractional_improvement",
           ns_final_scan_best_trial_fractional_improvement);
    md.put("ns_gid_final_scan_acceptable_candidate_remaining",
           ns_final_scan_acceptable_candidate_remaining);
    md.put("ns_gid_final_scan_global_acceptable_candidate_count",
           ns_final_scan_global_acceptable_candidate_count);
    md.put("ns_gid_final_scan_decision_candidate_lag_samples",
           ns_final_scan_decision_candidate_lag);
    md.put("ns_gid_final_scan_decision_trial_residual_l2",
           ns_final_scan_decision_trial_residual_l2);
    md.put("ns_gid_final_scan_decision_trial_fractional_improvement",
           ns_final_scan_decision_trial_fractional_improvement);
    md.put("ns_gid_final_scan_decision", ns_final_scan_decision);
    md.put("ns_gid_refit_epochs", ns_refit_epochs);
    md.put("ns_gid_refit_resume_count", ns_refit_resume_count);
    md.put("ns_gid_noise_samples_at_or_above_peak_threshold",
           ns_noise_samples_at_or_above_peak_threshold);
    md.put("ns_gid_initial_stationary_null_noise_amplitude_samples",
           ns_noise_amplitude_sample_count);
    md.put("ns_gid_initial_stationary_null_search_lag_count",
           ns_initial_stationary_null_search_lag_count);
    md.put("ns_gid_initial_stationary_null_expected_noise_exceedances",
           ns_initial_stationary_null_expected_noise_exceedances);
    for (size_t i = 0; i < ns_candidate_lag_history.size(); ++i) {
      const string prefix("ns_gid_iteration_" + to_string(i) + "_");
      md.put(prefix + "candidate_lag_samples", ns_candidate_lag_history[i]);
      md.put(prefix + "candidate_lag_time", ns_candidate_lag_time_history[i]);
      md.put(prefix + "candidate_amplitude", ns_candidate_amplitude_history[i]);
      md.put(prefix + "threshold", ns_candidate_threshold_history[i]);
      md.put(prefix + "significance", ns_candidate_significance_history[i]);
      md.put(prefix + "accepted", ns_candidate_accepted_history[i]);
      md.put(prefix + "post_residual_rms_ratio",
             ns_candidate_post_residual_rms_ratio_history[i]);
      md.put(prefix + "residual_l2_before_candidate",
             ns_candidate_residual_l2_before_history[i]);
      const bool trial_evaluated =
          ns_candidate_trial_evaluated_history[i] != 0;
      const bool metric_available =
          ns_candidate_metric_available_history[i] != 0;
      md.put(prefix + "trial_evaluated", trial_evaluated);
      md.put(prefix + "metric_available", metric_available);
      if (metric_available) {
        md.put(prefix + "residual_l2_trial_pre_refit",
               ns_candidate_trial_residual_l2_history[i]);
        md.put(prefix + "residual_l2_post_refit",
               ns_candidate_post_refit_residual_l2_history[i]);
        md.put(prefix + "candidate_fractional_improvement",
               ns_candidate_fractional_improvement_history[i]);
        md.put(prefix + "state_fractional_improvement",
               ns_candidate_state_fractional_improvement_history[i]);
      }
      md.put(prefix + "periodic_refit_applied",
             ns_candidate_periodic_refit_applied_history[i] != 0);
      md.put(prefix + "final_refit_applied",
             ns_candidate_final_refit_applied_history[i] != 0);
      md.put(prefix + "refit_applied",
             ns_candidate_periodic_refit_applied_history[i] != 0 ||
                 ns_candidate_final_refit_applied_history[i] != 0);
      md.put(prefix + "stop_condition", ns_candidate_stop_history[i]);
    }
    for (size_t k = 0; k < ns_noise_component_rms.size(); ++k)
      md.put("ns_gid_component_noise_rms_" + to_string(k),
             ns_noise_component_rms[k]);
    md.put("ns_gid_last_peak_significance", ns_last_peak_significance);
    md.put("ns_gid_external_wavelet_used", external_wavelet_loaded);
    md.put("ns_gid_external_noise_used", external_noise_loaded);
    md.put("ns_gid_external_noise_spectrum_used",
           external_noise_spectrum_loaded);
    md.put("ns_gid_residual_l2_initial", resid_l2_initial);
    md.put("ns_gid_residual_l2_final", resid_l2_final);
    md.put("ns_gid_residual_rms_initial", ns_residual_rms_initial);
    md.put("ns_gid_residual_rms_final", ns_residual_rms_final);
    md.put("ns_gid_residual_rms", ns_residual_rms_final);
    md.put("ns_gid_noise_rms", ns_noise_amplitude_rms);
    const double ns_initial_residual_noise_rms_ratio =
        (ns_noise_amplitude_rms > 0.0)
            ? ns_residual_rms_initial / ns_noise_amplitude_rms
            : 0.0;
    md.put("ns_gid_initial_residual_noise_rms_ratio",
           ns_initial_residual_noise_rms_ratio);
    md.put("ns_gid_residual_rms_final_fraction", residual_rms_fraction);
    md.put("ns_gid_residual_rms_reduction_fraction",
           1.0 - residual_rms_fraction);
    md.put("ns_gid_residual_energy_final_fraction",
           residual_rms_fraction_valid
               ? residual_rms_fraction * residual_rms_fraction
               : numeric_limits<double>::quiet_NaN());
    md.put("ns_gid_residual_energy_reduction_fraction",
           residual_rms_fraction_valid
               ? 1.0 - residual_rms_fraction * residual_rms_fraction
               : numeric_limits<double>::quiet_NaN());
    md.put("ns_gid_residual_noise_ratio",
           (ns_noise_l2 > 0.0) ? resid_l2_final / ns_noise_l2 : 0.0);
    md.put("ns_gid_residual_l2_ratio_legacy",
           (ns_noise_l2 > 0.0) ? resid_l2_final / ns_noise_l2 : 0.0);
    md.put("ns_gid_residual_rms_ratio",
           (ns_noise_amplitude_rms > 0.0)
               ? ns_residual_rms_final / ns_noise_amplitude_rms
               : 0.0);
    md.put("ns_gid_residual_noise_rms_ratio",
           (ns_noise_amplitude_rms > 0.0)
               ? ns_residual_rms_final / ns_noise_amplitude_rms
               : 0.0);
    md.put("ns_gid_fractional_improvement_final",
           ns_fractional_improvement_final);
    md.put("ns_gid_fractional_improvement_state_final",
           ns_fractional_improvement_state_final);
    md.put("ns_gid_final_refit_applied", ns_final_refit_applied);
    NoiseStableDecon *nsop = dynamic_cast<NoiseStableDecon *>(preprocessor.get());
    if (nsop != nullptr) {
      Metadata nsmd(nsop->QCMetrics());
      md.put("ns_gid_gain_max_requested",
             nsmd.get_double("ns_gid_gain_max_requested"));
      md.put("ns_gid_gain_max_actual",
             nsmd.get_double("ns_gid_gain_max_actual"));
      md.put("ns_gid_mu_min", nsmd.get_double("ns_gid_mu_min"));
      md.put("ns_gid_alpha", nsmd.get_double("ns_gid_alpha"));
      md.put("ns_gid_noise_amplification",
             nsmd.get_double("ns_gid_noise_amplification"));
      md.put("ns_gid_effective_bandwidth_fraction",
             nsmd.get_double("ns_gid_effective_bandwidth_fraction"));
      md.put("ns_gid_operator_nfft", nsmd.get_int("ns_gid_operator_nfft"));
    }
  }
  return md;
}
} // namespace mspass::algorithms::deconvolution
