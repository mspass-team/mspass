#include "mspass/algorithms/deconvolution/TimeDomainGIDDecon.h"
#include "gsl/gsl_cblas.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
#include "mspass/algorithms/deconvolution/LeastSquareDecon.h"
#include "mspass/algorithms/deconvolution/MultiTaperXcorDecon.h"
#include "mspass/algorithms/deconvolution/NoiseStableDecon.h"
#include "mspass/algorithms/deconvolution/WaterLevelDecon.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <math.h>
#include <list>
#include <sstream>
#include <vector>
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using namespace mspass::algorithms;

double Linf(dmatrix &d) {
  int nc, nr;
  nr = d.rows();
  nc = d.columns();
  double dmax(0.0);
  for (int i = 0; i < nr; ++i) {
    for (int j = 0; j < nc; ++j) {
      double amp = fabs(d(i, j));
      if (amp > dmax)
        dmax = amp;
    }
  }
  return dmax;
}
/* Similar function for L2 norm to Linf but here we use dnrm2. */
double L2(dmatrix &d) {
  int nd;
  nd = d.rows() * d.columns();
  double dl2;
  dl2 = cblas_dnrm2(nd, d.get_address(0, 0), 1);
  return dl2;
}
TimeDomainGIDDecon::TimeDomainGIDDecon(const AntelopePf &mdtoplevel)
    : ScalarDecon() {
  const string base_error("TimeDomainGIDDecon AntelopePf constructor:  ");
  /* The pf used for initializing this object has Antelope Arr section
  for each algorithm.   Since the generalized iterative method is a
  two-stage algorithm we have a section for the iterative algorithm
  and a variable section for the preprocessor algorithm.  We use
  the AntelopePf to parse this instead of raw antelope pfget
  C calls. */
  try {
    config_pf_text = AntelopePfToText(mdtoplevel);
    AntelopePf md = mdtoplevel.get_branch("deconvolution_operator_type");
    AntelopePf mdgiter = md.get_branch("time_domain_gid_deconvolution");
    IterDeconType dct = ParseGIDDeconType(mdgiter, "TimeDomainGIDDecon");
    this->decon_type = dct;
    double ts, te;
    ts = mdgiter.get<double>("full_data_window_start");
    te = mdgiter.get<double>("full_data_window_end");
    dwin = TimeWindow(ts, te);
    ts = mdgiter.get<double>("deconvolution_data_window_start");
    te = mdgiter.get<double>("deconvolution_data_window_end");
    fftwin = TimeWindow(ts, te);
    ts = mdgiter.get<double>("noise_window_start");
    te = mdgiter.get<double>("noise_window_end");
    nwin = TimeWindow(ts, te);
    ValidateWindowDuration(dwin, "full_data_window", base_error);
    ValidateWindowDuration(fftwin, "deconvolution_data_window", base_error);
    ValidateWindowDuration(nwin, "noise_window", base_error);
    /* We need to make sure the noise and decon windows are inside the
     * full_data_window*/
    if (fftwin.start < dwin.start || fftwin.end > dwin.end) {
      stringstream ss;
      ss << base_error << "decon window error" << endl
         << "Wavelet inversion window is not inside analysis window" << endl
         << "full_data_window (analysis) range=" << dwin.start << " to "
         << dwin.end << endl
         << "decon_window (wavelet inversion) range=" << fftwin.start << " to "
         << fftwin.end << endl;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    noise_component = mdgiter.get<int>("noise_component");
    ValidateThreeComponentIndex(noise_component, "noise_component", base_error);
    target_dt = mdgiter.get<double>("target_sample_interval");
    ValidatePositive(target_dt, "target_sample_interval", base_error);
    int maxns = static_cast<int>((fftwin.end - fftwin.start) / target_dt);
    ++maxns; // Add one - points not intervals
    int nfft = nextPowerOf2(maxns);
    /* This should override this even if it was previously set */
    mdgiter.put("operator_nfft", nfft);
    this->ScalarDecon::changeparameter(mdgiter);
    this->shapingwavelet = ShapingWavelet(mdgiter, nfft);
    AntelopePf mdleaf;
    /* Each leaf inverse operator must use the same deconvolution window as
     * the outer GID engine. */
    int n1, n2; // temporaries used below - declarations inside case labels are
                // awkward with the switch structure
    preprocessor = nullptr;
    cnrprocessor = nullptr;
    external_wavelet_loaded = false;
    external_noise_loaded = false;
    external_noise_spectrum_loaded = false;
    residual_noise_from_external = false;
    leaf_parameters_changed = false;
    switch (decon_type) {
    case WATER_LEVEL:
      mdleaf = md.get_branch("water_level");
      ValidateGIDLeafWindow(mdleaf, fftwin, "water level", base_error);
      preprocessor = std::make_unique<WaterLevelDecon>(mdleaf);
      break;
    case LEAST_SQ:
      mdleaf = md.get_branch("least_square");
      ValidateGIDLeafWindow(mdleaf, fftwin, "least square", base_error);
      preprocessor = std::make_unique<LeastSquareDecon>(mdleaf);
      break;
    case MULTI_TAPER:
      mdleaf = md.get_branch("multi_taper");
      ValidateGIDLeafWindow(mdleaf, fftwin, "multi taper", base_error);
      /* Here we also have to test the noise parameters, but the gid
      window can be different from that passed to the multitaper method.
      Hence we test only that the multitaper noise window is within the bounds
      of the gid noise window */
      n1 = static_cast<int>((fftwin.end - fftwin.start) / target_dt) + 1;
      n2 = static_cast<int>((nwin.end - nwin.start) / target_dt) + 1;
      if (n1 > n2) {
        stringstream ss;
        ss << base_error << "inconsistent noise window specification" << endl
           << "multitaper parameters specify taper length=" << n1 << " samples"
           << endl
           << "GID noise window parameters define noise_window_start="
           << nwin.start << " and noise_window_end=" << nwin.end << endl
           << "The GID window has a length of " << n2 << " samples" << endl
           << "GID implementation insists multitaper noise window be smaller "
              "or equal to GID noise window"
           << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = std::make_unique<MultiTaperXcorDecon>(mdleaf);
      break;
    case CNR:
      mdleaf = md.get_branch("cnr");
      ValidateGIDLeafWindow(mdleaf, fftwin, "CNR", base_error);
      cnrprocessor = std::make_unique<CNRDeconEngine>(mdleaf);
      break;
    case GROUP_SPARSE:
      mdleaf = md.get_branch("ns_gid");
      ValidateGIDLeafWindow(mdleaf, fftwin, "group sparse NS-GID inverse",
                            base_error);
      preprocessor = std::make_unique<NoiseStableDecon>(mdleaf);
      break;
    case NS_GID:
    default:
      mdleaf = md.get_branch("ns_gid");
      ValidateGIDLeafWindow(mdleaf, fftwin, "NS-GID", base_error);
      preprocessor = std::make_unique<NoiseStableDecon>(mdleaf);
      break;
    };
    changed_leaf_metadata = Metadata(mdleaf);
    /* Because this may evolve we make this a private method to
    make changes easier to implement. */
    this->construct_weight_penalty_function(mdgiter);
    /* Set convergence parameters from md keys */
    iter_max = mdgiter.get<int>("maximum_iterations");
    ValidatePositiveInteger(iter_max, "maximum_iterations", base_error);
    lw_linf_floor = mdgiter.get<double>("lag_weight_Linf_floor");
    ValidateNonnegative(lw_linf_floor, "lag_weight_Linf_floor", base_error);
    lw_l2_floor = mdgiter.get<double>("lag_weight_rms_floor");
    ValidateNonnegative(lw_l2_floor, "lag_weight_rms_floor", base_error);
    resid_linf_prob =
        mdgiter.get<double>("residual_noise_rms_probability_floor");
    ValidateProbability(resid_linf_prob, "residual_noise_rms_probability_floor",
                        base_error);
    resid_l2_tol = mdgiter.get<double>("residual_fractional_improvement_floor");
    ValidateNonnegative(resid_l2_tol,
                        "residual_fractional_improvement_floor", base_error);
    ns_peak_sigma_threshold =
        GetDoubleDefault(mdgiter, "ns_gid_peak_sigma_threshold", 4.0);
    ValidatePositive(ns_peak_sigma_threshold, "ns_gid_peak_sigma_threshold",
                     base_error);
    ns_peak_probability_threshold = GetDoubleDefault(
        mdgiter, "ns_gid_peak_probability_threshold", 0.995);
    ValidateProbability(ns_peak_probability_threshold,
                        "ns_gid_peak_probability_threshold", base_error);
    ns_use_empirical_noise_threshold = GetBoolDefault(
        mdgiter, "ns_gid_use_empirical_noise_threshold", true);
    ns_residual_noise_ratio_floor = GetDoubleDefault(
        mdgiter, "ns_gid_residual_noise_ratio_floor", 1.0);
    ValidateNonnegative(ns_residual_noise_ratio_floor,
                        "ns_gid_residual_noise_ratio_floor", base_error);
    ns_max_spikes = GetIntDefault(mdgiter, "ns_gid_max_spikes", 0);
    if (ns_max_spikes < 0)
      throw MsPASSError(base_error + "ns_gid_max_spikes must be nonnegative",
                        ErrorSeverity::Fatal);
    ns_refit_interval = GetIntDefault(mdgiter, "ns_gid_refit_interval", 5);
    if (ns_refit_interval < 1)
      throw MsPASSError(base_error + "ns_gid_refit_interval must be positive",
                        ErrorSeverity::Fatal);
    ns_ridge_beta =
        GetDoubleDefault(mdgiter, "ns_gid_ridge_beta", 1.0e-10);
    ValidateNonnegative(ns_ridge_beta, "ns_gid_ridge_beta", base_error);
    external_wavelet_allowed = GetBoolDefault(
        mdgiter, "ns_gid_external_wavelet_allowed", true);
    group_sparse_lambda = GetDoubleDefault(mdgiter, "group_sparse_lambda", 0.0);
    ValidateNonnegative(group_sparse_lambda, "group_sparse_lambda", base_error);
    group_sparse_lambda_scale =
        GetDoubleDefault(mdgiter, "group_sparse_lambda_scale", 1.0);
    ValidateNonnegative(group_sparse_lambda_scale,
                        "group_sparse_lambda_scale", base_error);
    group_sparse_tolerance =
        GetDoubleDefault(mdgiter, "group_sparse_tolerance", 1.0e-4);
    ValidatePositive(group_sparse_tolerance, "group_sparse_tolerance",
                     base_error);
    group_sparse_max_iterations =
        GetIntDefault(mdgiter, "group_sparse_max_iterations", iter_max);
    ValidatePositiveInteger(group_sparse_max_iterations,
                            "group_sparse_max_iterations", base_error);
    group_sparse_active_threshold =
        GetDoubleDefault(mdgiter, "group_sparse_active_threshold", 2.0e-2);
    ValidateNonnegative(group_sparse_active_threshold,
                        "group_sparse_active_threshold", base_error);
    group_sparse_active_threshold_scale =
        GetDoubleDefault(mdgiter, "group_sparse_active_threshold_scale", 1.0);
    ValidateNonnegative(group_sparse_active_threshold_scale,
                        "group_sparse_active_threshold_scale", base_error);
    group_sparse_active_threshold_quantile = GetDoubleDefault(
        mdgiter, "group_sparse_active_threshold_quantile", 0.90);
    ValidateProbability(group_sparse_active_threshold_quantile,
                        "group_sparse_active_threshold_quantile", base_error);
    this->invalidate_processing_state();
  } catch (...) {
    throw;
  };
}
TimeDomainGIDDecon::~TimeDomainGIDDecon() {}

void TimeDomainGIDDecon::invalidate_processing_state() {
  result.clear();
  spikes.clear();
  lag_weights.clear();
  actual_o_fir.clear();
  actual_o_0 = 0;
  adaptive_penalty_memory.clear();
  adaptive_penalty_retention.clear();
  adaptive_penalty_last_confidence = 0.0;
  adaptive_penalty_last_immediate_strength = 0.0;
  adaptive_penalty_last_specificity = 0.0;
  adaptive_penalty_last_decay_factor = 0.0;
  adaptive_penalty_noise_amplitude = 0.0;
  adaptive_penalty_memory_linf = 0.0;
  adaptive_penalty_memory_l2 = 0.0;
  wavelet_pad = 0;
  iter_count = 0;
  lw_linf_initial = 0.0;
  lw_linf_prev = 0.0;
  lw_l2_initial = 0.0;
  lw_l2_prev = 0.0;
  resid_linf_initial = 0.0;
  resid_linf_prev = 0.0;
  resid_l2_initial = 0.0;
  resid_l2_prev = 0.0;
  ns_fractional_improvement_final = 0.0;
  ns_last_peak_significance = 0.0;
  ns_peak_threshold = 0.0;
  ns_noise_l2 = 0.0;
  ns_noise_amplitude_rms = 0.0;
  ns_converged = false;
  ns_stop_reason = "not_started";
  gid_converged = false;
  gid_stop_reason = "not_started";
  group_sparse_lambda_used = 0.0;
  group_sparse_objective_initial = 0.0;
  group_sparse_objective_final = 0.0;
  group_sparse_fractional_improvement_final = 0.0;
  group_sparse_active_threshold_quantile_value = 0.0;
  group_sparse_active_threshold_used = 0.0;
  group_sparse_iterations = 0;
  group_sparse_active_groups = 0;
  group_sparse_converged = false;
  processed = false;
}

void TimeDomainGIDDecon::changeparameter(const Metadata &md) {
  const bool cnr_mode(this->decon_type == CNR);
  ValidateGIDLeafOperatorMetadata(
      md, fftwin, target_dt, "TimeDomainGIDDecon::changeparameter", cnr_mode);
  this->invalidate_processing_state();
  if (cnr_mode)
    this->cnrprocessor->changeparameter(md);
  else
    this->preprocessor->changeparameter(md);
  changed_leaf_metadata = Metadata(md);
  leaf_parameters_changed = true;
}

CoreTimeSeries TimeDomainGIDDecon::ideal_output() {
  return this->ScalarDecon::output_shaping_wavelet();
}

CoreTimeSeries TimeDomainGIDDecon::actual_output() {
  if (!processed)
    throw MsPASSError(
        "TimeDomainGIDDecon::actual_output: process must be called first",
        ErrorSeverity::Invalid);
  if (decon_type == CNR)
    return cnrprocessor->actual_output(current_wavelet);
  return preprocessor->actual_output();
}

CoreTimeSeries TimeDomainGIDDecon::inverse_wavelet() {
  return this->inverse_wavelet(0.0);
}

CoreTimeSeries TimeDomainGIDDecon::inverse_wavelet(double t0parent) {
  if (!processed)
    throw MsPASSError(
        "TimeDomainGIDDecon::inverse_wavelet: process must be called first",
        ErrorSeverity::Invalid);
  if (decon_type == CNR)
    return cnrprocessor->inverse_wavelet(current_wavelet, t0parent);
  return preprocessor->inverse_wavelet(t0parent);
}

void TimeDomainGIDDecon::construct_weight_penalty_function(const Metadata &md) {
  try {
    lag_weight_penalty_function =
        md.is_defined("lag_weight_penalty_function")
            ? md.get_string("lag_weight_penalty_function")
            : "none";
    lag_weight_penalty_scale_factor =
        md.is_defined("lag_weight_penalty_scale_factor")
            ? md.get<double>("lag_weight_penalty_scale_factor")
            : 1.0;
    if (!isfinite(lag_weight_penalty_scale_factor) ||
        lag_weight_penalty_scale_factor <= 0.0 ||
        lag_weight_penalty_scale_factor > 1.0)
      throw MsPASSError("TimeDomainGIDDecon::construct_weight_penalty_function: "
                        "lag_weight_penalty_scale_factor must be in (0, 1]",
                        ErrorSeverity::Fatal);
    lag_weight_function_width =
        md.is_defined("lag_weight_function_width")
            ? md.get<int>("lag_weight_function_width")
            : 0;
    if (md.is_defined("lag_weight_function_width"))
      ValidatePositiveInteger(
          lag_weight_function_width, "lag_weight_function_width",
          "TimeDomainGIDDecon::construct_weight_penalty_function");
    if (lag_weight_penalty_function == "none") {
      wtf = vector<double>{1.0};
    } else if (lag_weight_penalty_function == "shaping_wavelet") {
      CoreTimeSeries shaping(this->output_shaping_wavelet());
      wtf = BuildGIDLagWeightPenaltyFunctionFromKernel(
          lag_weight_penalty_function, lag_weight_penalty_scale_factor,
          shaping.s, shaping.sample_number(0.0),
          "TimeDomainGIDDecon::construct_weight_penalty_function");
    } else if (lag_weight_penalty_function == "resolution_kernel" ||
               GIDLagWeightPenaltyUsesAdaptiveMemory(
                   lag_weight_penalty_function)) {
      wtf = vector<double>{1.0};
    } else {
      wtf = BuildGIDLagWeightPenaltyFunction(
          md, "TimeDomainGIDDecon::construct_weight_penalty_function");
    }
    nwtf = static_cast<int>(wtf.size());
  } catch (...) {
    throw;
  };
}
void rescale_spike_amplitude(ThreeCSpike &spk, const CoreSeismogram &target,
                             const vector<double> &actual_o_fir,
                             const int actual_o_0) {
  double denom =
      cblas_ddot(actual_o_fir.size(), &(actual_o_fir[0]), 1,
                 &(actual_o_fir[0]), 1);
  if (denom <= 0.0)
    return;
  int col0 = spk.col - actual_o_0;
  for (int k = 0; k < 3; ++k) {
    double num = FIRDataOverlap(actual_o_fir, target, k, col0);
    spk.u[k] = num / denom;
  }
  spk.amp =
      sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
}

int TimeDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin_in) {
  try {
    this->invalidate_processing_state();
    d_all.kill();
    ndwin = 0;
    ValidateWindowDuration(dwin_in, "signal_window",
                           "TimeDomainGIDDecon::load");
    if ((dwin_in.start > fftwin.start) || (dwin_in.end < fftwin.end)) {
      return 1;
    }
    dwin = dwin_in;
    /* First we load the requested window.  Note we MUST always make this window
    a bit larger than the range of desired lags as the iterative algorithm will
    not allow lags at the edges (defined by a construction parameter
    wavelet_pad)
    */
    d_all = WindowData(draw, dwin);
    if (d_all.dead() || d_all.npts() <= 0)
      return 1;
    ndwin = d_all.npts();
    return 0;
  } catch (...) {
    throw;
  };
}
int TimeDomainGIDDecon::loadnoise(const CoreSeismogram &draw,
                                TimeWindow nwin_in) {
  try {
    this->invalidate_processing_state();
    n.kill();
    nnwin = 0;
    ns_noise_components.clear();
    residual_noise_from_external = false;
    ValidateWindowDuration(nwin_in, "noise_window",
                           "TimeDomainGIDDecon::loadnoise");
    nwin = nwin_in;
    n = WindowData(draw, nwin);
    if (n.dead() || n.npts() <= 0)
      return 1;
    nnwin = n.npts();
    if (decon_type == NS_GID || decon_type == GROUP_SPARSE) {
      ns_noise_components.clear();
      ns_noise_components.reserve(3);
      for (int k = 0; k < 3; ++k) {
        CoreTimeSeries ncomp(ExtractComponent(draw, k));
        ncomp = WindowData(ncomp, nwin);
        ns_noise_components.push_back(ncomp.s);
      }
      return 0;
    }
    this->compute_resid_linf_floor(n);
    return 0;
  } catch (...) {
    throw;
  };
}
int TimeDomainGIDDecon::loadwavelet(const TimeSeries &wavelet) {
  if (!external_wavelet_allowed)
    throw MsPASSError("TimeDomainGIDDecon::loadwavelet: external wavelets are "
                      "disabled by ns_gid_external_wavelet_allowed",
                      ErrorSeverity::Invalid);
  if (wavelet.dead())
    throw MsPASSError("TimeDomainGIDDecon::loadwavelet: external wavelet is "
                      "marked dead",
                      ErrorSeverity::Invalid);
  if (wavelet.npts() <= 0)
    throw MsPASSError("TimeDomainGIDDecon::loadwavelet: external wavelet is "
                      "empty",
                      ErrorSeverity::Invalid);
  ValidateExternalTimeSeriesSampleInterval(
      wavelet, target_dt, "TimeDomainGIDDecon::loadwavelet");
  this->invalidate_processing_state();
  external_wavelet_loaded = false;
  external_wavelet = wavelet;
  external_wavelet_loaded = true;
  return 0;
}
int TimeDomainGIDDecon::loadwavelet(const CoreTimeSeries &wavelet) {
  TimeSeries ts(wavelet, "TimeDomainGIDDecon");
  return this->loadwavelet(ts);
}
int TimeDomainGIDDecon::loadnoise(const TimeSeries &noise_in) {
  if (noise_in.dead())
    throw MsPASSError("TimeDomainGIDDecon::loadnoise: external noise is "
                      "marked dead",
                      ErrorSeverity::Invalid);
  if (noise_in.npts() <= 0)
    throw MsPASSError("TimeDomainGIDDecon::loadnoise: external noise is empty",
                      ErrorSeverity::Invalid);
  ValidateExternalTimeSeriesSampleInterval(
      noise_in, target_dt, "TimeDomainGIDDecon::loadnoise");
  const bool keep_residual_noise =
      n.live() && n.npts() > 0 && !residual_noise_from_external;
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  external_noise_spectrum = PowerSpectrum();
  external_noise = noise_in;
  external_noise_loaded = true;
  external_noise_spectrum_loaded = false;
  if (!keep_residual_noise) {
    ns_noise_components.clear();
    ns_noise_components.assign(3, noise_in.s);
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
int TimeDomainGIDDecon::loadnoise(const CoreTimeSeries &noise_in) {
  TimeSeries ts(noise_in, "TimeDomainGIDDecon");
  return this->loadnoise(ts);
}
int TimeDomainGIDDecon::loadnoise(const PowerSpectrum &noise_spectrum_in) {
  if (decon_type != NS_GID && decon_type != GROUP_SPARSE)
    throw MsPASSError("TimeDomainGIDDecon::loadnoise: external PowerSpectrum "
                      "noise is only supported for ns_gid and group_sparse; "
                      "pass a TimeSeries noise estimate for multi_taper or "
                      "use the configured noise window for other GID modes",
                      ErrorSeverity::Invalid);
  ValidatePowerSpectrumCoversDC(noise_spectrum_in,
                                "TimeDomainGIDDecon::loadnoise");
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  external_noise = TimeSeries();
  external_noise_spectrum = noise_spectrum_in;
  external_noise_spectrum_loaded = true;
  external_noise_loaded = false;
  if (residual_noise_from_external) {
    n.kill();
    nnwin = 0;
    ns_noise_components.clear();
    residual_noise_from_external = false;
  }
  return 0;
}
void TimeDomainGIDDecon::clear_external_wavelet() {
  external_wavelet_loaded = false;
  external_wavelet = TimeSeries();
  this->invalidate_processing_state();
}
void TimeDomainGIDDecon::clear_external_noise() {
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  external_noise = TimeSeries();
  external_noise_spectrum = PowerSpectrum();
  if (residual_noise_from_external) {
    n.kill();
    nnwin = 0;
    ns_noise_components.clear();
    residual_noise_from_external = false;
  }
  this->invalidate_processing_state();
}
int TimeDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                           TimeWindow nwin) {
  try {
    this->invalidate_processing_state();
    d_all.kill();
    n.kill();
    ndwin = 0;
    nnwin = 0;
    ns_noise_components.clear();
    ValidateWindowDuration(dwin, "signal_window", "TimeDomainGIDDecon::load");
    ValidateWindowDuration(nwin, "noise_window", "TimeDomainGIDDecon::load");
    if ((dwin.start > fftwin.start) || (dwin.end < fftwin.end)) {
      return 1;
    }
    int iretn, iret;
    iretn = this->loadnoise(draw, nwin);
    iret = this->load(draw, dwin);
    return (iretn + iret);
  } catch (...) {
    throw;
  };
}
/* These are the set of private methods called from the process method */
void TimeDomainGIDDecon::update_residual_matrix(ThreeCSpike spk) {
  try {
    const string base_error("TimeDomainGIDDecon::update_residual_matrix:  ");
    int ncol = this->r.u.columns();
    int col0 = spk.col - actual_o_0;
    ;
    /* Avoid seg faults and test range here.  This is an exception condition
     * because if the lag weights are created correctly the edge conditions
     * here should not be possible */
    if (col0 < 0)
      throw MsPASSError(base_error +
                            "Coding problem - computed lag is negative.  "
                            "lag_weights array is probably incorrect",
                        ErrorSeverity::Fatal);
    if ((col0 + actual_o_fir.size()) > ncol)
      throw MsPASSError(base_error +
                            "Coding problem - computed lag is too large and "
                            "would overflow residual matrix and seg fault.\n"

                            + "lag_weights array is probably incorrect",
                        ErrorSeverity::Fatal);
    for (int k = 0; k < 3; ++k) {
      /*Use the gsl version of daxpy hre to avoid type collisions with perf.h.
       */
      cblas_daxpy(actual_o_fir.size(), -spk.u[k], &(actual_o_fir[0]), 1,
                  this->r.u.get_address(k, col0), 3);
    }
  } catch (...) {
    throw;
  };
}
/* This method multiplies the lag weights by the penalty function created by
the constructor centered at lag = col.  This mirrors the experimental
iterdecon penalty model: repeated hits keep suppressing the same lag or
neighborhood, which encourages the next iteration to examine weaker arrivals. */

void TimeDomainGIDDecon::update_lag_weights(
    int col, const double candidate_amplitude) {
  try {
    if (GIDLagWeightPenaltyUsesAdaptiveMemory(lag_weight_penalty_function)) {
      GIDAdaptivePenaltyMetrics metrics(ApplyGIDAdaptiveMemoryPenalty(
          lag_weights, adaptive_penalty_memory, adaptive_penalty_retention,
          actual_o_fir, actual_o_0, col, lag_weight_penalty_scale_factor,
          candidate_amplitude, adaptive_penalty_noise_amplitude,
          "TimeDomainGIDDecon::update_lag_weights"));
      adaptive_penalty_last_confidence = metrics.confidence;
      adaptive_penalty_last_immediate_strength = metrics.immediate_strength;
      adaptive_penalty_last_specificity = metrics.specificity;
      adaptive_penalty_last_decay_factor = metrics.decay_factor;
      adaptive_penalty_noise_amplitude = metrics.noise_amplitude;
      adaptive_penalty_memory_linf = metrics.memory_linf;
      adaptive_penalty_memory_l2 = metrics.memory_l2;
      nwtf = metrics.effective_width;
    } else {
      ApplyGIDLagWeightPenalty(lag_weights, wtf, col);
    }
  } catch (...) {
    throw;
  };
}
double TimeDomainGIDDecon::compute_resid_linf_floor(
    const CoreSeismogram &noise) {
  try {
    /*Note - this needs an enhancement.   We should not include points
    in a padded region accounting for the inverse filter padding. */
    vector<double> amps(ThreeCAmplitudes(noise.u));
    sort(amps.begin(), amps.end());
    int floor_position;
    floor_position = static_cast<int>(resid_linf_prob * ((double)amps.size()));
    if (floor_position < 0)
      floor_position = 0;
    if (amps.empty())
      throw MsPASSError("TimeDomainGIDDecon::compute_resid_linf_floor: "
                        "noise window is empty",
                        ErrorSeverity::Invalid);
    if (floor_position >= static_cast<int>(amps.size()))
      floor_position = static_cast<int>(amps.size()) - 1;
    resid_linf_floor = amps[floor_position];
    return resid_linf_floor;
  } catch (...) {
    throw;
  };
}
/*! \brief Trim impulse response function for efficiency.

This helper trims a fir filter signal to reduce the computational cost of
time domain subtraction of the expected output signal in the generalized
iterative method.   It first computes an envelope function.   It uses a cruder
algorithm than the more conventional hilbert-transform based envelope using
smoothing of the absolute values of the fir filter amplitudes.  This was
done because the Hilbert envelope is a complicated calculation and I (glp)
didn't want to validate the required combination of a Hilbert transform code
and the secondary problem of using that to compute an envelope function.
May want to retrofit that eventually, but for the initial version I am
assuming the smoothing method will work fine on deconvolution impulse
response functions because they are mostly a near spike with ringing with a
period near that of twice the sample interval.   Hence a simple smoother
a few samples wide should create a pretty effective envelope estimate.

\param d - fir filter to be trimmed
\param floor - length will be determined from the sample where the envelope
 amplitude is peak amplitude times this value. (1/floor is a rough
 signal-to-noise floor).

\return  a copy of d shortened on both ends.
 */
CoreTimeSeries trim(const CoreTimeSeries &d, double floor = 0.005) {
  try {
    vector<double> work;
    /* First fill work with absolute values of d.s from t=0 to end */
    int i, ii, k, kk;
    int i0 = d.sample_number(0.0);
    for (i = i0; i < d.npts(); ++i)
      work.push_back(fabs(d.s[i]));
    /* Establish a smoother width from first zero crossing or small
     * absolute amplitude.*/
    double peakamp = work[0];
    for (i = i0; i < d.npts(); ++i) {
      if (d.s[i] < 0.0)
        break;
      if ((fabs(d.s[i]) / peakamp) < 0.001)
        break;
    }
    /* This should never happen, but is an escape valve. */
    if (i == (d.npts() - 1))
      return d;
    ii = i - i0;
    const int minimum_smoother_width(5);
    int smoother_width = ii - 1;
    if (smoother_width < minimum_smoother_width)
      smoother_width = minimum_smoother_width;
    /* Make sure smoother_width is odd */
    if (smoother_width % 2 == 0)
      ++smoother_width;
    /* This assumes work[0] is the peak amplitude */
    double ampfloor = work[0] * floor;
    /* We compute a crude envelope with smoothed fabs amplitudes.   Start at
    point smoother_width/2.   */
    int soffset = smoother_width / 2;
    int half_width(0);
    double avg(0.0);
    for (i = soffset, ii = soffset; ii < work.size(); ++i, ++ii) {
      for (k = 0, kk = i - soffset, avg = 0.0; k < smoother_width; ++k, ++kk)
        avg += work[kk];
      avg /= static_cast<double>(smoother_width); // mean calculation
      if (avg < ampfloor) {
        half_width = ii;
        break;
      }
    }
    if (half_width == 0) {
      return d; // In this situation just return the original
    }
    double winsize = (static_cast<double>(half_width)) * d.dt();
    TimeWindow cutwin(-winsize, winsize);
    return (WindowData(d, cutwin));
  } catch (...) {
    throw;
  };
}
void TimeDomainGIDDecon::process() {
  const string base_error("TimeDomainGIDDecon::process method:  ");
  string process_stage("initialization");
  this->invalidate_processing_state();
  ns_stop_reason = (decon_type == NS_GID) ? "running" : "not_enabled";
  gid_stop_reason = "running";
  gid_converged = false;
  try {
    if (d_all.dead() || d_all.npts() <= 0)
      throw MsPASSError(base_error + "valid data window has not been loaded",
                        ErrorSeverity::Invalid);
    if (n.dead() || n.npts() <= 0)
      throw MsPASSError(base_error + "valid noise window has not been loaded",
                        ErrorSeverity::Invalid);
    /* We first have to run the signal processing style deconvolution.
    This is defined by the base pointer available through the symbol
    preprocessor.   All those algorithms require load methods to be called
    to initiate the computation.  A complication is that the multitaper is
    different and requires a noise signal to also be loaded through loadnoise.
    That complicates this a bit below, but the flow of the algorithm should
    still be clear.   Outer loop is over the three components were we assemble
    a full 3c record.   Note this is the same algorithm use in trace_decon
    for anything but this iterative algorithm.
    */
    /* d_decon will hold the preprocessor output.  We normally expect to
    derive it by windowing of t_all.  We assume WindowData will be
    successful - constructor should guarantee that. */
    process_stage = "window input data";
    d_decon = WindowData(d_all, fftwin);
    dmatrix uwork(d_decon.u);
    uwork.zero();
    /* We assume loadnoise has been called previously to put the
    right data here. We need a scalar function to pass to the multitaper
    algorithm though. */
    if (decon_type == MULTI_TAPER) {
      process_stage = "load multitaper noise";
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
    }
    CoreTimeSeries srcwavelet;
    process_stage = "load source wavelet";
    if (external_wavelet_loaded)
      srcwavelet = CoreTimeSeries(external_wavelet);
    else
      srcwavelet = CoreTimeSeries(ExtractComponent(d_decon, 2));
    current_wavelet = TimeSeries(srcwavelet, "TimeDomainGIDDecon");
    if (decon_type == CNR) {
      process_stage = "CNR preprocessing";
      TimeSeries nwavelet;
      if (external_noise_loaded)
        nwavelet = TimeSeries(external_noise);
      else
        nwavelet =
            TimeSeries(ExtractComponent(n, noise_component),
                       "TimeDomainGIDDecon");
      cnrprocessor->initialize_inverse_operator(current_wavelet, nwavelet);
      Seismogram dwork(d_decon);
      PowerSpectrum psnoise(cnrprocessor->compute_noise_spectrum(nwavelet));
      dwork = cnrprocessor->process(dwork, psnoise, 0.02, 2.0);
      int copysize = dwork.npts();
      if (copysize > d_decon.npts())
        copysize = d_decon.npts();
      for (int k = 0; k < 3; ++k)
        cblas_dcopy(copysize, dwork.u.get_address(k, 0), 3,
                    uwork.get_address(k, 0), 3);
    } else {
      if (decon_type == NS_GID || decon_type == GROUP_SPARSE) {
        process_stage = "NS-GID load inverse-operator noise";
        NoiseStableDecon *nsop = dynamic_cast<NoiseStableDecon *>(preprocessor.get());
        if (external_noise_spectrum_loaded)
          nsop->loadnoise(external_noise_spectrum);
        else if (external_noise_loaded)
          nsop->loadnoise(external_noise);
        else {
          CoreTimeSeries nts;
          if (!ns_noise_components.empty()) {
            size_t component_index =
                min(static_cast<size_t>(noise_component),
                    ns_noise_components.size() - 1);
            nts = CoreTimeSeries(ns_noise_components[component_index].size());
            nts.s = ns_noise_components[component_index];
            nts.set_dt(n.dt());
            nts.set_t0(n.t0());
            nts.set_tref(n.timetype());
            nts.set_live();
          } else {
            nts = ExtractComponent(n, noise_component);
          }
          nsop->loadnoise(nts);
        }
      }
      for (int k = 0; k < 3; ++k) {
        process_stage = "scalar preprocessing";
        CoreTimeSeries dcomp(ExtractComponent(d_decon, k));
        /* Need the qualifier or we get the wrong overloaded
         * load method */
        preprocessor->ScalarDecon::load(srcwavelet.s, dcomp.s);
        preprocessor->process();
        vector<double> deconout(preprocessor->getresult());
        int copysize = deconout.size();
        if (copysize > d_decon.npts())
          copysize = d_decon.npts();
        cblas_dcopy(copysize, &(deconout[0]), 1, uwork.get_address(k, 0), 3);
      }
    }
    d_decon.u = uwork;
    /* The inverse wavelet and the actual output signals are determined in all
    current algorithms from srcwavelet.   Hence, what is now stored will work.
    If this is extended make sure that condition is satisfied.

    The actual output/resolution kernel is derived from the inverse operator
    already applied above.  It is used for residual updates in the original
    data domain.  Legacy GID stopping criteria also need the inverse wavelet
    to map the noise window into the inverse domain; NS-GID uses a separate
    noise-aware threshold and skips that extra inverse construction. */
    CoreTimeSeries winv;
    if (decon_type != NS_GID && decon_type != GROUP_SPARSE) {
      process_stage = "compute inverse wavelet";
      if (decon_type == CNR)
        winv = cnrprocessor->inverse_wavelet(current_wavelet, d_decon.t0());
      else
        winv = preprocessor->inverse_wavelet(d_decon.t0());
    }
    /* The actual output signal is used in the iterative
     * recursion of this algorithm.  For efficiency it is important
     * to trim the fir filter.  The call to trim does that.*/
    process_stage = "compute inverse-domain resolution kernel";
    CoreTimeSeries actual_out;
    if (decon_type == CNR)
      actual_out = cnrprocessor->actual_output(current_wavelet);
    else
      actual_out = preprocessor->actual_output();
    actual_out = trim(actual_out);
    if (actual_out.npts() > d_decon.npts() / 2) {
      TimeWindow compact_kernel(-2.0, 2.0);
      actual_out = WindowData(
          actual_out,
          ClipTimeWindowToSeries(actual_out, compact_kernel, process_stage));
    }
    int prezero_available =
        static_cast<int>(round((-fftwin.start) / d_decon.dt()));
    int postzero_available = d_decon.npts() - prezero_available - 1;
    int actual_zero = actual_out.sample_number(0.0);
    int actual_postzero = actual_out.npts() - actual_zero - 1;
    if ((actual_zero > prezero_available) ||
        (actual_postzero > postzero_available)) {
      TimeWindow compact_kernel(
          max(-2.0, -static_cast<double>(prezero_available) * d_decon.dt()),
          min(2.0, static_cast<double>(postzero_available) * d_decon.dt()));
      actual_out = WindowData(
          actual_out,
          ClipTimeWindowToSeries(actual_out, compact_kernel, process_stage));
    }
    actual_o_fir = actual_out.s;
    actual_o_0 = actual_out.sample_number(0.0);
    if (actual_o_0 < 0 || actual_o_0 >= static_cast<int>(actual_o_fir.size()))
      throw MsPASSError(base_error +
                            "actual output zero-lag sample is outside kernel",
                        ErrorSeverity::Invalid);
    double peak_scale = fabs(actual_o_fir[actual_o_0]);
    if (peak_scale <= 0.0)
      throw MsPASSError(base_error + "actual output has zero peak",
                        ErrorSeverity::Invalid);
    vector<double>::iterator aoptr;
    for (aoptr = actual_o_fir.begin(); aoptr != actual_o_fir.end(); ++aoptr)
      (*aoptr) /= peak_scale;
    if (lag_weight_penalty_function == "resolution_kernel" ||
        GIDLagWeightPenaltyUsesAdaptiveMemory(lag_weight_penalty_function)) {
      wtf = BuildGIDLagWeightPenaltyFunctionFromKernel(
          lag_weight_penalty_function, lag_weight_penalty_scale_factor,
          actual_o_fir, actual_o_0,
          "TimeDomainGIDDecon::process");
      nwtf = static_cast<int>(wtf.size());
    }
    /* This is the size of the inverse wavelet convolution transient
    we use it to prevent iterations in transient region of the deconvolved
    data */
    wavelet_pad = actual_o_fir.size();
    if (2 * wavelet_pad > ndwin) {
      stringstream ss;
      ss << base_error << "Inadequate data window size" << endl
         << "trimmed FIR filter size for actual output signal=" << wavelet_pad
         << endl
         << "Data window length=" << ndwin << endl
         << "Window size must be larger than two times FIR filter size" << endl;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    if (decon_type == NS_GID || decon_type == GROUP_SPARSE) {
      process_stage = "NS-GID estimate inverse-filtered noise threshold";
      int ns_noise_npts(0);
      if (!ns_noise_components.empty()) {
        ns_noise_npts = static_cast<int>(ns_noise_components[0].size());
        for (size_t kc = 1; kc < ns_noise_components.size(); ++kc)
          ns_noise_npts =
              min(ns_noise_npts, static_cast<int>(ns_noise_components[kc].size()));
      } else {
        ns_noise_npts = static_cast<int>(n.npts());
      }
      ns_noise_npts = min(ns_noise_npts, static_cast<int>(d_decon.npts()));
      if (ns_noise_npts <= 0)
        throw MsPASSError(base_error +
                              "NS-GID requires a nonempty noise window to "
                              "estimate candidate significance",
                          ErrorSeverity::Invalid);
      dmatrix nfiltered(3, ns_noise_npts);
      nfiltered.zero();
      for (int kc = 0; kc < 3; ++kc) {
        vector<double> ncomp;
        if (!ns_noise_components.empty()) {
          size_t component_index =
              min(static_cast<size_t>(kc), ns_noise_components.size() - 1);
          ncomp = ns_noise_components[component_index];
        } else {
          CoreTimeSeries nts(ExtractComponent(n, kc));
          ncomp = nts.s;
        }
        if (ncomp.size() > static_cast<size_t>(ns_noise_npts))
          ncomp.resize(ns_noise_npts);
        preprocessor->ScalarDecon::load(srcwavelet.s, ncomp);
        preprocessor->process();
        vector<double> deconout(preprocessor->getresult());
        int copysize =
            min(static_cast<int>(deconout.size()), ns_noise_npts);
        if (copysize > 0)
          cblas_dcopy(copysize, &(deconout[0]), 1,
                      nfiltered.get_address(kc, 0), 3);
      }
      preprocessor->ScalarDecon::load(srcwavelet.s, srcwavelet.s);
      preprocessor->process();
      vector<double> noise_amps(ThreeCAmplitudes(nfiltered));
      sort(noise_amps.begin(), noise_amps.end());
      if (!noise_amps.empty()) {
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
        ns_noise_amplitude_rms =
            sqrt(sumsq / static_cast<double>(noise_amps.size()));
        double sigma_floor = ns_peak_sigma_threshold * ns_noise_amplitude_rms;
        ns_peak_threshold =
            ns_use_empirical_noise_threshold ? max(empirical, sigma_floor)
                                             : sigma_floor;
      } else {
        ns_peak_threshold = 0.0;
        ns_noise_amplitude_rms = 0.0;
      }
      ns_noise_l2 = cblas_dnrm2(3 * ns_noise_npts,
                                nfiltered.get_address(0, 0), 1);
      adaptive_penalty_noise_amplitude =
          (ns_noise_amplitude_rms > 0.0) ? ns_noise_amplitude_rms
                                         : ns_peak_threshold;
    } else {
      process_stage = "legacy GID inverse-filter noise";
      /* Map a local copy of the noise window into the inverse domain to get the
       * levels correct for legacy GID stopping criteria. */
      CoreSeismogram nwork(sparse_convolve(winv, n));
      TimeWindow trimwin;
      trimwin.start = nwork.t0() + (nwork.dt()) * ((double)(winv.npts()));
      trimwin.end = nwork.endtime() - (nwork.dt()) * ((double)(winv.npts()));
      nwork = WindowData(nwork, trimwin);
      this->compute_resid_linf_floor(nwork);
      adaptive_penalty_noise_amplitude =
          EstimateThreeCColumnAmplitudeRMS(nwork);
      if (adaptive_penalty_noise_amplitude <= 0.0)
        adaptive_penalty_noise_amplitude = resid_linf_floor;
    }
    /* d_all now contains the deconvolved data.  Now enter the
    generalized iterative method recursion */
    int i, k;
    r = d_decon;
    spikes.clear();
    process_stage = "initialize sparse iteration";
    lag_weights.clear();
    vector<double> wamps; // weighted squared amplitudes
    wamps.reserve(r.npts());
    /* We need these iterators repeatedly in the main loop below */
    vector<double>::iterator amax;
    for (i = 0; i < r.npts(); ++i)
      lag_weights.push_back(1.0);
    for (i = 0; i < r.npts(); ++i) {
      int col0 = i - actual_o_0;
      if ((col0 < 0) || ((col0 + actual_o_fir.size()) > r.npts()))
        lag_weights[i] = 0.0;
    }
    adaptive_penalty_memory.assign(lag_weights.size(), 0.0);
    adaptive_penalty_retention.assign(lag_weights.size(), 0.0);
    /* These are initial values of convergence parameters */
    lw_linf_initial = 1.0;
    lw_l2_initial = 1.0;
    resid_linf_initial = Linf(r.u);
    resid_l2_initial = L2(r.u);
    lw_linf_prev = lw_linf_initial;
    lw_l2_prev = lw_l2_initial;
    resid_linf_prev = resid_linf_initial;
    resid_l2_prev = resid_l2_initial;
    iter_count = 0;
    ns_converged = false;
    ns_stop_reason = (decon_type == NS_GID) ? "running" : "not_enabled";
    gid_stop_reason = "running";
    gid_converged = false;
    ns_last_peak_significance = 0.0;
    if (decon_type == GROUP_SPARSE) {
      process_stage = "group-sparse regularized solve";
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
          group_sparse_active_threshold_quantile, "TimeDomainGIDDecon");
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
      RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                           ns_ridge_beta);
      spikes.remove_if([this](const ThreeCSpike &spk) {
        return spk.amp <= group_sparse_active_threshold_used;
      });
      group_sparse_active_groups = static_cast<int>(spikes.size());
      process_stage = "recompute group-sparse final residual";
      r = d_decon;
      for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
        this->update_residual_matrix(*sptr);
      resid_linf_prev = Linf(r.u);
      resid_l2_prev = L2(r.u);
      group_sparse_objective_final =
          GroupSparseObjective(r, spikes, group_sparse_lambda_used);
      group_sparse_fractional_improvement_final =
          (group_sparse_objective_initial - group_sparse_objective_final) /
          max(1.0, group_sparse_objective_initial);
      if (!lag_weights.empty()) {
        auto lwmax = max_element(lag_weights.begin(), lag_weights.end());
        lw_linf_prev = (lwmax != lag_weights.end()) ? *lwmax : 0.0;
        lw_l2_prev = cblas_dnrm2(lag_weights.size(), &(lag_weights[0]), 1);
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
    do {
      process_stage = "sparse iteration";
      /* Compute the vector of amplitudes and find the maximum */
      wamps.clear();
      for (int j = 0; j < r.npts(); ++j) {
        double amp2(0.0);
        for (k = 0; k < 3; ++k)
          amp2 += r.u(k, j) * r.u(k, j);
        wamps.push_back(amp2 * lag_weights[j] * lag_weights[j]);
      }
      amax = max_element(wamps.begin(), wamps.end());
      if (decon_type == NS_GID) {
        const int imax = distance(wamps.begin(), amax);
        double candidate_amp2(0.0);
        for (k = 0; k < 3; ++k)
          candidate_amp2 += r.u(k, imax) * r.u(k, imax);
        const double candidate_amp = sqrt(max(0.0, candidate_amp2));
        ns_last_peak_significance =
            (ns_peak_threshold > 0.0) ? candidate_amp / ns_peak_threshold
                                      : 0.0;
        if (ns_peak_threshold > 0.0 && candidate_amp < ns_peak_threshold) {
          ns_stop_reason = "candidate_not_significant";
          ns_converged = true;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
          break;
        }
        if ((ns_max_spikes > 0) &&
            (static_cast<int>(spikes.size()) >= ns_max_spikes)) {
          ns_stop_reason = "max_spikes";
          ns_converged = true;
          gid_stop_reason = ns_stop_reason;
          gid_converged = ns_converged;
          break;
        }
      }
      /* The generic distance algorithm used here returns an integer
      that would work to access amps[imax] so we can use the same index
      for the column of the data in d.u. */
      /* Save the 3c amplitude at this lag to the spike condensed
      representation of the output*/
      bool accepted(false);
      while (!accepted && (*amax > 0.0)) {
        int imax = distance(wamps.begin(), amax);
        double candidate_amp2(0.0);
        for (k = 0; k < 3; ++k)
          candidate_amp2 += r.u(k, imax) * r.u(k, imax);
        const double candidate_amp = sqrt(max(0.0, candidate_amp2));
        ThreeCSpike spk(r.u, imax);
        rescale_spike_amplitude(spk, r, actual_o_fir, actual_o_0);
        CoreSeismogram saved_r(r);
        this->update_residual_matrix(spk);
        double trial_l2 = L2(r.u);
        if (trial_l2 < resid_l2_prev) {
          spikes.push_back(spk);
          this->update_lag_weights(imax, candidate_amp);
          ++iter_count;
          accepted = true;
          if (decon_type == NS_GID && ns_refit_interval > 0 &&
              (static_cast<int>(spikes.size()) % ns_refit_interval) == 0) {
            RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                                 ns_ridge_beta);
            r = d_decon;
            for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
              this->update_residual_matrix(*sptr);
          }
        } else {
          r = saved_r;
          wamps[imax] = 0.0;
          amax = max_element(wamps.begin(), wamps.end());
        }
      }
      if (!accepted) {
        gid_stop_reason = "no_acceptable_candidate";
        gid_converged = true;
        break;
      }
    } while (this->has_not_converged());
    process_stage = "refit sparse amplitudes";
    double ridge_beta = (decon_type == NS_GID) ? ns_ridge_beta : 1.0e-10;
    RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                         ridge_beta);
    process_stage = "recompute final residual";
    r = d_decon;
    for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
      this->update_residual_matrix(*sptr);
    resid_linf_prev = Linf(r.u);
    resid_l2_prev = L2(r.u);
    if (!lag_weights.empty()) {
      auto lwmax = max_element(lag_weights.begin(), lag_weights.end());
      lw_linf_prev = (lwmax != lag_weights.end()) ? *lwmax : 0.0;
      lw_l2_prev = cblas_dnrm2(lag_weights.size(), &(lag_weights[0]), 1);
    } else {
      lw_linf_prev = 0.0;
      lw_l2_prev = 0.0;
    }
    if (decon_type == NS_GID && ns_stop_reason == "running") {
      if (iter_count >= iter_max)
        ns_stop_reason = "max_iterations";
      else
        ns_stop_reason = "converged";
      ns_converged = (iter_count < iter_max);
      gid_stop_reason = ns_stop_reason;
      gid_converged = ns_converged;
    }
    if (decon_type != NS_GID && gid_stop_reason == "running") {
      gid_stop_reason = (iter_count >= iter_max) ? "max_iterations"
                                                 : "converged";
      gid_converged = (gid_stop_reason != "max_iterations");
    }
    processed = true;
  } catch (const MsPASSError &err) {
    throw MsPASSError(base_error + "failed during " + process_stage + "\n" +
                          string(err.what()),
                      err.severity());
  } catch (...) {
    throw;
  };
}

bool TimeDomainGIDDecon::has_not_converged() {
  try {
    double lw_linf_now, lw_l2_now, resid_linf_now, resid_l2_now;
    vector<double>::iterator vptr;
    vptr = max_element(lag_weights.begin(), lag_weights.end());
    lw_linf_now = (*vptr);
    lw_l2_now = cblas_dnrm2(lag_weights.size(), &(lag_weights[0]), 1);
    resid_linf_now = Linf(r.u);
    resid_l2_now = L2(r.u);
    /* We use a standard calculation for residual l2 as fractional rms change */
    double eps;
    eps = (resid_l2_prev - resid_l2_now) / resid_l2_initial;
    ns_fractional_improvement_final = eps;
    lw_linf_prev = lw_linf_now;
    lw_l2_prev = lw_l2_now;
    resid_linf_prev = resid_linf_now;
    resid_l2_prev = resid_l2_now;
    if (decon_type == NS_GID) {
      if ((ns_max_spikes > 0) && (static_cast<int>(spikes.size()) >= ns_max_spikes)) {
        ns_stop_reason = "max_spikes";
        ns_converged = true;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
        return false;
      }
      if (ns_noise_l2 > 0.0 &&
          (resid_l2_now / ns_noise_l2) <= ns_residual_noise_ratio_floor) {
        ns_stop_reason = "residual_reached_noise_floor";
        ns_converged = true;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
        return false;
      }
      if (eps < resid_l2_tol) {
        ns_stop_reason = "fractional_improvement_floor";
        ns_converged = true;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
        return false;
      }
      if (iter_count >= iter_max) {
        ns_stop_reason = "max_iterations";
        ns_converged = false;
        gid_stop_reason = ns_stop_reason;
        gid_converged = ns_converged;
        return false;
      }
      return true;
    }
    if (lw_linf_now < lw_linf_floor) {
      gid_stop_reason = "lag_weight_linf_floor";
      gid_converged = true;
      return false;
    }
    if (lw_l2_now < lw_l2_floor) {
      gid_stop_reason = "lag_weight_l2_floor";
      gid_converged = true;
      return false;
    }
    if (resid_linf_now < resid_linf_floor) {
      gid_stop_reason = "residual_linf_floor";
      gid_converged = true;
      return false;
    }
    if (eps < resid_l2_tol) {
      gid_stop_reason = "fractional_improvement_floor";
      gid_converged = true;
      return false;
    }
    if (iter_count >= iter_max) {
      gid_stop_reason = "max_iterations";
      gid_converged = false;
      return false;
    }
    return true;
  } catch (...) {
    throw;
  };
}
CoreSeismogram TimeDomainGIDDecon::sparse_output() {
  try {
    if (!processed)
      throw MsPASSError(
          "TimeDomainGIDDecon::sparse_output: process must be called first",
          ErrorSeverity::Invalid);
    CoreSeismogram result(d_all);
    /* We will make the output the size of the processing window for the
    iteration.  May want to alter this to trim the large lag that would not
    be allowed due to wavelet duration anyway, BUT for GID method the
    wavelet should be compact enough that should be a small factor.  Hence
    for now I omit that complexity until proven to be an issue. */
    result = WindowData(result, dwin);
    result.u.zero();
    /* The spike sequences uses the time reference of the data in the
    private copy r.   This is the computed offset in samples to correct
    lags in the spikes list container to be at correct time in result */
    double dt0;
    int delta_col;
    dt0 = result.t0() - r.t0();
    delta_col = round(dt0 / r.dt());
    list<ThreeCSpike>::iterator sptr;
    int k, resultcol;
    for (sptr = spikes.begin(); sptr != spikes.end(); ++sptr) {
      resultcol = (sptr->col) - delta_col;
      if ((resultcol < 0) || (resultcol >= result.npts()))
        continue;
      for (k = 0; k < 3; ++k)
        result.u(k, resultcol) = sptr->u[k];
    }
    return result;
  } catch (...) {
    throw;
  };
}
vector<double> TimeDomainGIDDecon::lag_weight_vector() const {
  if (!processed)
    throw MsPASSError(
        "TimeDomainGIDDecon::lag_weight_vector: process must be called first",
        ErrorSeverity::Invalid);
  return lag_weights;
}
CoreSeismogram TimeDomainGIDDecon::getresult() {
  try {
    if (!processed)
      throw MsPASSError(
          "TimeDomainGIDDecon::getresult: process must be called first",
          ErrorSeverity::Invalid);
    CoreSeismogram sparse(this->sparse_output());
    CoreTimeSeries shaping(this->output_shaping_wavelet());
    CoreSeismogram shaped(sparse_convolve(shaping, sparse));
    return WindowData(shaped, dwin);
  } catch (...) {
    throw;
  };
}
Metadata TimeDomainGIDDecon::QCMetrics() {
  Metadata md;
  PutPrefixedMetadata(md, changed_leaf_metadata, "gid_leaf_");
  md.put("decon_operator", string("TimeDomainGIDDecon"));
  md.put("deconvolution_type", GIDDeconTypeName(decon_type));
  md.put("decon_processed", processed);
  md.put("decon_sample_interval", target_dt);
  md.put("decon_window_start", dwin.start);
  md.put("decon_window_end", dwin.end);
  md.put("deconvolution_window_start", fftwin.start);
  md.put("deconvolution_window_end", fftwin.end);
  md.put("noise_window_start", nwin.start);
  md.put("noise_window_end", nwin.end);
  md.put("gid_leaf_parameters_changed", leaf_parameters_changed);
  md.put("gid_processed", processed);
  md.put("gid_converged", gid_converged);
  md.put("gid_stop_reason", gid_stop_reason);
  md.put("gid_iterations", iter_count);
  md.put("gid_number_spikes", static_cast<int>(spikes.size()));
  md.put("gid_maximum_iterations", iter_max);
  if (decon_type != GROUP_SPARSE) {
    md.put("gid_penalty_function", lag_weight_penalty_function);
    md.put("gid_penalty_scale_factor", lag_weight_penalty_scale_factor);
    md.put("gid_penalty_width", lag_weight_function_width);
    md.put("gid_penalty_effective_width", nwtf);
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
  md.put("gid_external_noise_used", external_noise_loaded);
  md.put("gid_external_noise_spectrum_used",
         external_noise_spectrum_loaded);
  md.put("iteration_count", iter_count);
  md.put("residual_Linf_initial", resid_linf_initial);
  md.put("residual_Linf_final", resid_linf_prev);
  md.put("residual_L2_initial", resid_l2_initial);
  md.put("residual_L2_final", resid_l2_prev);
  md.put("lag_weight_Linf_final", lw_linf_prev);
  md.put("lag_weight_L2_final", lw_l2_prev);
  md.put("gid_actual_o_fir_npts", static_cast<int>(actual_o_fir.size()));
  md.put("gid_actual_o_fir_zero_lag_index", actual_o_0);
  md.put("gid_actual_o_fir_peak_normalized",
         processed && !actual_o_fir.empty());
  if (decon_type == GROUP_SPARSE) {
    md.put("group_sparse_enabled", true);
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
    md.put("group_sparse_noise_threshold", ns_peak_threshold);
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
    md.put("ns_gid_converged", ns_converged);
    md.put("ns_gid_iterations", iter_count);
    md.put("ns_gid_number_spikes", static_cast<int>(spikes.size()));
    md.put("ns_gid_peak_threshold", ns_peak_threshold);
    md.put("ns_gid_noise_amplitude_rms", ns_noise_amplitude_rms);
    md.put("ns_gid_last_peak_significance", ns_last_peak_significance);
    md.put("ns_gid_external_wavelet_used", external_wavelet_loaded);
    md.put("ns_gid_external_noise_used", external_noise_loaded);
    md.put("ns_gid_external_noise_spectrum_used",
           external_noise_spectrum_loaded);
    md.put("ns_gid_residual_l2_initial", resid_l2_initial);
    md.put("ns_gid_residual_l2_final", resid_l2_prev);
    md.put("ns_gid_residual_noise_ratio",
           (ns_noise_l2 > 0.0) ? resid_l2_prev / ns_noise_l2 : 0.0);
    md.put("ns_gid_fractional_improvement_final",
           ns_fractional_improvement_final);
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
