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

} // namespace

FrequencyDomainGIDDecon::FrequencyDomainGIDDecon(const AntelopePf &mdtoplevel)
    : ScalarDecon(), preprocessor(nullptr), cnrprocessor(nullptr) {
  const string base_error("FrequencyDomainGIDDecon constructor: ");
  try {
    config_pf_text = AntelopePfToText(mdtoplevel);
    AntelopePf md = mdtoplevel.get_branch("deconvolution_operator_type");
    AntelopePf mdgid = md.get_branch("frequency_domain_gid_deconvolution");
    decon_type = ParseGIDDeconType(mdgid, "FrequencyDomainGIDDecon");
    dwin = TimeWindow(mdgid.get<double>("full_data_window_start"),
                      mdgid.get<double>("full_data_window_end"));
    fftwin = TimeWindow(mdgid.get<double>("deconvolution_data_window_start"),
                        mdgid.get<double>("deconvolution_data_window_end"));
    nwin = TimeWindow(mdgid.get<double>("noise_window_start"),
                      mdgid.get<double>("noise_window_end"));
    if (fftwin.start < dwin.start || fftwin.end > dwin.end)
      throw MsPASSError(base_error +
                            "deconvolution window must be inside full window",
                        ErrorSeverity::Invalid);
    noise_component = mdgid.get<int>("noise_component");
    target_dt = mdgid.get<double>("target_sample_interval");
    int maxns = static_cast<int>((fftwin.end - fftwin.start) / target_dt) + 1;
    int nfft = nextPowerOf2(maxns);
    mdgid.put("operator_nfft", nfft);
    this->ScalarDecon::changeparameter(mdgid);
    this->shapingwavelet = ShapingWavelet(mdgid, nfft);
    iter_max = mdgid.get<int>("maximum_iterations");
    residual_ratio_floor = mdgid.get<double>("residual_ratio_floor");
    residual_improvement_floor =
        mdgid.get<double>("residual_fractional_improvement_floor");

    AntelopePf mdleaf;
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
      preprocessor = std::make_unique<MultiTaperXcorDecon>(mdleaf);
      break;
    case CNR:
      mdleaf = md.get_branch("cnr");
      ValidateGIDLeafWindow(mdleaf, fftwin, "CNR", base_error);
      cnrprocessor = std::make_unique<CNRDeconEngine>(mdleaf);
      break;
    case NS_GID:
    default:
      mdleaf = md.get_branch("ns_gid");
      ValidateGIDLeafWindow(mdleaf, fftwin, "NS-GID", base_error);
      preprocessor = std::make_unique<NoiseStableDecon>(mdleaf);
      break;
    };
    external_wavelet_loaded = false;
    external_noise_loaded = false;
    external_noise_spectrum_loaded = false;
    external_wavelet_allowed = GetBoolDefault(
        mdgid, "ns_gid_external_wavelet_allowed", true);
    ns_peak_sigma_threshold =
        GetDoubleDefault(mdgid, "ns_gid_peak_sigma_threshold", 4.0);
    ns_peak_probability_threshold = GetDoubleDefault(
        mdgid, "ns_gid_peak_probability_threshold", 0.995);
    ns_use_empirical_noise_threshold = GetBoolDefault(
        mdgid, "ns_gid_use_empirical_noise_threshold", true);
    ns_residual_noise_ratio_floor = GetDoubleDefault(
        mdgid, "ns_gid_residual_noise_ratio_floor", 1.0);
    ns_max_spikes = GetIntDefault(mdgid, "ns_gid_max_spikes", 0);
    ns_refit_interval = GetIntDefault(mdgid, "ns_gid_refit_interval", 5);
    ns_ridge_beta =
        GetDoubleDefault(mdgid, "ns_gid_ridge_beta", 1.0e-10);
    this->invalidate_processing_state();
    configuration_pickleable = true;
  } catch (...) {
    throw;
  };
}

FrequencyDomainGIDDecon::~FrequencyDomainGIDDecon() {}

void FrequencyDomainGIDDecon::invalidate_processing_state() {
  result.clear();
  spikes.clear();
  actual_o_fir.clear();
  iter_count = 0;
  resid_l2_initial = 0.0;
  resid_l2_prev = 0.0;
  resid_l2_final = 0.0;
  resid_linf_initial = 0.0;
  resid_linf_final = 0.0;
  actual_o_0 = 0;
  ns_fractional_improvement_final = 0.0;
  ns_last_peak_significance = 0.0;
  ns_peak_threshold = 0.0;
  ns_noise_l2 = 0.0;
  ns_converged = false;
  ns_stop_reason = "not_started";
  processed = false;
}

void FrequencyDomainGIDDecon::changeparameter(const Metadata &md) {
  const bool cnr_mode(decon_type == CNR);
  ValidateGIDLeafOperatorMetadata(
      md, fftwin, target_dt, "FrequencyDomainGIDDecon::changeparameter",
      cnr_mode);
  configuration_pickleable = false;
  this->invalidate_processing_state();
  if (cnr_mode)
    cnrprocessor->changeparameter(md);
  else
    preprocessor->changeparameter(md);
}

int FrequencyDomainGIDDecon::load(const CoreSeismogram &draw,
                                  TimeWindow dwin_in) {
  configuration_pickleable = false;
  this->invalidate_processing_state();
  d_all.kill();
  ndwin = 0;
  if ((dwin_in.start > fftwin.start) || (dwin_in.end < fftwin.end))
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
  configuration_pickleable = false;
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  n.kill();
  nnwin = 0;
  nwin = nwin_in;
  n = WindowData(draw, nwin);
  if (n.dead() || n.npts() <= 0)
    return 1;
  nnwin = n.npts();
  return 0;
}

int FrequencyDomainGIDDecon::loadwavelet(const TimeSeries &wavelet) {
  configuration_pickleable = false;
  this->invalidate_processing_state();
  external_wavelet_loaded = false;
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
  external_wavelet = wavelet;
  external_wavelet_loaded = true;
  return 0;
}

int FrequencyDomainGIDDecon::loadwavelet(const CoreTimeSeries &wavelet) {
  TimeSeries ts(wavelet, "FrequencyDomainGIDDecon");
  return this->loadwavelet(ts);
}

int FrequencyDomainGIDDecon::loadnoise(const TimeSeries &noise_in) {
  configuration_pickleable = false;
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  n.kill();
  nnwin = 0;
  if (noise_in.dead())
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external noise is "
                      "marked dead",
                      ErrorSeverity::Invalid);
  if (noise_in.npts() <= 0)
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external noise is "
                      "empty",
                      ErrorSeverity::Invalid);
  ValidateExternalTimeSeriesSampleInterval(
      noise_in, target_dt, "FrequencyDomainGIDDecon::loadnoise");
  external_noise = noise_in;
  external_noise_loaded = true;
  external_noise_spectrum_loaded = false;
  n = CoreSeismogram(noise_in.npts());
  n.set_t0(noise_in.t0());
  n.set_dt(noise_in.dt());
  n.set_live();
  n.set_tref(noise_in.timetype());
  for (int k = 0; k < 3; ++k)
    cblas_dcopy(noise_in.npts(), &(noise_in.s[0]), 1, n.u.get_address(k, 0), 3);
  nnwin = n.npts();
  return 0;
}

int FrequencyDomainGIDDecon::loadnoise(const CoreTimeSeries &noise_in) {
  TimeSeries ts(noise_in, "FrequencyDomainGIDDecon");
  return this->loadnoise(ts);
}

int FrequencyDomainGIDDecon::loadnoise(const PowerSpectrum &noise_spectrum_in) {
  configuration_pickleable = false;
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  if (decon_type != NS_GID)
    throw MsPASSError("FrequencyDomainGIDDecon::loadnoise: external "
                      "PowerSpectrum noise is only supported for ns_gid; pass "
                      "a TimeSeries noise estimate for multi_taper or use the "
                      "configured noise window for other GID modes",
                      ErrorSeverity::Invalid);
  ValidatePowerSpectrumCoversDC(noise_spectrum_in,
                                "FrequencyDomainGIDDecon::loadnoise");
  external_noise_spectrum = noise_spectrum_in;
  external_noise_spectrum_loaded = true;
  external_noise_loaded = false;
  return 0;
}
void FrequencyDomainGIDDecon::clear_external_wavelet() {
  configuration_pickleable = false;
  external_wavelet_loaded = false;
  this->invalidate_processing_state();
}
void FrequencyDomainGIDDecon::clear_external_noise() {
  configuration_pickleable = false;
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  this->invalidate_processing_state();
}

int FrequencyDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                                  TimeWindow nwin) {
  configuration_pickleable = false;
  this->invalidate_processing_state();
  d_all.kill();
  n.kill();
  ndwin = 0;
  nnwin = 0;
  if ((dwin.start > fftwin.start) || (dwin.end < fftwin.end))
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
  if (external_wavelet_loaded)
    srcwavelet = CoreTimeSeries(external_wavelet);
  else
    srcwavelet = CoreTimeSeries(ExtractComponent(d_decon, 2));
  current_wavelet = TimeSeries(srcwavelet, "FrequencyDomainGIDDecon");
  if (decon_type == CNR) {
    TimeSeries nwavelet(ExtractComponent(n, noise_component),
                        "FrequencyDomainGIDDecon");
    cnrprocessor->initialize_inverse_operator(current_wavelet, nwavelet);
    PowerSpectrum psnoise(cnrprocessor->compute_noise_spectrum(nwavelet));
    Seismogram dwork(d_decon);
    dwork = cnrprocessor->process(dwork, psnoise, 0.02, 2.0);
    int copysize = dwork.npts();
    if (copysize > d_decon.npts())
      copysize = d_decon.npts();
    for (int k = 0; k < 3; ++k)
      cblas_dcopy(copysize, dwork.u.get_address(k, 0), 3,
                  uwork.get_address(k, 0), 3);
  } else {
    if (decon_type == MULTI_TAPER) {
      if (external_noise_loaded) {
        dynamic_cast<MultiTaperXcorDecon *>(preprocessor.get())
            ->loadnoise(external_noise.s);
      } else {
        CoreTimeSeries nts(ExtractComponent(n, noise_component));
        dynamic_cast<MultiTaperXcorDecon *>(preprocessor.get())->loadnoise(nts.s);
      }
    } else if (decon_type == NS_GID) {
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
    preprocessor->ScalarDecon::load(srcwavelet.s, srcwavelet.s);
    preprocessor->process();
    for (int k = 0; k < 3; ++k) {
      CoreTimeSeries dcomp(ExtractComponent(d_decon, k));
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

  CoreTimeSeries actual_out;
  if (decon_type == CNR)
    actual_out = cnrprocessor->actual_output(current_wavelet);
  else
    actual_out = preprocessor->actual_output();
  if (actual_out.npts() > d_decon.npts() / 2) {
    TimeWindow compact_kernel(-2.0, 2.0);
    actual_out = WindowData(actual_out, compact_kernel);
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
}

CoreTimeSeries FrequencyDomainGIDDecon::ideal_output() {
  return this->ScalarDecon::output_shaping_wavelet();
}

CoreTimeSeries FrequencyDomainGIDDecon::actual_output() {
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
  if (decon_type != NS_GID)
    return 0.0;
  CoreSeismogram nwork(n);
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
  double rms = sqrt(sumsq / static_cast<double>(noise_amps.size()));
  ns_noise_l2 = matrix_l2(nwork.u);
  return ns_use_empirical_noise_threshold ? max(empirical,
                                                ns_peak_sigma_threshold * rms)
                                          : ns_peak_sigma_threshold * rms;
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
}

void FrequencyDomainGIDDecon::process() {
  const string base_error("FrequencyDomainGIDDecon::process: ");
  string process_stage("initialize inverse operator");
  this->invalidate_processing_state();
  ns_stop_reason = (decon_type == NS_GID) ? "running" : "not_enabled";
  try {
    if (d_all.dead() || d_all.npts() <= 0)
      throw MsPASSError(base_error + "valid data window has not been loaded",
                        ErrorSeverity::Invalid);
    if (n.dead() || n.npts() <= 0)
      throw MsPASSError(base_error + "valid noise window has not been loaded",
                        ErrorSeverity::Invalid);
    this->initialize_inverse_operator();
    process_stage = "compute NS-GID peak threshold";
    ns_peak_threshold = this->compute_ns_peak_threshold();
    process_stage = "initialize residual";
    r = d_decon;
    spikes.clear();
    iter_count = 0;
    ns_converged = false;
    ns_stop_reason = (decon_type == NS_GID) ? "running" : "not_enabled";
    ns_last_peak_significance = 0.0;
    resid_l2_initial = matrix_l2(r.u);
    resid_linf_initial = matrix_linf(r.u);
    resid_l2_prev = resid_l2_initial;
    if (resid_l2_initial <= 0.0)
      throw MsPASSError(base_error + "input data residual is zero",
                        ErrorSeverity::Invalid);
    vector<double> amps;
    amps.reserve(r.npts());
    process_stage = "sparse iteration";
    for (int iiter = 0; iiter < iter_max; ++iiter) {
      amps.clear();
      for (int i = 0; i < r.npts(); ++i) {
        int col0 = i - actual_o_0;
        if ((col0 < 0) || ((col0 + actual_o_fir.size()) > r.npts())) {
          amps.push_back(0.0);
        } else {
          double amp2(0.0);
          for (int k = 0; k < 3; ++k)
            amp2 += r.u(k, i) * r.u(k, i);
          amps.push_back(amp2);
        }
      }
      auto amax = max_element(amps.begin(), amps.end());
      if ((amax == amps.end()) || (*amax <= 0.0))
        break;
      if (decon_type == NS_GID) {
        double candidate_amp = sqrt(max(0.0, *amax));
        ns_last_peak_significance =
            (ns_peak_threshold > 0.0) ? candidate_amp / ns_peak_threshold
                                      : 0.0;
        if (ns_peak_threshold > 0.0 && candidate_amp < ns_peak_threshold) {
          ns_stop_reason = "candidate_not_significant";
          ns_converged = true;
          break;
        }
        if ((ns_max_spikes > 0) &&
            (static_cast<int>(spikes.size()) >= ns_max_spikes)) {
          ns_stop_reason = "max_spikes";
          ns_converged = true;
          break;
        }
      }
      int imax = distance(amps.begin(), amax);
      bool accepted(false);
      while (!accepted && (*amax > 0.0)) {
        imax = distance(amps.begin(), amax);
        ThreeCSpike spk(r.u, imax);
        this->rescale_spike(spk);
        CoreSeismogram saved_r(r);
        this->update_residual_matrix(spk);
        double trial_l2 = matrix_l2(r.u);
        if (trial_l2 < resid_l2_prev) {
          spikes.push_back(spk);
          iter_count = iiter + 1;
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
          amps[imax] = 0.0;
          amax = max_element(amps.begin(), amps.end());
        }
      }
      if (!accepted)
        break;
      resid_l2_final = matrix_l2(r.u);
      resid_linf_final = matrix_linf(r.u);
      double ratio = resid_l2_final / resid_l2_initial;
      double improvement = (resid_l2_prev - resid_l2_final) / resid_l2_initial;
      ns_fractional_improvement_final = improvement;
      resid_l2_prev = resid_l2_final;
      if (decon_type == NS_GID && ns_noise_l2 > 0.0 &&
          (resid_l2_final / ns_noise_l2) <= ns_residual_noise_ratio_floor) {
        ns_stop_reason = "residual_reached_noise_floor";
        ns_converged = true;
        break;
      }
      if (ratio <= residual_ratio_floor) {
        if (decon_type == NS_GID) {
          ns_stop_reason = "residual_ratio_floor";
          ns_converged = true;
        }
        break;
      }
      if (improvement <= residual_improvement_floor) {
        if (decon_type == NS_GID) {
          ns_stop_reason = "fractional_improvement_floor";
          ns_converged = true;
        }
        break;
      }
    }
    if (decon_type == NS_GID && ns_stop_reason == "running") {
      if (iter_count >= iter_max)
        ns_stop_reason = "max_iterations";
      else {
        ns_stop_reason = "converged";
        ns_converged = true;
      }
    }
    process_stage = "refit sparse amplitudes";
    double ridge_beta = (decon_type == NS_GID) ? ns_ridge_beta : 1.0e-10;
    RefitSpikeAmplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                         ridge_beta);
    process_stage = "recompute final residual";
    r = d_decon;
    for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
      this->update_residual_matrix(*sptr);
    resid_l2_final = matrix_l2(r.u);
    resid_linf_final = matrix_linf(r.u);
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
  result = WindowData(result, dwin);
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

CoreSeismogram FrequencyDomainGIDDecon::getresult() {
  if (!processed)
    throw MsPASSError(
        "FrequencyDomainGIDDecon::getresult: process must be called first",
        ErrorSeverity::Invalid);
  CoreSeismogram sparse(this->sparse_output());
  CoreTimeSeries shaping(this->output_shaping_wavelet());
  CoreSeismogram shaped(sparse_convolve(shaping, sparse));
  return WindowData(shaped, dwin);
}

Metadata FrequencyDomainGIDDecon::QCMetrics() {
  Metadata md;
  md.put("gid_processed", processed);
  md.put("iteration_count", iter_count);
  md.put("residual_Linf_initial", resid_linf_initial);
  md.put("residual_Linf_final", resid_linf_final);
  md.put("residual_L2_initial", resid_l2_initial);
  md.put("residual_L2_final", resid_l2_final);
  md.put("ns_gid_enabled", decon_type == NS_GID);
  if (decon_type == NS_GID) {
    md.put("ns_gid_stop_reason", ns_stop_reason);
    md.put("ns_gid_converged", ns_converged);
    md.put("ns_gid_iterations", iter_count);
    md.put("ns_gid_number_spikes", static_cast<int>(spikes.size()));
    md.put("ns_gid_peak_threshold", ns_peak_threshold);
    md.put("ns_gid_last_peak_significance", ns_last_peak_significance);
    md.put("ns_gid_external_wavelet_used", external_wavelet_loaded);
    md.put("ns_gid_residual_l2_initial", resid_l2_initial);
    md.put("ns_gid_residual_l2_final", resid_l2_final);
    md.put("ns_gid_residual_noise_ratio",
           (ns_noise_l2 > 0.0) ? resid_l2_final / ns_noise_l2 : 0.0);
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
