#include "mspass/algorithms/deconvolution/TimeDomainGIDDecon.h"
#include "gsl/gsl_cblas.h"
#include "mspass/algorithms/algorithms.h"
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
#include <math.h>
#include <list>
#include "misc/blas.h"
#include <vector>
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using namespace mspass::algorithms;

IterDeconType parse_for_itertype(const AntelopePf &md) {
  string sval = md.get_string("deconvolution_type");
  if (sval == "water_level")
    return WATER_LEVEL;
  else if (sval == "least_square")
    return LEAST_SQ;
  else if (sval == "multi_taper")
    return MULTI_TAPER;
  else if ((sval == "cnr") || (sval == "cnr3c"))
    return CNR;
  else if ((sval == "ns_gid") || (sval == "noise_stable") ||
           (sval == "noise_aware_stable"))
    return NS_GID;
  else
    throw MsPASSError("TimeDomainGIDDecon: unknown or illegal value of "
                      "deconvolution_type parameter=" +
                          sval,
                      ErrorSeverity::Invalid);
}
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
double get_double_default_gid(const Metadata &md, const string &key,
                              const double default_value) {
  if (md.is_defined(key))
    return md.get_double(key);
  return default_value;
}
int get_int_default_gid(const Metadata &md, const string &key,
                        const int default_value) {
  if (md.is_defined(key))
    return md.get_int(key);
  return default_value;
}
bool get_bool_default_gid(const Metadata &md, const string &key,
                          const bool default_value) {
  if (md.is_defined(key))
    return md.get_bool(key);
  return default_value;
}
TimeDomainGIDDecon::TimeDomainGIDDecon(const AntelopePf &mdtoplevel)
    : ScalarDecon() {
  const string base_error("TimeDomainGIDDecon AntelopePf contructor:  ");
  stringstream ss; // used for constructing error messages
  /* The pf used for initializing this object has Antelope Arr section
  for each algorithm.   Since the generalized iterative method is a
  two-stage algorithm we have a section for the iterative algorithm
  and a (variable) sectin for the preprocessor algorithm.  We use
  the AntelopePf to parse this instead of raw antelope pfget
  C calls. */
  try {
    AntelopePf md = mdtoplevel.get_branch("deconvolution_operator_type");
    AntelopePf mdgiter = md.get_branch("time_domain_gid_deconvolution");
    IterDeconType dct = parse_for_itertype(mdgiter);
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
    double target_dt = mdgiter.get<double>("target_sample_interval");
    int maxns = static_cast<int>((fftwin.end - fftwin.start) / target_dt);
    ++maxns; // Add one - points not intervals
    int nfft = nextPowerOf2(maxns);
    /* This should override this even if it was previously set */
    mdgiter.put("operator_nfft", nfft);
    this->ScalarDecon::changeparameter(mdgiter);
    AntelopePf mdleaf;
    /* We make sure the window parameters in each algorithm mactch what
    is set for this algorithm.  Abort if they are not consistent.  The
    test code is a bit repetitious but a necessary evil to allow the message
    to be clearer. */
    int n1, n2; // temporaries used below -  needed because declrations illegal
                // inside case
    preprocessor = nullptr;
    cnrprocessor = nullptr;
    external_wavelet_loaded = false;
    external_noise_loaded = false;
    external_noise_spectrum_loaded = false;
    switch (decon_type) {
    case WATER_LEVEL:
      mdleaf = md.get_branch("water_level");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "water level method specification of processing window is not"
              " consistent with gid parameters"
           << endl
           << "water level parameters:  deconvolution_data_window_start=" << ts
           << ", decon_window_end=" << te << endl
           << "GID parameters: decon_window_start=" << fftwin.start
           << ", decon_window_end=" << fftwin.end << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = new WaterLevelDecon(mdleaf);
      break;
    case LEAST_SQ:
      mdleaf = md.get_branch("least_square");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "least square method specification of processing window is not"
              " consistent with gid parameters"
           << endl
           << "least square parameters:  deconvolution_data_window_start=" << ts
           << ", decon_window_end=" << te << endl
           << "GID parameters: decon_window_start=" << fftwin.start
           << ", decon_window_end=" << fftwin.end << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = new LeastSquareDecon(mdleaf);
      break;
    case MULTI_TAPER:
      mdleaf = md.get_branch("multi_taper");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "mulittaper method specification of processing window is not"
              " consistent with gid parameters"
           << endl
           << "multitaper parameters:  deconvolution_data_window_start=" << ts
           << ", decon_window_end=" << te << endl
           << "GID parameters: decon_window_start=" << fftwin.start
           << ", decon_window_end=" << fftwin.end << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      /* Here we also have to test the noise parameters, but the gid
      window can be different fromt that passed to the mulittaper method.
      Hence we test only that the mulittaper noise window is within the bounds
      of the gid noise window */
      n1 = static_cast<int>((fftwin.end - fftwin.start) / target_dt) + 1;
      n2 = static_cast<int>((nwin.end - nwin.start) / target_dt) + 1;
      if (n1 > n2) {
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
      preprocessor = new MultiTaperXcorDecon(mdleaf);
      break;
    case CNR:
      mdleaf = md.get_branch("cnr");
      cnrprocessor = new CNRDeconEngine(mdleaf);
      break;
    case NS_GID:
    default:
      mdleaf = md.get_branch("ns_gid");
      ts = mdleaf.get<double>("deconvolution_data_window_start");
      te = mdleaf.get<double>("deconvolution_data_window_end");
      if ((ts != fftwin.start) || (te != fftwin.end)) {
        ss << base_error
           << "NS-GID inverse operator window is not consistent with gid "
              "parameters"
           << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
      preprocessor = new NoiseStableDecon(mdleaf);
      break;
    };
    /* Because this may evolve we make this a private method to
    make changes easier to implement. */
    this->construct_weight_penalty_function(mdgiter);
    /* Set convergencd parameters from md keys */
    iter_max = mdgiter.get<int>("maximum_iterations");
    lw_linf_floor = mdgiter.get<double>("lag_weight_Linf_floor");
    lw_l2_floor = mdgiter.get<double>("lag_weight_rms_floor");
    resid_linf_prob =
        mdgiter.get<double>("residual_noise_rms_probability_floor");
    resid_l2_tol = mdgiter.get<double>("residual_fractional_improvement_floor");
    ns_peak_sigma_threshold =
        get_double_default_gid(mdgiter, "ns_gid_peak_sigma_threshold", 4.0);
    ns_peak_probability_threshold = get_double_default_gid(
        mdgiter, "ns_gid_peak_probability_threshold", 0.995);
    ns_use_empirical_noise_threshold = get_bool_default_gid(
        mdgiter, "ns_gid_use_empirical_noise_threshold", true);
    ns_residual_noise_ratio_floor = get_double_default_gid(
        mdgiter, "ns_gid_residual_noise_ratio_floor", 1.0);
    ns_max_spikes = get_int_default_gid(mdgiter, "ns_gid_max_spikes", 0);
    ns_refit_interval = get_int_default_gid(mdgiter, "ns_gid_refit_interval", 1);
    ns_ridge_beta =
        get_double_default_gid(mdgiter, "ns_gid_ridge_beta", 1.0e-10);
    external_wavelet_allowed = get_bool_default_gid(
        mdgiter, "ns_gid_external_wavelet_allowed", true);
    ns_peak_threshold = 0.0;
    ns_last_peak_significance = 0.0;
    ns_noise_l2 = 0.0;
    ns_fractional_improvement_final = 0.0;
    ns_converged = false;
    ns_stop_reason = "not_started";
  } catch (...) {
    throw;
  };
}
TimeDomainGIDDecon::~TimeDomainGIDDecon() {
  delete preprocessor;
  delete cnrprocessor;
}

CoreTimeSeries TimeDomainGIDDecon::ideal_output() {
  if (decon_type == CNR)
    return cnrprocessor->ideal_output();
  return preprocessor->ideal_output();
}

CoreTimeSeries TimeDomainGIDDecon::actual_output() {
  if (decon_type == CNR)
    return cnrprocessor->actual_output(current_wavelet);
  return preprocessor->actual_output();
}

CoreTimeSeries TimeDomainGIDDecon::inverse_wavelet() {
  return this->inverse_wavelet(0.0);
}

CoreTimeSeries TimeDomainGIDDecon::inverse_wavelet(double t0parent) {
  if (decon_type == CNR)
    return cnrprocessor->inverse_wavelet(current_wavelet, t0parent);
  return preprocessor->inverse_wavelet(t0parent);
}

void TimeDomainGIDDecon::construct_weight_penalty_function(const Metadata &md) {
  try {
    const string base_error(
        "TimeDomainGIDDecon::construct_weight_penalty_function:  ");
    int i;
    /* All options use this scale factor */
    double wtf_scale = md.get<double>("lag_weight_penalty_scale_factor");
    if ((wtf_scale <= 0) || (wtf_scale > 1.0))
      throw MsPASSError(
          base_error +
              "Illegal value for parameter lag_weight_penalty_scale_factor\n" +
              "Must be a number in the interval (0,1]",
          ErrorSeverity::Invalid);
    /* This keyword defines the options for defining the penalty function */
    string wtf_type = md.get_string("lag_weight_penalty_function");
    /* Most options use this parameter so we set it outside the
    conditional.  With usual use of pffiles this should not be a big issue.
    */
    wtf.clear();
    nwtf = md.get<int>("lag_weight_function_width");
    /* nwtf must be forced to be an odd number to force the function to
    be symmetric. */
    if ((nwtf % 2) == 0)
      ++nwtf;
    if (wtf_type == "boxcar") {
      for (i = 0; i < nwtf; ++i)
        wtf.push_back(wtf_scale);
    } else if (wtf_type == "cosine_taper") {
      /* This creates one cycle of cosine function with wavelength(period)
      of nwtf, offset by 0.5 and scaled in amplitude.   That means it tapers to
      1 at edges and has a minimum value of 1-wtf_scale. */
      double period = (double)(nwtf + 1); // set period so points one left and
                                          // right (1) can be dropped
      for (i = 0; i < nwtf; ++i) {
        double f;
        f = 0.5 * (-cos(2.0 * M_PI * ((double)(i + 1)) / period));
        f += 0.5;
        f = 1.0 - wtf_scale * f; // This makes minimum f=1-wtf_scale
        /* Avoid negatives */
        if (f < 0)
          f = 0.0;
        wtf.push_back(f);
      }
    } else if (wtf_type == "shaping_wavelet") {
      /* In this method we use the points in the wavelet at 1/2 max.
      We extract it from the shaping wavelet - the TimeSeries is baggage
      but not an efficiency issue since this is called only once */
      CoreTimeSeries ir = this->preprocessor->actual_output();
      /* assume the wavelet is symmetric and get the half max sample position
      from positive side only */
      int i0 = ir.sample_number(0.0);
      double peakirval = ir.s[i0];
      double halfmax = peakirval / 2.0;
      for (i = i0; i < ir.npts(); ++i) {
        if (ir.s[i] < halfmax)
          break;
      }
      int halfwidth = i;
      int peakwidth = 2 * i + 1; // guaranteed odd numbers
      for (i = i0 - halfwidth; i < (i0 + peakwidth); ++i) {
        double f;
        f = ir.s[i];
        f /= peakirval; // make sure scaled so peaks is 1.0
        f *= wtf_scale;
        f = 1.0 - f;
        wtf.push_back(f);
      }
    } else {
      throw MsPASSError(
          base_error +
              "illegal value for parameter lag_weight_penalty_function=" +
              wtf_type,
          ErrorSeverity::Invalid);
    }
  } catch (...) {
    throw;
  };
}
/* Some helpers for new implementation.*/
/* This procedure returns a vector of 3c amplitudes from a dmatrix extracted
from a Seismogram. */
vector<double> amp3c(dmatrix &d) {
  vector<double> result;
  int ncol = d.columns();
  int i, k;
  for (i = 0; i < ncol; ++i) {
    double mag;
    for (k = 0, mag = 0.0; k < 3; ++k) {
      mag += d(k, i) * d(k, i);
    }
    result.push_back(sqrt(mag));
  }
  return result;
}

double fir_self_overlap(const vector<double> &fir, const int col0_i,
                        const int col0_j, const int ncols) {
  const int nf = static_cast<int>(fir.size());
  const int offset = col0_i - col0_j;
  const int p_start = max({0, -col0_i, -offset});
  const int p_end = min({nf, ncols - col0_i, nf - offset});
  const int n = p_end - p_start;
  if (n <= 0)
    return 0.0;
  return cblas_ddot(n, &(fir[p_start]), 1, &(fir[p_start + offset]), 1);
}

double fir_data_overlap(const vector<double> &fir, const CoreSeismogram &target,
                        const int component, const int col0) {
  const int nf = static_cast<int>(fir.size());
  const int p_start = max(0, -col0);
  const int p_end = min(nf, static_cast<int>(target.npts()) - col0);
  const int n = p_end - p_start;
  if (n <= 0)
    return 0.0;
  return cblas_ddot(n, &(fir[p_start]), 1,
                    target.u.get_address(component, col0 + p_start), 3);
}

vector<double> solve_dense_system(const vector<vector<double>> &a,
                                  const vector<double> &b) {
  const int n = b.size();
  vector<double> result(n, 0.0);
  if (n <= 0)
    return result;
  vector<double> A(n * n, 0.0);
  vector<double> B(b);
  for (int row = 0; row < n; ++row) {
    for (int col = 0; col < n; ++col)
      A[col * n + row] = a[row][col];
  }
  int nrhs = 1;
  int n_lapack = n;
  int lda = n;
  int ldb = n;
  int info = 0;
  char lower = 'L';
  dpotrf(&lower, n_lapack, &(A[0]), lda, info);
  if (info == 0) {
    n_lapack = n;
    dpotrs(&lower, n_lapack, nrhs, &(A[0]), lda, &(B[0]), ldb, info);
    if (info == 0)
      return B;
  }

  for (int row = 0; row < n; ++row) {
    B[row] = b[row];
    for (int col = 0; col < n; ++col)
      A[col * n + row] = a[row][col];
  }
  vector<int> ipiv(n, 0);
  n_lapack = n;
  dgesv(n_lapack, nrhs, &(A[0]), lda, &(ipiv[0]), &(B[0]), ldb, info);
  if (info == 0)
    result = B;
  return result;
}

void refit_spike_amplitudes(list<ThreeCSpike> &spikes,
                            const CoreSeismogram &target,
                            const vector<double> &actual_o_fir,
                            const int actual_o_0,
                            const double ridge_beta = 1.0e-10) {
  const int nspikes = spikes.size();
  if (nspikes <= 0)
    return;
  vector<ThreeCSpike *> spike_ptrs;
  spike_ptrs.reserve(nspikes);
  for (auto &spk : spikes)
    spike_ptrs.push_back(&spk);
  vector<vector<double>> gram(nspikes, vector<double>(nspikes, 0.0));
  for (int i = 0; i < nspikes; ++i) {
    int col0_i = spike_ptrs[i]->col - actual_o_0;
    for (int j = i; j < nspikes; ++j) {
      int col0_j = spike_ptrs[j]->col - actual_o_0;
      double gij =
          fir_self_overlap(actual_o_fir, col0_i, col0_j, target.npts());
      gram[i][j] = gij;
      gram[j][i] = gij;
    }
  }
  double maxdiag(0.0);
  for (int i = 0; i < nspikes; ++i)
    maxdiag = max(maxdiag, fabs(gram[i][i]));
  double damping = maxdiag * ridge_beta;
  for (int i = 0; i < nspikes; ++i)
    gram[i][i] += damping;
  for (int component = 0; component < 3; ++component) {
    vector<double> rhs(nspikes, 0.0);
    for (int i = 0; i < nspikes; ++i) {
      int col0 = spike_ptrs[i]->col - actual_o_0;
      rhs[i] = fir_data_overlap(actual_o_fir, target, component, col0);
    }
    vector<double> amps = solve_dense_system(gram, rhs);
    for (int i = 0; i < nspikes; ++i)
      spike_ptrs[i]->u[component] = amps[i];
  }
  for (auto &spk : spikes)
    spk.amp =
        sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
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
    double num = fir_data_overlap(actual_o_fir, target, k, col0);
    spk.u[k] = num / denom;
  }
  spk.amp =
      sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
}

int TimeDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin_in) {
  try {
    dwin = dwin_in;
    /* First we load the requested window.  Note we MUST always make this window
    a bit larger than the range of desired lags as the iterative algorithm will
    not allow lags at the edges (defined by a construction parameter
    wavelet_pad)
    */
    d_all = WindowData(draw, dwin);
    ndwin = d_all.npts();
    return 0;
  } catch (...) {
    throw;
  };
}
int TimeDomainGIDDecon::loadnoise(const CoreSeismogram &draw,
                                TimeWindow nwin_in) {
  try {
    nwin = nwin_in;
    n = WindowData(draw, nwin);
    nnwin = n.npts();
    if (decon_type == NS_GID) {
      ns_noise_components.clear();
      ns_noise_components.reserve(3);
      for (int k = 0; k < 3; ++k) {
        CoreTimeSeries ncomp(ExtractComponent(draw, k));
        ncomp = WindowData(ncomp, nwin);
        ns_noise_components.push_back(ncomp.s);
      }
      return 0;
    }
    double ret = this->compute_resid_linf_floor();
    if (ret > 0)
      return 0;
    else
      return 1;
  } catch (...) {
    throw;
  };
}
int TimeDomainGIDDecon::loadwavelet(const TimeSeries &wavelet) {
  if (!external_wavelet_allowed)
    throw MsPASSError("TimeDomainGIDDecon::loadwavelet: external wavelets are "
                      "disabled by ns_gid_external_wavelet_allowed",
                      ErrorSeverity::Invalid);
  external_wavelet = wavelet;
  external_wavelet_loaded = true;
  return 0;
}
int TimeDomainGIDDecon::loadwavelet(const CoreTimeSeries &wavelet) {
  TimeSeries ts(wavelet, "TimeDomainGIDDecon");
  return this->loadwavelet(ts);
}
int TimeDomainGIDDecon::loadnoise(const TimeSeries &noise_in) {
  external_noise = noise_in;
  external_noise_loaded = true;
  external_noise_spectrum_loaded = false;
  ns_noise_components.assign(3, noise_in.s);
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
int TimeDomainGIDDecon::loadnoise(const CoreTimeSeries &noise_in) {
  TimeSeries ts(noise_in, "TimeDomainGIDDecon");
  return this->loadnoise(ts);
}
int TimeDomainGIDDecon::loadnoise(const PowerSpectrum &noise_spectrum_in) {
  external_noise_spectrum = noise_spectrum_in;
  external_noise_spectrum_loaded = true;
  external_noise_loaded = false;
  ns_noise_components.clear();
  return 0;
}
int TimeDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                           TimeWindow nwin) {
  try {
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
    if ((col0 + actual_o_fir.size()) >= ncol)
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
/* This method adds (not multiply add) the weighting function created by
the constructor centered at lag = col.  Because a range can be hit multiple
times we test for negatives and zero them in the loop.   This is also
we we use an explicit loop instead ofa call to daxpy as in the residual
update method.  Note like update_residual_matrix we assume nwtf is
correct and don't test for memory faults for efficiency  */

void TimeDomainGIDDecon::update_lag_weights(int col) {
  try {
    int i, ii;
    int first_col = col - nwtf / 2;
    for (i = 0, ii = first_col; i < nwtf; ++i, ++ii) {
      if ((ii < 0) || (ii >= lag_weights.size()))
        continue;
      lag_weights[ii] -= wtf[i];
      if (lag_weights[ii] < 0.0)
        lag_weights[ii] = 0;
    }
  } catch (...) {
    throw;
  };
}
double TimeDomainGIDDecon::compute_resid_linf_floor() {
  try {
    /*Note - this needs an enhancement.   We should not include points
    in a padd region accounting for the inverse filter padding. */
    vector<double> amps(amp3c(n.u));
    sort(amps.begin(), amps.end());
    int floor_position;
    floor_position = static_cast<int>(resid_linf_prob * ((double)amps.size()));
    if (floor_position < 0)
      floor_position = 0;
    if (floor_position >= amps.size())
      floor_position = amps.size() - 1;
    resid_linf_floor = amps[floor_position];
    return resid_linf_floor;
  } catch (...) {
    throw;
  };
}
/*! \brief Trim impulse response function for efficiency.

This helper trims a fir filter signal to reduce the computational cost of
time domain subtraction of the expected output signal in the generalized
iterative method.   It first computes an envlope function.   It uses a cruder
algorithm than the more conventional hilbert-transform based envelope using
smoothing of the absolute values of the fir filter amplitudes.  This was
done because the hilbert envelope is a complicated calculation and I (glp)
didn't want debug the required combination of a hilbert transform code
and the secondary problem of using that to compute an envelope function.
May want to retrofit that eventually, but for the initial version I am
assuming the smoothing method will work fine on deconvolution impulse
response functions because the are mostly a near spike with ringing with a
period near that of twice the sample interval.   Hence a simple smoother
a few samples wide should create a pretty effective envelope estimate.

\param d - fir filter to be trimmed
\param floor - length will be determined from sample here the envelope
 amplitude is peak amplitude times this value. (1/floor is kind of an
 snf floor).

\return  a copy of d shortened on both ends.
 */
CoreTimeSeries trim(const CoreTimeSeries &d, double floor = 0.005) {
  try {
    vector<double> work;
    /* First till work with absolute values of d.s from t=0 to end */
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
      /* Consider deleting this message if we confirm the assumption about
      this algorithm stated above is valid - smoother algorithm works because
      the oscillations in are high frequency */
      cerr << "TimeDomainGIDDecon::trim method (WARNING):  "
           << "trim algorithm failed. " << endl
           << "Minimum amplitude at ends of impulse response function=" << avg
           << endl
           << "Trim floor argument specified=" << floor << endl;
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
  try {
    /* We first have to run the signal processing style deconvolution.
    This is defined by the base pointer available through the symbol
    preprocessor.   All those altorithms require load methods to be called
    to initate the computation.  A complication is that the multitaper is
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
    /* We assume loadnoise has been called previously to set put the
    right data here. We need a scalar function to pass to the multtitaper
    algorithm though. */
    if (decon_type == MULTI_TAPER) {
      process_stage = "load multitaper noise";
      CoreTimeSeries nts(ExtractComponent(n, noise_component));
      dynamic_cast<MultiTaperXcorDecon *>(preprocessor)->loadnoise(nts.s);
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
      TimeSeries nwavelet(ExtractComponent(n, noise_component),
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
      if (decon_type == NS_GID) {
        process_stage = "NS-GID load inverse-operator noise";
        NoiseStableDecon *nsop = dynamic_cast<NoiseStableDecon *>(preprocessor);
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

    We extract the inverse filter and use a time domain convolution with
    data in the longer time window.   Note for efficiency may want to
    convert this to a frequency domain convolution if it proves to be
    a bottleneck */
    process_stage = "compute inverse wavelet";
    CoreTimeSeries winv = this->inverse_wavelet(d_decon.t0());
    /* The actual output signal is used in the iterative
     * recursion of this algorithm.  For efficiency it is important
     * to trim the fir filter.  The call to trim does that.*/
    process_stage = "compute actual output wavelet";
    CoreTimeSeries actual_out(this->actual_output());
    actual_out = trim(actual_out);
    if (actual_out.npts() > d_decon.npts() / 2) {
      TimeWindow compact_kernel(-2.0, 2.0);
      actual_out = WindowData(actual_out, compact_kernel);
    }
    int prezero_available =
        static_cast<int>(round((-fftwin.start) / d_decon.dt()));
    int postzero_available = d_decon.npts() - prezero_available - 1;
    int actual_zero = actual_out.sample_number(0.0);
    int actual_postzero = actual_out.npts() - actual_zero - 1;
    if ((actual_zero > prezero_available) ||
        (actual_postzero > postzero_available)) {
      TimeWindow compact_kernel(-2.0, 2.0);
      actual_out = WindowData(actual_out, compact_kernel);
    }
    actual_o_fir = actual_out.s;
    actual_o_0 = actual_out.sample_number(0.0);
    double peak_scale = actual_o_fir[actual_o_0];
    vector<double>::iterator aoptr;
    for (aoptr = actual_o_fir.begin(); aoptr != actual_o_fir.end(); ++aoptr)
      (*aoptr) /= peak_scale;
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
    if (decon_type == NS_GID) {
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
      vector<double> noise_amps(amp3c(nfiltered));
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
        double rms = sqrt(sumsq / static_cast<double>(noise_amps.size()));
        double sigma_floor = ns_peak_sigma_threshold * rms;
        ns_peak_threshold =
            ns_use_empirical_noise_threshold ? max(empirical, sigma_floor)
                                             : sigma_floor;
      } else {
        ns_peak_threshold = 0.0;
      }
      ns_noise_l2 = cblas_dnrm2(3 * ns_noise_npts,
                                nfiltered.get_address(0, 0), 1);
    } else {
      process_stage = "legacy GID inverse-filter noise";
      /* Replace n by convolution with inverse wavelet to get the levels
       * correct for legacy GID stopping criteria. */
      n = sparse_convolve(winv, n);
      TimeWindow trimwin;
      trimwin.start = n.t0() + (n.dt()) * ((double)(winv.npts()));
      trimwin.end = n.endtime() - (n.dt()) * ((double)(winv.npts()));
      n = WindowData(n, trimwin);
    }
    // double nfloor;
    // nfloor=compute_resid_linf_floor();
    // DEBUG - for debug always print this.  Should be a verbose option
    // cerr << "Computed noise floor="<<nfloor<<endl;

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
      if ((col0 < 0) || ((col0 + actual_o_fir.size()) >= r.npts()))
        lag_weights[i] = 0.0;
    }
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
    ns_stop_reason = "running";
    ns_last_peak_significance = 0.0;
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
        const double candidate_amp = sqrt(max(0.0, *amax));
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
      /* The generic distance algorithm used here returns an integer
      that would work to access amps[imax] so we can use the same index
      for the column of the data in d.u. */
      /* Save the 3c amplitude at this lag to the spike condensed
      respresentation of the output*/
      bool accepted(false);
      while (!accepted && (*amax > 0.0)) {
        int imax = distance(wamps.begin(), amax);
        ThreeCSpike spk(r.u, imax);
        rescale_spike_amplitude(spk, r, actual_o_fir, actual_o_0);
        CoreSeismogram saved_r(r);
        this->update_residual_matrix(spk);
        double trial_l2 = L2(r.u);
        if (trial_l2 < resid_l2_prev) {
          spikes.push_back(spk);
          this->update_lag_weights(imax);
          ++iter_count;
          accepted = true;
          if (decon_type == NS_GID && ns_refit_interval > 0 &&
              (static_cast<int>(spikes.size()) % ns_refit_interval) == 0) {
            refit_spike_amplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
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
      if (!accepted)
        break;
    } while (this->has_not_converged());
    process_stage = "refit sparse amplitudes";
    double ridge_beta = (decon_type == NS_GID) ? ns_ridge_beta : 1.0e-10;
    refit_spike_amplitudes(spikes, d_decon, actual_o_fir, actual_o_0,
                           ridge_beta);
    process_stage = "recompute final residual";
    r = d_decon;
    for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
      this->update_residual_matrix(*sptr);
    resid_linf_prev = Linf(r.u);
    resid_l2_prev = L2(r.u);
    if (decon_type == NS_GID && ns_stop_reason == "running") {
      if (iter_count >= iter_max)
        ns_stop_reason = "max_iterations";
      else
        ns_stop_reason = "converged";
      ns_converged = (iter_count < iter_max);
    }
    if ((decon_type != NS_GID) && iter_count >= iter_max)
      throw MsPASSError("TimeDomainGIDDecon::process did not converge",
                        ErrorSeverity::Suspect);
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
      if (iter_count >= iter_max) {
        ns_stop_reason = "max_iterations";
        return false;
      }
      if ((ns_max_spikes > 0) && (static_cast<int>(spikes.size()) >= ns_max_spikes)) {
        ns_stop_reason = "max_spikes";
        ns_converged = true;
        return false;
      }
      if (ns_noise_l2 > 0.0 &&
          (resid_l2_now / ns_noise_l2) <= ns_residual_noise_ratio_floor) {
        ns_stop_reason = "residual_reached_noise_floor";
        ns_converged = true;
        return false;
      }
      if (eps < resid_l2_tol) {
        ns_stop_reason = "fractional_improvement_floor";
        return false;
      }
      return true;
    }
    if (iter_count > iter_max) {
      return false;
    }
    if (lw_linf_now < lw_linf_floor) {
      return false;
    }
    if (lw_l2_now < lw_l2_floor) {
      return false;
    }
    if (resid_linf_now < resid_linf_floor) {
      return false;
    }
    if (eps < resid_l2_tol) {
      return false;
    }
    return true;
  } catch (...) {
    throw;
  };
}
CoreSeismogram TimeDomainGIDDecon::getresult() {
  try {
    string base_error("TimeDomainGIDDecon::getresult:  ");
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
Metadata TimeDomainGIDDecon::QCMetrics() {
  Metadata md;
  md.put("iteration_count", iter_count);
  md.put("residual_Linf_initial", resid_linf_initial);
  md.put("residual_Linf_final", resid_linf_prev);
  md.put("residual_L2_initial", resid_l2_initial);
  md.put("residual_L2_final", resid_l2_prev);
  md.put("lag_weight_Linf_final", lw_linf_prev);
  md.put("lag_weight_L2_final", lw_l2_prev);
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
    md.put("ns_gid_residual_l2_final", resid_l2_prev);
    md.put("ns_gid_residual_noise_ratio",
           (ns_noise_l2 > 0.0) ? resid_l2_prev / ns_noise_l2 : 0.0);
    md.put("ns_gid_fractional_improvement_final",
           ns_fractional_improvement_final);
    NoiseStableDecon *nsop = dynamic_cast<NoiseStableDecon *>(preprocessor);
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
