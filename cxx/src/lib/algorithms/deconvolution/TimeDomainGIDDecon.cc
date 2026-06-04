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
    target_dt = mdgiter.get<double>("target_sample_interval");
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
    switch (decon_type) {
    case WATER_LEVEL:
      mdleaf = md.get_branch("water_level");
      ValidateGIDLeafWindow(mdleaf, fftwin, "water level", base_error);
      preprocessor = new WaterLevelDecon(mdleaf);
      break;
    case LEAST_SQ:
      mdleaf = md.get_branch("least_square");
      ValidateGIDLeafWindow(mdleaf, fftwin, "least square", base_error);
      preprocessor = new LeastSquareDecon(mdleaf);
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
      preprocessor = new MultiTaperXcorDecon(mdleaf);
      break;
    case CNR:
      mdleaf = md.get_branch("cnr");
      ValidateGIDLeafWindow(mdleaf, fftwin, "CNR", base_error);
      cnrprocessor = new CNRDeconEngine(mdleaf);
      break;
    case NS_GID:
    default:
      mdleaf = md.get_branch("ns_gid");
      ValidateGIDLeafWindow(mdleaf, fftwin, "NS-GID", base_error);
      preprocessor = new NoiseStableDecon(mdleaf);
      break;
    };
    /* Because this may evolve we make this a private method to
    make changes easier to implement. */
    this->construct_weight_penalty_function(mdgiter);
    /* Set convergence parameters from md keys */
    iter_max = mdgiter.get<int>("maximum_iterations");
    lw_linf_floor = mdgiter.get<double>("lag_weight_Linf_floor");
    lw_l2_floor = mdgiter.get<double>("lag_weight_rms_floor");
    resid_linf_prob =
        mdgiter.get<double>("residual_noise_rms_probability_floor");
    resid_l2_tol = mdgiter.get<double>("residual_fractional_improvement_floor");
    ns_peak_sigma_threshold =
        GetDoubleDefault(mdgiter, "ns_gid_peak_sigma_threshold", 4.0);
    ns_peak_probability_threshold = GetDoubleDefault(
        mdgiter, "ns_gid_peak_probability_threshold", 0.995);
    ns_use_empirical_noise_threshold = GetBoolDefault(
        mdgiter, "ns_gid_use_empirical_noise_threshold", true);
    ns_residual_noise_ratio_floor = GetDoubleDefault(
        mdgiter, "ns_gid_residual_noise_ratio_floor", 1.0);
    ns_max_spikes = GetIntDefault(mdgiter, "ns_gid_max_spikes", 0);
    ns_refit_interval = GetIntDefault(mdgiter, "ns_gid_refit_interval", 5);
    ns_ridge_beta =
        GetDoubleDefault(mdgiter, "ns_gid_ridge_beta", 1.0e-10);
    external_wavelet_allowed = GetBoolDefault(
        mdgiter, "ns_gid_external_wavelet_allowed", true);
    this->invalidate_processing_state();
  } catch (...) {
    throw;
  };
}
TimeDomainGIDDecon::~TimeDomainGIDDecon() {
  delete preprocessor;
  delete cnrprocessor;
}

void TimeDomainGIDDecon::invalidate_processing_state() {
  result.clear();
  spikes.clear();
  lag_weights.clear();
  actual_o_fir.clear();
  actual_o_0 = 0;
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
  ns_converged = false;
  ns_stop_reason = "not_started";
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
      throw MsPASSError(
          base_error +
              "lag_weight_penalty_function=shaping_wavelet is disabled.  "
              "The actual output/resolution kernel is data dependent and is "
              "not available during construction; use cosine_taper or boxcar.",
          ErrorSeverity::Invalid);
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
    nwin = nwin_in;
    n = WindowData(draw, nwin);
    if (n.dead() || n.npts() <= 0)
      return 1;
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
    this->compute_resid_linf_floor();
    return 0;
  } catch (...) {
    throw;
  };
}
int TimeDomainGIDDecon::loadwavelet(const TimeSeries &wavelet) {
  this->invalidate_processing_state();
  external_wavelet_loaded = false;
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
  external_wavelet = wavelet;
  external_wavelet_loaded = true;
  return 0;
}
int TimeDomainGIDDecon::loadwavelet(const CoreTimeSeries &wavelet) {
  TimeSeries ts(wavelet, "TimeDomainGIDDecon");
  return this->loadwavelet(ts);
}
int TimeDomainGIDDecon::loadnoise(const TimeSeries &noise_in) {
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  ns_noise_components.clear();
  n.kill();
  nnwin = 0;
  if (noise_in.dead())
    throw MsPASSError("TimeDomainGIDDecon::loadnoise: external noise is "
                      "marked dead",
                      ErrorSeverity::Invalid);
  if (noise_in.npts() <= 0)
    throw MsPASSError("TimeDomainGIDDecon::loadnoise: external noise is empty",
                      ErrorSeverity::Invalid);
  ValidateExternalTimeSeriesSampleInterval(
      noise_in, target_dt, "TimeDomainGIDDecon::loadnoise");
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
  this->invalidate_processing_state();
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  ns_noise_components.clear();
  if (decon_type != NS_GID)
    throw MsPASSError("TimeDomainGIDDecon::loadnoise: external PowerSpectrum "
                      "noise is only supported for ns_gid; pass a TimeSeries "
                      "noise estimate for multi_taper or use the configured "
                      "noise window for other GID modes",
                      ErrorSeverity::Invalid);
  ValidatePowerSpectrumCoversDC(noise_spectrum_in,
                                "TimeDomainGIDDecon::loadnoise");
  external_noise_spectrum = noise_spectrum_in;
  external_noise_spectrum_loaded = true;
  external_noise_loaded = false;
  ns_noise_components.clear();
  return 0;
}
void TimeDomainGIDDecon::clear_external_wavelet() {
  external_wavelet_loaded = false;
  this->invalidate_processing_state();
}
void TimeDomainGIDDecon::clear_external_noise() {
  external_noise_loaded = false;
  external_noise_spectrum_loaded = false;
  ns_noise_components.clear();
  this->invalidate_processing_state();
}
int TimeDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                           TimeWindow nwin) {
  try {
    this->invalidate_processing_state();
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
    vector<double> amps(ThreeCAmplitudes(n.u));
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
    /* We assume loadnoise has been called previously to set put the
    right data here. We need a scalar function to pass to the multitaper
    algorithm though. */
    if (decon_type == MULTI_TAPER) {
      process_stage = "load multitaper noise";
      if (external_noise_loaded) {
        dynamic_cast<MultiTaperXcorDecon *>(preprocessor)
            ->loadnoise(external_noise.s);
      } else {
        CoreTimeSeries nts(ExtractComponent(n, noise_component));
        dynamic_cast<MultiTaperXcorDecon *>(preprocessor)->loadnoise(nts.s);
      }
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

    The actual output/resolution kernel is derived from the inverse operator
    already applied above.  It is used for residual updates in the original
    data domain.  Legacy GID stopping criteria also need the inverse wavelet
    to map the noise window into the inverse domain; NS-GID uses a separate
    noise-aware threshold and skips that extra inverse construction. */
    CoreTimeSeries winv;
    if (decon_type != NS_GID) {
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
      nnwin = n.npts();
      this->compute_resid_linf_floor();
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
      representation of the output*/
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
      if (!accepted)
        break;
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
  md.put("gid_processed", processed);
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
