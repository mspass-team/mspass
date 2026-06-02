#include "mspass/algorithms/deconvolution/FrequencyDomainGIDDecon.h"
#include "gsl/gsl_cblas.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/algorithms/deconvolution/LeastSquareDecon.h"
#include "mspass/algorithms/deconvolution/MultiTaperXcorDecon.h"
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
IterDeconType parse_frequency_gid_type(const AntelopePf &md) {
  string sval = md.get_string("deconvolution_type");
  if (sval == "water_level")
    return WATER_LEVEL;
  if (sval == "least_square")
    return LEAST_SQ;
  if (sval == "multi_taper")
    return MULTI_TAPER;
  if ((sval == "cnr") || (sval == "cnr3c"))
    return CNR;
  throw MsPASSError("FrequencyDomainGIDDecon: unknown deconvolution_type=" +
                        sval,
                    ErrorSeverity::Invalid);
}

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

vector<double> padded_vector(const vector<double> &x, const int n) {
  vector<double> result(n, 0.0);
  const int ncopy = min(static_cast<int>(x.size()), n);
  for (int i = 0; i < ncopy; ++i)
    result[i] = x[i];
  return result;
}

vector<double> solve_dense_system_fd(vector<vector<double>> a,
                                     vector<double> b) {
  const int n = b.size();
  for (int i = 0; i < n; ++i) {
    int pivot = i;
    double pivot_abs = fabs(a[i][i]);
    for (int r = i + 1; r < n; ++r) {
      double candidate = fabs(a[r][i]);
      if (candidate > pivot_abs) {
        pivot = r;
        pivot_abs = candidate;
      }
    }
    if (pivot_abs <= 0.0)
      continue;
    if (pivot != i) {
      swap(a[i], a[pivot]);
      swap(b[i], b[pivot]);
    }
    double diag = a[i][i];
    for (int c = i; c < n; ++c)
      a[i][c] /= diag;
    b[i] /= diag;
    for (int r = 0; r < n; ++r) {
      if (r == i)
        continue;
      double factor = a[r][i];
      if (factor == 0.0)
        continue;
      for (int c = i; c < n; ++c)
        a[r][c] -= factor * a[i][c];
      b[r] -= factor * b[i];
    }
  }
  return b;
}

void refit_spike_amplitudes_fd(list<ThreeCSpike> &spikes,
                               const CoreSeismogram &target,
                               const vector<double> &actual_o_fir,
                               const int actual_o_0) {
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
      double gij(0.0);
      for (int p = 0; p < actual_o_fir.size(); ++p) {
        int target_col = col0_i + p;
        int q = target_col - col0_j;
        if ((target_col >= 0) && (target_col < target.npts()) && (q >= 0) &&
            (q < actual_o_fir.size()))
          gij += actual_o_fir[p] * actual_o_fir[q];
      }
      gram[i][j] = gij;
      gram[j][i] = gij;
    }
  }
  double maxdiag(0.0);
  for (int i = 0; i < nspikes; ++i)
    maxdiag = max(maxdiag, fabs(gram[i][i]));
  double damping = maxdiag * 1.0e-10;
  for (int i = 0; i < nspikes; ++i)
    gram[i][i] += damping;
  for (int component = 0; component < 3; ++component) {
    vector<double> rhs(nspikes, 0.0);
    for (int i = 0; i < nspikes; ++i) {
      int col0 = spike_ptrs[i]->col - actual_o_0;
      for (int p = 0; p < actual_o_fir.size(); ++p) {
        int target_col = col0 + p;
        if ((target_col >= 0) && (target_col < target.npts()))
          rhs[i] += actual_o_fir[p] * target.u(component, target_col);
      }
    }
    vector<double> amps = solve_dense_system_fd(gram, rhs);
    for (int i = 0; i < nspikes; ++i)
      spike_ptrs[i]->u[component] = amps[i];
  }
  for (auto &spk : spikes)
    spk.amp =
        sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
}
} // namespace

FrequencyDomainGIDDecon::FrequencyDomainGIDDecon(const AntelopePf &mdtoplevel)
    : ScalarDecon(), preprocessor(nullptr), cnrprocessor(nullptr) {
  const string base_error("FrequencyDomainGIDDecon constructor: ");
  try {
    AntelopePf md = mdtoplevel.get_branch("deconvolution_operator_type");
    AntelopePf mdgid = md.get_branch("frequency_domain_gid_deconvolution");
    decon_type = parse_frequency_gid_type(mdgid);
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
    double target_dt = mdgid.get<double>("target_sample_interval");
    int maxns = static_cast<int>((fftwin.end - fftwin.start) / target_dt) + 1;
    /* This top-level size is for GID output bookkeeping and shaping-wavelet
     * construction.  Scalar inverse-operator preprocessors use their own
     * linear FFT padding, and inverse_filter_residual allocates a local padded
     * work buffer sized to the actual convolution it performs. */
    nfft = nextPowerOf2(maxns);
    mdgid.put("operator_nfft", nfft);
    this->ScalarDecon::changeparameter(mdgid);
    shapingwavelet = ShapingWavelet(mdgid, nfft);
    iter_max = mdgid.get<int>("maximum_iterations");
    residual_ratio_floor = mdgid.get<double>("residual_ratio_floor");
    residual_improvement_floor =
        mdgid.get<double>("residual_fractional_improvement_floor");

    AntelopePf mdleaf;
    switch (decon_type) {
    case WATER_LEVEL:
      mdleaf = md.get_branch("water_level");
      preprocessor = new WaterLevelDecon(mdleaf);
      break;
    case LEAST_SQ:
      mdleaf = md.get_branch("least_square");
      preprocessor = new LeastSquareDecon(mdleaf);
      break;
    case MULTI_TAPER:
      mdleaf = md.get_branch("multi_taper");
      preprocessor = new MultiTaperXcorDecon(mdleaf);
      break;
    case CNR:
    default:
      mdleaf = md.get_branch("cnr");
      cnrprocessor = new CNRDeconEngine(mdleaf);
      break;
    };
  } catch (...) {
    throw;
  };
}

FrequencyDomainGIDDecon::~FrequencyDomainGIDDecon() {
  delete preprocessor;
  delete cnrprocessor;
}

void FrequencyDomainGIDDecon::changeparameter(const Metadata &md) {
  if (decon_type == CNR)
    cnrprocessor->changeparameter(md);
  else
    preprocessor->changeparameter(md);
}

int FrequencyDomainGIDDecon::load(const CoreSeismogram &draw,
                                  TimeWindow dwin_in) {
  dwin = dwin_in;
  d_all = WindowData(draw, dwin);
  ndwin = d_all.npts();
  return 0;
}

int FrequencyDomainGIDDecon::loadnoise(const CoreSeismogram &draw,
                                       TimeWindow nwin_in) {
  nwin = nwin_in;
  n = WindowData(draw, nwin);
  nnwin = n.npts();
  return 0;
}

int FrequencyDomainGIDDecon::load(const CoreSeismogram &draw, TimeWindow dwin,
                                  TimeWindow nwin) {
  int iretn = this->loadnoise(draw, nwin);
  int iretd = this->load(draw, dwin);
  return iretn + iretd;
}

void FrequencyDomainGIDDecon::initialize_inverse_operator() {
  d_decon = WindowData(d_all, fftwin);
  dmatrix uwork(d_decon.u);
  uwork.zero();
  CoreTimeSeries srcwavelet(ExtractComponent(d_decon, 2));
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
      CoreTimeSeries nts(ExtractComponent(n, noise_component));
      dynamic_cast<MultiTaperXcorDecon *>(preprocessor)->loadnoise(nts.s);
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

  CoreTimeSeries actual_out(this->actual_output());
  if (actual_out.npts() > d_decon.npts() / 2) {
    TimeWindow compact_kernel(-2.0, 2.0);
    actual_out = WindowData(actual_out, compact_kernel);
  }
  actual_o_fir = actual_out.s;
  actual_o_0 = actual_out.sample_number(0.0);
  double peak_scale = fabs(actual_o_fir[actual_o_0]);
  if (peak_scale <= 0.0)
    throw MsPASSError("FrequencyDomainGIDDecon: actual output has zero peak",
                      ErrorSeverity::Invalid);
  for (auto &x : actual_o_fir)
    x /= peak_scale;
}

CoreTimeSeries FrequencyDomainGIDDecon::ideal_output() {
  if (decon_type == CNR)
    return cnrprocessor->ideal_output();
  return preprocessor->ideal_output();
}

CoreTimeSeries FrequencyDomainGIDDecon::actual_output() {
  if (decon_type == CNR)
    return cnrprocessor->actual_output(current_wavelet);
  return preprocessor->actual_output();
}

CoreTimeSeries FrequencyDomainGIDDecon::inverse_wavelet() {
  return this->inverse_wavelet(0.0);
}

CoreTimeSeries FrequencyDomainGIDDecon::inverse_wavelet(double t0parent) {
  if (decon_type == CNR)
    return cnrprocessor->inverse_wavelet(current_wavelet, t0parent);
  return preprocessor->inverse_wavelet(t0parent);
}

dmatrix FrequencyDomainGIDDecon::inverse_filter_residual(
    const CoreSeismogram &residual) {
  CoreTimeSeries winv_ts(this->inverse_wavelet(residual.t0()));
  const int conv_nfft = nextPowerOf2(winv_ts.npts() + residual.npts() - 1);
  vector<double> winv_work = padded_vector(winv_ts.s, conv_nfft);
  ComplexArray winv_fft(conv_nfft, &(winv_work[0]));
  gsl_fft_complex_wavetable *wavetable =
      gsl_fft_complex_wavetable_alloc(conv_nfft);
  gsl_fft_complex_workspace *workspace =
      gsl_fft_complex_workspace_alloc(conv_nfft);
  gsl_fft_complex_forward(winv_fft.ptr(), 1, conv_nfft, wavetable, workspace);

  dmatrix candidate(3, residual.npts());
  candidate.zero();
  for (int k = 0; k < 3; ++k) {
    vector<double> rwork(conv_nfft, 0.0);
    for (int i = 0; i < residual.npts(); ++i)
      rwork[i] = residual.u(k, i);
    ComplexArray rfft(conv_nfft, &(rwork[0]));
    gsl_fft_complex_forward(rfft.ptr(), 1, conv_nfft, wavetable, workspace);
    ComplexArray gout = winv_fft * rfft;
    gsl_fft_complex_inverse(gout.ptr(), 1, conv_nfft, wavetable, workspace);
    for (int i = 0; i < residual.npts(); ++i)
      candidate(k, i) = gout[i].real();
  }
  gsl_fft_complex_wavetable_free(wavetable);
  gsl_fft_complex_workspace_free(workspace);
  return candidate;
}

void FrequencyDomainGIDDecon::rescale_spike(ThreeCSpike &spk) {
  int col0 = spk.col - actual_o_0;
  double denom(0.0);
  for (double x : actual_o_fir)
    denom += x * x;
  if (denom <= 0.0)
    return;
  for (int k = 0; k < 3; ++k) {
    double num(0.0);
    for (int i = 0; i < actual_o_fir.size(); ++i) {
      int col = col0 + i;
      if ((col >= 0) && (col < r.npts()))
        num += actual_o_fir[i] * r.u(k, col);
    }
    spk.u[k] = num / denom;
  }
  spk.amp =
      sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
}

void FrequencyDomainGIDDecon::update_residual_matrix(const ThreeCSpike &spk) {
  int col0 = spk.col - actual_o_0;
  for (int k = 0; k < 3; ++k) {
    for (int i = 0; i < actual_o_fir.size(); ++i) {
      int col = col0 + i;
      if ((col >= 0) && (col < r.npts()))
        r.u(k, col) -= spk.u[k] * actual_o_fir[i];
    }
  }
}

void FrequencyDomainGIDDecon::process() {
  const string base_error("FrequencyDomainGIDDecon::process: ");
  try {
    this->initialize_inverse_operator();
    r = d_decon;
    spikes.clear();
    iter_count = 0;
    resid_l2_initial = matrix_l2(r.u);
    resid_linf_initial = matrix_linf(r.u);
    resid_l2_prev = resid_l2_initial;
    if (resid_l2_initial <= 0.0)
      throw MsPASSError(base_error + "input data residual is zero",
                        ErrorSeverity::Invalid);
    for (int iiter = 0; iiter < iter_max; ++iiter) {
      dmatrix candidate(r.u);
      vector<double> amps;
      amps.reserve(r.npts());
      for (int i = 0; i < r.npts(); ++i) {
        int col0 = i - actual_o_0;
        if ((col0 < 0) || ((col0 + actual_o_fir.size()) >= r.npts())) {
          amps.push_back(0.0);
        } else {
          double amp2(0.0);
          for (int k = 0; k < 3; ++k)
            amp2 += candidate(k, i) * candidate(k, i);
          amps.push_back(sqrt(amp2));
        }
      }
      auto amax = max_element(amps.begin(), amps.end());
      if ((amax == amps.end()) || (*amax <= 0.0))
        break;
      int imax = distance(amps.begin(), amax);
      bool accepted(false);
      while (!accepted && (*amax > 0.0)) {
        imax = distance(amps.begin(), amax);
        ThreeCSpike spk(candidate, imax);
        this->rescale_spike(spk);
        CoreSeismogram saved_r(r);
        this->update_residual_matrix(spk);
        double trial_l2 = matrix_l2(r.u);
        if (trial_l2 < resid_l2_prev) {
          spikes.push_back(spk);
          iter_count = iiter + 1;
          accepted = true;
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
      resid_l2_prev = resid_l2_final;
      if ((ratio <= residual_ratio_floor) ||
          (improvement <= residual_improvement_floor))
        break;
    }
    refit_spike_amplitudes_fd(spikes, d_decon, actual_o_fir, actual_o_0);
    r = d_decon;
    for (auto sptr = spikes.begin(); sptr != spikes.end(); ++sptr)
      this->update_residual_matrix(*sptr);
    resid_l2_final = matrix_l2(r.u);
    resid_linf_final = matrix_linf(r.u);
  } catch (...) {
    throw;
  };
}

CoreSeismogram FrequencyDomainGIDDecon::getresult() {
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
  /* The inverse operator used to build the residual has already shaped the
   * candidate RF.  Do not apply the shaping wavelet a second time. */
  return result;
}

Metadata FrequencyDomainGIDDecon::QCMetrics() {
  Metadata md;
  md.put("iteration_count", iter_count);
  md.put("residual_Linf_initial", resid_linf_initial);
  md.put("residual_Linf_final", resid_linf_final);
  md.put("residual_L2_initial", resid_l2_initial);
  md.put("residual_L2_final", resid_l2_final);
  return md;
}
} // namespace mspass::algorithms::deconvolution
