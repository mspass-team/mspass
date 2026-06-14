#include "mspass/algorithms/deconvolution/MultiTaperSpecDivDecon.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
#include "mspass/algorithms/deconvolution/dpss.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/utility.h"
#include <algorithm>
#include <cfloat>
#include <cmath>
#include <string>
#include <vector>
/* this include is local to this directory*/
#include "mspass/algorithms/deconvolution/common_multitaper.h"
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using mspass::algorithms::amplitudes::normalize;

MultiTaperSpecDivDecon::MultiTaperSpecDivDecon(const Metadata &md)
    : FFTDeconOperator(md), ScalarDecon(md) {

  try {
    this->read_metadata(md, false);
  } catch (...) {
    throw;
  };
  /* assume tapers matrix is created in read_metadata.   We call reserve
  on the three stl vector containers for efficiency. */
  ScalarDecon::data.reserve(nfft);
  ScalarDecon::wavelet.reserve(nfft);
  noise.reserve(nfft);
}

MultiTaperSpecDivDecon::MultiTaperSpecDivDecon(
    const MultiTaperSpecDivDecon &parent)
    : FFTDeconOperator(parent), ScalarDecon(parent), noise(parent.noise),
      tapers(parent.tapers), winv_taper(parent.winv_taper),
      ao_fft(parent.ao_fft), rfestimates(parent.rfestimates) {
  nw = parent.nw;
  nseq = parent.nseq;
  damp = parent.damp;
  taperlen = parent.taperlen;
}
int MultiTaperSpecDivDecon::read_metadata(const Metadata &md, bool refresh) {
  try {
    const string base_error("MultiTaperSpecDivDecon::read_metadata method: ");
    int i, j, ii;
    /* We use these temporaries to test for changes when we are not
    initializing */
    int nfft_old, nseq_old, tl_old;
    double nw_old;
    if (refresh) {
      nfft_old = nfft;
      nseq_old = nseq;
      tl_old = taperlen;
      nw_old = nw;
    }
    taperlen = ComputeTaperLength(md);
    int nfft_from_win = ComputeFFTLength(md);
    // window based nfft always overrides that extracted directly from md */
    if (nfft_from_win != nfft) {
      this->change_size(nfft_from_win);
    }
    sample_shift = ComputeDeconSampleShift(md);
    if (sample_shift < 0)
      throw MsPASSError(base_error +
                            "illegal sample_shift parameter - must be ge 0",
                        ErrorSeverity::Fatal);
    if (sample_shift > nfft)
      throw MsPASSError(base_error +
                            "computed sample_shift exceeds length of fft",
                        ErrorSeverity::Fatal);
    damp = GetDoubleRequired(md, "damping_factor");
    if (damp <= 0.0) {
      throw MsPASSError(base_error +
                            "damping_factor must be positive to regularize "
                            "multitaper spectral division.",
                        ErrorSeverity::Fatal);
    }
    nw = GetDoubleRequired(md, "time_bandwidth_product");
    /* Wang originally had this as nw*2-2 but Park and Levin say
    the maximum is nw*2-1 which we use here.  P&L papers all use mw=2.5
    with K(seql here) of 3 */
    // seql=md.get_int("lower_dpss");
    nseq = GetIntRequired(md, "number_tapers");
    int nseqtest = static_cast<int>(2.0 * nw);
    if (nseq > nseqtest || (nseq < 1)) {
      throw MsPASSError(base_error +
                            "number_tapers must be between 1 and "
                            "2*time_bandwidth_product",
                        ErrorSeverity::Fatal);
    }
    int seql = nseq - 1;
    bool parameters_changed(false);
    if (refresh) {
      if ((nfft != nfft_old) || (nseq != nseq_old) || (taperlen != tl_old) ||
          (nw != nw_old))
        parameters_changed = true;
    }
    /* The shaping wavelet can change independently of the DPSS parameters, so
     * rebuild it on every parameter refresh. */
    shapingwavelet = ShapingWavelet(md, nfft);
    /* Odd negative logic here.   Idea is to always call this section
    with a constructor, but bypass it when called in refresh mode
    if we don't need to recompute the slepian functions */
    if ((!refresh) || parameters_changed) {
      vector<double> work(nseq * taperlen, 0.0);
      /* This procedure allows selection of Slepian tapers over a range
      from seql to sequ.  We always want the first nseq values, so
      set them as follows */
      seql = 0;
      int sequ = nseq - 1;
      dpss_calc(taperlen, nw, seql, sequ, &(work[0]));
      /* The tapers are stored in row order in work.  We preserve that
      here but use the dmatrix to store the values as transpose*/
      tapers = dmatrix(nseq, taperlen);
      // vector<double> norms;
      for (i = 0, ii = 0; i < nseq; ++i) {
        for (j = 0; j < taperlen; ++j) {
          tapers(i, j) = work[ii];
          ++ii;
        }
      }
    }
    return 0;
  } catch (...) {
    throw;
  };
}
int MultiTaperSpecDivDecon::loadnoise(const vector<double> &n) {
  const string base_error("MultiTaperSpecDivDecon::loadnoise: ");
  if (n.empty())
    throw MsPASSError(base_error + "noise vector cannot be empty",
                      ErrorSeverity::Invalid);
  if (n.size() > taperlen)
    throw MsPASSError(base_error +
                          "noise vector is longer than taperlen; multitaper "
                          "inputs must not exceed the configured taper length",
                      ErrorSeverity::Invalid);
  /* For this implementation we insist n be the same length
   * as d (assumed taperlen) to avoid constant recomputing slepians. */
  if (n.size() == taperlen)
    noise = n;
  else {
    int nn = n.size();
    int k;
    noise.clear();
    for (k = 0; k < static_cast<int>(taperlen); ++k)
      noise.push_back(0.0);
    /* This zero pads noise on the right when input series length is short. */
    for (k = 0; k < nn; ++k)
      noise[k] = n[k];
  }
  return 0;
}
int MultiTaperSpecDivDecon::load(const vector<double> &w,
                                 const vector<double> &d,
                                 const vector<double> &n) {
  try {
    const string base_error("MultiTaperSpecDivDecon::load: ");
    if (w.size() > taperlen || d.size() > taperlen)
      throw MsPASSError(
          base_error +
              "wavelet/data vectors are longer than taperlen; multitaper "
              "inputs must not exceed the configured taper length",
          ErrorSeverity::Invalid);
    int lnr = this->loadnoise(n);
    int ldr;
    ldr = this->ScalarDecon::load(w, d);
    return (lnr + ldr);
  } catch (...) {
    throw;
  };
}
MultiTaperSpecDivDecon::MultiTaperSpecDivDecon(const Metadata &md,
                                               const vector<double> &n,
                                               const vector<double> &w,
                                               const vector<double> &d) {
  try {
    this->read_metadata(md, false);
  } catch (...) {
    throw;
  }
  this->load(w, d, n);
}
vector<ComplexArray>
MultiTaperSpecDivDecon::taper_data(const vector<double> &signal) {
  const string base_error("taper_data procedure:  ");
  /* We put in this sanity check */
  if (signal.size() > taperlen)
    throw MsPASSError(base_error +
                          "Illegal input parameters.  Vector of data received "
                          "is longer than the configured taper length",
                      ErrorSeverity::Invalid);
  /* The tapered data are stored in this vector of arrays */
  int i, j;
  vector<ComplexArray> tdata;
  int ntapers = tapers.rows();
  tdata.reserve(ntapers);
  vector<double> work;
  work.reserve(nfft);
  for (j = 0; j < nfft; ++j)
    work.push_back(0.0);
  for (i = 0; i < ntapers; ++i) {
    /* This will assure part of vector between end of
     * data and nfft is zero padded */
    std::fill(work.begin(), work.end(), 0.0);
    const int ncopy =
        std::min(static_cast<int>(signal.size()),
                 static_cast<int>(this->tapers.columns()));
    for (j = 0; j < ncopy; ++j) {
      work[j] = tapers(i, j) * signal[j];
    }
    /* Force zero pads always */
    ComplexArray cwork(nfft, work);
    tdata.push_back(cwork);
  }
  return tdata;
}
void MultiTaperSpecDivDecon::process() {
  const string base_error("MultiTaperSpecDivDecon::process():  ");
  result.clear();
  winv = ComplexArray();
  rfestimates.clear();
  winv_taper.clear();
  ao_fft.clear();
  try {
    const int output_length = data.size();
    if (output_length > nfft)
      throw MsPASSError(base_error + "data vector exceeds padded fft length",
                        ErrorSeverity::Invalid);
    /* WARNING about this algorithm. At present there is nothing to stop
    a coding error of calling the algorithm with inconsistent signal and
    noise data vectors. */
    if (noise.size() <= 0) {
      throw MsPASSError(base_error + "noise data is empty.",
                        ErrorSeverity::Invalid);
    }
    if (wavelet.size() > taperlen || data.size() > taperlen ||
        noise.size() > taperlen)
      throw MsPASSError(base_error +
                            "wavelet, data, and noise vectors must not exceed "
                            "the configured taper length",
                        ErrorSeverity::Invalid);
    /* Tapered wavelet/noise spectra are used only to estimate a stable
    source-power denominator.  The final inverse uses the untapered source
    phase and is applied to the untapered, zero-padded data window. */
    int i, j;
    vector<ComplexArray> wdata;
    wdata = taper_data(wavelet);
    for (i = 0; i < nseq; ++i) {
      gsl_fft_complex_forward(wdata[i].ptr(), 1, nfft, wavetable, workspace);
    }
    vector<ComplexArray> ndata;
    ndata = taper_data(noise);
    for (i = 0; i < nseq; ++i) {
      gsl_fft_complex_forward(ndata[i].ptr(), 1, nfft, wavetable, workspace);
    }
    double max_source_power(0.0);
    for (i = 0; i < nseq; ++i) {
      for (j = 0; j < nfft; ++j) {
        double *zw = wdata[i].ptr(j);
        double swp = (*zw) * (*zw) + (*(zw + 1)) * (*(zw + 1));
        if (swp > max_source_power)
          max_source_power = swp;
      }
    }
    const double relative_floor = max(DBL_EPSILON, 1.0e-12 * max_source_power);
    vector<double> source_power_denominator(nfft, 0.0);
    for (i = 0; i < nseq; ++i) {
      for (j = 0; j < nfft; ++j) {
        double *zw = wdata[i].ptr(j);
        const double source_power =
            (*zw) * (*zw) + (*(zw + 1)) * (*(zw + 1));
        double *zn = ndata[i].ptr(j);
        const double noise_power =
            (*zn) * (*zn) + (*(zn + 1)) * (*(zn + 1));
        double regularized_power = max(source_power, damp * noise_power);
        regularized_power = max(regularized_power, relative_floor);
        source_power_denominator[j] +=
            regularized_power / static_cast<double>(nseq);
      }
    }
    vector<double> padded_data(nfft, 0.0);
    vector<double> padded_wavelet(nfft, 0.0);
    int ndcopy = std::min(static_cast<int>(data.size()), nfft);
    int nwcopy = std::min(static_cast<int>(wavelet.size()), nfft);
    for (i = 0; i < ndcopy; ++i)
      padded_data[i] = data[i];
    for (i = 0; i < nwcopy; ++i)
      padded_wavelet[i] = wavelet[i];
    ComplexArray untapered_data(nfft, padded_data);
    ComplexArray untapered_wavelet(nfft, padded_wavelet);
    gsl_fft_complex_forward(untapered_data.ptr(), 1, nfft, wavetable,
                            workspace);
    gsl_fft_complex_forward(untapered_wavelet.ptr(), 1, nfft, wavetable,
                            workspace);

    ComplexArray inv(untapered_wavelet);
    inv.conj();
    for (j = 0; j < nfft; ++j) {
      double *zinv = inv.ptr(j);
      (*zinv) /= source_power_denominator[j];
      (*(zinv + 1)) /= source_power_denominator[j];
    }
    vector<ComplexArray> winv_taper_work;
    vector<ComplexArray> rfestimates_work;
    vector<ComplexArray> ao_fft_work;
    winv_taper_work.push_back(inv);
    ComplexArray rfwork(inv);
    rfwork = rfwork * untapered_data;
    rfestimates_work.push_back(rfwork);
    ComplexArray aowork(inv);
    aowork = aowork * untapered_wavelet;
    ao_fft_work.push_back(aowork);
    vector<double> ao_scale(nfft, 0.0);
    for (i = 0; i < static_cast<int>(ao_fft_work.size()); ++i) {
      ComplexArray work(ao_fft_work[i]);
      work = (*shapingwavelet.wavelet()) * work;
      gsl_fft_complex_inverse(work.ptr(), 1, nfft, wavetable, workspace);
      for (j = 0; j < nfft; ++j)
        ao_scale[j] += work[j].real();
    }
    double ao_nrmscl = 1.0 / static_cast<double>(ao_fft_work.size());
    for (j = 0; j < nfft; ++j)
      ao_scale[j] *= ao_nrmscl;
    ComplexArray ao_work(nfft, ao_scale);
    vector<double> ao_lag = ExtractLagWindow(ao_work, output_length, sample_shift);
    if (sample_shift < 0 || sample_shift >= static_cast<int>(ao_lag.size()))
      throw MsPASSError(base_error +
                            "zero-lag sample is outside extracted lag window",
                        ErrorSeverity::Invalid);
    double ao_peak = ao_lag[sample_shift];
    if (fabs(ao_peak) <= 0.0)
      throw MsPASSError(base_error + "actual output has zero peak",
                        ErrorSeverity::Invalid);
    for (i = 0; i < static_cast<int>(rfestimates_work.size()); ++i) {
      for (j = 0; j < nfft; ++j) {
        double *zrf = rfestimates_work[i].ptr(j);
        (*zrf) /= ao_peak;
        (*(zrf + 1)) /= ao_peak;
        double *zw = winv_taper_work[i].ptr(j);
        (*zw) /= ao_peak;
        (*(zw + 1)) /= ao_peak;
        double *zao = ao_fft_work[i].ptr(j);
        (*zao) /= ao_peak;
        (*(zao + 1)) /= ao_peak;
      }
    }

    /* To mesh with the API of other methods we now compute the average
    rf estimate.  We compute this as a simple average in the padded work
    buffer, then extract the requested lag window. */
    vector<double> padded_result(nfft, 0.0);
    vector<double> wtmp;
    for (i = 0; i < static_cast<int>(rfestimates_work.size()); ++i) {
      ComplexArray work(rfestimates_work[i]);
      /* We always apply the shaping wavelet to the rf estimate.  We do it
      here before averaging. */
      work = (*shapingwavelet.wavelet()) * work;
      gsl_fft_complex_inverse(work.ptr(), 1, nfft, wavetable, workspace);
      for (j = 0; j < nfft; ++j) {
        padded_result[j] += work[j].real();
      }
    }
    double nrmscl = 1.0 / static_cast<double>(rfestimates_work.size());
    for (j = 0; j < nfft; ++j)
      padded_result[j] *= nrmscl;
    ComplexArray rf_work(nfft, padded_result);
    result = ExtractLagWindow(rf_work, output_length, sample_shift);
    winv = winv_taper_work[0];
    winv_taper = winv_taper_work;
    rfestimates = rfestimates_work;
    ao_fft = ao_fft_work;
  } catch (...) {
    throw;
  };
}
CoreTimeSeries MultiTaperSpecDivDecon::actual_output() {
  try {
    if (ao_fft.empty())
      throw MsPASSError("MultiTaperSpecDivDecon::actual_output: process must "
                        "be called before actual_output",
                        ErrorSeverity::Invalid);
    int i, k;
    vector<double> ao;
    ao.reserve(nfft);
    for (k = 0; k < nfft; ++k)
      ao.push_back(0.0);
    for (i = 0; i < static_cast<int>(ao_fft.size()); ++i) {
      ComplexArray work(ao_fft[i]);
      work = (*shapingwavelet.wavelet()) * work;
      gsl_fft_complex_inverse(work.ptr(), 1, nfft, wavetable, workspace);
      for (k = 0; k < nfft; ++k)
        ao[k] += work[k].real();
    }
    double nrmscl = 1.0 / static_cast<double>(ao_fft.size());
    for (k = 0; k < nfft; ++k)
      ao[k] *= nrmscl;
    /* We always shift this wavelet to the center of the data vector.
    We handle the time through the CoreTimeSeries object. */
    int i0 = nfft / 2;
    ao = circular_shift(ao, i0);
    ao = normalize<double>(ao);
    CoreTimeSeries result(nfft);
    /* Getting dt from here is unquestionably a flaw in the api, but will
    retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
    double dt = this->shapingwavelet.sample_interval();
    /* t0 is time of sample zero - hence normally negative*/
    /* Old API
    result.t0=dt*(-(double)i0);
    result.dt=dt;
    result.live=true;
    result.tref=TimeReferenceType::Relative;
    result.s=ao;
    result.ns=nfft;
    */

    result.set_t0(dt * (-(double)i0));
    result.set_dt(dt);
    result.set_live();
    result.set_npts(nfft);
    result.set_tref(TimeReferenceType::Relative);
    for (k = 0; k < nfft; ++k)
      result.s[k] = ao[k];
    return result;
  } catch (...) {
    throw;
  };
}

CoreTimeSeries MultiTaperSpecDivDecon::inverse_wavelet(const double t0parent) {
  try {
    /* Getting dt from here is unquestionably a flaw in the api, but will
     *         retain for now.   Perhaps should a copy of dt in the ScalarDecon
     * object. */
    double dt = this->shapingwavelet.sample_interval();
    /* algorithm assumes this data vector in result is initialized to nfft zeros
     */
    CoreTimeSeries result(this->nfft);
    if (winv_taper.empty())
      throw MsPASSError("MultiTaperSpecDivDecon::inverse_wavelet: process must "
                        "be called before inverse_wavelet",
                        ErrorSeverity::Invalid);
    for (int i = 0; i < static_cast<int>(winv_taper.size()); ++i) {
      CoreTimeSeries work(this->FFTDeconOperator::FourierInverse(
          this->winv_taper[i], *shapingwavelet.wavelet(), dt, t0parent));
      if (i == 0)
        result = work;
      else
        result += work;
    }
    double nrmscal = 1.0 / static_cast<double>(winv_taper.size());
    for (int k = 0; k < result.s.size(); ++k)
      result.s[k] *= nrmscal;
    return result;
  } catch (...) {
    throw;
  };
}
CoreTimeSeries MultiTaperSpecDivDecon::inverse_wavelet() {
  try {
    return this->inverse_wavelet(0.0);
  } catch (...) {
    throw;
  };
}
std::vector<CoreTimeSeries>
MultiTaperSpecDivDecon::all_inverse_wavelets(const double t0parent) {
  try {
    if (winv_taper.empty())
      throw MsPASSError("MultiTaperSpecDivDecon::all_inverse_wavelets: "
                        "process must be called before all_inverse_wavelets",
                        ErrorSeverity::Invalid);
    std::vector<CoreTimeSeries> all;
    all.reserve(winv_taper.size());
    double dt = this->shapingwavelet.sample_interval();
    for (int i = 0; i < static_cast<int>(winv_taper.size()); ++i) {
      CoreTimeSeries work(this->FFTDeconOperator::FourierInverse(
          this->winv_taper[i], *shapingwavelet.wavelet(), dt, t0parent));
      all.push_back(work);
    }
    return all;
  } catch (...) {
    throw;
  };
}
std::vector<CoreTimeSeries>
MultiTaperSpecDivDecon::all_rfestimates(const double t0parent) {
  try {
    if (rfestimates.empty())
      throw MsPASSError("MultiTaperSpecDivDecon::all_rfestimates: process "
                        "must be called before all_rfestimates",
                        ErrorSeverity::Invalid);
    std::vector<CoreTimeSeries> all;
    all.reserve(rfestimates.size());
    double dt = this->shapingwavelet.sample_interval();
    for (int i = 0; i < static_cast<int>(rfestimates.size()); ++i) {
      /* Although this method of FFTDeconOperator was originally
       * written to return inverse wavelet, it can work in this
       * context too */
      CoreTimeSeries work(this->FFTDeconOperator::FourierInverse(
          this->rfestimates[i], *shapingwavelet.wavelet(), dt, t0parent));
      all.push_back(work);
    }
    return all;
  } catch (...) {
    throw;
  };
}
std::vector<CoreTimeSeries>
MultiTaperSpecDivDecon::all_actual_outputs(const double t0parent) {
  try {
    if (ao_fft.empty())
      throw MsPASSError("MultiTaperSpecDivDecon::all_actual_outputs: process "
                        "must be called before all_actual_outputs",
                        ErrorSeverity::Invalid);
    std::vector<CoreTimeSeries> all;
    all.reserve(ao_fft.size());
    double dt = this->shapingwavelet.sample_interval();
    for (int i = 0; i < static_cast<int>(ao_fft.size()); ++i) {
      /* Although this method of FFTDeconOperator was originally
       * written to return inverse wavelet, it can work in this
       * context too */
      CoreTimeSeries work(this->FFTDeconOperator::FourierInverse(
          this->ao_fft[i], *shapingwavelet.wavelet(), dt, t0parent));
      all.push_back(work);
    }
    return all;
  } catch (...) {
    throw;
  };
}

Metadata MultiTaperSpecDivDecon::QCMetrics() {
  const bool did_process(!winv_taper.empty());
  Metadata md(this->BasicQCMetrics("MultiTaperSpecDivDecon", did_process));
  md.put("multitaper_operator_type", string("specdiv_power_stabilized"));
  md.put("multitaper_operator_nfft", nfft);
  md.put("decon_operator_nfft", nfft);
  md.put("decon_operator_sample_shift", sample_shift);
  md.put("multitaper_taper_length", static_cast<int>(taperlen));
  md.put("multitaper_number_tapers", nseq);
  md.put("multitaper_time_bandwidth_product", nw);
  md.put("multitaper_damping_factor", damp);
  md.put("multitaper_processed", did_process);
  md.put("multitaper_number_outputs", static_cast<int>(winv_taper.size()));
  return md;
}
} // namespace mspass::algorithms::deconvolution
