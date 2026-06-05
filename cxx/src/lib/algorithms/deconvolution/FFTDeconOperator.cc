#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <math.h>
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

FFTDeconOperator::FFTDeconOperator() {
  nfft = 0;
  sample_shift = 0;
  wavetable = NULL;
  workspace = NULL;
}
FFTDeconOperator::FFTDeconOperator(const Metadata &md) {
  try {
    const string base_error("FFTDeconOperator Metadata constructor:  ");
    const double ts = md.get_double("deconvolution_data_window_start");
    const double te = md.get_double("deconvolution_data_window_end");
    const double dt = md.get_double("target_sample_interval");
    ValidateWindowDuration(TimeWindow(ts, te), "deconvolution_data_window",
                           base_error);
    if (!std::isfinite(dt) || dt <= 0.0)
      throw MsPASSError(base_error +
                            "target_sample_interval must be positive",
                        ErrorSeverity::Fatal);
    int nfftpf = md.get_int("operator_nfft");
    /* We force a power of 2 algorithm for efficiency and always round up*/
    this->nfft = nextPowerOf2(nfftpf);
    /* We compute the sample shift from the window start time and dt.  This
     * assures the output will be phase shifted so zero lag is at the zero
     * position of the array.   Necessary because operators using this object
     * internally only return a raw vector of samples not a child of
     * BasicTimeSeries. */
    this->sample_shift = ComputeDeconSampleShift(md);
    if (this->sample_shift < 0)
      throw MsPASSError(base_error +
                            "illegal sample_shift parameter - must be ge 0",
                        ErrorSeverity::Fatal);
    if ((this->sample_shift) > nfft)
      throw MsPASSError(
          base_error + "Computed shift parameter exceeds length of fft\n" +
              "Deconvolution data window parameters are probably nonsense",
          ErrorSeverity::Fatal);
    wavetable = gsl_fft_complex_wavetable_alloc(nfft);
    workspace = gsl_fft_complex_workspace_alloc(nfft);
  } catch (...) {
    throw;
  };
}
FFTDeconOperator::FFTDeconOperator(const FFTDeconOperator &parent) {
  nfft = parent.nfft;
  sample_shift = parent.sample_shift;
  /* copies need their own work space */
  wavetable = gsl_fft_complex_wavetable_alloc(nfft);
  workspace = gsl_fft_complex_workspace_alloc(nfft);
}
FFTDeconOperator::~FFTDeconOperator() {
  if (wavetable != NULL)
    gsl_fft_complex_wavetable_free(wavetable);
  if (workspace != NULL)
    gsl_fft_complex_workspace_free(workspace);
}
FFTDeconOperator &FFTDeconOperator::operator=(const FFTDeconOperator &parent) {
  if (this != &parent) {
    nfft = parent.nfft;
    sample_shift = parent.sample_shift;
    /* copies need their own work space */
    wavetable = gsl_fft_complex_wavetable_alloc(nfft);
    workspace = gsl_fft_complex_workspace_alloc(nfft);
  }
  return *this;
}
void FFTDeconOperator::changeparameter(const Metadata &md) {
  try {
    const string base_error("FFTDeconOperator::changeparameter:  ");
    const double ts = md.get_double("deconvolution_data_window_start");
    const double te = md.get_double("deconvolution_data_window_end");
    const double dt = md.get_double("target_sample_interval");
    ValidateWindowDuration(TimeWindow(ts, te), "deconvolution_data_window",
                           base_error);
    if (!std::isfinite(dt) || dt <= 0.0)
      throw MsPASSError(base_error +
                            "target_sample_interval must be positive",
                        ErrorSeverity::Fatal);
    const int nfft_test = nextPowerOf2(md.get_int("operator_nfft"));
    const int sample_shift_test = ComputeDeconSampleShift(md);
    if (sample_shift_test < 0)
      throw MsPASSError(base_error +
                            "illegal sample_shift parameter - must be ge 0",
                        ErrorSeverity::Fatal);
    if (sample_shift_test > nfft_test)
      throw MsPASSError(
          base_error + "computed sample_shift exceeds length of fft",
          ErrorSeverity::Fatal);
    if (nfft_test != nfft) {
      gsl_fft_complex_wavetable *new_wavetable =
          gsl_fft_complex_wavetable_alloc(nfft_test);
      gsl_fft_complex_workspace *new_workspace =
          gsl_fft_complex_workspace_alloc(nfft_test);
      if (wavetable != NULL)
        gsl_fft_complex_wavetable_free(wavetable);
      if (workspace != NULL)
        gsl_fft_complex_workspace_free(workspace);
      wavetable = new_wavetable;
      workspace = new_workspace;
      nfft = nfft_test;
    }
    sample_shift = sample_shift_test;
  } catch (...) {
    throw;
  };
}
void FFTDeconOperator::change_size(const int n) {
  try {
    if (nfft != 0) {
      if (wavetable != NULL)
        gsl_fft_complex_wavetable_free(wavetable);
      if (workspace != NULL)
        gsl_fft_complex_workspace_free(workspace);
    }
    nfft = n;
    wavetable = gsl_fft_complex_wavetable_alloc(nfft);
    workspace = gsl_fft_complex_workspace_alloc(nfft);
  } catch (...) {
    throw;
  };
}
/* Helper method to avoid repetitious code in Fourier methods.   Computes the
 inverse FIR filter for the inverse_wavelet methods of fourier decon operators.

 winv - fourier coefficients of inverse wavelet
 sw - fourier coefficients of shaping wavelet that is to be applied to winv
 dt - data sample interval (number of points is derived form winv)
 t0parent is as described in ScalarDecon::inverse_wavelet defintions.
 */

CoreTimeSeries FFTDeconOperator::FourierInverse(const ComplexArray &winv,
                                                const ComplexArray &sw,
                                                const double dt,
                                                const double t0parent) {
  try {
    const string base_error("FFTDeconOperator::FourierInverse:  ");
    /* Compute wrap point for circular shift operator - note negative
    because a positive t0 is requires wrapping at a point i0 to the left
    of the original zero point*/
    /* Testing with matlab prototypes showed Fourier inverse filters
     * showed they created valid results only if the filter used a
     * circular shift of nfft/2.   This sets that as a requirement here. */
    int ntest;
    ntest = winv.size();
    if (ntest != nfft)
      throw MsPASSError(
          base_error +
              "wavelet inverse fourier array size mismatch with operator",
          ErrorSeverity::Invalid);
    if (sw.size() != nfft)
      throw MsPASSError(
          base_error +
              "shaping wavelet fourier array size mismatch with operator",
          ErrorSeverity::Invalid);
    ComplexArray winv_work(winv);
    /* This applies the shaping wavelet*/
    winv_work *= sw;
    gsl_fft_complex_inverse(winv_work.ptr(), 1, nfft, wavetable, workspace);
    CoreTimeSeries result;
    result.set_t0(t0parent);
    result.set_dt(dt);
    result.set_live();
    /* Note this new api method initializes s all zeros so we need only set
    the values not use push back below */
    result.set_npts(nfft);
    result.set_tref(TimeReferenceType::Relative);
    for (int k = 0; k < winv_work.size(); ++k)
      result.s[k] = winv_work[k].real();
    return result;
  } catch (...) {
    throw;
  };
}

/* helpers*/
void ValidateWindowDuration(const TimeWindow w, const string &window_name,
                            const string &caller) {
  if (!std::isfinite(w.start) || !std::isfinite(w.end))
    throw MsPASSError(caller + ": " + window_name +
                          " start and end must be finite",
                      ErrorSeverity::Fatal);
  if (w.end <= w.start)
    throw MsPASSError(caller + ": " + window_name +
                          " end must be greater than start",
                      ErrorSeverity::Fatal);
}

int ComputeFFTLength(const TimeWindow w, const double dt) {
  int nsamples, nfft;
  const string caller("FFTDeconOperator::ComputeFFTLength");
  ValidateWindowDuration(w, "deconvolution window", caller);
  if (!std::isfinite(dt) || dt <= 0.0)
    throw MsPASSError(caller + ": target_sample_interval must be positive",
                      ErrorSeverity::Fatal);
  nsamples = static_cast<int>(round((w.end - w.start) / dt)) + 1;
  if (nsamples < 2)
    throw MsPASSError(caller +
                          ": deconvolution window must contain at least two "
                      "samples",
                      ErrorSeverity::Fatal);
  /* Use a padded work buffer by default.  The scalar FFT decon operators are
   * intended for linear seismic time windows, not circular convolution of the
   * finite window.  For two loaded windows of length N, 2*N-1 samples is the
   * exact linear convolution/correlation length.  Rounding that value up to a
   * power of two gives the FFT implementation enough guard samples without the
   * extra cost and spectral over-sampling of a three-window buffer. */
  nfft = nextPowerOf2(2 * nsamples - 1);
  return nfft;
}
/* Newbies note this works because of the fundamental concept of
overloading in C++ */
int ComputeFFTLength(const Metadata &md) {
  try {
    double ts, te, dt;
    ts = md.get<double>("deconvolution_data_window_start");
    te = md.get<double>("deconvolution_data_window_end");
    TimeWindow w(ts, te);
    dt = md.get<double>("target_sample_interval");
    int nfft;
    nfft = ComputeFFTLength(w, dt);
    if (nfft < 2)
      throw MsPASSError(
          string("FFTDeconOperator ComputeFFTLength procedure:  ") +
              "Computed fft length is less than 2 - check window parameters",
          ErrorSeverity::Fatal);
    return nfft;
  } catch (MetadataGetError &mde) {
    throw mde;
  };
}
int ComputeDeconSampleShift(const Metadata &md) {
  double dt_to_use = md.get_double("target_sample_interval");
  double dwinstart = md.get_double("deconvolution_data_window_start");
  if (!std::isfinite(dt_to_use) || dt_to_use <= 0.0)
    throw MsPASSError(
        "ComputeDeconSampleShift: target_sample_interval must be positive",
        ErrorSeverity::Fatal);
  if (!std::isfinite(dwinstart))
    throw MsPASSError(
        "ComputeDeconSampleShift: deconvolution_data_window_start must be "
        "finite",
        ErrorSeverity::Fatal);
  int i0 = round(dwinstart / dt_to_use);
  return -i0;
}
void ValidatePowerSpectrumCoversDC(const PowerSpectrum &spectrum,
                                   const string &caller) {
  if (spectrum.dead())
    throw MsPASSError(caller + ": noise PowerSpectrum is marked dead",
                      ErrorSeverity::Invalid);
  if (spectrum.nf() < 2 || spectrum.spectrum.size() < 2)
    throw MsPASSError(caller +
                          ": noise PowerSpectrum is too short; at least two "
                          "frequency bins are required",
                      ErrorSeverity::Invalid);
  if (spectrum.df() <= 0.0)
    throw MsPASSError(caller +
                          ": noise PowerSpectrum has nonpositive frequency "
                          "spacing",
                      ErrorSeverity::Invalid);
  const double fmax =
      spectrum.f0() + spectrum.df() * static_cast<double>(spectrum.nf() - 1);
  if (spectrum.f0() > 0.0 || fmax <= 0.0)
    throw MsPASSError(caller + ": noise PowerSpectrum must cover DC frequency",
                      ErrorSeverity::Invalid);
}
vector<double> ExtractLagWindow(ComplexArray &fft_buffer,
                                const int output_length,
                                const int sample_shift) {
  const string base_error("ExtractLagWindow:  ");
  const int nfft = fft_buffer.size();
  if (output_length < 0)
    throw MsPASSError(base_error + "output_length cannot be negative",
                      ErrorSeverity::Invalid);
  if (sample_shift < 0)
    throw MsPASSError(base_error + "sample_shift cannot be negative",
                      ErrorSeverity::Invalid);
  if (sample_shift > nfft)
    throw MsPASSError(base_error + "sample_shift exceeds fft buffer length",
                      ErrorSeverity::Invalid);
  if (output_length > nfft)
    throw MsPASSError(base_error + "output_length exceeds fft buffer length",
                      ErrorSeverity::Invalid);
  vector<double> result;
  result.reserve(output_length);
  const int nnegative = std::min(sample_shift, output_length);
  for (int k = nnegative; k > 0; --k)
    result.push_back(fft_buffer[nfft - k].real());
  for (int k = 0; k < output_length - nnegative; ++k)
    result.push_back(fft_buffer[k].real());
  return result;
}
} // namespace mspass::algorithms::deconvolution
