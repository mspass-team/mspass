#include "mspass/algorithms/deconvolution/LeastSquareDecon.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/algorithms/deconvolution/MultiTaperXcorDecon.h"
#include "mspass/utility/MsPASSError.h"
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using mspass::algorithms::amplitudes::normalize;

LeastSquareDecon::LeastSquareDecon(const LeastSquareDecon &parent)
    : FFTDeconOperator(parent), ScalarDecon(parent) {
  damp = parent.damp;
}
int LeastSquareDecon::read_metadata(const Metadata &md) {
  try {
    const string base_error("SimpleLeastTaperDecon::read_metadata method: ");
    const int nfft_from_win = ComputeFFTLength(md);
    const int new_sample_shift = ComputeDeconSampleShift(md);
    const double new_damp = md.get_double("damping_factor");
    if (new_damp <= 0.0) {
      throw MsPASSError(base_error +
                            "damping_factor must be positive.  A zero value "
                            "is an unstable undamped spectral division.",
                        ErrorSeverity::Fatal);
    }
    if (new_sample_shift < 0 || new_sample_shift > nfft_from_win)
      throw MsPASSError(base_error +
                            "computed sample_shift is outside fft buffer",
                        ErrorSeverity::Fatal);
    ShapingWavelet new_shapingwavelet(md, nfft_from_win);
    // window based nfft always overrides that extracted directly from md */
    if (nfft_from_win != nfft) {
      this->change_size(nfft_from_win);
    }
    sample_shift = new_sample_shift;
    damp = new_damp;
    /* Note this depends on nfft inheritance from FFTDeconOperator.
     * That is a bit error prone with changes*/
    shapingwavelet = new_shapingwavelet;
    return 0;
  } catch (...) {
    throw;
  };
}
/* This constructor is little more than a call to read_metadata for this
operator */
LeastSquareDecon::LeastSquareDecon(const Metadata &md) : FFTDeconOperator(md) {
  try {
    this->read_metadata(md);
  } catch (...) {
    throw;
  }
}
/* This method is really an alias for read_metadata for this operator */
void LeastSquareDecon::changeparameter(const Metadata &md) {
  try {
    this->read_metadata(md);
  } catch (...) {
    throw;
  };
}
LeastSquareDecon::LeastSquareDecon(const Metadata &md, const vector<double> &w,
                                   const vector<double> &d) {
  try {
    this->read_metadata(md);
  } catch (...) {
    throw;
  };
  wavelet = w;
  data = d;
}
void LeastSquareDecon::process() {

  const string base_error("LeastSquareDecon::process:  ");
  result.clear();
  const int output_length = data.size();
  if (output_length > nfft)
    throw MsPASSError(base_error + "data vector exceeds padded fft length",
                      ErrorSeverity::Invalid);
  // apply fft to the input trace data
  vector<double> data_padded(data);
  if (data_padded.size() < nfft)
    data_padded.resize(nfft, 0.0);
  ComplexArray d_fft(nfft, &(data_padded[0]));
  gsl_fft_complex_forward(d_fft.ptr(), 1, nfft, wavetable, workspace);

  // apply fft to wavelet
  if (wavelet.size() > nfft)
    throw MsPASSError(base_error + "wavelet vector exceeds padded fft length",
                      ErrorSeverity::Invalid);
  vector<double> wavelet_padded(wavelet);
  if (wavelet_padded.size() < nfft)
    wavelet_padded.resize(nfft, 0.0);
  ComplexArray b_fft(nfft, &(wavelet_padded[0]));
  gsl_fft_complex_forward(b_fft.ptr(), 1, nfft, wavetable, workspace);

  // deconvolution: RF=conj(B).*D./(conj(B).*B+damp)
  b_fft.conj();
  ComplexArray rf_fft, conj_b_fft(b_fft);
  b_fft.conj();

  double b_rms = b_fft.rms();
  if (b_rms == 0.0)
    throw MsPASSError(base_error + "wavelet data vector is all zeros",
                      ErrorSeverity::Invalid);
  ComplexArray denom(conj_b_fft * b_fft);
  double theta(b_rms * damp);
  /* This is like normal equation form for damped inverse so theta as computed
  needs to be squared */
  theta = theta * theta;
  for (int k = 0; k < nfft; ++k) {
    double *ptr;
    ptr = denom.ptr(k);
    /* ptr points to the real part - an oddity of this interface */
    *ptr += theta;
  }
  rf_fft = (conj_b_fft * d_fft) / denom;
  // rf_fft=(conj_b_fft*d_fft)/(conj_b_fft*b_fft+b_rms*damp);
  /* Compute the frequency domain version of the inverse wavelet now
  and save it the object for efficiency. */
  // winv=conj_b_fft/(conj_b_fft*b_fft+b_rms*damp);
  winv = conj_b_fft / denom;

  // apply shaping wavelet but only to rf estimate - actual output and
  // inverse_wavelet methods apply it to when needed there for efficiency
  ShapingWavelet sw(this->get_shaping_wavelet());
  rf_fft = (*sw.wavelet()) * rf_fft;

  // ifft gets result
  gsl_fft_complex_inverse(rf_fft.ptr(), 1, nfft, wavetable, workspace);
  result = ExtractLagWindow(rf_fft, output_length, sample_shift);
}
CoreTimeSeries LeastSquareDecon::actual_output() {
  try {
    vector<double> wavelet_padded(wavelet);
    if (wavelet_padded.size() < nfft)
      wavelet_padded.resize(nfft, 0.0);
    ComplexArray W(nfft, &(wavelet_padded[0]));
    gsl_fft_complex_forward(W.ptr(), 1, nfft, wavetable, workspace);
    ComplexArray ao_fft;
    ao_fft = winv * W;
    /* We always apply the shaping wavelet - this perhaps should be optional
    but probably better done with a none option for the shaping wavelet */
    ao_fft = (*shapingwavelet.wavelet()) * ao_fft;
    gsl_fft_complex_inverse(ao_fft.ptr(), 1, nfft, wavetable, workspace);
    vector<double> ao;
    ao.reserve(nfft);
    for (int k = 0; k < ao_fft.size(); ++k)
      ao.push_back(ao_fft[k].real());
    /* We always shift this wavelet to the center of the data vector.
    We handle the time through the CoreTimeSeries object. */
    int i0 = nfft / 2;
    ao = circular_shift(ao, i0);
    CoreTimeSeries result(nfft);
    /* Getting dt from here is unquestionably a flaw in the api, but will
    retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
    double dt = this->shapingwavelet.sample_interval();

    result.set_t0(-dt * ((double)i0));
    result.set_dt(dt);
    result.set_live();
    result.set_npts(nfft);
    result.set_tref(TimeReferenceType::Relative);
    for (int k = 0; k < nfft; ++k)
      result.s[k] = ao[k];
    result.s = normalize<double>(result.s);
    return result;
  } catch (...) {
    throw;
  };
}

CoreTimeSeries LeastSquareDecon::inverse_wavelet(const double t0parent) {
  try {
    /* Getting dt from here is unquestionably a flaw in the api, but will
     *         retain for now.   Perhaps should a copy of dt in the ScalarDecon
     * object. */
    double dt = this->shapingwavelet.sample_interval();
    return (this->FourierInverse(this->winv, *shapingwavelet.wavelet(), dt,
                                 t0parent));
  } catch (...) {
    throw;
  };
}

CoreTimeSeries LeastSquareDecon::inverse_wavelet() {
  try {
    return this->inverse_wavelet(0.0);
  } catch (...) {
    throw;
  };
}
Metadata LeastSquareDecon::QCMetrics() {
  /* Return only an empty Metadata container.  Done as it is
  easier to maintain the code letting python do this work.
  This also anticipates new metrics being added which would be
  easier in python.*/
  Metadata md;
  return md;
}
} // namespace mspass::algorithms::deconvolution
