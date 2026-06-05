#include "mspass/algorithms/deconvolution/WaterLevelDecon.h"
#include "mspass/algorithms/amplitudes.h"
#include <cfloat> // Needed for DBL_EPSILON
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using mspass::algorithms::amplitudes::normalize;

WaterLevelDecon::WaterLevelDecon(const WaterLevelDecon &parent)
    : FFTDeconOperator(parent), ScalarDecon(parent) {
  wlv = parent.wlv;
}
int WaterLevelDecon::read_metadata(const Metadata &md) {
  try {
    const string base_error("WaterLevelDecon::read_metadata method: ");
    const int nfft_from_win = ComputeFFTLength(md);
    const int new_sample_shift = ComputeDeconSampleShift(md);
    const double new_wlv = md.get_double("water_level");
    if (new_wlv <= 0.0) {
      throw MsPASSError(base_error +
                            "water_level must be positive.  A zero value is "
                            "an unstable bare spectral division.",
                        ErrorSeverity::Fatal);
    }
    if (new_sample_shift < 0 || new_sample_shift > nfft_from_win)
      throw MsPASSError(base_error +
                            "computed sample_shift is outside fft buffer",
                        ErrorSeverity::Fatal);
    ShapingWavelet new_shapingwavelet(md, nfft_from_win);
    // window based nfft always overrides that extracted directly from md */
    if (nfft_from_win != FFTDeconOperator::nfft) {
      this->change_size(nfft_from_win);
    }
    sample_shift = new_sample_shift;
    wlv = new_wlv;
    shapingwavelet = new_shapingwavelet;
    return 0;
  } catch (...) {
    throw;
  };
}
/* This constructor is little more than a call to read_metadata for this
opeator */
WaterLevelDecon::WaterLevelDecon(const Metadata &md) : FFTDeconOperator(md) {
  try {
    this->read_metadata(md);
  } catch (...) {
    throw;
  }
}
/* This method is really an alias for read_metadata for this operator */
void WaterLevelDecon::changeparameter(const Metadata &md) {
  try {
    this->read_metadata(md);
  } catch (...) {
    throw;
  };
}
WaterLevelDecon::WaterLevelDecon(const Metadata &md, const vector<double> &w,
                                 const vector<double> &d) {
  try {
    this->read_metadata(md);
  } catch (...) {
    throw;
  };
  wavelet = w;
  data = d;
}
void WaterLevelDecon::process() {

  result.clear();
  const string base_error("WaterLevelDecon::process():  ");
  const int output_length = data.size();
  if (output_length > nfft)
    throw MsPASSError(base_error + "data vector exceeds padded fft length",
                      ErrorSeverity::Invalid);
  // apply fft to the input trace data
  //  data and wavelet sizes need to be zero padded if the are short
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

  double b_rms = b_fft.rms();
  if (b_rms == 0.0)
    throw MsPASSError(
        "WaterLevelDecon::process():  wavelet data vector is all zeros");

  // water level - count the number of points below water level
  int nunderwater(0);
  for (int i = 0; i < nfft; i++) {
    /* We have to avoid hard zeros because the formula below will
     * yield an NaN when that happens from 0/0 */
    double bamp;
    bamp = abs(b_fft[i]);
    /*WARNING:  this is dependent on implementation detail of ComplexArray
    that defines the data as a FortranComplex64 - real,imag pairs. */
    if (bamp < b_rms * wlv) {
      /* Use 64 bit epsilon as the floor for defining a pure zero */
      if (bamp / b_rms < DBL_EPSILON) {
        *b_fft.ptr(i) = b_rms * wlv;
        *(b_fft.ptr(i) + 1) = b_rms * wlv;
      } else {
        // real part
        *b_fft.ptr(i) = (*b_fft.ptr(i) / abs(b_fft[i])) * b_rms * wlv;
        // imag part
        *(b_fft.ptr(i) + 1) =
            (*(b_fft.ptr(i) + 1) / abs(b_fft[i])) * b_rms * wlv;
      }
      ++nunderwater;
    }
  }
  regularization_fraction = ((double)nunderwater) / ((double)nfft);
  ComplexArray rf_fft;
  rf_fft = d_fft / b_fft;
  /* Make numerator for inverse from zero lag spike */
  vector<double> d0(nfft, 0.0);
  d0[0] = 1.0;
  ComplexArray delta0(nfft, d0);
  gsl_fft_complex_forward(delta0.ptr(), 1, nfft, wavetable, workspace);
  winv = delta0 / b_fft;

  // apply shaping wavelet to rf estimate
  rf_fft = (*shapingwavelet.wavelet()) * rf_fft;

  // ifft gets result
  gsl_fft_complex_inverse(rf_fft.ptr(), 1, nfft, wavetable, workspace);
  result = ExtractLagWindow(rf_fft, output_length, sample_shift);
}
CoreTimeSeries WaterLevelDecon::actual_output() {
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
    for (unsigned int k = 0; k < ao_fft.size(); ++k)
      ao.push_back(ao_fft[k].real());
    /* We always shift this wavelet to the center of the data vector.
    We handle the time through the CoreTimeSeries object. */
    int i0 = nfft / 2;
    ao = circular_shift(ao, i0);
    ao = normalize<double>(ao);
    CoreTimeSeries result(nfft);
    /* Getting dt from here is unquestionably a flaw in the api, but will
    retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
    double dt = this->shapingwavelet.sample_interval();
    result.set_t0(-dt * ((double)i0));
    result.set_dt(dt);
    result.set_live();
    result.set_tref(TimeReferenceType::Relative);
    result.set_npts(nfft);
    for (int k = 0; k < nfft; ++k)
      result.s[k] = ao[k];
    ;
    return result;
  } catch (...) {
    throw;
  };
}

CoreTimeSeries WaterLevelDecon::inverse_wavelet(const double t0parent) {
  try {
    /* Getting dt from here is unquestionably a flaw in the api, but will
     *         retain for now.   Perhaps should a copy of dt in the ScalarDecon
     * object. */
    double dt = this->shapingwavelet.sample_interval();
    return (this->FFTDeconOperator::FourierInverse(
        this->winv, *shapingwavelet.wavelet(), dt, t0parent));
  } catch (...) {
    throw;
  };
}
CoreTimeSeries WaterLevelDecon::inverse_wavelet() {
  try {
    return this->inverse_wavelet(0.0);
  } catch (...) {
    throw;
  };
}
Metadata WaterLevelDecon::QCMetrics() {
  /* Return only an empty Metadata container.  Done as it is
  easier to maintain the code letting python do this work.
  This also anticipates new metrics being added which would be
  easier in python.*/
  Metadata md;
  return md;
}
} // namespace mspass::algorithms::deconvolution
