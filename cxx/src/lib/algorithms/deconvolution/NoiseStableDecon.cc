#include "mspass/algorithms/deconvolution/NoiseStableDecon.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>

namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using mspass::algorithms::amplitudes::normalize;

namespace {
double get_double_default(const Metadata &md, const string &key,
                          const double default_value) {
  if (md.is_defined(key))
    return md.get_double(key);
  return default_value;
}
bool get_bool_default(const Metadata &md, const string &key,
                      const bool default_value) {
  if (md.is_defined(key))
    return md.get_bool(key);
  return default_value;
}
double folded_frequency(const int k, const int nfft, const double dt) {
  const int kfold = min(k, nfft - k);
  return static_cast<double>(kfold) / (static_cast<double>(nfft) * dt);
}
} // namespace

NoiseStableDecon::NoiseStableDecon()
    : FFTDeconOperator(), ScalarDecon(), mu_min(3.0e-3), alpha(1.0),
      noise_floor(1.0e-12), gain_max(1.0e3), snr_taper_low(1.0),
      snr_taper_high(3.0), use_reliability_taper(false),
      noise_vector_loaded(false), noise_spectrum_loaded(false),
      max_gain_actual(0.0), noise_amplification(0.0),
      effective_bandwidth_fraction(0.0) {}

NoiseStableDecon::NoiseStableDecon(const Metadata &md) : FFTDeconOperator(md) {
  this->read_metadata(md);
}

NoiseStableDecon::NoiseStableDecon(const Metadata &md, const vector<double> &w,
                                   const vector<double> &d)
    : FFTDeconOperator(md) {
  this->read_metadata(md);
  wavelet = w;
  data = d;
}

int NoiseStableDecon::read_metadata(const Metadata &md) {
  const string base_error("NoiseStableDecon::read_metadata: ");
  int nfft_from_win = ComputeFFTLength(md);
  if (nfft_from_win != nfft)
    this->change_size(nfft_from_win);
  mu_min = get_double_default(md, "ns_gid_mu_min", 3.0e-3);
  alpha = get_double_default(md, "ns_gid_alpha", 1.0);
  noise_floor = get_double_default(md, "ns_gid_noise_floor", 1.0e-12);
  gain_max = get_double_default(md, "ns_gid_gain_max", 1.0e3);
  snr_taper_low = get_double_default(md, "ns_gid_snr_taper_low", 1.0);
  snr_taper_high = get_double_default(md, "ns_gid_snr_taper_high", 3.0);
  use_reliability_taper =
      get_bool_default(md, "ns_gid_use_reliability_taper", false);
  if (mu_min <= 0.0)
    throw MsPASSError(base_error + "ns_gid_mu_min must be positive",
                      ErrorSeverity::Invalid);
  if (alpha < 0.0)
    throw MsPASSError(base_error + "ns_gid_alpha cannot be negative",
                      ErrorSeverity::Invalid);
  if (noise_floor <= 0.0)
    throw MsPASSError(base_error + "ns_gid_noise_floor must be positive",
                      ErrorSeverity::Invalid);
  if (gain_max <= 0.0)
    throw MsPASSError(base_error + "ns_gid_gain_max must be positive",
                      ErrorSeverity::Invalid);
  if (snr_taper_high <= snr_taper_low)
    throw MsPASSError(base_error +
                          "ns_gid_snr_taper_high must exceed taper_low",
                      ErrorSeverity::Invalid);
  shapingwavelet = ShapingWavelet(md, nfft);
  max_gain_actual = 0.0;
  noise_amplification = 0.0;
  effective_bandwidth_fraction = 0.0;
  return 0;
}

void NoiseStableDecon::changeparameter(const Metadata &md) {
  this->read_metadata(md);
}

void NoiseStableDecon::loadnoise(const vector<double> &noise_in) {
  noise = noise_in;
  noise_vector_loaded = true;
  noise_spectrum_loaded = false;
}

void NoiseStableDecon::loadnoise(const CoreTimeSeries &noise_in) {
  this->loadnoise(noise_in.s);
}

void NoiseStableDecon::loadnoise(const PowerSpectrum &noise_spectrum_in) {
  if (noise_spectrum_in.dead())
    throw MsPASSError("NoiseStableDecon::loadnoise: noise PowerSpectrum is "
                      "marked dead",
                      ErrorSeverity::Invalid);
  if (noise_spectrum_in.nf() <= 0 || noise_spectrum_in.spectrum.empty())
    throw MsPASSError("NoiseStableDecon::loadnoise: noise PowerSpectrum is "
                      "empty",
                      ErrorSeverity::Invalid);
  noise_spectrum = noise_spectrum_in;
  noise_spectrum_loaded = true;
  noise_vector_loaded = false;
}

double NoiseStableDecon::reliability_taper(const double snr) const {
  if (!use_reliability_taper)
    return 1.0;
  if (snr <= snr_taper_low)
    return 0.0;
  if (snr >= snr_taper_high)
    return 1.0;
  double x = (snr - snr_taper_low) / (snr_taper_high - snr_taper_low);
  return x * x * (3.0 - 2.0 * x);
}

vector<double> NoiseStableDecon::noise_power_spectrum(const double dt) {
  vector<double> pn(nfft, 0.0);
  if (noise_spectrum_loaded) {
    for (int k = 0; k < nfft; ++k) {
      double f = folded_frequency(k, nfft, dt);
      pn[k] = max(0.0, noise_spectrum.power(f));
    }
  } else if (noise_vector_loaded && !noise.empty()) {
    vector<double> noise_padded(noise);
    if (noise_padded.size() > nfft)
      noise_padded.resize(nfft);
    else if (noise_padded.size() < nfft)
      noise_padded.resize(nfft, 0.0);
    ComplexArray n_fft(nfft, &(noise_padded[0]));
    gsl_fft_complex_forward(n_fft.ptr(), 1, nfft, wavetable, workspace);
    for (int k = 0; k < nfft; ++k) {
      Complex64 z = n_fft[k];
      pn[k] = norm(z);
    }
  }
  return pn;
}

void NoiseStableDecon::process() {
  const string base_error("NoiseStableDecon::process: ");
  result.clear();
  const int output_length = data.size();
  if (output_length <= 0)
    throw MsPASSError(base_error + "no data loaded", ErrorSeverity::Invalid);
  if (wavelet.empty())
    throw MsPASSError(base_error + "no wavelet loaded", ErrorSeverity::Invalid);
  if (output_length > nfft || wavelet.size() > nfft)
    throw MsPASSError(base_error + "loaded vectors exceed padded FFT length",
                      ErrorSeverity::Invalid);

  vector<double> data_padded(data);
  data_padded.resize(nfft, 0.0);
  ComplexArray d_fft(nfft, &(data_padded[0]));
  gsl_fft_complex_forward(d_fft.ptr(), 1, nfft, wavetable, workspace);

  vector<double> wavelet_padded(wavelet);
  wavelet_padded.resize(nfft, 0.0);
  ComplexArray s_fft(nfft, &(wavelet_padded[0]));
  gsl_fft_complex_forward(s_fft.ptr(), 1, nfft, wavetable, workspace);
  if (s_fft.rms() <= 0.0)
    throw MsPASSError(base_error + "wavelet vector is all zeros",
                      ErrorSeverity::Invalid);

  const double dt = shapingwavelet.sample_interval();
  vector<double> pn = this->noise_power_spectrum(dt);
  winv = ComplexArray(nfft);
  max_gain_actual = 0.0;
  noise_amplification = 0.0;
  double spectral_power_peak = 0.0;
  for (int k = 0; k < nfft; ++k)
    spectral_power_peak = max(spectral_power_peak, norm(s_fft[k]));
  const double mu_floor = mu_min * spectral_power_peak;
  int usable_bins = 0;
  for (int k = 0; k < nfft; ++k) {
    Complex64 s = s_fft[k];
    double absS2 = norm(s);
    double absS = sqrt(absS2);
    double snr = absS2 / (pn[k] + noise_floor);
    double mu_noise = alpha * absS2 / (snr + 1.0e-12);
    double mu_gain = max(0.0, absS / gain_max - absS2);
    double mu = max({mu_floor, mu_noise, mu_gain});
    double b = this->reliability_taper(snr);
    Complex64 g(0.0, 0.0);
    if ((absS2 + mu) > 0.0)
      g = b * conj(s) / (absS2 + mu);
    double gain = abs(g);
    if (gain > gain_max) {
      g *= gain_max / gain;
      gain = gain_max;
    }
    double *ptr = winv.ptr(k);
    ptr[0] = g.real();
    ptr[1] = g.imag();
    max_gain_actual = max(max_gain_actual, gain);
    noise_amplification += gain * gain * pn[k];
    if (b > 0.0 && gain > 0.0)
      ++usable_bins;
  }
  noise_amplification = sqrt(noise_amplification / static_cast<double>(nfft));
  effective_bandwidth_fraction =
      static_cast<double>(usable_bins) / static_cast<double>(nfft);

  ComplexArray rf_fft = winv * d_fft;
  gsl_fft_complex_inverse(rf_fft.ptr(), 1, nfft, wavetable, workspace);
  result = ExtractLagWindow(rf_fft, output_length, sample_shift);
}

CoreTimeSeries NoiseStableDecon::actual_output() {
  vector<double> wavelet_padded(wavelet);
  if (wavelet_padded.size() < nfft)
    wavelet_padded.resize(nfft, 0.0);
  ComplexArray W(nfft, &(wavelet_padded[0]));
  gsl_fft_complex_forward(W.ptr(), 1, nfft, wavetable, workspace);
  ComplexArray ao_fft = winv * W;
  gsl_fft_complex_inverse(ao_fft.ptr(), 1, nfft, wavetable, workspace);
  vector<double> ao;
  ao.reserve(nfft);
  for (int k = 0; k < ao_fft.size(); ++k)
    ao.push_back(ao_fft[k].real());
  int i0 = nfft / 2;
  ao = circular_shift(ao, i0);
  CoreTimeSeries result(nfft);
  double dt = shapingwavelet.sample_interval();
  result.set_t0(-dt * static_cast<double>(i0));
  result.set_dt(dt);
  result.set_live();
  result.set_npts(nfft);
  result.set_tref(TimeReferenceType::Relative);
  for (int k = 0; k < nfft; ++k)
    result.s[k] = ao[k];
  result.s = normalize<double>(result.s);
  return result;
}

CoreTimeSeries NoiseStableDecon::inverse_wavelet(const double t0parent) {
  double dt = shapingwavelet.sample_interval();
  ComplexArray no_shaping(nfft);
  for (int k = 0; k < nfft; ++k) {
    double *ptr = no_shaping.ptr(k);
    ptr[0] = 1.0;
    ptr[1] = 0.0;
  }
  return this->FourierInverse(winv, no_shaping, dt, t0parent);
}

CoreTimeSeries NoiseStableDecon::inverse_wavelet() {
  return this->inverse_wavelet(0.0);
}

Metadata NoiseStableDecon::QCMetrics() {
  Metadata md;
  md.put("ns_gid_gain_max_requested", gain_max);
  md.put("ns_gid_gain_max_actual", max_gain_actual);
  md.put("ns_gid_mu_min", mu_min);
  md.put("ns_gid_alpha", alpha);
  md.put("ns_gid_noise_amplification", noise_amplification);
  md.put("ns_gid_effective_bandwidth_fraction",
         effective_bandwidth_fraction);
  md.put("ns_gid_operator_nfft", nfft);
  md.put("ns_gid_use_reliability_taper", use_reliability_taper);
  return md;
}
} // namespace mspass::algorithms::deconvolution
