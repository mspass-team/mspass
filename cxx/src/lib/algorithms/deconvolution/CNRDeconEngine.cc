#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
#include "mspass/utility/MsPASSError.h"
#include <limits>
#include <sstream>
#include <string>

namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;
using namespace mspass::algorithms::deconvolution;
using mspass::algorithms::amplitudes::normalize;

/* Validate sample rate with a small fractional tolerance for clocks whose
 * reported intervals have harmless rounding skew. */
bool sample_interval_invalid(const mspass::seismic::BasicTimeSeries &d,
                             const double operator_dt) {
  const double DTSKEW(0.0001);
  if (!std::isfinite(d.dt()) || d.dt() <= 0.0)
    return true;
  const double frac = abs(d.dt() - operator_dt) / operator_dt;
  return frac >= DTSKEW;
}

/* CNR samples the regularizing spectrum through the operator Nyquist.  A
 * generic valid PowerSpectrum can still be incompatible when it was computed
 * from a different parent sample interval or was frequency-truncated. */
void ValidateCNRNoiseSpectrum(const PowerSpectrum &spectrum,
                              const double operator_dt,
                              const string &caller) {
  ValidatePowerSpectrumCoversDC(spectrum, caller);
  const double DTSKEW(0.0001);
  if (!std::isfinite(spectrum.dt()) || spectrum.dt() <= 0.0 ||
      abs(spectrum.dt() - operator_dt) / operator_dt >= DTSKEW) {
    stringstream ss;
    ss << caller << ": noise PowerSpectrum parent sample interval="
       << spectrum.dt() << " does not match operator sample interval="
       << operator_dt;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
  const double operator_nyquist = 1.0 / (2.0 * operator_dt);
  const double spectrum_fmax =
      spectrum.f0() + spectrum.df() * static_cast<double>(spectrum.nf() - 1);
  const double tolerance = max(1.0e-12, operator_nyquist * 1.0e-10);
  if (spectrum_fmax + tolerance < operator_nyquist) {
    stringstream ss;
    ss << caller << ": noise PowerSpectrum maximum frequency="
       << spectrum_fmax << " does not cover operator Nyquist="
       << operator_nyquist;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
}

TimeSeries InvalidActualOutput(const TimeSeries &wavelet,
                               const string &message) {
  TimeSeries badout(wavelet);
  badout.kill();
  badout.set_npts(0);
  badout.elog.log_error("CNRDeconEngine::actual_output", message,
                        ErrorSeverity::Invalid);
  return badout;
}

TimeSeries InvalidInverseWavelet(const TimeSeries &wavelet,
                                 const string &message) {
  TimeSeries badout(wavelet);
  badout.kill();
  badout.set_npts(0);
  badout.elog.log_error("CNRDeconEngine::inverse_wavelet", message,
                        ErrorSeverity::Invalid);
  return badout;
}

/* Finite SNR convention shared by both public processing paths.  A nonzero
 * signal over an exact (or numerically negligible) zero-noise estimate is the
 * ideal-noise-free limit, represented by the same finite cap used in GWL
 * inverse construction.  Zero signal and zero noise contain no bandwidth
 * information and therefore map to zero, not NaN. */
double FiniteCNRAmplitudeSNR(const double signal_amplitude,
                             const double noise_amplitude) {
  constexpr double ZERO_NOISE_SNR_CAP = 10000.0;
  if (signal_amplitude <= 0.0)
    return 0.0;
  if (noise_amplitude <= 0.0 ||
      (noise_amplitude / signal_amplitude) < DBL_EPSILON)
    return ZERO_NOISE_SNR_CAP;
  const double ratio = signal_amplitude / noise_amplitude;
  return std::isfinite(ratio) ? ratio : ZERO_NOISE_SNR_CAP;
}

void ValidateCNRDatum(const Seismogram &d, const int operator_nfft,
                      const string &caller) {
  if (!std::isfinite(d.t0()))
    throw MsPASSError(caller + ": datum start time must be finite",
                      ErrorSeverity::Invalid);
  if (d.npts() <= 0)
    throw MsPASSError(caller + ": datum must contain at least one sample",
                      ErrorSeverity::Invalid);
  if (d.npts() > operator_nfft) {
    stringstream ss;
    ss << caller << ": datum length=" << d.npts()
       << " exceeds the configured FFT buffer length=" << operator_nfft;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
  for (int k = 0; k < 3; ++k) {
    for (int j = 0; j < d.npts(); ++j) {
      if (!std::isfinite(d.u(k, j))) {
        stringstream ss;
        ss << caller << ": datum contains a nonfinite sample at component="
           << k << ", sample=" << j;
        throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
      }
    }
  }
}

string InvalidCNRNoiseSamples(const TimeSeries &d) {
  double peak_amplitude(0.0);
  for (const double x : d.s) {
    if (!std::isfinite(x))
      return "Noise data contain nonfinite samples";
    peak_amplitude = max(peak_amplitude, abs(x));
  }
  /* A length-N FFT magnitude is bounded by N*peak.  Keep an additional
   * sqrt(2) margin because power sums squared real and imaginary parts. */
  const double safe_peak =
      sqrt(DBL_MAX / 2.0) / static_cast<double>(d.npts());
  if (peak_amplitude > safe_peak)
    return "Noise data amplitude is too large to compute a finite power "
           "spectrum";
  return string();
}

string InvalidCNRNoiseSamples(const Seismogram &d) {
  double peak_amplitude(0.0);
  for (int k = 0; k < 3; ++k) {
    for (int j = 0; j < d.npts(); ++j) {
      const double x = d.u(k, j);
      if (!std::isfinite(x))
        return "Noise data contain nonfinite samples";
      peak_amplitude = max(peak_amplitude, abs(x));
    }
  }
  const double safe_peak =
      sqrt(DBL_MAX / 2.0) / static_cast<double>(d.npts());
  if (peak_amplitude > safe_peak)
    return "Noise data amplitude is too large to compute a finite power "
           "spectrum";
  return string();
}

CNRDeconEngine::CNRDeconEngine() : FFTDeconOperator() {
  /* This constructor does not initialize everything.  It initializes
  only the simple types and the values are not necessarily reasonable. */
  algorithm = CNR3C_algorithms::colored_noise_damping;
  damp = 1.0;
  noise_floor = 1.5;
  band_snr_floor = 1.5;
  operator_dt = 1.0;
  shaping_wavelet_number_poles = 3;
  winlength = 0;
  snr_regularization_floor = 2.0;
  /* These are computed private variables - we initialize them all to 0*/
  regularization_bandwidth_fraction = 0.0;
  winv_t0_lag = 0;
  for (auto i = 0; i < 3; ++i) {
    peak_snr[i] = 0.0;
    signal_bandwidth_fraction[i] = 0.0;
  }
}
CNRDeconEngine::CNRDeconEngine(const AntelopePf &pf)
    : FFTDeconOperator(dynamic_cast<const Metadata &>(pf)) {
  try {
    /* Initialize inverse-dependent state so a failed first initialization is
     * deterministic and can be rolled back safely. */
    this->regularization_bandwidth_fraction = 0.0;
    this->winv_t0_lag = 0;
    for (auto i = 0; i < 3; ++i) {
      this->peak_snr[i] = 0.0;
      this->signal_bandwidth_fraction[i] = 0.0;
    }
    string stmp;
    stmp = pf.get_string("algorithm");
    if (stmp == "generalized_water_level") {
      this->algorithm = CNR3C_algorithms::generalized_water_level;
    } else if (stmp == "colored_noise_damping") {
      this->algorithm = CNR3C_algorithms::colored_noise_damping;
    } else {
      throw MsPASSError("CNRDeconEngine(constructor):  invalid value for "
                        "parameter algorithm=" +
                            stmp,
                        ErrorSeverity::Fatal);
    }
    this->damp = GetDoubleRequired(pf, "damping_factor");
    if (this->damp <= 0.0) {
      throw MsPASSError("CNRDeconEngine(constructor): damping_factor must be "
                        "positive for stable regularized deconvolution",
                        ErrorSeverity::Fatal);
    }
    /* Note this paramter is used for both the damping method and the
    generalized_water_level */
    this->noise_floor = GetDoubleRequired(pf, "noise_floor");
    this->snr_regularization_floor =
        GetDoubleRequired(pf, "snr_regularization_floor");
    this->band_snr_floor = GetDoubleRequired(pf, "snr_data_bandwidth_floor");
    this->operator_dt = GetDoubleRequired(pf, "target_sample_interval");
    if (this->operator_dt <= 0.0)
      throw MsPASSError("CNRDeconEngine(constructor): "
                        "target_sample_interval must be positive",
                        ErrorSeverity::Fatal);
    /* These parameters are not cached to the object directly but
    are used to initialize the multitaper engines.   A window is used
    instead of number of samples as it is less error prone to a user than
    requiring them to compute the number from the sample interval. */
    double ts, te;
    ts = GetDoubleRequired(pf, "deconvolution_data_window_start");
    te = GetDoubleRequired(pf, "deconvolution_data_window_end");
    ValidateWindowDuration(TimeWindow(ts, te), "deconvolution_data_window",
                           "CNRDeconEngine(constructor)");
    this->winlength = round((te - ts) / this->operator_dt) + 1;
    /* In this algorithm we are very careful to avoid circular convolution
    artifacts that I (glp) suspect may be a problem in some frequency domain
    implementations of rf deconvolution.   Here we set the length of the fft
    (nfft) to a minimum of 3 times the window size.   That allows 1 window
    of padding around both ends of the waveform being deconvolved.  Circular
    shift is used to put the result back in a rational time base. */
    int minwinsize = 3 * (this->winlength);
    /* This complicated set of tests to set nfft is needed to mesh with
     * ShapingWavelet constructor and FFTDeconOperator api constraints created
     * by use in other classes in this directory that also use these */
    int nfftneeded = nextPowerOf2(
        max(minwinsize, GetIntRequired(pf, "operator_nfft")));
    /* This compication is needed because FFTDeconOperator(pf) is called
    prior to this function and it requires operator_nfft.   We need to
    be sure it size is consistent with window size that we just computed */
    if (nfftneeded != this->get_size()) {
      FFTDeconOperator::change_size(nfftneeded);
      Metadata pfcopy(pf);
      pfcopy.put("operator_nfft", nfftneeded);
      this->shapingwavelet = ShapingWavelet(pfcopy);
    } else {
      this->shapingwavelet = ShapingWavelet(pf);
    }
    /* ShapingWavelet has more options than can be accepted in this algorithm
    so this test is needed */
    string swname = this->shapingwavelet.type();
    if (!((swname == "ricker") || (swname == "butterworth"))) {
      throw MsPASSError(
          string("CNRDeconEngine(AntelopePf constructor):  ") +
              "Cannot use shaping wavelet type=" + swname +
              "\nMust be either ricker or butterworth for this algorithm",
          ErrorSeverity::Fatal);
    }
    if (swname == "butterworth") {
      /* These MUST be consistent with FFTDeconOperator pf constructor
      names.   */
      int npoles_lo, npoles_hi;
      npoles_lo = GetIntRequired(pf, "npoles_lo");
      npoles_hi = GetIntRequired(pf, "npoles_hi");
      if (npoles_hi != npoles_lo) {
        stringstream ss;
        ss << "CNRDeconEngine(Metadata constructor):  "
           << "Butterworth filter high and low number of poles must be equal "
              "for this operator"
           << endl
           << "Found npoles_lo=" << npoles_lo << " and npoles_hi=" << npoles_hi
           << endl
           << "Edit parameter file to make those two parameters equal and rerun"
           << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
      }
      this->shaping_wavelet_number_poles = npoles_lo;
    }
    this->configured_shapingwavelet = this->shapingwavelet;
    /* As with signal we use this for initializing the noise engine
    rather than the number of points, which is all the engine cares about. */
    ts = GetDoubleRequired(pf, "noise_window_start");
    te = GetDoubleRequired(pf, "noise_window_end");
    ValidateWindowDuration(TimeWindow(ts, te), "noise_window",
                           "CNRDeconEngine(constructor)");
    int noise_winlength = round((te - ts) / operator_dt) + 1;
    double tbp = GetDoubleRequired(pf, "time_bandwidth_product");
    long ntapers = GetLongRequired(pf, "number_tapers");
    if (ntapers <= 0 || ntapers > numeric_limits<int>::max())
      throw MsPASSError(
          "CNRDeconEngine(constructor): number_tapers is outside the "
          "supported positive integer range",
          ErrorSeverity::Fatal);
    const int ntapers_to_use = static_cast<int>(ntapers);
    /* The inverse operator samples its regularizing spectrum through
     * Nyquist.  Use the operator's even FFT grid so an internally computed
     * spectrum contains a real Nyquist ordinate rather than requiring
     * extrapolation from an odd-length transform. */
    this->noise_engine =
        MTPowerSpectrumEngine(noise_winlength, tbp, ntapers_to_use,
                              this->get_size(), this->operator_dt);
    this->signal_engine =
        MTPowerSpectrumEngine(this->winlength, tbp, ntapers_to_use,
                              this->winlength, this->operator_dt);
  } catch (...) {
    throw;
  };
}
/* Standard copy constructor */
CNRDeconEngine::CNRDeconEngine(const CNRDeconEngine &parent)
    : FFTDeconOperator(parent), shapingwavelet(parent.shapingwavelet),
      configured_shapingwavelet(parent.configured_shapingwavelet),
      signal_engine(parent.signal_engine), noise_engine(parent.noise_engine),
      winv(parent.winv) {
  this->algorithm = parent.algorithm;
  this->damp = parent.damp;
  this->noise_floor = parent.noise_floor;
  this->band_snr_floor = parent.band_snr_floor;
  this->operator_dt = parent.operator_dt;
  this->winlength = parent.winlength;
  this->shaping_wavelet_number_poles = parent.shaping_wavelet_number_poles;
  this->snr_regularization_floor = parent.snr_regularization_floor;
  this->regularization_bandwidth_fraction =
      parent.regularization_bandwidth_fraction;
  for (int i = 0; i < 3; ++i) {
    this->peak_snr[i] = parent.peak_snr[i];
    this->signal_bandwidth_fraction[i] = parent.signal_bandwidth_fraction[i];
  }
  winv_t0_lag = parent.winv_t0_lag;
}

void CNRDeconEngine::changeparameter(const Metadata &md) {
  try {
    string stmp(md.get_string("algorithm"));
    if (stmp == "generalized_water_level") {
      this->algorithm = CNR3C_algorithms::generalized_water_level;
    } else if (stmp == "colored_noise_damping") {
      this->algorithm = CNR3C_algorithms::colored_noise_damping;
    } else {
      throw MsPASSError("CNRDeconEngine::changeparameter: invalid value for "
                        "parameter algorithm=" +
                            stmp,
                        ErrorSeverity::Fatal);
    }
    this->damp = GetDoubleRequired(md, "damping_factor");
    if (this->damp <= 0.0)
      throw MsPASSError("CNRDeconEngine::changeparameter: damping_factor must "
                        "be positive for stable regularized deconvolution",
                        ErrorSeverity::Fatal);
    this->noise_floor = GetDoubleRequired(md, "noise_floor");
    this->snr_regularization_floor =
        GetDoubleRequired(md, "snr_regularization_floor");
    this->band_snr_floor = GetDoubleRequired(md, "snr_data_bandwidth_floor");
    this->operator_dt = GetDoubleRequired(md, "target_sample_interval");
    if (this->operator_dt <= 0.0)
      throw MsPASSError("CNRDeconEngine::changeparameter: "
                        "target_sample_interval must be positive",
                        ErrorSeverity::Fatal);

    double ts(GetDoubleRequired(md, "deconvolution_data_window_start"));
    double te(GetDoubleRequired(md, "deconvolution_data_window_end"));
    ValidateWindowDuration(TimeWindow(ts, te), "deconvolution_data_window",
                           "CNRDeconEngine::changeparameter");
    this->winlength = round((te - ts) / this->operator_dt) + 1;
    int nfftneeded = nextPowerOf2(
        max(3 * this->winlength, GetIntRequired(md, "operator_nfft")));
    if (nfftneeded != this->get_size())
      FFTDeconOperator::change_size(nfftneeded);
    this->change_shift(ComputeDeconSampleShift(md));
    if (this->get_shift() < 0 || this->get_shift() > this->get_size())
      throw MsPASSError("CNRDeconEngine::changeparameter: computed sample "
                        "shift is inconsistent with FFT length",
                        ErrorSeverity::Fatal);

    Metadata mdcopy(md);
    mdcopy.put("operator_nfft", nfftneeded);
    this->shapingwavelet = ShapingWavelet(mdcopy);
    string swname(this->shapingwavelet.type());
    if (!((swname == "ricker") || (swname == "butterworth"))) {
      throw MsPASSError(
          string("CNRDeconEngine::changeparameter: ") +
              "Cannot use shaping wavelet type=" + swname +
              "\nMust be either ricker or butterworth for this algorithm",
          ErrorSeverity::Fatal);
    }
    if (swname == "butterworth") {
      int npoles_lo(GetIntRequired(md, "npoles_lo"));
      int npoles_hi(GetIntRequired(md, "npoles_hi"));
      if (npoles_hi != npoles_lo) {
        stringstream ss;
        ss << "CNRDeconEngine::changeparameter: Butterworth filter high and "
              "low number of poles must be equal"
           << endl
           << "Found npoles_lo=" << npoles_lo << " and npoles_hi=" << npoles_hi
           << endl;
        throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
      }
      this->shaping_wavelet_number_poles = npoles_lo;
    }
    this->configured_shapingwavelet = this->shapingwavelet;

    ts = GetDoubleRequired(md, "noise_window_start");
    te = GetDoubleRequired(md, "noise_window_end");
    ValidateWindowDuration(TimeWindow(ts, te), "noise_window",
                           "CNRDeconEngine::changeparameter");
    int noise_winlength = round((te - ts) / operator_dt) + 1;
    double tbp = GetDoubleRequired(md, "time_bandwidth_product");
    long ntapers = GetLongRequired(md, "number_tapers");
    if (ntapers <= 0 || ntapers > numeric_limits<int>::max())
      throw MsPASSError(
          "CNRDeconEngine::changeparameter: number_tapers is outside the "
          "supported positive integer range",
          ErrorSeverity::Fatal);
    const int ntapers_to_use = static_cast<int>(ntapers);
    this->noise_engine =
        MTPowerSpectrumEngine(noise_winlength, tbp, ntapers_to_use,
                              this->get_size(), this->operator_dt);
    this->signal_engine =
        MTPowerSpectrumEngine(this->winlength, tbp, ntapers_to_use,
                              this->winlength, this->operator_dt);
  } catch (...) {
    throw;
  };
}

CNRDeconEngine &CNRDeconEngine::operator=(const CNRDeconEngine &parent) {
  if (&parent != this) {
    FFTDeconOperator::operator=(parent);
    this->shapingwavelet = parent.shapingwavelet;
    this->configured_shapingwavelet = parent.configured_shapingwavelet;
    this->signal_engine = parent.signal_engine;
    this->noise_engine = parent.noise_engine;
    this->winv = parent.winv;
    this->winv_t0_lag = parent.winv_t0_lag;
    this->algorithm = parent.algorithm;
    this->damp = parent.damp;
    this->noise_floor = parent.noise_floor;
    this->band_snr_floor = parent.band_snr_floor;
    this->operator_dt = parent.operator_dt;
    this->winlength = parent.winlength;
    this->shaping_wavelet_number_poles = parent.shaping_wavelet_number_poles;
    this->snr_regularization_floor = parent.snr_regularization_floor;
    this->regularization_bandwidth_fraction =
        parent.regularization_bandwidth_fraction;
    for (int i = 0; i < 3; ++i) {
      this->peak_snr[i] = parent.peak_snr[i];
      this->signal_bandwidth_fraction[i] = parent.signal_bandwidth_fraction[i];
    }
  }
  return *this;
}
void CNRDeconEngine::initialize_inverse_operator(const TimeSeries &wavelet,
                                                 const TimeSeries &noise_data) {
  const string alg("CNRDeconEngine::initialize_inverse_operator");
  if (wavelet.dead() || noise_data.dead()) {
    string message;
    message = alg + string(":  Received TimeSeries inputs marked dead\n");
    if (wavelet.dead())
      message += "wavelet signal input was marked dead\n";
    if (noise_data.dead())
      message += "noise data segment was marked dead\n";
    throw MsPASSError(message, ErrorSeverity::Invalid);
  }
  const int minimum_noise_samples =
      max(1, this->noise_engine.number_tapers());
  if (noise_data.npts() < minimum_noise_samples) {
    stringstream ss;
    ss << alg << ": noise data must contain at least "
       << minimum_noise_samples
       << " samples to construct the configured multitaper spectrum";
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
  try {
    /* Validate here, before compute_noise_spectrum can resize its cached
     * multitaper engine.  MTPowerSpectrumEngine intentionally has a looser
     * clock tolerance than the fixed-sample-rate CNR operator. */
    if (sample_interval_invalid(noise_data, this->operator_dt)) {
      stringstream ss;
      ss << alg << ": noise data sample interval=" << noise_data.dt()
         << " does not match operator sample interval=" << this->operator_dt;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    PowerSpectrum psnoise(this->compute_noise_spectrum(noise_data));
    if (psnoise.dead()) {
      string message;
      message = alg + string("compute_noise_spectrum method failed - cannot "
                             "compute inverse opeator");
      throw MsPASSError(message, ErrorSeverity::Invalid);
    }
    this->initialize_inverse_operator(wavelet, psnoise);
  } catch (...) {
    throw;
  };
}
void CNRDeconEngine::initialize_inverse_operator(
    const TimeSeries &wavelet, const PowerSpectrum &noise_spectrum) {
  string alg("CNRDeconEngine::initialize_inverse_operator");
  if (wavelet.dead()) {
    string message;
    message = alg + string("Received wavelet signal marked dead");
    throw MsPASSError(message, ErrorSeverity::Invalid);
  }
  if (noise_spectrum.dead()) {
    string message;
    message = alg + string("Received a PowerSpectrum object marked dead");
    throw MsPASSError(message, ErrorSeverity::Invalid);
  }
  try {
    if (sample_interval_invalid(wavelet, this->operator_dt)) {
      stringstream ss;
      ss << alg << ": wavelet sample interval=" << wavelet.dt()
         << " does not match operator sample interval=" << this->operator_dt;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    ValidateCNRNoiseSpectrum(noise_spectrum, this->operator_dt, alg);
    this->compute_winv(wavelet, noise_spectrum);
  } catch (...) {
    throw;
  };
}
PowerSpectrum CNRDeconEngine::compute_noise_spectrum(const TimeSeries &n) {
  if (n.dead()) {
    PowerSpectrum badout;
    badout.elog.log_error("CNRDeconEngine:compute_noise",
                          "Received noise data segment marked dead",
                          ErrorSeverity::Invalid);
    return badout;
  }
  const int minimum_noise_samples = max(1, noise_engine.number_tapers());
  if (n.npts() < minimum_noise_samples) {
    PowerSpectrum badout;
    stringstream ss;
    ss << "Noise data must contain at least " << minimum_noise_samples
       << " samples to construct the configured multitaper spectrum";
    badout.elog.log_error(
        "CNRDeconEngine::compute_noise_spectrum", ss.str(),
        ErrorSeverity::Invalid);
    badout.kill();
    return badout;
  }
  if (sample_interval_invalid(n, this->operator_dt)) {
    PowerSpectrum badout;
    stringstream ss;
    ss << "Noise data sample interval=" << n.dt()
       << " does not match operator sample interval=" << this->operator_dt;
    badout.elog.log_error("CNRDeconEngine::compute_noise_spectrum", ss.str(),
                          ErrorSeverity::Invalid);
    badout.kill();
    return badout;
  }
  const string sample_error = InvalidCNRNoiseSamples(n);
  if (!sample_error.empty()) {
    PowerSpectrum badout;
    badout.elog.log_error("CNRDeconEngine::compute_noise_spectrum",
                          sample_error, ErrorSeverity::Invalid);
    badout.kill();
    return badout;
  }
  try {
    if (n.npts() != noise_engine.taper_length()) {
      /* use this varaint of the construtor too allow the fft size to
       * be automatically changed if necessary.  */
      this->noise_engine = MTPowerSpectrumEngine(
          n.npts(), noise_engine.time_bandwidth_product(),
          noise_engine.number_tapers(), this->get_size(), this->operator_dt);
    }
    return this->noise_engine.apply(n);
  } catch (...) {
    throw;
  };
}
PowerSpectrum CNRDeconEngine::compute_noise_spectrum(const Seismogram &n) {
  if (n.dead()) {
    PowerSpectrum badout;
    badout.elog.log_error("CNRDeconEngine:compute_noise",
                          "Received noise data segment marked dead",
                          ErrorSeverity::Invalid);
    return badout;
  }
  const int minimum_noise_samples = max(1, noise_engine.number_tapers());
  if (n.npts() < minimum_noise_samples) {
    PowerSpectrum badout;
    stringstream ss;
    ss << "Noise data must contain at least " << minimum_noise_samples
       << " samples to construct the configured multitaper spectrum";
    badout.elog.log_error(
        "CNRDeconEngine::compute_noise_spectrum", ss.str(),
        ErrorSeverity::Invalid);
    badout.kill();
    return badout;
  }
  if (sample_interval_invalid(n, this->operator_dt)) {
    PowerSpectrum badout;
    stringstream ss;
    ss << "Noise data sample interval=" << n.dt()
       << " does not match operator sample interval=" << this->operator_dt;
    badout.elog.log_error("CNRDeconEngine::compute_noise_spectrum", ss.str(),
                          ErrorSeverity::Invalid);
    badout.kill();
    return badout;
  }
  const string sample_error = InvalidCNRNoiseSamples(n);
  if (!sample_error.empty()) {
    PowerSpectrum badout;
    badout.elog.log_error("CNRDeconEngine::compute_noise_spectrum",
                          sample_error, ErrorSeverity::Invalid);
    badout.kill();
    return badout;
  }
  try {
    PowerSpectrum avg3c;
    TimeSeries tswork;
    if (n.npts() != noise_engine.taper_length()) {
      noise_engine = MTPowerSpectrumEngine(
          n.npts(), noise_engine.time_bandwidth_product(),
          noise_engine.number_tapers(), this->get_size(), this->operator_dt);
    }
    for (int k = 0; k < 3; ++k) {
      tswork = TimeSeries(ExtractComponent(n, k), "Invalid");
      PowerSpectrum psnoise = this->noise_engine.apply(tswork);
      if (psnoise.dead()) {
        return psnoise;
      }
      if (k == 0)
        avg3c = psnoise;
      else
        avg3c += psnoise;
    }
    /* We define total power as the average on all three
    components */
    double scl = 1.0 / 3.0;
    for (int i = 0; i < avg3c.nf(); ++i)
      avg3c.spectrum[i] *= scl;
    return avg3c;
  } catch (...) {
    throw;
  };
}

/*! private method of this class that computes the internal
  variable "winv" - a ComplexArray containing the spectrum of the
  inverse wavelet.   It assumes the content of the internally
  cached "wavelet" TimeSeries object contains the data to be
  used to compute winv.   Which inverse operator to use is
  controlled by this->algorithm.

  Do not use this method outside of the internal use.   Its
  intrinsic state dependence on loading the wavelet data before
  calling it is very error prone.  It is done here largely for
  convenience to modularize the algorithm.
  */
void CNRDeconEngine::compute_winv(const TimeSeries &wavelet,
                                  const PowerSpectrum &psnoise) {
  /* Because this is a private method we don't test if wavelet and psnoise
  are marked dead.  Methods that call this one should always do so though.*/
  const int previous_nfft = this->get_size();
  const ComplexArray previous_winv(this->winv);
  const int previous_t0_lag = this->winv_t0_lag;
  const double previous_regularization_fraction =
      this->regularization_bandwidth_fraction;
  try {
    /* Need to always create a local copy to allow taper option to work
       corectly. Also wavelet is passed const so taper would not work anyway */
    TimeSeries w(wavelet);
    if (w.npts() <= 0)
      throw MsPASSError(
          "CNRDeconEngine::compute_winv: wavelet must contain at least one "
          "sample",
          ErrorSeverity::Invalid);
    if (!std::isfinite(w.t0()))
      throw MsPASSError(
          "CNRDeconEngine::compute_winv: wavelet start time must be finite",
          ErrorSeverity::Invalid);
    bool has_nonzero_sample(false);
    for (const double x : w.s) {
      if (!std::isfinite(x))
        throw MsPASSError(
            "CNRDeconEngine::compute_winv: wavelet contains NaN or infinite "
            "samples",
            ErrorSeverity::Invalid);
      if (x != 0.0)
        has_nonzero_sample = true;
    }
    if (!has_nonzero_sample)
      throw MsPASSError(
          "CNRDeconEngine::compute_winv: wavelet must contain at least one "
          "nonzero sample",
          ErrorSeverity::Invalid);
    const int candidate_t0_lag = w.sample_number(0.0);
    if (candidate_t0_lag < 0 || candidate_t0_lag >= this->get_size()) {
      stringstream ss;
      ss << "CNRDeconEngine::compute_winv: wavelet zero-time sample lag="
         << candidate_t0_lag << " is outside the FFT buffer range [0,"
         << this->get_size() << ")";
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    switch (algorithm) {
    case CNR3C_algorithms::generalized_water_level:
      compute_gwl_inverse(w, psnoise);
      break;
    case CNR3C_algorithms::colored_noise_damping:
    default:
      compute_gdamp_inverse(w, psnoise);
    };
    /* Commit the phase reference only after the matching inverse operator was
     * constructed successfully. */
    this->winv_t0_lag = candidate_t0_lag;
  } catch (...) {
    /* Some inverse builders can resize the FFT workspace before a later
     * operation fails.  Restore every inverse-dependent value so callers may
     * continue using the last successfully initialized operator. */
    if (this->get_size() != previous_nfft)
      FFTDeconOperator::change_size(previous_nfft);
    this->winv = previous_winv;
    this->winv_t0_lag = previous_t0_lag;
    this->regularization_bandwidth_fraction =
        previous_regularization_fraction;
    throw;
  };
}

void CNRDeconEngine::compute_gwl_inverse(const TimeSeries &wavelet,
                                         const PowerSpectrum &psnoise) {
  try {
    if (wavelet.npts() > FFTDeconOperator::nfft) {
      stringstream ss;
      ss << "CNRDeconEngine::compute_gwl_inverse(): wavelet length="
         << wavelet.npts() << " exceeds the fixed FFT buffer length="
         << FFTDeconOperator::nfft << endl
         << "Use a wavelet no longer than the configured CNR FFT buffer";
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    /* ComplexArray pads the source vector with zeros when nfft is larger. */
    ComplexArray cwvec(FFTDeconOperator::nfft, wavelet.s);
    gsl_fft_complex_forward(cwvec.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                            workspace);
    /* This computes the (regularized) denominator for the decon operator*/
    double df, fNy;
    df = 1.0 / (operator_dt * static_cast<double>(FFTDeconOperator::nfft));
    fNy = df * static_cast<double>(FFTDeconOperator::nfft / 2);
    /* We need largest noise amplitude to establish a relative noise floor.
    We use this std::algorithm to find it in the spectrum vector */
    vector<double>::iterator maxnoise;
    /* Copy needed because max_element alters content of work vector */
    vector<double> work(psnoise.spectrum);
    maxnoise = std::max_element(work.begin(), work.end());
    // spectrum is power, we need amplitude so sqrt here
    double scaled_noise_floor = noise_floor * sqrt(*maxnoise);
    vector<double> wavelet_snr;
    wavelet_snr.clear();
    vector<bool> spectral_null(FFTDeconOperator::nfft, false);
    int nreg(0);
    for (int j = 0; j < FFTDeconOperator::nfft; ++j) {
      double *z = cwvec.ptr(j);
      double re = (*z);
      double im = (*(z + 1));
      double amp = hypot(re, im);
      double f;
      f = df * static_cast<double>(j);
      if (f > fNy)
        f = 2.0 * fNy - f; // Fold frequency axis
      double namp = sqrt(psnoise.power(f));
      /* An exact spectral null carries no phase or amplitude information, so
       * its Moore-Penrose inverse gain is zero.  Use a finite placeholder for
       * the vector division below, then explicitly zero that output bin. */
      if (amp == 0.0) {
        spectral_null[j] = true;
        *z = 1.0;
        *(z + 1) = 0.0;
        wavelet_snr.push_back(0.0);
        ++nreg;
        continue;
      }
      /* Avoid divide by zero that could randomly happen with simulation data*/
      double snr;
      if ((namp / amp) < DBL_EPSILON)
        snr = 10000.0;
      else
        snr = amp / namp;
      wavelet_snr.push_back(snr);
      if (snr < snr_regularization_floor) {
        /* Form the regularized complex value from its unit phase instead of
         * multiplying by target/amp.  The latter can overflow for a very
         * small but nonzero bin even though the desired value is finite. */
        const double target_amp =
            snr_regularization_floor * max(namp, scaled_noise_floor);
        re = target_amp * (re / amp);
        im = target_amp * (im / amp);
        *z = re;
        *(z + 1) = im;
        ++nreg;
      }
    }
    /* This is used in QCMetric */
    this->regularization_bandwidth_fraction =
        static_cast<double>(nreg) / static_cast<double>(FFTDeconOperator::nfft);
    vector<double> d0(FFTDeconOperator::nfft, 0.0);
    d0[0] = 1.0;
    ComplexArray delta0(FFTDeconOperator::nfft, d0);
    gsl_fft_complex_forward(delta0.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                            workspace);
    winv = delta0 / cwvec;
    for (int j = 0; j < FFTDeconOperator::nfft; ++j) {
      if (spectral_null[j]) {
        double *z = winv.ptr(j);
        *z = 0.0;
        *(z + 1) = 0.0;
      }
    }
  } catch (...) {
    throw;
  };
}
/* Note this is intentionally not a reference to assure this is a copy */
void CNRDeconEngine::compute_gdamp_inverse(const TimeSeries &wavelet,
                                           const PowerSpectrum &psnoise) {
  try {
    /* Assume if we got here wavelet.npts() == nfft*/
    ComplexArray b_fft;
    if (wavelet.npts() == FFTDeconOperator::nfft) {
      b_fft = ComplexArray(wavelet.npts(), wavelet.s);
    } else if (wavelet.npts() < FFTDeconOperator::nfft) {
      /* In this case we zero pad*/
      std::vector<double> btmp;
      btmp.reserve(FFTDeconOperator::nfft);
      for (auto i = 0; i < wavelet.npts(); ++i)
        btmp.push_back(wavelet.s[i]);
      for (auto i = wavelet.npts(); i < FFTDeconOperator::nfft; ++i)
        btmp.push_back(0.0);
      b_fft = ComplexArray(FFTDeconOperator::nfft, btmp);
    } else {
      stringstream ss;
      ss << "CNRDeconEngine::compute_gdamp_inverse(): wavelet length="
         << wavelet.npts() << " exceeds the fixed FFT buffer length="
         << FFTDeconOperator::nfft << endl
         << "Use a wavelet no longer than the configured CNR FFT buffer";
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    gsl_fft_complex_forward(b_fft.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                            workspace);
    ComplexArray conj_b_fft(b_fft);
    conj_b_fft.conj();
    ComplexArray denom(conj_b_fft * b_fft);
    /* Compute scaling constants for noise based on noise_floor and the
    noise spectrum */
    double df, fNy;
    df = 1.0 / (operator_dt * static_cast<double>(FFTDeconOperator::nfft));
    fNy = df * static_cast<double>(FFTDeconOperator::nfft / 2);
    /* We need largest noise amplitude to establish a relative noise floor.
    We use this std::algorithm to find it in the spectrum vector */
    vector<double>::iterator maxnoise;
    /* Copy needed because max_element alters content of work vector */
    vector<double> work(psnoise.spectrum);
    maxnoise = std::max_element(work.begin(), work.end());
    // Spectrum is power but need amplitude in this context so sqrt here
    double scaled_noise_floor = noise_floor * sqrt(*maxnoise);
    vector<bool> spectral_null(nfft, false);

    for (int k = 0; k < nfft; ++k) {
      double *ptr;
      ptr = denom.ptr(k);
      double f;
      f = df * static_cast<double>(k);
      if (f > fNy)
        f = 2.0 * fNy - f; // Fold frequency axis
      double namp = sqrt(psnoise.power(f));
      double theta;
      if (namp > scaled_noise_floor) {
        theta = damp * namp;
      } else {
        theta = damp * scaled_noise_floor;
      }
      /* This uses a normal equation form so theta must be squared to
      be a form of the standard damped least squares inverse */
      theta = theta * theta;
      /* ptr points to the real part - an oddity of this interface */
      *ptr += theta;
      /* With an all-zero noise spectrum theta is zero.  If the source also
       * has an exact spectral null this normal-equation denominator is zero;
       * assign the Moore-Penrose gain of zero instead of evaluating 0/0. */
      if ((*ptr == 0.0) && (*(ptr + 1) == 0.0)) {
        spectral_null[k] = true;
        *ptr = 1.0;
      }
    }
    winv = conj_b_fft / denom;
    for (int k = 0; k < nfft; ++k) {
      if (spectral_null[k]) {
        double *z = winv.ptr(k);
        *z = 0.0;
        *(z + 1) = 0.0;
      }
    }
  } catch (...) {
    throw;
  };
}
/* Computes deconvolution of data in d using inverse operator that was assumed
to be previously loaded via the initialize_wavelet method.   Note a potential
confusion is that because this opeation is done in the frequency domain
we do NOT apply the shaping wavelet filter to either the data (numerator) or
the denominator (the inverse wavelet) as required by convolutional quelling
(an obscure name for this regularization from an old Backus paper).   The reason
is the form is (f)(d)/(f)(w)  where f is the shaping wavelet filter, d is
the numerator fft, and w is the wavelet fft.   The f terms cancel so we
don't apply them to either the numerator or denominator.  We do need to
post filter with the shaping wavelet, which is done here, or the output
will almost always be junk.
*/
Seismogram CNRDeconEngine::process(const Seismogram &d,
                                   const PowerSpectrum &psnoise,
                                   const double fl, const double fh) {
  const string alg("CNRDeconEngine::process");
  if (d.dead() || psnoise.dead()) {
    Seismogram dout(d);
    dout.set_npts(0);
    if (d.dead()) {
      dout.elog.log_error(
          alg, "received Seismogram input segment marked dead - cannot process",
          ErrorSeverity::Invalid);
    }
    if (psnoise.dead()) {
      dout.elog.log_error(
          alg, "received PowerSpectrum object marked dead - cannot process",
          ErrorSeverity::Invalid);
    }
    dout.kill();
    return dout;
  }
  const ShapingWavelet previous_shapingwavelet(this->shapingwavelet);
  double candidate_peak_snr[3] = {0.0, 0.0, 0.0};
  double candidate_signal_bandwidth_fraction[3] = {0.0, 0.0, 0.0};
  try {
    string base_error("CNRDeconEngine::process:  ");
    if (sample_interval_invalid(d, this->operator_dt)) {
      stringstream ss;
      ss << alg << ": datum sample interval=" << d.dt()
         << " does not match operator sample interval=" << this->operator_dt;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    ValidateCNRNoiseSpectrum(psnoise, this->operator_dt, alg);
    ValidateCNRDatum(d, FFTDeconOperator::nfft, alg);
    this->update_shaping_wavelet(fl, fh);
    Seismogram rfest(d);
    /* The inverse wavelet may have a different time origin from d.  Its
     * zero-time sample, recorded when winv was initialized, is the phase
     * reference for the inverse FFT output. */
    const int t0_shift = this->winv_t0_lag;
    vector<double> wvec;
    wvec.reserve(FFTDeconOperator::nfft);
    /* The set_npts method is assumed to not only set that attribute
     * but initialize the u matrix to a 3xnfft matrix.*/
    if (rfest.npts() != FFTDeconOperator::nfft)
      rfest.set_npts(FFTDeconOperator::nfft);
    int nhighsnr;
    double df;
    df = 1.0 / (operator_dt * static_cast<double>(FFTDeconOperator::nfft));
    for (int k = 0; k < 3; ++k) {
      TimeSeries work;
      work = TimeSeries(ExtractComponent(d, k), "Invalid");
      wvec.clear();
      int ntocopy = FFTDeconOperator::nfft;
      if (ntocopy > work.npts())
        ntocopy = work.npts();
      for (int j = 0; j < ntocopy; ++j)
        wvec.push_back(work.s[j]);
      for (int j = ntocopy; j < FFTDeconOperator::nfft; ++j)
        wvec.push_back(0.0);

      ComplexArray numerator(FFTDeconOperator::nfft, &(wvec[0]));
      gsl_fft_complex_forward(numerator.ptr(), 1, FFTDeconOperator::nfft,
                              wavetable, workspace);
      for (int j = 0; j < FFTDeconOperator::nfft; ++j) {
        const Complex64 z = numerator[j];
        if (!std::isfinite(z.real()) || !std::isfinite(z.imag()))
          throw MsPASSError(
              base_error + "datum FFT contains nonfinite values",
              ErrorSeverity::Invalid);
      }
      /* This loop computes QCMetrics of bandwidth fraction that
      is above a defined snr floor - not necessarily the same as the
      regularization floor used in computing the inverse */
      double snrmax;
      snrmax = 0.0;
      nhighsnr = 0;
      for (int j = 0; j < FFTDeconOperator::nfft / 2; ++j) {
        double f;
        f = df * static_cast<double>(j);
        Complex64 z = numerator[j];
        double sigamp = abs(z);
        double namp = sqrt(psnoise.power(f));
        const double snr = FiniteCNRAmplitudeSNR(sigamp, namp);

        if (snr > snrmax)
          snrmax = snr;
        if (snr > this->band_snr_floor)
          ++nhighsnr;
      }
      candidate_signal_bandwidth_fraction[k] =
          static_cast<double>(nhighsnr) /
          static_cast<double>(FFTDeconOperator::nfft / 2);
      candidate_peak_snr[k] = snrmax;
      ComplexArray rftmp = numerator * winv;
      rftmp = (*this->shapingwavelet.wavelet()) * rftmp;
      gsl_fft_complex_inverse(rftmp.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                              workspace);
      wvec.clear();
      for (int j = 0; j < FFTDeconOperator::nfft; ++j) {
        const Complex64 z = rftmp[j];
        if (!std::isfinite(z.real()) || !std::isfinite(z.imag()))
          throw MsPASSError(
              base_error + "inverse FFT output contains nonfinite values",
              ErrorSeverity::Invalid);
        wvec.push_back(z.real());
      }
      /* Note we used a time domain shift instead of using a linear phase
      shift in the frequency domain because the time domain operator has a lower
      operation count than the frequency domain algorithm and is thus more
      efficient.*/
      if (t0_shift != 0)
        wvec = circular_shift(wvec, -t0_shift);
      for (int j = 0; j < FFTDeconOperator::nfft; ++j)
        rfest.u(k, j) = wvec[j];
    }
    for (int k = 0; k < 3; ++k) {
      this->peak_snr[k] = candidate_peak_snr[k];
      this->signal_bandwidth_fraction[k] =
          candidate_signal_bandwidth_fraction[k];
    }
    return rfest;
  } catch (...) {
    this->shapingwavelet = previous_shapingwavelet;
    throw;
  };
}

Seismogram CNRDeconEngine::process(const Seismogram &d,
                                   const PowerSpectrum &psnoise) {
  /* Dead inputs follow the public logged-dead return convention.  Do not
   * switch to configured shaping for this normal early return: no processing
   * occurred, so every engine state value must remain as it was on entry. */
  if (d.dead() || psnoise.dead())
    return this->process_with_current_shaping(d, psnoise);
  if (!d.dead() && !psnoise.dead()) {
    if (sample_interval_invalid(d, this->operator_dt)) {
      stringstream ss;
      ss << "CNRDeconEngine::process: datum sample interval=" << d.dt()
         << " does not match operator sample interval=" << this->operator_dt;
      throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
    }
    ValidateCNRNoiseSpectrum(psnoise, this->operator_dt,
                             "CNRDeconEngine::process");
    ValidateCNRDatum(d, FFTDeconOperator::nfft,
                     "CNRDeconEngine::process");
  }
  const ShapingWavelet previous_shapingwavelet(this->shapingwavelet);
  try {
    this->shapingwavelet = this->configured_shapingwavelet;
    return this->process_with_current_shaping(d, psnoise);
  } catch (...) {
    this->shapingwavelet = previous_shapingwavelet;
    throw;
  }
}

Seismogram CNRDeconEngine::process_with_current_shaping(
    const Seismogram &d, const PowerSpectrum &psnoise) {
  const string alg("CNRDeconEngine::process");
  if (d.dead() || psnoise.dead()) {
    Seismogram dout(d);
    dout.set_npts(0);
    if (d.dead()) {
      dout.elog.log_error(
          alg, "received Seismogram input segment marked dead - cannot process",
          ErrorSeverity::Invalid);
    }
    if (psnoise.dead()) {
      dout.elog.log_error(
          alg, "received PowerSpectrum object marked dead - cannot process",
          ErrorSeverity::Invalid);
    }
    dout.kill();
    return dout;
  }
  double candidate_peak_snr[3] = {0.0, 0.0, 0.0};
  double candidate_signal_bandwidth_fraction[3] = {0.0, 0.0, 0.0};
  try {
    string base_error("CNRDeconEngine::process:  ");
    Seismogram rfest(d);
    /* The inverse wavelet may have a different time origin from d.  Its
     * zero-time sample, recorded when winv was initialized, is the phase
     * reference for the inverse FFT output. */
    const int t0_shift = this->winv_t0_lag;
    vector<double> wvec;
    wvec.reserve(FFTDeconOperator::nfft);
    /* The set_npts method is assumed to not only set that attribute
     * but initialize the u matrix to a 3xnfft matrix.*/
    if (rfest.npts() != FFTDeconOperator::nfft)
      rfest.set_npts(FFTDeconOperator::nfft);
    int nhighsnr;
    double df;
    df = 1.0 / (operator_dt * static_cast<double>(FFTDeconOperator::nfft));
    for (int k = 0; k < 3; ++k) {
      TimeSeries work;
      work = TimeSeries(ExtractComponent(d, k), "Invalid");
      wvec.clear();
      int ntocopy = FFTDeconOperator::nfft;
      if (ntocopy > work.npts())
        ntocopy = work.npts();
      for (int j = 0; j < ntocopy; ++j)
        wvec.push_back(work.s[j]);
      for (int j = ntocopy; j < FFTDeconOperator::nfft; ++j)
        wvec.push_back(0.0);

      ComplexArray numerator(FFTDeconOperator::nfft, &(wvec[0]));
      gsl_fft_complex_forward(numerator.ptr(), 1, FFTDeconOperator::nfft,
                              wavetable, workspace);
      for (int j = 0; j < FFTDeconOperator::nfft; ++j) {
        const Complex64 z = numerator[j];
        if (!std::isfinite(z.real()) || !std::isfinite(z.imag()))
          throw MsPASSError(
              base_error + "datum FFT contains nonfinite values",
              ErrorSeverity::Invalid);
      }
      /* This loop computes QCMetrics of bandwidth fraction that
      is above a defined snr floor - not necessarily the same as the
      regularization floor used in computing the inverse */
      double snrmax;
      snrmax = 0.0;
      nhighsnr = 0;
      for (int j = 0; j < FFTDeconOperator::nfft / 2; ++j) {
        double f;
        f = df * static_cast<double>(j);
        Complex64 z = numerator[j];
        double sigamp = abs(z);
        double namp = sqrt(psnoise.power(f));
        const double snr = FiniteCNRAmplitudeSNR(sigamp, namp);

        if (snr > snrmax)
          snrmax = snr;
        if (snr > this->band_snr_floor)
          ++nhighsnr;
      }
      candidate_signal_bandwidth_fraction[k] =
          static_cast<double>(nhighsnr) /
          static_cast<double>(FFTDeconOperator::nfft / 2);
      candidate_peak_snr[k] = snrmax;
      ComplexArray rftmp = numerator * winv;
      rftmp = (*this->shapingwavelet.wavelet()) * rftmp;
      gsl_fft_complex_inverse(rftmp.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                              workspace);
      wvec.clear();
      for (int j = 0; j < FFTDeconOperator::nfft; ++j) {
        const Complex64 z = rftmp[j];
        if (!std::isfinite(z.real()) || !std::isfinite(z.imag()))
          throw MsPASSError(
              base_error + "inverse FFT output contains nonfinite values",
              ErrorSeverity::Invalid);
        wvec.push_back(z.real());
      }
      /* Note we used a time domain shift instead of using a linear phase
      shift in the frequency domain because the time domain operator has a lower
      operation count than the frequency domain algorithm and is thus more
      efficient.*/
      if (t0_shift != 0)
        wvec = circular_shift(wvec, -t0_shift);
      for (int j = 0; j < FFTDeconOperator::nfft; ++j)
        rfest.u(k, j) = wvec[j];
    }
    for (int k = 0; k < 3; ++k) {
      this->peak_snr[k] = candidate_peak_snr[k];
      this->signal_bandwidth_fraction[k] =
          candidate_signal_bandwidth_fraction[k];
    }
    return rfest;
  } catch (...) {
    throw;
  };
}

void CNRDeconEngine::update_shaping_wavelet(const double fl, const double fh) {
  string wtype;
  wtype = shapingwavelet.type();
  if (wtype == "butterworth") {
    /* shaping_wavelet_number_poles is a private attribute of the class*/
    shapingwavelet = ShapingWavelet(this->shaping_wavelet_number_poles, fl,
                                    this->shaping_wavelet_number_poles, fh,
                                    this->operator_dt, FFTDeconOperator::nfft);
  } else if (wtype == "ricker") {
    double favg = (fh - fl) / 2.0;
    shapingwavelet = ShapingWavelet(favg, operator_dt, FFTDeconOperator::nfft);
  } else {
    /* this really shouldn't happen but trap it anyway for completeness.
    Because it shouldn't happen we set the severity fatal*/
    throw MsPASSError(
        string("CNRDeconEngine::update_shaping_wavelet:  ") +
            "shaping wavelet has unsupported type defined=" + wtype,
        ErrorSeverity::Fatal);
  }
}
TimeSeries CNRDeconEngine::ideal_output() {
  try {
    CoreTimeSeries ideal_tmp = this->shapingwavelet.impulse_response();
    return TimeSeries(ideal_tmp, "Invalid");
  } catch (...) {
    throw;
  };
}
TimeSeries CNRDeconEngine::actual_output(const TimeSeries &wavelet) {
  if (wavelet.dead()) {
    return InvalidActualOutput(
        wavelet,
        "received wavelet data via arg0 marked dead - cannot proceed");
  }
  if (wavelet.npts() <= 0)
    return InvalidActualOutput(
        wavelet, "wavelet must contain at least one sample");
  if (sample_interval_invalid(wavelet, this->operator_dt)) {
    stringstream ss;
    ss << "wavelet sample interval=" << wavelet.dt()
       << " does not match operator sample interval=" << this->operator_dt;
    return InvalidActualOutput(wavelet, ss.str());
  }
  if (wavelet.npts() > FFTDeconOperator::nfft) {
    stringstream ss;
    ss << "wavelet length=" << wavelet.npts()
       << " exceeds the fixed FFT buffer length="
       << FFTDeconOperator::nfft;
    return InvalidActualOutput(wavelet, ss.str());
  }
  if (!std::isfinite(wavelet.t0()))
    return InvalidActualOutput(wavelet,
                               "wavelet start time must be finite");
  bool has_nonzero_sample(false);
  for (const double x : wavelet.s) {
    if (!std::isfinite(x))
      return InvalidActualOutput(wavelet,
                                 "wavelet contains nonfinite samples");
    if (x != 0.0)
      has_nonzero_sample = true;
  }
  if (!has_nonzero_sample)
    return InvalidActualOutput(
        wavelet, "wavelet must contain at least one nonzero sample");
  const int input_t0_lag = wavelet.sample_number(0.0);
  if (input_t0_lag < 0 || input_t0_lag >= FFTDeconOperator::nfft) {
    stringstream ss;
    ss << "wavelet zero-time sample lag=" << input_t0_lag
       << " is outside the FFT buffer range [0," << FFTDeconOperator::nfft
       << ")";
    return InvalidActualOutput(wavelet, ss.str());
  }

  TimeSeries result(
      wavelet); // Use this to clone metadata and elog from wavelet
  result.set_npts(FFTDeconOperator::nfft);
  /* Force these even though they are likely already defined as
  in the parent wavelet TimeSeries. */
  result.set_live();
  /* We always shift this wavelet to the center of the data vector.
  We handle the time through the CoreTimeSeries object. */
  int i0 = FFTDeconOperator::nfft / 2;
  result.set_t0(operator_dt * (-(double)i0));
  result.set_dt(this->operator_dt);
  result.set_tref(TimeReferenceType::Relative);
  /* We need to require that wavelet time range is consistent with
   * operator.   We assume relative time so we demand wavelet t0 be less
   * than or equal to nff2/2 to assure a wavelet signal is in the
   * the range -nfft/2 to nff2/2.  */
  int w_t0_lag(input_t0_lag);
  /* We correct the relative phase of the input wavelet to that
  saved when winv was created. */
  w_t0_lag -= this->winv_t0_lag;
  /* note we handle two extremes differently*/
  if (w_t0_lag >= FFTDeconOperator::nfft) {
    stringstream ss;
    ss << "actual_output method received wavelet with t0=" << wavelet.t0()
       << " that resolves to offset of " << w_t0_lag << " samples" << endl
       << "That exceeds buffer for frequency domain calculation of size="
       << FFTDeconOperator::nfft << endl
       << "Cannot compute actual_output because we assume signal is in range "
          "t>0"
       << endl;
    result.elog.log_error("CNRDeconEngine::actual_output", ss.str(),
                          ErrorSeverity::Invalid);
    result.kill();
    result.set_npts(0);
    return result;
  } else if (w_t0_lag > (FFTDeconOperator::nfft) / 2) {
    stringstream ss;
    ss << "Warning: actual output method received wavelet with t0="
       << wavelet.t0() << " that resolves to offset of " << w_t0_lag
       << " samples" << endl
       << "That exceeds the midpoint of the frequency domain buffer used by "
          "this method="
       << FFTDeconOperator::nfft / 2 << endl
       << "Result may be incorrect as the function assumes the signal is after "
          "time 0"
       << endl;
    result.elog.log_error("CNRDeconEngine::actual_output", ss.str(),
                          ErrorSeverity::Complaint);
  }

  try {
    std::vector<double> work;
    if (wavelet.npts() == FFTDeconOperator::nfft) {
      work = wavelet.s;
    } else {
      work.reserve(FFTDeconOperator::nfft);
      int i, nend;
      for (i = 0; i < FFTDeconOperator::nfft; ++i)
        work.push_back(0.0);
      if (wavelet.npts() > FFTDeconOperator::nfft)
        nend = FFTDeconOperator::nfft;
      else
        nend = wavelet.npts();
      for (i = 0; i < nend; ++i)
        work[i] = wavelet.s[i];
    }
    /* This converts wavelet to zero phase - needed to preserve timing.*/
    work = circular_shift(work, w_t0_lag);
    ComplexArray W(FFTDeconOperator::nfft, &(work[0]));
    gsl_fft_complex_forward(W.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                            workspace);
    ComplexArray ao_fft;
    ao_fft = this->winv * W;
    ComplexArray *stmp = this->shapingwavelet.wavelet();
    /* We always apply the shaping wavelet - this perhaps should be optional
    but probably better done with a none option for the shaping wavelet */
    ao_fft = (*stmp) * ao_fft;
    gsl_fft_complex_inverse(ao_fft.ptr(), 1, FFTDeconOperator::nfft, wavetable,
                            workspace);
    vector<double> ao;
    ao.reserve(FFTDeconOperator::nfft);
    for (int k = 0; k < ao_fft.size(); ++k)
      ao.push_back(ao_fft[k].real());
    ao = circular_shift(ao, i0);
    double ao_energy(0.0);
    for (const double x : ao) {
      if (!std::isfinite(x))
        return InvalidActualOutput(
            wavelet, "computed actual output contains nonfinite samples");
      ao_energy += x * x;
    }
    if (!std::isfinite(ao_energy) || ao_energy <= 0.0)
      return InvalidActualOutput(
          wavelet,
          "computed actual output has nonpositive or nonfinite energy");
    ao = normalize<double>(ao);
    /* set_npts always initializes the s buffer so it is more efficient to
    copy ao elements rather than what was here before:
      result.s=ao;
    */
    for (int k = 0; k < FFTDeconOperator::nfft; ++k)
      result.s[k] = ao[k];
    return result;
  } catch (...) {
    throw;
  };
}
TimeSeries CNRDeconEngine::inverse_wavelet(const TimeSeries &wavelet,
                                           const double tshift0) {
  if (wavelet.dead())
    return InvalidInverseWavelet(
        wavelet,
        "received wavelet data via arg0 marked dead - cannot proceed");
  if (wavelet.npts() <= 0)
    return InvalidInverseWavelet(
        wavelet, "wavelet must contain at least one sample");
  if (sample_interval_invalid(wavelet, this->operator_dt)) {
    stringstream ss;
    ss << "wavelet sample interval=" << wavelet.dt()
       << " does not match operator sample interval=" << this->operator_dt;
    return InvalidInverseWavelet(wavelet, ss.str());
  }
  if (!std::isfinite(wavelet.t0()))
    return InvalidInverseWavelet(wavelet,
                                 "wavelet start time must be finite");
  if (!std::isfinite(tshift0))
    return InvalidInverseWavelet(wavelet,
                                 "requested time shift must be finite");
  try {
    /* The FFT used to construct winv treats the installed source as a vector
     * whose physical zero occurs at winv_t0_lag.  Its inverse therefore has
     * that reference at the wrapped index (nfft-winv_t0_lag) mod nfft.
     * Move that reference to sample zero, then preserve FourierInverse's
     * public convention that tshift0 is the returned series start time.  Do
     * not add wavelet.t0(): the phase of winv already contains that offset. */
    const int inverse_reference_index =
        (FFTDeconOperator::nfft - this->winv_t0_lag) %
        FFTDeconOperator::nfft;
    CoreTimeSeries invcore(this->FFTDeconOperator::FourierInverse(
        this->winv, *this->shapingwavelet.wavelet(), operator_dt, tshift0));
    invcore.s = circular_shift(invcore.s, inverse_reference_index);
    TimeSeries result(invcore, "Invalid");
    /* Copy the error log from wavelet and post some information parameters
    to metadata */
    result.elog = wavelet.elog;
    result.put("waveform_type", "deconvolution_inverse_wavelet");
    result.put("decon_type", "CNRDeconEngine");
    return result;
  } catch (...) {
    throw;
  };
}

Metadata CNRDeconEngine::QCMetrics() {
  Metadata result;
  result.put("decon_operator", string("CNRDeconEngine"));
  result.put("decon_processed", this->regularization_bandwidth_fraction > 0.0);
  result.put("decon_operator_nfft", nfft);
  result.put("decon_operator_sample_shift", sample_shift);
  result.put("decon_sample_interval", operator_dt);
  result.put("cnr_regularization_bandwidth_fraction",
             this->regularization_bandwidth_fraction);
  result.put("waveletbf", this->regularization_bandwidth_fraction);
  result.put("maxsnr0", peak_snr[0]);
  result.put("maxsnr1", peak_snr[1]);
  result.put("maxsnr2", peak_snr[2]);
  result.put("signalbf0", signal_bandwidth_fraction[0]);
  result.put("signalbf1", signal_bandwidth_fraction[1]);
  result.put("signalbf2", signal_bandwidth_fraction[2]);
  return result;
}
} // namespace mspass::algorithms::deconvolution
