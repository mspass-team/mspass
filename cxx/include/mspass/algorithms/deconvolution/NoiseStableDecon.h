#ifndef __NOISE_STABLE_DECON_H__
#define __NOISE_STABLE_DECON_H__
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/utility/Metadata.h"
#include <vector>

namespace mspass::algorithms::deconvolution {
/*! \brief Noise-aware stable FFT inverse used by NS-GID.
 *
 * This scalar operator computes
 *
 *   G(f)=B(f) conj(S(f))/(|S(f)|^2 + mu(f))
 *
 * with frequency-dependent damping from an optional noise spectrum and an
 * explicit gain cap.  The minimum damping parameter is scaled by peak wavelet
 * spectral power.  It is intended as the inverse operator used to form the GID
 * peak-picking function.  As a standalone scalar deconvolution operator it
 * applies one stable inverse filter and returns the finite-bandwidth result;
 * it does not run sparse iteration and does not apply the GID output shaping
 * wavelet.
 */
class NoiseStableDecon : public FFTDeconOperator, public ScalarDecon {
public:
  NoiseStableDecon();
  NoiseStableDecon(const mspass::utility::Metadata &md);
  NoiseStableDecon(const mspass::utility::Metadata &md,
                   const std::vector<double> &wavelet,
                   const std::vector<double> &data);
  void changeparameter(const mspass::utility::Metadata &md);
  void process();
  void loadnoise(const std::vector<double> &noise);
  void loadnoise(const mspass::seismic::CoreTimeSeries &noise);
  void loadnoise(const mspass::seismic::PowerSpectrum &noise_spectrum);
  mspass::seismic::CoreTimeSeries actual_output();
  mspass::seismic::CoreTimeSeries inverse_wavelet(const double t0parent);
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  mspass::utility::Metadata QCMetrics();
  double max_gain() const { return max_gain_actual; }
  double gain_cap() const { return gain_max; }

private:
  int read_metadata(const mspass::utility::Metadata &md);
  std::vector<double> noise_power_spectrum(const double dt);
  double reliability_taper(const double snr) const;
  double mu_min, alpha, noise_floor, gain_max;
  double snr_taper_low, snr_taper_high;
  bool use_reliability_taper;
  bool noise_vector_loaded, noise_spectrum_loaded;
  std::vector<double> noise;
  mspass::seismic::PowerSpectrum noise_spectrum;
  double max_gain_actual, noise_amplification, effective_bandwidth_fraction;
};
} // namespace mspass::algorithms::deconvolution
#endif
