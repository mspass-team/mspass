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
  /*! \brief Construct from metadata defining FFT and NS-GID parameters.

  \param md metadata containing FFT sizing, shaping-wavelet, and noise-stable
  inverse parameters.
  */
  NoiseStableDecon(const mspass::utility::Metadata &md);
  /*! \brief Construct and load wavelet and data vectors.

  \param md metadata containing FFT sizing, shaping-wavelet, and noise-stable
  inverse parameters.
  \param wavelet source wavelet samples.
  \param data data samples to deconvolve with the source wavelet.
  */
  NoiseStableDecon(const mspass::utility::Metadata &md,
                   const std::vector<double> &wavelet,
                   const std::vector<double> &data);
  void changeparameter(const mspass::utility::Metadata &md);
  /*! \brief Compute the noise-stable inverse filter and result.

  The method requires loaded wavelet and data vectors and rebuilds cached
  output, inverse-filter, and QC state.
  */
  void process();
  /*! \brief Load a noise waveform used to form frequency-dependent damping.

  \param noise scalar noise samples.
  */
  void loadnoise(const std::vector<double> &noise);
  /*! \brief Load a noise waveform from a core time series.

  \param noise scalar noise time series.
  */
  void loadnoise(const mspass::seismic::CoreTimeSeries &noise);
  /*! \brief Load a precomputed noise power spectrum.

  \param noise_spectrum spectrum that covers DC and describes noise power.
  */
  void loadnoise(const mspass::seismic::PowerSpectrum &noise_spectrum);
  mspass::seismic::CoreTimeSeries actual_output();
  /*! \brief Return the inverse filter impulse response.

  \param t0parent parent waveform start time used to set the output time
  standard.
  */
  mspass::seismic::CoreTimeSeries inverse_wavelet(const double t0parent);
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  mspass::utility::Metadata QCMetrics();
  /*! Return the largest gain reached by the processed inverse filter. */
  double max_gain() const { return max_gain_actual; }
  /*! Return the configured hard cap on inverse-filter gain. */
  double gain_cap() const { return gain_max; }

private:
  int read_metadata(const mspass::utility::Metadata &md);
  std::vector<double> noise_power_spectrum(const double dt);
  double reliability_taper(const double snr) const;
  double mu_min, alpha, noise_floor, gain_max;
  double snr_taper_low, snr_taper_high;
  bool use_reliability_taper;
  bool noise_vector_loaded, noise_spectrum_loaded;
  bool processed;
  std::vector<double> noise;
  mspass::seismic::PowerSpectrum noise_spectrum;
  double max_gain_actual, noise_amplification, effective_bandwidth_fraction;
};
} // namespace mspass::algorithms::deconvolution
#endif
