#ifndef __FREQUENCY_DOMAIN_GID_DECON__
#define __FREQUENCY_DOMAIN_GID_DECON__
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/algorithms/deconvolution/TimeDomainGIDDecon.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include <list>
#include <string>
#include <vector>

namespace mspass::algorithms::deconvolution {
class FrequencyDomainGIDDecon : public ScalarDecon {
public:
  FrequencyDomainGIDDecon(const mspass::utility::AntelopePf &md);
  FrequencyDomainGIDDecon(const FrequencyDomainGIDDecon &parent) = delete;
  FrequencyDomainGIDDecon &operator=(const FrequencyDomainGIDDecon &parent) =
      delete;
  ~FrequencyDomainGIDDecon();
  void changeparameter(const mspass::utility::Metadata &md);
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin);
  int loadnoise(const mspass::seismic::CoreSeismogram &d,
                mspass::algorithms::TimeWindow nwin);
  int loadwavelet(const mspass::seismic::TimeSeries &wavelet);
  int loadwavelet(const mspass::seismic::CoreTimeSeries &wavelet);
  int loadnoise(const mspass::seismic::TimeSeries &noise);
  int loadnoise(const mspass::seismic::CoreTimeSeries &noise);
  int loadnoise(const mspass::seismic::PowerSpectrum &noise_spectrum);
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin,
           mspass::algorithms::TimeWindow nwin);
  void process();
  mspass::seismic::CoreSeismogram getresult();
  mspass::seismic::CoreSeismogram sparse_output();
  /*! Legacy alias for output_shaping_wavelet inherited from ScalarDecon. */
  mspass::seismic::CoreTimeSeries ideal_output();
  mspass::seismic::CoreTimeSeries actual_output();
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  mspass::seismic::CoreTimeSeries inverse_wavelet(double t0parent);
  mspass::utility::Metadata QCMetrics();

private:
  mspass::seismic::CoreSeismogram d_all, d_decon, r, n;
  mspass::algorithms::TimeWindow dwin, nwin, fftwin;
  int ndwin, nnwin, noise_component;
  int actual_o_0, iter_count, iter_max;
  double residual_ratio_floor, residual_improvement_floor;
  double resid_l2_initial, resid_l2_prev, resid_l2_final;
  double resid_linf_initial, resid_linf_final;
  IterDeconType decon_type;
  ScalarDecon *preprocessor;
  CNRDeconEngine *cnrprocessor;
  mspass::seismic::TimeSeries current_wavelet;
  mspass::seismic::TimeSeries external_wavelet;
  mspass::seismic::TimeSeries external_noise;
  mspass::seismic::PowerSpectrum external_noise_spectrum;
  bool external_wavelet_loaded, external_noise_loaded,
      external_noise_spectrum_loaded, external_wavelet_allowed;
  std::vector<double> actual_o_fir;
  std::list<ThreeCSpike> spikes;
  double ns_peak_sigma_threshold, ns_peak_probability_threshold;
  double ns_residual_noise_ratio_floor, ns_peak_threshold;
  double ns_last_peak_significance, ns_noise_l2;
  double ns_fractional_improvement_final, ns_ridge_beta;
  int ns_max_spikes, ns_refit_interval;
  bool ns_use_empirical_noise_threshold, ns_converged;
  std::string ns_stop_reason;

  void initialize_inverse_operator();
  double compute_ns_peak_threshold();
  void rescale_spike(ThreeCSpike &spk);
  void update_residual_matrix(const ThreeCSpike &spk);
};
} // namespace mspass::algorithms::deconvolution
#endif
