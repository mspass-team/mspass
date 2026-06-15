#ifndef __FREQUENCY_DOMAIN_GID_DECON__
#define __FREQUENCY_DOMAIN_GID_DECON__
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/algorithms/deconvolution/ThreeCSpike.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include <list>
#include <memory>
#include <string>
#include <vector>

namespace mspass::algorithms::deconvolution {
/*! \brief Frequency-domain generalized iterative deconvolution.

This engine shares the sparse GID iteration semantics of TimeDomainGIDDecon
but applies the candidate inverse operator in the frequency domain.  Processing
requires both a loaded signal window and residual-domain noise window.  External
PowerSpectrum noise is accepted by ns_gid and group_sparse because both use
NS-GID inverse-operator regularization; it does not replace load(d, dwin, nwin)
or loadnoise(d, nwin).
*/
class FrequencyDomainGIDDecon : public ScalarDecon {
public:
  /*! \brief Construct from the top-level deconvolution parameter file.

  The constructor reads the frequency_domain_gid_deconvolution branch, builds
  the configured leaf inverse operator, and initializes the signal, noise, and
  FFT processing windows.

  \param md top-level Antelope parameter file for the deconvolution operator.
  */
  FrequencyDomainGIDDecon(const mspass::utility::AntelopePf &md);
  FrequencyDomainGIDDecon(const FrequencyDomainGIDDecon &parent) = delete;
  FrequencyDomainGIDDecon &operator=(const FrequencyDomainGIDDecon &parent) =
      delete;
  ~FrequencyDomainGIDDecon();
  void changeparameter(const mspass::utility::Metadata &md);
  /*! \brief Load the three-component signal data used for deconvolution.

  The input seismogram is windowed to dwin.  The supplied window must contain
  the configured deconvolution FFT window.

  \param d input three-component seismogram.
  \param dwin full data window to retain around the deconvolution interval.
  \return 0 on success and 1 if the requested data window is unusable.
  */
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin);
  /*! \brief Load the residual-domain noise window from a seismogram.

  \param d input three-component seismogram containing the noise window.
  \param nwin time window used to estimate residual noise.
  \return 0 on success and 1 if the requested noise window is unusable.
  */
  int loadnoise(const mspass::seismic::CoreSeismogram &d,
                mspass::algorithms::TimeWindow nwin);
  /*! \brief Load an externally supplied source wavelet.

  External wavelets are accepted only when enabled by the configuration and
  must match the configured target sample interval.

  \param wavelet source wavelet to use instead of the internally estimated one.
  \return 0 on success.
  */
  int loadwavelet(const mspass::seismic::TimeSeries &wavelet);
  /*! \brief Load an externally supplied core source wavelet.

  \param wavelet core time series converted to a TimeSeries wavelet.
  \return 0 on success.
  */
  int loadwavelet(const mspass::seismic::CoreTimeSeries &wavelet);
  /*! \brief Load an externally supplied scalar noise estimate.

  The sample interval must match the configured target sample interval.  If no
  residual-domain noise seismogram is already loaded, this vector is also used
  to synthesize the residual noise estimate required by the sparse iteration.

  \param noise scalar noise waveform.
  \return 0 on success.
  */
  int loadnoise(const mspass::seismic::TimeSeries &noise);
  /*! \brief Load an externally supplied core scalar noise estimate.

  \param noise core time series converted to a TimeSeries noise waveform.
  \return 0 on success.
  */
  int loadnoise(const mspass::seismic::CoreTimeSeries &noise);
  /*! \brief Load an externally supplied noise power spectrum.

  This overload is supported for ns_gid and group_sparse modes where the leaf
  inverse operator uses the spectrum for noise-stable regularization.

  \param noise_spectrum spectrum that covers DC and describes noise power.
  \return 0 on success.
  */
  int loadnoise(const mspass::seismic::PowerSpectrum &noise_spectrum);
  /*! Clear any externally loaded wavelet and invalidate processed state. */
  void clear_external_wavelet();
  /*! Clear externally loaded noise or noise spectrum and invalidate state. */
  void clear_external_noise();
  /*! Return the configured deconvolution-window start time. */
  double deconvolution_window_start() const { return this->fftwin.start; };
  /*! Return the configured deconvolution-window end time. */
  double deconvolution_window_end() const { return this->fftwin.end; };
  /*! Return the current residual-noise window start time. */
  double noise_window_start() const { return this->nwin.start; };
  /*! Return the current residual-noise window end time. */
  double noise_window_end() const { return this->nwin.end; };
  /*! Return the serialized configuration parameter-file text. */
  std::string configuration_pf_text() const { return this->config_pf_text; };
  /*! Return true if changeparameter has updated the leaf inverse operator. */
  bool leaf_parameters_have_changed() const {
    return this->leaf_parameters_changed;
  };
  /*! Return the metadata most recently passed to changeparameter. */
  mspass::utility::Metadata changed_leaf_parameters() const {
    return this->changed_leaf_metadata;
  };
  /*! Return true if an external wavelet is currently loaded. */
  bool external_wavelet_is_loaded() const {
    return this->external_wavelet_loaded;
  };
  /*! Return true if an external scalar noise waveform is currently loaded. */
  bool external_noise_is_loaded() const { return this->external_noise_loaded; };
  /*! Return true if an external noise power spectrum is currently loaded. */
  bool external_noise_spectrum_is_loaded() const {
    return this->external_noise_spectrum_loaded;
  };
  /*! Return the loaded external wavelet, or an empty TimeSeries if absent. */
  mspass::seismic::TimeSeries loaded_external_wavelet() const {
    if (!this->external_wavelet_loaded)
      return mspass::seismic::TimeSeries();
    return this->external_wavelet;
  };
  /*! Return the loaded external noise, or an empty TimeSeries if absent. */
  mspass::seismic::TimeSeries loaded_external_noise() const {
    if (!this->external_noise_loaded)
      return mspass::seismic::TimeSeries();
    return this->external_noise;
  };
  /*! Return the loaded external noise spectrum, or an empty spectrum. */
  mspass::seismic::PowerSpectrum loaded_external_noise_spectrum() const {
    if (!this->external_noise_spectrum_loaded)
      return mspass::seismic::PowerSpectrum();
    return this->external_noise_spectrum;
  };
  /*! \brief Load signal and residual-noise windows from one seismogram.

  This convenience overload calls loadnoise(d, nwin) and load(d, dwin).

  \param d input three-component seismogram.
  \param dwin full data window to retain around the deconvolution interval.
  \param nwin time window used to estimate residual noise.
  \return sum of the signal and noise load return codes.
  */
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin,
           mspass::algorithms::TimeWindow nwin);
  /*! \brief Run the configured sparse GID iteration.

  Processing requires both a loaded data window and a loaded residual-noise
  estimate.  The method resets prior processed products before computing a new
  inverse operator, sparse spike train, and QC state.
  */
  void process();
  /*! \brief Return the shaped deconvolution result.

  The sparse output is convolved with the configured shaping wavelet and
  returned in the loaded data window.
  */
  mspass::seismic::CoreSeismogram getresult();
  /*! \brief Return the unshaped sparse spike train.

  Values are inserted at the selected sparse lags in a seismogram aligned with
  the loaded data window.
  */
  mspass::seismic::CoreSeismogram sparse_output();
  /*! Return the final lag weights used by the sparse picker. */
  std::vector<double> lag_weight_vector() const;
  /*! Legacy alias for output_shaping_wavelet inherited from ScalarDecon. */
  mspass::seismic::CoreTimeSeries ideal_output();
  /*! Return the resolution kernel of the active leaf inverse operator. */
  mspass::seismic::CoreTimeSeries actual_output();
  /*! Return the inverse wavelet using a zero parent start time. */
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  /*! \brief Return the inverse wavelet with the parent time correction.

  \param t0parent start time of the parent waveform used to set output time.
  */
  mspass::seismic::CoreTimeSeries inverse_wavelet(double t0parent);
  mspass::utility::Metadata QCMetrics();

private:
  mspass::seismic::CoreSeismogram d_all, d_decon, r, n;
  mspass::algorithms::TimeWindow dwin, nwin, fftwin;
  std::string config_pf_text;
  double target_dt;
  int ndwin, nnwin, noise_component;
  int actual_o_0, iter_count, iter_max;
  double residual_ratio_floor, residual_improvement_floor;
  double resid_l2_initial, resid_l2_prev, resid_l2_final;
  double resid_linf_initial, resid_linf_final;
  double lag_weight_linf_final, lag_weight_l2_final;
  IterDeconType decon_type;
  std::unique_ptr<ScalarDecon> preprocessor;
  std::unique_ptr<CNRDeconEngine> cnrprocessor;
  mspass::seismic::TimeSeries current_wavelet;
  mspass::seismic::TimeSeries external_wavelet;
  mspass::seismic::TimeSeries external_noise;
  mspass::seismic::PowerSpectrum external_noise_spectrum;
  bool external_wavelet_loaded, external_noise_loaded,
      external_noise_spectrum_loaded, external_wavelet_allowed;
  bool processed;
  bool residual_noise_from_external;
  bool leaf_parameters_changed;
  mspass::utility::Metadata changed_leaf_metadata;
  std::vector<double> actual_o_fir;
  std::vector<double> lag_weights, lag_weight_penalty;
  std::vector<double> adaptive_penalty_memory;
  std::vector<double> adaptive_penalty_retention;
  std::list<ThreeCSpike> spikes;
  double ns_peak_sigma_threshold, ns_peak_probability_threshold;
  double ns_residual_noise_ratio_floor, ns_peak_threshold;
  double ns_last_peak_significance, ns_noise_l2, ns_noise_amplitude_rms;
  double ns_fractional_improvement_final, ns_ridge_beta;
  int ns_max_spikes, ns_refit_interval;
  bool ns_use_empirical_noise_threshold, ns_converged;
  std::string ns_stop_reason;
  bool gid_converged;
  std::string gid_stop_reason;
  std::string lag_weight_penalty_function;
  double lag_weight_penalty_scale_factor;
  int lag_weight_function_width;
  double adaptive_penalty_last_confidence;
  double adaptive_penalty_last_immediate_strength;
  double adaptive_penalty_last_specificity;
  double adaptive_penalty_last_decay_factor;
  double adaptive_penalty_noise_amplitude;
  double adaptive_penalty_memory_linf;
  double adaptive_penalty_memory_l2;
  double group_sparse_lambda, group_sparse_lambda_scale;
  double group_sparse_lambda_used, group_sparse_tolerance;
  double group_sparse_active_threshold, group_sparse_active_threshold_scale;
  double group_sparse_active_threshold_quantile;
  double group_sparse_active_threshold_quantile_value;
  double group_sparse_active_threshold_used;
  double group_sparse_objective_initial, group_sparse_objective_final;
  double group_sparse_fractional_improvement_final;
  double group_sparse_debiased_objective_final;
  double group_sparse_debiased_fractional_improvement_final;
  int group_sparse_max_iterations, group_sparse_iterations;
  int group_sparse_active_groups;
  bool group_sparse_converged;

  void initialize_inverse_operator();
  void invalidate_processing_state();
  double compute_ns_peak_threshold();
  void rescale_spike(ThreeCSpike &spk);
  void update_residual_matrix(const ThreeCSpike &spk);
  void update_lag_weights(const int col, const double candidate_amplitude);
};
} // namespace mspass::algorithms::deconvolution
#endif
