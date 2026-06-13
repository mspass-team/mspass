#ifndef __TIME_DOMAIN_GID_DECON__
#define __TIME_DOMAIN_GID_DECON__
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/ThreeCSpike.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include <list>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
namespace mspass::algorithms::deconvolution {
/*! \brief Three-component generalized iterative deconvolution in time.

This class implements the Wang and Pavlis generalized iterative deconvolution
idea for a three-component seismogram.  Each iteration selects a vector spike
from an inverse-filtered residual, subtracts the inverse operator's
actual-output/resolution kernel in the original data domain, and stores the
accepted sparse spike train internally.  The public receiver-function output is
the sparse train convolved with the configured output shaping wavelet.

The constructor reads parameters and allocates reusable operators.  A datum is
processed only after loading signal and noise windows, either with
load(d, dwin, nwin) or with loadnoise followed by load.  External prepared
wavelets and noise estimates can be supplied explicitly; otherwise the engine
uses the configured receiver-function compatibility behavior.

External TimeSeries noise can supply both the inverse-operator noise estimate
and, when no windowed residual noise has been loaded, the residual-domain
stopping threshold.  External PowerSpectrum noise is only an inverse-operator
regularization estimate for ns_gid; callers must still load residual-domain
noise with load(d, dwin, nwin) or loadnoise(d, nwin) before process().
*/

class TimeDomainGIDDecon : public ScalarDecon {
public:
  /*! \brief Create and initialize an operator for repeated processing.

  The parameter file defines the inverse-operator mode, iteration controls,
  output shaping wavelet, signal/noise windows, and optional NS-GID stability
  parameters. */
  TimeDomainGIDDecon(const mspass::utility::AntelopePf &md);
  TimeDomainGIDDecon(const TimeDomainGIDDecon &parent) = delete;
  TimeDomainGIDDecon &operator=(const TimeDomainGIDDecon &parent) = delete;
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
  void clear_external_wavelet();
  void clear_external_noise();
  double deconvolution_window_start() const { return this->fftwin.start; };
  double deconvolution_window_end() const { return this->fftwin.end; };
  double noise_window_start() const { return this->nwin.start; };
  double noise_window_end() const { return this->nwin.end; };
  std::string configuration_pf_text() const { return this->config_pf_text; };
  bool leaf_parameters_have_changed() const {
    return this->leaf_parameters_changed;
  };
  mspass::utility::Metadata changed_leaf_parameters() const {
    return this->changed_leaf_metadata;
  };
  bool external_wavelet_is_loaded() const {
    return this->external_wavelet_loaded;
  };
  bool external_noise_is_loaded() const { return this->external_noise_loaded; };
  bool external_noise_spectrum_is_loaded() const {
    return this->external_noise_spectrum_loaded;
  };
  mspass::seismic::TimeSeries loaded_external_wavelet() const {
    if (!this->external_wavelet_loaded)
      return mspass::seismic::TimeSeries();
    return this->external_wavelet;
  };
  mspass::seismic::TimeSeries loaded_external_noise() const {
    if (!this->external_noise_loaded)
      return mspass::seismic::TimeSeries();
    return this->external_noise;
  };
  mspass::seismic::PowerSpectrum loaded_external_noise_spectrum() const {
    if (!this->external_noise_spectrum_loaded)
      return mspass::seismic::PowerSpectrum();
    return this->external_noise_spectrum;
  };
  /*! \brief Load signal and residual-noise windows.

  This method is little more than a call to loadnoise followed
  immediately by a call to load.  Call process after this method returns
  zero. */
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin,
           mspass::algorithms::TimeWindow nwin);
  void process();
  ~TimeDomainGIDDecon();
  mspass::seismic::CoreSeismogram getresult();
  mspass::seismic::CoreSeismogram sparse_output();
  std::vector<double> lag_weight_vector() const;
  /*! Legacy alias for output_shaping_wavelet inherited from ScalarDecon. */
  mspass::seismic::CoreTimeSeries ideal_output();
  mspass::seismic::CoreTimeSeries actual_output();
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  mspass::seismic::CoreTimeSeries inverse_wavelet(double t0parent);
  mspass::utility::Metadata QCMetrics();

private:
  /* These are data at different stages of process.  d_all is the
  largest signal window that is assumed to have been initialized by the
  load method for this object.  d_decon is the
  result of applying the preprocessor (signal processing) deconvolution to
  all three components. d_decon is computed from d, which is a windowed
  version of the input data received by the load method.  It has a
  time duration less than or at least equal to that of d_all.
  r is the residual, which is accumulated during the iterative method.
  The time duration of r is the same as d.  It is initialized by
  convolving the inverse filter with d_all.
  n is the noise data.  It should normally be at least as long a d_all*/
  mspass::seismic::CoreSeismogram d_all, d_decon, r, n;
  /* We save the set of data lengths for clarity and a minor bit efficiency.
  ndwin is the number of samples in d_all and r.
  nnwin is the number of samples in n. */
  int ndwin, nnwin;
  /* Save the TimeWindow objects that define the extent of d_all, d_decon,
  and n.   Some things need at least some of these downstream */
  mspass::algorithms::TimeWindow dwin, nwin, fftwin;
  std::string config_pf_text;
  double target_dt;
  /*! For preprocessor algorithms that are scalar we specify which channel
  is used to define the noise for regularization */
  int noise_component;
  /*! The algorithm accumulates spikes in this linked list.   To create
  the result as a normal time series this is converted to a conventional
  form in the getresult method */
  std::list<ThreeCSpike> spikes;
  /*! Penalty function weights.

  This algorithm uses a penalty function that downweights a range of lags
  around the time of each spike subtracted in the iteration.   This stl
  vector contains the accumulated weighting function at the end of the
  iteration.  It is used for QC */
  std::vector<double> lag_weights;
  /* This vector contains the function time shifted and added to lag_weights
  vector after each iteration.   */
  std::vector<double> wtf;
  int nwtf; // size of wtf - cached because wtf is inside the deepest loop

  /* This is a pointer to the BasicDeconOperator class used for preprocessing
  Classic use of inheritance to simplify the api. */
  std::unique_ptr<ScalarDecon> preprocessor;
  std::unique_ptr<CNRDeconEngine> cnrprocessor;
  mspass::seismic::TimeSeries current_wavelet;
  mspass::seismic::TimeSeries external_wavelet;
  mspass::seismic::TimeSeries external_noise;
  mspass::seismic::PowerSpectrum external_noise_spectrum;
  std::vector<std::vector<double>> ns_noise_components;
  bool external_wavelet_loaded, external_noise_loaded,
      external_noise_spectrum_loaded, external_wavelet_allowed;
  bool processed;
  bool residual_noise_from_external;
  bool leaf_parameters_changed;
  mspass::utility::Metadata changed_leaf_metadata;

  /* This parameter is set in the constructor.  It would normally be half the
  length of the fir representation of the inverse wavelet.*/
  int wavelet_pad;
  /*! \brief Shortened inverse wavelet used for iteration.

  The iteration is done here in the time domain.  The wavelet returned by
  the preprocessor algorithm will commonly have lots of zeros or small numbers
  outside a central area.  The constructor defines how that long wavelet is
  shortened to build this one. actual_o_fir is the resolution kernel vector.
  */
  std::vector<double> actual_o_fir;
  int actual_o_0; // offset from sample zero for zero lag position
  IterDeconType decon_type;
  /* This is called by the constructor to create the wtf penalty function */
  void construct_weight_penalty_function(const mspass::utility::Metadata &md);
  void invalidate_processing_state();
  /*! Subtract current spike signal from data.

  \param spk - vector amplitude and lag of spike - subtract ideal
  output at this lag.
  */
  void update_residual_matrix(ThreeCSpike spk);
  /*! Updates the lag weight vector.

  This implementation has an enhanced time weighting function
  beyond that given in Wang and Pavlis's paper.   This private method
  updates the weight vector using the lag position (in samples) of the
  current spike. */
  void update_lag_weights(int col, const double candidate_amplitude);
  /*! This private method is called after loading noise to set the quantity
  resid_linf_floor = convergence criteria on amplitude.  That parameter is
  computed from sorting the filtered, preevent noise and setting the
  threshold from a probability level.  Returns the computed floor but
  also sets resid_linf_floor in this base object */
  double compute_resid_linf_floor(
      const mspass::seismic::CoreSeismogram &noise);
  /*! Apply all convergence tests. Returns false to terminate the iteration
  loop.  This puts all such calculations in one place. */
  bool has_not_converged();
  /* These are convergence attributes.   lw_inf indicates Linf norm of
  lag_weight array, lw_l2 is L2 metric of lag_weight, resid_inf is Linf
  norm of residual vector, and resid_l2 is L2 of resid matrix.   prev
  modifier means the size of that quantity in the previous iteration.  initial
  means initial value at the top of the loop.*/
  double lw_linf_initial, lw_linf_prev;
  double lw_l2_initial, lw_l2_prev;
  double resid_linf_initial, resid_linf_prev;
  double resid_l2_initial, resid_l2_prev;
  /* These are convergence parameters for the different tests */
  int iter_count, iter_max; // actual iteration count and ceiling to break loop
  /*lw metrics are scaled with range of 0 to 1.  l2 gets scaled by number of
  points and so can use a similar absolute scale. */
  double lw_linf_floor, lw_l2_floor;
  /* We use a probability level to define the floor Linf of the residual
  matrix.   For L2 we use the conventional fractional improvement metric. */
  double resid_linf_prob, resid_linf_floor;
  double resid_l2_tol;
  double ns_peak_sigma_threshold, ns_peak_probability_threshold;
  double ns_residual_noise_ratio_floor, ns_peak_threshold;
  double ns_last_peak_significance, ns_noise_l2, ns_noise_amplitude_rms;
  int ns_max_spikes, ns_refit_interval;
  double ns_ridge_beta, ns_fractional_improvement_final;
  bool ns_use_empirical_noise_threshold, ns_converged;
  std::string ns_stop_reason;
  bool gid_converged;
  std::string gid_stop_reason;
  std::string lag_weight_penalty_function;
  double lag_weight_penalty_scale_factor;
  int lag_weight_function_width;
  std::vector<double> adaptive_penalty_memory;
  std::vector<double> adaptive_penalty_retention;
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
  int group_sparse_max_iterations, group_sparse_iterations;
  int group_sparse_active_groups;
  bool group_sparse_converged;

};
} // namespace mspass::algorithms::deconvolution
#endif
