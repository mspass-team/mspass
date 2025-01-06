#ifndef __CNR_DECON_ENGINE_H__
#define __CNR_DECON_ENGINE_H__
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/shared_ptr.hpp>

#include "mspass/utility/AntelopePf.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/algorithms/Taper.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"

namespace mspass::algorithms::deconvolution{
/* This enum is file scope to intentionally exclude it from python wrappers.
It is used internally to define the algorithm the processor is to run.
I (glp) chose that approach over the inheritance approach used in the scalar
methods as an artistic choice.   It is a matter of opinion which approach
is better.  This makes one symbol do multiple things with changes done in
the parameter setup as opposed to having to select the right symbolic name
to construct.  Anyway, this enum defines algorithms that can be chosen for
processing.
*/
enum class CNR3C_algorithms{
    generalized_water_level,
    colored_noise_damping
};
class CNRDeconEngine : public FFTDeconOperator
{
public:
  CNRDeconEngine();
  /* design note - delete when finished.

     The constructor uses the pf to initialize operator properties.
     Important in that is the multitaper engine that has a significant
     initialization cost.   the initialize_inverse_operator is a messy
     way to define load data and then compute the inverse stored
     internally.   Doing it that way, however, allows the same code
     to be used for both single station and array decon.   In single
     station use every datum has to call initiallize_inverse_operator
     while with an array decon the operator is define once for an
     ensemble and applied to all members.   THE ASSUMPTION is all the
     grungy work to assure all that gets done correction is handled
     in python.
     */
   /*! Construct from AntelopePf container.

   This is currently the only valid constructor for this object.
   The operator requires a fair number of parameters for construction
   that include secondary groups of parameters in a hierarchy.
   An AntelopePf is one way to handle that with a human readable form.
   An alternative is yaml but that is not currently supported as
   an external format.  The AntelopePf object API is a generic
   way to handle parameter data hierarchies that is in MsPASS.
   Most user's will interact with the object only indirectly
   via python wrappers.   All a user needs to do is define all the
   required key-value pairs and get the obscure pf format correct.
   To use this constructor one passes an instance of the object
   of type AntelopePf that is normally constructed by reading a
   text file with the "pf" format or retrieved from another
   AntelopePf with the get_branch method.
   */
  CNRDeconEngine(const mspass::utility::AntelopePf& pf);
  CNRDeconEngine(const CNRDeconEngine& parent);
  void initialize_inverse_operator(const mspass::seismic::TimeSeries& wavelet,
          const mspass::seismic::TimeSeries& noise_data);
  void initialize_inverse_operator(const mspass::seismic::TimeSeries& wavelet,
          const mspass::seismic::PowerSpectrum& noise_spectrum);
  virtual ~CNRDeconEngine(){};
  mspass::seismic::Seismogram process(const mspass::seismic::Seismogram& d,
      const mspass::seismic::PowerSpectrum& psnoise,
        const double fl, const double fh);
  double get_operator_dt() const
  {
    return this->operator_dt;
  }
  /*! Compute noise spectrum using internal Multitaper operator.
  Normally used for noise spectrum but can be used for signal.
  Necessary to assure consistent scaling between signal and noise
  spectrum estimators.  */
  mspass::seismic::PowerSpectrum compute_noise_spectrum(
      const mspass::seismic::TimeSeries& d2use);
  /*! Compute noise spectrum from three component data.

  Overloaded version returns average power spectrum of all three components.  Measure
  of overall 3c power comparable to scalar signal power.   Could do
  sum of component spectra but that would cause a scaling problem for
  use in regularization. For most data this will tend to be dominated by
  the component with the highest noise level*/
  mspass::seismic::PowerSpectrum compute_noise_spectrum(
      const mspass::seismic::Seismogram& d2use);
  mspass::seismic::TimeSeries ideal_output();
  mspass::seismic::TimeSeries actual_output(
      const mspass::seismic::TimeSeries& wavelet);
  mspass::seismic::TimeSeries inverse_wavelet(
      const mspass::seismic::TimeSeries& wavelet, const double t0shift);
  mspass::utility::Metadata QCMetrics();
  CNRDeconEngine& operator=(const CNRDeconEngine& parent);

private:
    CNR3C_algorithms algorithm;
    /* For the colored noise damping algorithm the damper is frequency dependent.
       The same issue in water level that requires a floor on the water level
       applies to damping.   We use noise_floor to create a lower bound on
       damper values.   Note the damping constant at each frequency is
       damp*noise except where noise is below noise_floor defined relative to
       maximum noise value where it is set to n_peak*noise_floor*damp. */
    double damp;
    double noise_floor;
    /* SNR bandbwidth estimates count frequencies with snr above this value */
    double band_snr_floor;
    double operator_dt;   // Data must match this sample interval
    int shaping_wavelet_number_poles;
    mspass::algorithms::deconvolution::ShapingWavelet shapingwavelet;
    /* Expected time window size in samples.   When signal lengths
    match this value the slepian tapers are not recomputed.  When there
    is a mismatch it will change.  That means this can change dynamically
    when run on multiple data objects. */
    int winlength;
    mspass::algorithms::deconvolution::MTPowerSpectrumEngine signal_engine;
    mspass::algorithms::deconvolution::MTPowerSpectrumEngine noise_engine;

    /* This algorithm uses a mix of damping and water level.   Above this floor,
    which acts a bit like a water level, no regularization is done.  If
    snr is less than this value we regularize with damp*noise_amplitude.
    Note the noise_floor parameter puts a lower bound on the frequency dependent
    regularization.   If noise amplitude (not power) is less than noise_floor
    the floor is set like a water level as noise_max*noise_level.*/
    double snr_regularization_floor;
    /* These are QC metrics computed by process method.  Saved to allow them
    to be use in QCmetrics method. */
    double regularization_bandwidth_fraction;
    double peak_snr[3];
    double signal_bandwidth_fraction[3];
    mspass::algorithms::deconvolution::ComplexArray winv;
    /* This is the lag from sample 0 for the time defines as 0 for the
    wavelet used to compute the inverse.  It is needed to resolve time
    in the actual_output method.*/
    int winv_t0_lag;
    /*** Private methods *****/
    void update_shaping_wavelet(const double fl, const double fh);
    /* These are two algorithms for computing inverse operator in the frequency domain*/
    void compute_winv(const mspass::seismic::TimeSeries& wavelet,
        const mspass::seismic::PowerSpectrum& psnoise);
    void compute_gwl_inverse(const mspass::seismic::TimeSeries& wavelet,
        const mspass::seismic::PowerSpectrum& psnoise);
    void compute_gdamp_inverse(const mspass::seismic::TimeSeries& wavelet,
        const mspass::seismic::PowerSpectrum& psnoise);
  friend boost::serialization::access;
  template<class Archive>
  void serialize(Archive& ar, const unsigned int version)
  {
      std::cout <<"Entered serialize function"<<std::endl;
      ar & boost::serialization::base_object<FFTDeconOperator>(*this);
      ar & algorithm;
      ar & damp;
      ar & noise_floor;
      ar & band_snr_floor;
      ar & operator_dt;
      ar & shaping_wavelet_number_poles;
      ar & shapingwavelet;
      ar & signal_engine;
      ar & noise_engine;
      ar & snr_regularization_floor;
      ar & winv;
      ar & winv_t0_lag;
      ar & regularization_bandwidth_fraction;
      ar & peak_snr;
      ar & signal_bandwidth_fraction;
      std::cout << "Exiting serialize function"<<std::endl;
  }
};
}  // End namespace enscapsulation
#endif
