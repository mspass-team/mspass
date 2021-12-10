#ifndef __CNR3C_DECON_H__
#define __CNR3C_DECON_H__
#include <vector>
#include <math.h>
#include "mspass/utility/AntelopePf.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/algorithms/Taper.h"
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass::algorithms::deconvolution{

/*! \brief Absract base class for algorithms handling full 3C data.
*/
class Base3CDecon
{
public:
    virtual ~Base3CDecon() {};
    /*
    virtual void change_parameters(const mspass::BasicMetadata &md)=0;
    virtual void loaddata(mspass::Seismogram& d,const int comp)=0;
    virtual void loadwavelet(const mspass::TimeSeries& w)=0;
    */
    /* \brief Return the ideal output of the deconvolution operator.

    All deconvolution operators have a implicit or explicit ideal output
    signal. e.g. for a spiking Wiener filter it is a delta function with or
    without a lag.  For a shaping wavelt it is the time domain version of the
    wavelet. */
    virtual mspass::seismic::Seismogram process()=0;
    /*! \brif Return the actual output of the deconvolution operator.

    The actual output is defined as w^-1*w and is compable to resolution
    kernels in linear inverse theory.   Although not required we would
    normally expect this function to be peaked at 0.   Offsets from 0
    would imply a bias. */
    virtual mspass::seismic::TimeSeries actual_output()=0;

    /*! \brief Return a FIR represention of the inverse filter.

    After any deconvolution is computed one can sometimes produce a finite
    impulse response (FIR) respresentation of the inverse filter.  */
    //virtual mspass::TimeSeries inverse_wavelet() = 0;
    virtual mspass::seismic::TimeSeries inverse_wavelet(double) = 0;
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    virtual mspass::utility::Metadata QCMetrics()=0;
};
/* This enum is used internally to define the algorithm the processor is to run.
I (glp) chose that approach over the inheritance approach used in the scalar
methods as an artistic choice.   It is a matter of opinion which approach
is better.  This makes one symbol do multiple things with changes done in
the parameter setup as opposed to having to select the right symbolic name
to construct.  The main difference is it avoids the messy trampoline class
needed with pybind11 to create wrappers for the python bindings.
This enum will not be exposed to python as it is totally internal to the
CNR3CDecon class.
*/
enum class CNR3C_algorithms{
    generalized_water_level,
    colored_noise_damping,
    undefined
};
/*! \brief Colored Noise Regularized 3C Deconvolution opertor.

This algorithm is somewhat of a synthesis of the best features from the
various frequency comain methods currently in use for receiver function
estimation.  Key features are:
1. Data are tapered by a generic function with a range of options.
2.  The output is always a Seismogram, which in MsPASS means 3C data
3.  The inverse is regularized by a scaled 3C noise estimate.
4.  Perhaps most importantly the shaping wavelet is computed dynamically
based on bandwidth estimated from the combination power spectrum estimates
of the signal and noise for both the data being deconvolved and the
wavelet used to compute the inverse.  For array methods the two noise
estimates can be drastically different.

This object is what might be called a processing object.  That is, it
does not use the model of construction creating the thing it defines
(Seismogram, for example, uses that paradigm).  Instead the constructor
defines the operator.  Data are processed by first calling one of the
loaddata and loadnoise methods.  Once valid data is loaded call the process
method to deconvolve the loaded data. Note this approach assumes the
data, wavelet, and noise used for the processing are internally consistent.
The user is warned there a limited safeties to validate any consistency because
it would be hard to do so without causing more problems that it would
solve.  In MsPASS we expect to hide this a bit behind some python wrappers
to create more safety mechanisms.
*/
//class CNR3CDecon : public mspass::FFTDeconOperator
class CNR3CDecon : public Base3CDecon, public FFTDeconOperator
{
public:
  /*! Default constructor.  Puts operator in an invalid state.*/
  CNR3CDecon();
  /*! \brief Construct from a tree of parameters loaded into an AntelopePf object.

  There number of inputs to construct this  operator are quite large.  An
  AntelelopePf is one way to store this information and the format assumed by
  this constructor.  Note this object could be constructed from a yaml or
  xml file, in principle, but for the present we use Antelope's pf file \
  format.  i.e. a typical program would read a pf file and to construct the
  object passed as pf. */
  CNR3CDecon(const mspass::utility::AntelopePf& pf);
  /*! Standard copy constructor. */
  CNR3CDecon(const CNR3CDecon& parent);
  /*! Standard destructor. */
  ~CNR3CDecon();
  CNR3CDecon& operator=(const CNR3CDecon& parent);
  /*! \brief Change the setup of the operator on the fly.

  Sometimes an operator needs to have it's properties adjusted on the fly.
  this method can be used to redefine its operational properties instead of
  creating a new instance with the constructor.  Use this method with care
  as it requires the same set of parameters as the AntelopePf constructor.
  The data are actually passed through the base class for an AntelopePF but
  currently it is implicitly expected to be data compatible with the pf
  constructor.
  */
  void change_parameters(const mspass::utility::BasicMetadata& md);
  /*! \brief Load data with one component used as wavelet estimate.

  Use this method for conventional receiver function data if one of
  components is to be used as the wavelet estimate (conventionally Z or L).
  Optionally they can also load preevent noise defined by the noise
  window parameters defined by Pf constructor.

  \param d is the data to be loaded.  If loadnoise is true it is assumed to
    be long enough to span the range of both the signal and noise windows
    defined in constuction.
  \param wcomp is the component that is used as an estimate of the
    wavelet.   In standard P receiver functions this would be the Z
    component but for S it would be the radial component
  \param loadnoise when true method also loads noise data from predefined
    window.

  \exception Will throw an MsPASSError for a number of potential problems
    that can arise. All such errors will have an invalid condition set.

  */
  void loaddata(mspass::seismic::Seismogram& d, const int wcomp,const bool loadnoise=false);
  /*! \brief Load data and optionally load noise.
   *
   This method must be called before running process to get a unique result.
   Input data are windowed by the processing window parameters and inserted into
   an internal buffer with sufficient padding to avoid fft circular convolution
   artifacts.   Noise can optionally be loaded but use that approach ONLY
   if the time span of d contains both the processing and noise time windows.
   Use loadnoise if the input data do not match that model.

   \param d is the input data (see note about time span above)
   \param loadnoise when true the function will attempt to load data for
     the noise based regularization from the noise window defined for the operator.
   \exception MsPASSError may be thrown for a number of potential error conditions.
   */
  void loaddata(mspass::seismic::Seismogram& d, const bool loadnoise=false);
  /*! \brief Load noise data directly.

   This method can be used to load noise to be used to compute signal to noise
   related metrics.   The spectrum of the noise is computed from a
   mutlitaper spectral estimator for the data passed as n.  Best results will be
   obtain if the length of n is larger than the operator size defined by it's
   internal noise window (defined in constructor by noise_window_start and
   and parameters).   Note for this constructor the actual time of the noise
   window passed is ignored, but the length it defines is used to define the
   length of the computed spectrum.  It is better to have the input noise
   slightly larger than the operator length to be consistent with the expectations
   of the multitaper method.   If the noise window is short the spectrum is
   computed but will be biased to lower amplitudes because of zero padding.
   That happens because the operator will not recompute Slepian tapers if the
   data are short.
   */
  void loadnoise_data(const mspass::seismic::Seismogram& n);
  /*! \brief Load noise estimate directly as a PowerSpectrum object.
   *
   The actual noise regularization is computed by this algorithm from an internally
   stored PowerSpectrum object.   This method allows the power spectrum to be computed
   by some other method or using previously computed estimates rather than computing
   it through loadnoise.  For instance, one might want to use a robust
   form of Welch's method to estimate preevent noise that would be immune to
   spikes and sporatic noise bursts.

   */
  void loadnoise_data(const mspass::seismic::PowerSpectrum& n);
  /*! \brief Load data defining the wavelet to use for deconvolution.

    This algorithm assumes a deterministic model for deconvolution.  That is, we have
    an estimate of the source wavelet.   In conventional receiver functions this is
    the vertical or longitudinal component.   In current array methods it is always some
    stack (not necessarily a simple average) of vertical or longitudinal data from an ensemble.

    It is VERY IMPORTANT to realize that loadwavelet initiates the calculation of the inverse
    for the deconvolution.   That allows this same processing object to be efficiently used in
    array deconvolution and single station deconvolution.   For single station estimates
    loadwavelet should be called on each seismogram.  For array methods loadwavelet should
    be called once for the ensemble (common source gather) to which a wavelet is linked.
    The inverse is then applied to very signal in the ensemble with process.

    \param w is the wavelet.  Must be in relative time with 0 set to the estimated first break time.
    */
  void loadwavelet(const mspass::seismic::TimeSeries& w);
  /*! \brief Load noise data for wavelet directly.

   This method can be used to load noise to be used for regularization from an
   arbitrary time window.   The spectrum of the noise is computed from a
   mutlitaper spectral estimator for the data passed as n.  Best results will be
   obtain if the length of n is larger than the operator size defined by it's
   internal noise window (defined in constructor by noise_window_start and
   and parameters).   Note for this constructor the actual time of the noise
   window passed is ignored, but the length it defines is used to define the
   length of the computed spectrum.  It is better to have the input noise
   slightly larger than the operator length to be consistent with the expectations
   of the multitaper method.   If the noise window is short the spectrum is
   computed but will be biased to lower amplitudes because of zero padding.
   That happens because the operator will not recompute Slepian tapers if the
   data are short.
   */
  void loadnoise_wavelet(const mspass::seismic::TimeSeries& n);
  /*! \brief Load noise estimate for wavelet signal directly as a PowerSpectrum object.
   *
   The actual noise regularization is computed by this algorithm from an internally
   stored PowerSpectrum object.   This method allows the power spectrum to be computed
   by some other method or using previously computed estimates rather than computing
   it through loadnoise.

   */
  void loadnoise_wavelet(const mspass::seismic::PowerSpectrum& n);
  /* These same names are used in ScalarDecon but we don't inherit them
  here because this algorithm is 3C data centric there is a collision
  with the ScalarDecon api because of it.  */
  mspass::seismic::Seismogram process();

  /* \brief Return the ideal output of the deconvolution operator.

  All deconvolution operators have a implicit or explicit ideal output
  signal. e.g. for a spiking Wiener filter it is a delta function with or
  without a lag.  For a shaping wavelt it is the time domain version of the
  wavelet. */
  mspass::seismic::TimeSeries ideal_output();
  /*! \brif Return the actual output of the deconvolution operator.

  The actual output is defined as w^-1*w and is compable to resolution
  kernels in linear inverse theory.   Although not required we would
  normally expect this function to be peaked at 0.   Offsets from 0
  would imply a bias. */
  mspass::seismic::TimeSeries actual_output();

  /*! \brief Return a FIR represention of the inverse filter.

  After any deconvolution is computed one can sometimes produce a finite
  impulse response (FIR) respresentation of the inverse filter.  */
  mspass::seismic::TimeSeries inverse_wavelet(double tshift);
  /*! \brief Return appropriate quality measures.

  The QC values return use two concepts:  maximum snr is the largest signal
  to noise ratio estimated across the band exceeded a specified snr floor
  for the operator.   "bandwidth fraction" is the fraction of the total
  frequency band (ie. 0 to Nyquist) exceeding the operator snr floor.

  This function returns the following key-value pairs:
  waveletbf - bandwidth fraction for wavelet.
  maxsnr0,maxsnr1,maxsnr2 - maximum signal to noise ratio on each of the
    three components of the data being deconvolved.
  signalbf0,signalbf1,signalbf2 - bandwidth fraction for each of the three
    components of data deconvolved to produce the latest output.
   */
  mspass::utility::Metadata QCMetrics();
  /*! Return the power spectrum estimate linked to the wavelet signal. */
  mspass::seismic::PowerSpectrum wavelet_noise_spectrum()
  {
    return psnoise;
  };
  /*! Return the average 3C power spectrum for the latest loaded noise data. */
  mspass::seismic::PowerSpectrum data_noise_spectrum()
  {
    return psnoise_data;
  };
  /*! Return the power spectrum of the wavelet signal used for deconvolution. */
  mspass::seismic::PowerSpectrum wavelet_spectrum()
  {
    return pswavelet;
  };
  /*! Return the power spectrum of the signal loaded for deconvolution. */
  mspass::seismic::PowerSpectrum data_spectrum()
  {
    return pssignal;
  };
private:
  CNR3C_algorithms algorithm;
  bool taper_data;  //Set false only if none specified
  double operator_dt;   // Data must match this sample interval
  /* Expected time window size in samples
  (computed from processing_window and operator dt)*/
  int winlength;
  double decon_bandwidth_cutoff;
  /* Added Dec 2021 - this defines the upper starting frequency used for
  the EstimateBandwith function.   */
  double fhs;
  /* Defines relative time time window - ignored if length of input is
  consistent with number of samples expected in this window */
  mspass::algorithms::TimeWindow processing_window;
  mspass::algorithms::TimeWindow noise_window;
  /*! Operator used to compute power spectra using multitaper.
  Need different ones for diffent contexts to handle mixed window sizes */
  MTPowerSpectrumEngine signalengine,waveletengine;
  MTPowerSpectrumEngine dnoise_engine, wnoise_engine;
  ShapingWavelet shapingwavelet;
  /* This contains the noise power spectrum to use for regularization
  of the inverse.  It should normally be created from a longer window
  than the data.
  */
  mspass::seismic::PowerSpectrum psnoise;
  /* This contains the power spectrum of the data used to estimate
     snr-based QC estimates.   It can be the same as the data but
     not necessarily.
  */
  mspass::seismic::PowerSpectrum psnoise_data;
  /* Because of the design of the algorithm we also have to save a power
  spectral estimate for the wavelet and data signals.  We use those when
  automatic bandwidth adjustment is enabled.*/
  mspass::seismic::PowerSpectrum pssignal;
  mspass::seismic::PowerSpectrum pswavelet;
  /* Cached data to be deconvolved - result of loaddata methds*/
  mspass::seismic::Seismogram decondata;
  /* Cached wavelet for deconvolution - result of loadwavelet*/
  mspass::seismic::TimeSeries wavelet;
  /* As the name suggest we allow different tapers for data and wavelet */
  std::shared_ptr<mspass::algorithms::BasicTaper> wavelet_taper;
  std::shared_ptr<mspass::algorithms::BasicTaper> data_taper;
  /* For the colored noise damping algorithm the damper is frequency dependent.
     The same issue in water level that requires a floor on the water level
     applies to damping.   We use noise_floor to create a lower bound on
     damper values.   Note the damping constant at each frequency is
     damp*noise except where noise is below noise_floor defined relative to
     maximum noise value where it is set to n_peak*noise_floor*damp. */
  double damp, noise_floor;
  /* This algorithm uses a mix of damping and water level.   Above this floor,
  which acts a bit like a water level, no regularization is done.  If
  snr is less than this value we regularize with damp*noise_amplitude.
  Note the noise_floor parameter puts a lower bound on the frequency dependent
  regularization.   If noise amplitude (not power) is less than noise_floor
  the floor is set like a water level as noise_max*noise_level.*/
  double snr_regularization_floor;
  /* this parameter does a similar thing to regularization floor but is
  by the bandwidth estimation algorithm to define the edge of the working
  frequency band. */
  double snr_bandwidth;
  //ComplexArray winv;
  /* winv is in FFTDeconOperator*/
  ComplexArray ao_fft;
  mspass::algorithms::amplitudes::BandwidthData wavelet_bwd;
  mspass::algorithms::amplitudes::BandwidthData signal_bwd;
  /* We cache wavelet snr time series as it is more efficiently computed during
     the process routine and then used in (optional) qc methods */
  std::vector<double> wavelet_snr;
  /* SNR bandbwidth estimates count frequencies with snr above this value */
  double band_snr_floor;
  /* This array stores snr band fractions for each component.*/
  double signal_bandwidth_fraction[3];
  double peak_snr[3];
  double regularization_bandwidth_fraction;
  void read_parameters(const mspass::utility::AntelopePf& pf);
  int TestSeismogramInput(mspass::seismic::Seismogram& d,const int comp,const bool loaddata);
  void compute_gwl_inverse();
  void compute_gdamp_inverse();
  mspass::seismic::PowerSpectrum ThreeCPower(const mspass::seismic::Seismogram& d);
  void update_shaping_wavelet(const mspass::algorithms::amplitudes::BandwidthData& bwd);
};
}  // End namespace

#endif
