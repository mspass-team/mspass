#ifndef __CNR3C_DECON_H__
#define __CNR3C_DECON_H__
#include <vector>
#include "mspass/utility/AntelopePf.h"
#include "mspass/deconvolution/FFTDeconOperator.h"
#include "mspass/deconvolution/ShapingWavelet.h"
#include "mspass/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/deconvolution/PowerSpectrum.h"
#include "mspass/seismic/Taper.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass{
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
    virtual mspass::Seismogram process()=0;
    /*! \brif Return the actual output of the deconvolution operator.

    The actual output is defined as w^-1*w and is compable to resolution
    kernels in linear inverse theory.   Although not required we would
    normally expect this function to be peaked at 0.   Offsets from 0
    would imply a bias. */
    virtual mspass::TimeSeries actual_output()=0;

    /*! \brief Return a FIR represention of the inverse filter.

    After any deconvolution is computed one can sometimes produce a finite
    impulse response (FIR) respresentation of the inverse filter.  */
    //virtual mspass::TimeSeries inverse_wavelet() = 0;
    virtual mspass::TimeSeries inverse_wavelet(double) = 0;
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    virtual mspass::Metadata QCMetrics()=0;
};
/*! \brief Colored Noise Regularizd 3C Deconvolution opertor.

This algorithm is somewhat of a synthesis of the best features from the
various frequency comain methods currently in use for receiver function
estimation.  Key features are:
1. Data are tapered by a generic function with a range of options.
2.  The output is always a Seismogram, which in MsPASS means 3C data
3.  The inverse is regularized by a scaled 3C noise estimate.

This object is what might be called a processing object.  That is, it
does not use the model of construction creating the thing it defines
(Seismogram, for example, uses that paradigm).  Instead the constructor
defines the operator.  Data are processed by first calling one of the
loaddata and loadnoise methods.  Once valid data is loaded call the process
method to deconvolve the loaded data. Note this approach assumes the
data, wavelet, and noise used for the processing are internally consistent.
The user is warned there a no safeties to validate any consistency because
it would be hard to do so without causing more problems that it would
solve.  In MsPASS we expect to hide this a bit behind some python wrappers
to create some safeties.
*/
//class CNR3CDecon : public mspass::FFTDeconOperator
class CNR3CDecon : public mspass::Base3CDecon, public mspass::FFTDeconOperator
{
public:
  /*! Default constructor.  Puts operator in an invalid state.*/
  CNR3CDecon();
  CNR3CDecon(const mspass::AntelopePf& pf);
  CNR3CDecon(const CNR3CDecon& parent);
  ~CNR3CDecon();
  CNR3CDecon& operator=(const CNR3CDecon& parent);
  void change_parameters(const mspass::BasicMetadata& md);
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
  void loaddata(mspass::Seismogram& d, const int wcomp,const bool loadnoise=false);
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
  void loaddata(mspass::Seismogram& d, const bool loadnoise=false);
  /*! \brief Load noise data directly.

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
  void loadnoise(mspass::Seismogram& n);
  /*! \brief Load noise estimate directly as a PowerSpectrum object.
   *
   The actual noise regularization is computed by this algorithm from an internally 
   stored PowerSpectrum object.   This method allows the power spectrum to be computed
   by some other method or using previously computed estimates rather than computing 
   it through loadnoise. 

   */
  void loadnoise(const mspass::PowerSpectrum& n);
  void loadwavelet(const mspass::TimeSeries& w);
  /* These same names are used in ScalarDecon but we don't inherit them
  here because this algorithm is 3C data centric there is a collision
  with the ScalarDecon api because of it.  */
  mspass::Seismogram process();

  /* \brief Return the ideal output of the deconvolution operator.

  All deconvolution operators have a implicit or explicit ideal output
  signal. e.g. for a spiking Wiener filter it is a delta function with or
  without a lag.  For a shaping wavelt it is the time domain version of the
  wavelet. */
  mspass::TimeSeries ideal_output();
  /*! \brif Return the actual output of the deconvolution operator.

  The actual output is defined as w^-1*w and is compable to resolution
  kernels in linear inverse theory.   Although not required we would
  normally expect this function to be peaked at 0.   Offsets from 0
  would imply a bias. */
  mspass::TimeSeries actual_output();

  /*! \brief Return a FIR represention of the inverse filter.

  After any deconvolution is computed one can sometimes produce a finite
  impulse response (FIR) respresentation of the inverse filter.  */
  mspass::TimeSeries inverse_wavelet(double tshift);
  /*! \brief Return appropriate quality measures.

  Each operator commonly has different was to measure the quality of the
  result.  This method should return these in a generic Metadata object. */
  mspass::Metadata QCMetrics();
private:
  bool taper_data;  //Set false only if none specified
  double operator_dt;   // Data must match this sample interval
  /* Expected time window size in samples
  (computed from processing_window and operator dt)*/
  int winlength;
  /* Defines relative time time window - ignored if length of input is
  consistent with number of samples expected in this window */
  mspass::TimeWindow processing_window;
  mspass::TimeWindow noise_window;
  /*! Operator used to compute power spectra using multitaper */
  mspass::MTPowerSpectrumEngine specengine;
  /* This contains the noise amplitude spectrum to use for regularization
  of the inverse.  It should normally be created from a longer window
  than the data.
  */
  mspass::PowerSpectrum psnoise;
  mspass::Seismogram decondata;
  mspass::TimeSeries wavelet;
  /* AS the name suggest we allow different tapes for data and wavelet */
  mspass::BasicTaper *wavelet_taper;
  mspass::BasicTaper *data_taper;
  double damp;
  mspass::ShapingWavelet shapingwavelet;
  /* This algorithm uses a mix of damping and water level.   Above this floor,
  which acts a bit like a water level, no regularization is done.  If
  snr is less than this value we regularize with damp*noise_amplitude.*/
  double snr_regularization_floor;
  //ComplexArray winv;
  /* winv is in FFTDeconOperator*/
  mspass::ComplexArray ao_fft;
  /* This set of attributes are used for QCMetric calculations */
  /* This holds signal to noise ration (ampllitude) for wavelet with noise
  coming from data.   May need some tweeking for array decon as would tend to
  be overly pessimistic then.  However, it will tend to correctly penalize
  frequencies in the data with poor snr. */
  std::vector<double> wavelet_snr;
  /* SNR bandbwidth estimates count frequencies with snr above this value */
  double band_snr_floor;
  /* This array stores snr band fractions for each component.*/
  double signal_bandwidth_fraction[3];
  double peak_snr[3];
  double regularization_bandwidth_fraction;
  void read_parameters(const mspass::AntelopePf& pf);
  int TestSeismogramInput(Seismogram& d,const int comp,const bool loaddata);
};
}  // End namespace

#endif
