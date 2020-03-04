#ifndef __CNR3C_DECON_H__
#define __CNR3C_DECON_H__
#include <vector>
#include "mspass/utility/AntelopPf.h"
#include "mspass/deconvolution/ScalarDecon.h"
#include "mspass/deconvolution/FFTDeconOperator.h"
#include "mspass/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/CoreSeismogram.h"
namespace mspass{
/*! \brief Colored Noise Regularizd 3C Deconvolution opertor.

This algorithm is somewhat of a synthesis of the best features from the
various frequency comain methods currently in use for receiver function
estimation.  Key features are:
1. DAta are tapered by a generic function with a range of options.
2.  The output is always a Seismogram, which in MsPASS means 3C data
3.  The inverse is regularized by a scaled 3C noise estimate.
*/
class CNR3CDecon: public FFTDeconOperator, public Base3CDecon
{
public:
  CNR3CDecon(const mspass::AntelopePf& pf);
  ~CNR3CDecon() {};
  void changeparameter(const mspass::Metadata &md){};
  /*! \brief Load data with one component used as wavelet estimate.

  Use this method for conventional receiver function data if one of
  components is to be used as the wavelet estimate (conventionally Z or L).
  */
  void loaddata(const CoreSeismogram& d, int wcomp);
  void loaddata(const CoreSeismogram& d, const TimeSeries& w);
  void loadnoise(const CoreSeismogram& n);
  /* These same names are used in ScalarDecon but we don't inherit them
  here because this algorithm is 3C data centric there is a collision
  with the ScalarDecon api because of it.  */
  void process();

  /* \brief Return the ideal output of the deconvolution operator.

  All deconvolution operators have a implicit or explicit ideal output
  signal. e.g. for a spiking Wiener filter it is a delta function with or
  without a lag.  For a shaping wavelt it is the time domain version of the
  wavelet. */
  mspass::CoreTimeSeries ideal_output();
  /*! \brif Return the actual output of the deconvolution operator.

  The actual output is defined as w^-1*w and is compable to resolution
  kernels in linear inverse theory.   Although not required we would
  normally expect this function to be peaked at 0.   Offsets from 0
  would imply a bias. */
  mspass::CoreTimeSeries actual_output();

  /*! \brief Return a FIR represention of the inverse filter.

  After any deconvolution is computed one can sometimes produce a finite
  impulse response (FIR) respresentation of the inverse filter.  */
  mspass::CoreTimeSeries inverse_wavelet() = 0;
  mspass::CoreTimeSeries inverse_wavelet(double) = 0;
  /*! \brief Return appropriate quality measures.

  Each operator commonly has different was to measure the quality of the
  result.  This method should return these in a generic Metadata object. */
  mspass::Metadata QCMetrics();
private:
  /* This contains the noise amplitude spectrum to use for each component.
  The spectra may or may not be the same for each component.  May not want to
  do that thoiugh as a uless complexity of questionable merit.
  */
  dmatrix noise;
};
}  // End namespace

#endif
