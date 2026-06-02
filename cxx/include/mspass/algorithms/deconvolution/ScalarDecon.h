#ifndef __SIMPLE_DECON_H__
#define __SIMPLE_DECON_H__

#include "mspass/algorithms/deconvolution/BasicDeconOperator.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/Metadata.h"
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>

namespace mspass::algorithms::deconvolution {
/*! \brief Base class decon operator for single station 3C decon (receiver
functions).

A class of algorithms exist for computing so called receiver functions.
Simple for this application means a method that is applied to a single
station's data and computed in a scalar time series sense.   Thus the
interface assumes we want to always at least load an estimate of the source
wavelet for deconvolution and the data to which it is to be applied.

The design of this interface was made more complicated by a design goal
to allow application of different conventional methods as the first step
in the generalized iterative method.  The generalized method can select
one of the children of this base class.
*/
class ScalarDecon : public BasicDeconOperator {
public:
  ScalarDecon() : shapingwavelet() {};
  ScalarDecon(const mspass::utility::Metadata &md);
  ScalarDecon(const std::vector<double> &d, const std::vector<double> &w);
  ScalarDecon(const ScalarDecon &parent);
  /*! \brief Load all data required for decon.

  This method loads both the data vector and wavelet estimates as
  simple std::vectors.  Timing must be maintained externally.  This or
  the pair of methods loaddata and loadwavelet must be called before process.
  This method has a slight advantage in efficiency over successive calls to
  loaddata and loadwavelet for several reasons, but the difference is small.

  \return always returns 0
  */
  int load(const std::vector<double> &wavelet, const std::vector<double> &data);
  /*! Load only the data vector.*/
  int loaddata(const std::vector<double> &data);
  /*! Load only the wavelet estimate.*/
  int loadwavelet(const std::vector<double> &wavelet);
  virtual void process() = 0;
  ~ScalarDecon() {};
  ScalarDecon &operator=(const ScalarDecon &parent);
  std::vector<double> getresult() { return result; };
  /* This method does nothing, but needs to be defined to avoid
   * gcc compile errors in programs using children of this class.*/
  void changeparameter(const mspass::utility::Metadata &md);
  /*! Change the shaping wavelet that will be applied to output.
   *
   The suite of algorithms here use the concept of a shaping wavelet
   thoughout.  The shaping wavelet for most applications should have
   a zero phase impulse response.  This method changes the
   wavelet set with the operator. */
  void change_shaping_wavelet(const ShapingWavelet &nsw);
  /*! getter for ShapingWavelet stored with the operator. */
  ShapingWavelet get_shaping_wavelet() const { return this->shapingwavelet; };
  /*! \brief Return the output shaping wavelet.
   *
   * Wang and Pavlis (2016) call this wavelet ws(t).  GID methods convolve the
   * sparse impulse response with this wavelet to form the finite-duration
   * receiver-function representation used for stacking and imaging. */
  mspass::seismic::CoreTimeSeries output_shaping_wavelet() {
    return this->shapingwavelet.impulse_response();
  };
  /*! \brief Legacy alias for output_shaping_wavelet.
   *
   * Older MsPASS code called the output shaping wavelet "ideal_output".  New
   * code and documentation should prefer output_shaping_wavelet, which matches
   * the terminology of Wang and Pavlis (2016). */
  mspass::seismic::CoreTimeSeries ideal_output() {
    return this->output_shaping_wavelet();
  };
  /*! \brief Return the actual output of the deconvolution operator.

  The actual output is defined as w^-1*w and is compable to resolution
  kernels in linear inverse theory.   Although not required we would
  normally expect this function to be peaked at 0.   Offsets from 0
  would imply a bias. */
  virtual mspass::seismic::CoreTimeSeries actual_output() = 0;
  /*! \brief Alias for actual_output using inverse-theory terminology. */
  mspass::seismic::CoreTimeSeries resolution_kernel() {
    return this->actual_output();
  };

  /*! \brief Return a FIR represention of the inverse filter.

  After any deconvolution is computed one can sometimes produce a finite
  impulse response (FIR) respresentation of the inverse filter.  */
  virtual mspass::seismic::CoreTimeSeries inverse_wavelet() = 0;
  virtual mspass::seismic::CoreTimeSeries inverse_wavelet(double) = 0;
  /*! \brief Return appropriate quality measures.

  Each operator commonly has different was to measure the quality of the
  result.  This method should return these in a generic Metadata object. */
  virtual mspass::utility::Metadata QCMetrics() = 0;

protected:
  std::vector<double> data;
  std::vector<double> wavelet;
  std::vector<double> result;
  ShapingWavelet shapingwavelet;

private:
  friend boost::serialization::access;
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & data;
    ar & wavelet;
    ar & result;
    ar & shapingwavelet;
  }
};
} // namespace mspass::algorithms::deconvolution
#endif
