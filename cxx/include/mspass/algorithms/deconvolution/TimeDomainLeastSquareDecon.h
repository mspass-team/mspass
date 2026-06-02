#ifndef __TIME_DOMAIN_LEAST_SQUARE_DECON_H__
#define __TIME_DOMAIN_LEAST_SQUARE_DECON_H__
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/Metadata.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/base_object.hpp>
#include <vector>

namespace mspass::algorithms::deconvolution {
/*! \brief Time-domain damped least-squares deconvolution.

This class solves the linear convolution problem

    d_i = sum_j s_{i-j} m_j + n_i

directly in the time domain.  The convolution matrix is the cropped linear
Toeplitz operator implied by the loaded wavelet and data vectors.  The default
model length is the loaded data length, so output samples have the same
zero-lag convention as the input window, but the computation is not circular:
samples outside the wavelet support are treated as zero.

The normal equations are regularized as

    (S^T S + lambda I) m = S^T d

where lambda is `damping_factor * max(diag(S^T S))`.  `damping_factor` must be
positive; smaller values give higher resolution but amplify noise and
conditioning errors, while larger values suppress noise at the cost of
resolution.  The solve uses LAPACK Cholesky with LU fallback; it does not
explicitly invert the normal-equation matrix.
*/
class TimeDomainLeastSquareDecon : public ScalarDecon {
public:
  TimeDomainLeastSquareDecon();
  TimeDomainLeastSquareDecon(const TimeDomainLeastSquareDecon &parent);
  TimeDomainLeastSquareDecon(const mspass::utility::Metadata &md);
  TimeDomainLeastSquareDecon(const mspass::utility::Metadata &md,
                             const std::vector<double> &wavelet,
                             const std::vector<double> &data);
  void changeparameter(const mspass::utility::Metadata &md);
  void process();
  mspass::seismic::CoreTimeSeries actual_output();
  mspass::seismic::CoreTimeSeries inverse_wavelet(const double t0parent);
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  mspass::utility::Metadata QCMetrics();

private:
  void read_metadata(const mspass::utility::Metadata &md);
  std::vector<double> solve_for(const std::vector<double> &rhs_data) const;
  std::vector<double> apply_wavelet(const std::vector<double> &model) const;
  std::vector<double> apply_shaping_wavelet(
      const std::vector<double> &model);
  int output_length;
  int sample_shift;
  double damp;
  double dt;
  mutable double residual_norm;
  mutable double data_norm;
  mutable double regularization_parameter;

  friend boost::serialization::access;
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar &boost::serialization::base_object<ScalarDecon>(*this);
    ar & output_length;
    ar & sample_shift;
    ar & damp;
    ar & dt;
    ar & residual_norm;
    ar & data_norm;
    ar & regularization_parameter;
  }
};
} // namespace mspass::algorithms::deconvolution
#endif
