#include "mspass/algorithms/deconvolution/TimeDomainLeastSquareDecon.h"
#include "gsl/gsl_cblas.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/utility/MsPASSError.h"
#include <cmath>
#include <sstream>
#include "misc/blas.h"

namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using mspass::algorithms::amplitudes::normalize;

namespace {
double l2_norm(const vector<double> &x) {
  if (x.empty())
    return 0.0;
  return cblas_dnrm2(x.size(), &(x[0]), 1);
}

vector<double> solve_cholesky(vector<double> normal_row_major,
                              const vector<double> &rhs, int n,
                              const string &base_error) {
  vector<double> A(n * n, 0.0);
  for (int row = 0; row < n; ++row)
    for (int col = 0; col < n; ++col)
      A[col * n + row] = normal_row_major[row * n + col];

  vector<double> B(rhs);
  int nrhs = 1;
  int n_lapack = n;
  int lda = n;
  int ldb = n;
  int info = 0;
  char lower = 'L';
  dpotrf(&lower, n_lapack, &(A[0]), lda, info);
  if (info == 0) {
    n_lapack = n;
    dpotrs(&lower, n_lapack, nrhs, &(A[0]), lda, &(B[0]), ldb, info);
    if (info == 0)
      return B;
  }

  for (int row = 0; row < n; ++row) {
    B[row] = rhs[row];
    for (int col = 0; col < n; ++col)
      A[col * n + row] = normal_row_major[row * n + col];
  }
  vector<int> ipiv(n, 0);
  n_lapack = n;
  dgesv(n_lapack, nrhs, &(A[0]), lda, &(ipiv[0]), &(B[0]), ldb, info);
  if (info == 0)
    return B;

  throw MsPASSError(base_error +
                        "regularized normal equations are singular or "
                        "ill-conditioned; increase damping_factor",
                    ErrorSeverity::Invalid);
}
} // namespace

TimeDomainLeastSquareDecon::TimeDomainLeastSquareDecon()
    : ScalarDecon(), output_length(0), sample_shift(0), damp(1.0e-6), dt(1.0),
      residual_norm(0.0), data_norm(0.0), regularization_parameter(0.0) {}

TimeDomainLeastSquareDecon::TimeDomainLeastSquareDecon(
    const TimeDomainLeastSquareDecon &parent)
    : ScalarDecon(parent), output_length(parent.output_length),
      sample_shift(parent.sample_shift), damp(parent.damp), dt(parent.dt),
      residual_norm(parent.residual_norm), data_norm(parent.data_norm),
      regularization_parameter(parent.regularization_parameter) {}

TimeDomainLeastSquareDecon::TimeDomainLeastSquareDecon(const Metadata &md)
    : ScalarDecon(), output_length(0), sample_shift(0), damp(1.0e-6), dt(1.0),
      residual_norm(0.0), data_norm(0.0), regularization_parameter(0.0) {
  this->read_metadata(md);
}

TimeDomainLeastSquareDecon::TimeDomainLeastSquareDecon(
    const Metadata &md, const vector<double> &w, const vector<double> &d)
    : TimeDomainLeastSquareDecon(md) {
  wavelet = w;
  data = d;
}

void TimeDomainLeastSquareDecon::read_metadata(const Metadata &md) {
  const string base_error("TimeDomainLeastSquareDecon::read_metadata:  ");
  try {
    damp = md.get_double("damping_factor");
    if (damp <= 0.0)
      throw MsPASSError(base_error +
                            "damping_factor must be positive for stable "
                        "time-domain least-squares deconvolution",
                        ErrorSeverity::Fatal);
    dt = md.get_double("target_sample_interval");
    if (dt <= 0.0)
      throw MsPASSError(base_error + "target_sample_interval must be positive",
                        ErrorSeverity::Fatal);
    const double dwinstart = md.get_double("deconvolution_data_window_start");
    const double dwinend = md.get_double("deconvolution_data_window_end");
    if (dwinend <= dwinstart)
      throw MsPASSError(base_error +
                            "deconvolution_data_window_end must be greater "
                        "than deconvolution_data_window_start",
                        ErrorSeverity::Fatal);
    sample_shift = static_cast<int>(round(-dwinstart / dt));
    if (sample_shift < 0)
      throw MsPASSError(base_error +
                            "deconvolution_data_window_start cannot be "
                            "positive for this lag convention",
                        ErrorSeverity::Fatal);
    if (md.is_defined("model_length")) {
      output_length = md.get_int("model_length");
      if (output_length <= 0)
        throw MsPASSError(base_error + "model_length must be positive",
                          ErrorSeverity::Fatal);
    } else {
      output_length = 0;
    }
    int shaping_nfft(md.is_defined("operator_nfft") ? md.get_int("operator_nfft")
                                                    : 0);
    if (shaping_nfft <= 0)
      shaping_nfft = 1;
    shapingwavelet = ShapingWavelet(md, shaping_nfft);
  } catch (...) {
    throw;
  }
}

void TimeDomainLeastSquareDecon::changeparameter(const Metadata &md) {
  this->read_metadata(md);
}

vector<double> TimeDomainLeastSquareDecon::solve_for(
    const vector<double> &rhs_data) const {
  const string base_error("TimeDomainLeastSquareDecon::solve_for:  ");
  const int nd(rhs_data.size());
  const int nw(wavelet.size());
  const int nm(output_length > 0 ? output_length : nd);
  if (nd <= 0)
    throw MsPASSError(base_error + "data vector is empty",
                      ErrorSeverity::Invalid);
  if (nw <= 0)
    throw MsPASSError(base_error + "wavelet vector is empty",
                      ErrorSeverity::Invalid);
  if (l2_norm(wavelet) <= 0.0)
    throw MsPASSError(base_error + "wavelet data vector is all zeros",
                      ErrorSeverity::Invalid);

  vector<double> normal(nm * nm, 0.0);
  vector<double> rhs(nm, 0.0);
  for (int j = 0; j < nm; ++j) {
    for (int i = 0; i < nd; ++i) {
      const int iw = i - j + sample_shift;
      if (iw >= 0 && iw < nw)
        rhs[j] += wavelet[iw] * rhs_data[i];
    }
    for (int k = 0; k <= j; ++k) {
      double s(0.0);
      for (int i = 0; i < nd; ++i) {
        const int iwj = i - j + sample_shift;
        const int iwk = i - k + sample_shift;
        if (iwj >= 0 && iwj < nw && iwk >= 0 && iwk < nw)
          s += wavelet[iwj] * wavelet[iwk];
      }
      normal[j * nm + k] = s;
      normal[k * nm + j] = s;
    }
  }
  double maxdiag(0.0);
  for (int j = 0; j < nm; ++j)
    if (normal[j * nm + j] > maxdiag)
      maxdiag = normal[j * nm + j];
  if (maxdiag <= 0.0)
    throw MsPASSError(base_error + "convolution matrix has zero energy",
                      ErrorSeverity::Invalid);
  regularization_parameter = damp * maxdiag;
  for (int j = 0; j < nm; ++j)
    normal[j * nm + j] += regularization_parameter;
  return solve_cholesky(normal, rhs, nm, base_error);
}

vector<double>
TimeDomainLeastSquareDecon::apply_wavelet(const vector<double> &model) const {
  vector<double> predicted(data.size(), 0.0);
  for (int i = 0; i < static_cast<int>(predicted.size()); ++i) {
    double s(0.0);
    for (int j = 0; j < static_cast<int>(model.size()); ++j) {
      const int iw = i - j + sample_shift;
      if (iw >= 0 && iw < static_cast<int>(wavelet.size()))
        s += wavelet[iw] * model[j];
    }
    predicted[i] = s;
  }
  return predicted;
}

vector<double> TimeDomainLeastSquareDecon::apply_shaping_wavelet(
    const vector<double> &model) {
  if (shapingwavelet.type() == "none")
    return model;
  CoreTimeSeries sw(shapingwavelet.impulse_response());
  const int izero = sw.sample_number(0.0);
  vector<double> shaped(model.size(), 0.0);
  for (int i = 0; i < static_cast<int>(model.size()); ++i) {
    double s(0.0);
    for (int k = 0; k < sw.npts(); ++k) {
      const int j = i - (k - izero);
      if (j >= 0 && j < static_cast<int>(model.size()))
        s += sw.s[k] * model[j];
    }
    shaped[i] = s;
  }
  return shaped;
}

void TimeDomainLeastSquareDecon::process() {
  const string base_error("TimeDomainLeastSquareDecon::process:  ");
  result.clear();
  vector<double> model(this->solve_for(data));
  vector<double> predicted(this->apply_wavelet(model));
  data_norm = l2_norm(data);
  double r2(0.0);
  for (int i = 0; i < static_cast<int>(data.size()); ++i) {
    const double r = data[i] - predicted[i];
    r2 += r * r;
  }
  residual_norm = sqrt(r2);
  result = this->apply_shaping_wavelet(model);
  if (result.empty())
    throw MsPASSError(base_error + "computed an empty result",
                      ErrorSeverity::Fatal);
}

CoreTimeSeries TimeDomainLeastSquareDecon::actual_output() {
  if (wavelet.empty())
    throw MsPASSError("TimeDomainLeastSquareDecon::actual_output: wavelet has "
                      "not been loaded",
                      ErrorSeverity::Invalid);
  const string base_error("TimeDomainLeastSquareDecon::actual_output: ");
  const int nlag(output_length > 0
                     ? output_length
                     : (!data.empty() ? static_cast<int>(data.size())
                                      : static_cast<int>(wavelet.size())));
  if (sample_shift < 0 || sample_shift >= nlag)
    throw MsPASSError(base_error +
                          "zero-lag sample is outside extracted lag window",
                      ErrorSeverity::Invalid);
  vector<double> delta(nlag, 0.0);
  delta[sample_shift] = 1.0;
  vector<double> inv(this->solve_for(delta));
  vector<double> ao(nlag, 0.0);
  for (int i = 0; i < static_cast<int>(ao.size()); ++i) {
    for (int j = 0; j < static_cast<int>(inv.size()); ++j) {
      const int iw = i - j + sample_shift;
      if (iw >= 0 && iw < static_cast<int>(wavelet.size()))
        ao[i] += wavelet[iw] * inv[j];
    }
  }
  ao = this->apply_shaping_wavelet(ao);
  if (l2_norm(ao) > 0.0)
    ao = normalize<double>(ao);
  CoreTimeSeries result(ao.size());
  result.set_dt(dt);
  result.set_t0(-dt * static_cast<double>(sample_shift));
  result.set_tref(TimeReferenceType::Relative);
  result.set_live();
  result.s = ao;
  return result;
}

CoreTimeSeries
TimeDomainLeastSquareDecon::inverse_wavelet(const double t0parent) {
  if (wavelet.empty())
    throw MsPASSError("TimeDomainLeastSquareDecon::inverse_wavelet: wavelet "
                      "has not been loaded",
                      ErrorSeverity::Invalid);
  const string base_error("TimeDomainLeastSquareDecon::inverse_wavelet: ");
  const int nlag(output_length > 0
                     ? output_length
                     : (!data.empty() ? static_cast<int>(data.size())
                                      : static_cast<int>(wavelet.size())));
  if (sample_shift < 0 || sample_shift >= nlag)
    throw MsPASSError(base_error +
                          "zero-lag sample is outside extracted lag window",
                      ErrorSeverity::Invalid);
  vector<double> delta(nlag, 0.0);
  delta[sample_shift] = 1.0;
  vector<double> inv(this->solve_for(delta));
  CoreTimeSeries result(inv.size());
  result.set_dt(dt);
  result.set_t0(-dt * static_cast<double>(sample_shift) - t0parent);
  result.set_tref(TimeReferenceType::Relative);
  result.set_live();
  result.s = inv;
  return result;
}

CoreTimeSeries TimeDomainLeastSquareDecon::inverse_wavelet() {
  return this->inverse_wavelet(0.0);
}

Metadata TimeDomainLeastSquareDecon::QCMetrics() {
  Metadata md;
  md.put("damping_factor", damp);
  md.put("sample_shift", sample_shift);
  md.put("regularization_parameter", regularization_parameter);
  md.put("residual_norm", residual_norm);
  md.put("data_norm", data_norm);
  if (data_norm > 0.0)
    md.put("relative_residual_norm", residual_norm / data_norm);
  return md;
}
} // namespace mspass::algorithms::deconvolution
