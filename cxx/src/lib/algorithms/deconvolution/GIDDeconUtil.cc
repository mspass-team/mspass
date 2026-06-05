#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
#include "gsl/gsl_cblas.h"
#include "mspass/utility/MsPASSError.h"
#include "misc/blas.h"
#include <algorithm>
#include <boost/any.hpp>
#include <cmath>
#include <sstream>
#include <typeinfo>

namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::algorithms;
using namespace mspass::seismic;
using namespace mspass::utility;

IterDeconType ParseGIDDeconType(const Metadata &md, const string &caller) {
  string sval = md.get_string("deconvolution_type");
  if (sval == "water_level")
    return WATER_LEVEL;
  if (sval == "least_square")
    return LEAST_SQ;
  if (sval == "multi_taper")
    return MULTI_TAPER;
  if ((sval == "cnr") || (sval == "cnr3c"))
    return CNR;
  if ((sval == "ns_gid") || (sval == "noise_stable") ||
      (sval == "noise_aware_stable"))
    return NS_GID;
  throw MsPASSError(caller + ": unknown deconvolution_type=" + sval,
                    ErrorSeverity::Invalid);
}

double GetDoubleDefault(const Metadata &md, const string &key,
                        const double default_value) {
  if (md.is_defined(key))
    return md.get_double(key);
  return default_value;
}

int GetIntDefault(const Metadata &md, const string &key,
                  const int default_value) {
  if (md.is_defined(key))
    return md.get_int(key);
  return default_value;
}

bool GetBoolDefault(const Metadata &md, const string &key,
                    const bool default_value) {
  if (md.is_defined(key))
    return md.get_bool(key);
  return default_value;
}

void ValidateProbability(const double p, const string &key,
                         const string &caller) {
  if (!std::isfinite(p) || p < 0.0 || p > 1.0)
    throw MsPASSError(caller + ": " + key + " must be in [0, 1]",
                      ErrorSeverity::Invalid);
}

void ValidatePositive(const double x, const string &key, const string &caller) {
  if (!std::isfinite(x) || x <= 0.0)
    throw MsPASSError(caller + ": " + key + " must be positive",
                      ErrorSeverity::Invalid);
}

void ValidateNonnegative(const double x, const string &key,
                         const string &caller) {
  if (!std::isfinite(x) || x < 0.0)
    throw MsPASSError(caller + ": " + key + " must be nonnegative",
                      ErrorSeverity::Invalid);
}

void ValidatePositiveInteger(const int x, const string &key,
                             const string &caller) {
  if (x <= 0)
    throw MsPASSError(caller + ": " + key + " must be positive",
                      ErrorSeverity::Invalid);
}

void ValidateThreeComponentIndex(const int component, const string &key,
                                 const string &caller) {
  if (component < 0 || component > 2)
    throw MsPASSError(caller + ": " + key + " must be 0, 1, or 2",
                      ErrorSeverity::Invalid);
}

void PutPrefixedMetadata(Metadata &target, const Metadata &source,
                         const string &prefix) {
  for (auto const &key : source.keys()) {
    boost::any val(source.get_any(key));
    const string prefixed_key(prefix + key);
    if (val.type() == typeid(bool))
      target.put(prefixed_key, boost::any_cast<bool>(val));
    else if (val.type() == typeid(int))
      target.put(prefixed_key, boost::any_cast<int>(val));
    else if (val.type() == typeid(long))
      target.put<long>(prefixed_key, boost::any_cast<long>(val));
    else if (val.type() == typeid(float))
      target.put<float>(prefixed_key, boost::any_cast<float>(val));
    else if (val.type() == typeid(double))
      target.put(prefixed_key, boost::any_cast<double>(val));
    else if (val.type() == typeid(string))
      target.put(prefixed_key, boost::any_cast<string>(val));
    else
      throw MsPASSError("PutPrefixedMetadata: unsupported Metadata type for "
                            "key=" +
                            key,
                        ErrorSeverity::Invalid);
  }
}

namespace {
string pf_value_to_text(const Metadata &md, const string &key) {
  boost::any val(md.get_any(key));
  if (val.type() == typeid(bool))
    return boost::any_cast<bool>(val) ? "true" : "false";
  if (val.type() == typeid(int))
    return to_string(boost::any_cast<int>(val));
  if (val.type() == typeid(long))
    return to_string(boost::any_cast<long>(val));
  if (val.type() == typeid(float)) {
    ostringstream ss;
    ss << static_cast<double>(boost::any_cast<float>(val));
    string result(ss.str());
    auto epos = result.find_first_of("eE");
    if (epos != string::npos && result.find('.') == string::npos)
      result.insert(epos, ".0");
    else if (epos == string::npos && result.find('.') == string::npos)
      result += ".0";
    return result;
  }
  if (val.type() == typeid(double)) {
    ostringstream ss;
    ss.precision(17);
    ss << boost::any_cast<double>(val);
    string result(ss.str());
    auto epos = result.find_first_of("eE");
    if (epos != string::npos && result.find('.') == string::npos)
      result.insert(epos, ".0");
    else if (epos == string::npos && result.find('.') == string::npos)
      result += ".0";
    return result;
  }
  if (val.type() == typeid(string))
    return boost::any_cast<string>(val);
  throw MsPASSError("AntelopePfToText: unsupported Metadata type for key=" +
                        key,
                    ErrorSeverity::Invalid);
}
} // namespace

string AntelopePfToText(const AntelopePf &pf, const int indent) {
  const string pad(indent, ' ');
  ostringstream ss;
  vector<string> keys;
  for (auto const &key : pf.keys())
    keys.push_back(key);
  sort(keys.begin(), keys.end());
  for (auto const &key : keys)
    ss << pad << key << " " << pf_value_to_text(pf, key) << "\n";

  vector<string> tbl_keys;
  for (auto const &key : pf.tbl_keys())
    tbl_keys.push_back(key);
  sort(tbl_keys.begin(), tbl_keys.end());
  for (auto const &key : tbl_keys) {
    ss << pad << key << " &Tbl{\n";
    for (auto const &line : pf.get_tbl(key))
      ss << pad << "    " << line << "\n";
    ss << pad << "}\n";
  }

  vector<string> arr_keys;
  for (auto const &key : pf.arr_keys())
    arr_keys.push_back(key);
  sort(arr_keys.begin(), arr_keys.end());
  for (auto const &key : arr_keys) {
    ss << pad << key << " &Arr{\n";
    ss << AntelopePfToText(pf.get_branch(key), indent + 4);
    ss << pad << "}\n";
  }
  return ss.str();
}

vector<double> ThreeCAmplitudes(dmatrix &d) {
  vector<double> result;
  result.reserve(d.columns());
  for (int i = 0; i < d.columns(); ++i) {
    double amp2(0.0);
    for (int k = 0; k < 3; ++k)
      amp2 += d(k, i) * d(k, i);
    result.push_back(sqrt(amp2));
  }
  return result;
}

void ValidateGIDLeafWindow(const AntelopePf &mdleaf,
                           const TimeWindow &fftwin,
                           const string &leaf_name,
                           const string &base_error) {
  const double ts = mdleaf.get<double>("deconvolution_data_window_start");
  const double te = mdleaf.get<double>("deconvolution_data_window_end");
  if ((ts != fftwin.start) || (te != fftwin.end)) {
    stringstream ss;
    ss << base_error << leaf_name
       << " method specification of processing window is not consistent "
          "with GID parameters"
       << endl
       << leaf_name << " parameters: deconvolution_data_window_start=" << ts
       << ", deconvolution_data_window_end=" << te << endl
       << "GID parameters: deconvolution_data_window_start=" << fftwin.start
       << ", deconvolution_data_window_end=" << fftwin.end << endl;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
}

void ValidateGIDLeafOperatorMetadata(const Metadata &md,
                                     const TimeWindow &fftwin,
                                     const double target_dt,
                                     const string &caller,
                                     const bool allow_noise_window_keys) {
  static const vector<string> gid_level_keys{
      "deconvolution_type",
      "full_data_window_start",
      "full_data_window_end",
      "maximum_iterations",
      "lag_weight_penalty_function",
      "lag_weight_penalty_scale_factor",
      "lag_weight_function_width",
      "lag_weight_Linf_floor",
      "lag_weight_rms_floor",
      "residual_noise_rms_probability_floor",
      "residual_fractional_improvement_floor",
      "residual_ratio_floor",
      "noise_component",
      "ns_gid_peak_sigma_threshold",
      "ns_gid_peak_probability_threshold",
      "ns_gid_use_empirical_noise_threshold",
      "ns_gid_residual_noise_ratio_floor",
      "ns_gid_max_spikes",
      "ns_gid_refit_interval",
      "ns_gid_ridge_beta",
      "ns_gid_external_wavelet_allowed"};
  static const vector<string> noise_window_keys{"noise_window_start",
                                                "noise_window_end"};
  for (auto const &key : gid_level_keys) {
    if (md.is_defined(key))
      throw MsPASSError(caller + ": GID-level parameter " + key +
                            " requires constructing a new GID engine; "
                            "changeparameter only changes the current leaf "
                            "inverse operator",
                        ErrorSeverity::Invalid);
  }
  if (!allow_noise_window_keys) {
    for (auto const &key : noise_window_keys) {
      if (md.is_defined(key))
        throw MsPASSError(caller + ": GID-level parameter " + key +
                              " requires constructing a new GID engine; "
                              "changeparameter only changes the current leaf "
                              "inverse operator",
                          ErrorSeverity::Invalid);
    }
  }
  if (md.is_defined("deconvolution_data_window_start")) {
    const double ts = md.get_double("deconvolution_data_window_start");
    if (fabs(ts - fftwin.start) > 1.0e-10)
      throw MsPASSError(caller + ": leaf deconvolution_data_window_start does "
                                 "not match the GID deconvolution window",
                        ErrorSeverity::Invalid);
  }
  if (md.is_defined("deconvolution_data_window_end")) {
    const double te = md.get_double("deconvolution_data_window_end");
    if (fabs(te - fftwin.end) > 1.0e-10)
      throw MsPASSError(caller + ": leaf deconvolution_data_window_end does "
                                 "not match the GID deconvolution window",
                        ErrorSeverity::Invalid);
  }
  if (md.is_defined("target_sample_interval")) {
    const double dt = md.get_double("target_sample_interval");
    if (fabs(dt - target_dt) >
        1.0e-6 * max(1.0, max(fabs(dt), fabs(target_dt))))
      throw MsPASSError(caller + ": leaf target_sample_interval does not "
                                 "match the GID target_sample_interval",
                        ErrorSeverity::Invalid);
  }
  if (md.is_defined("shaping_wavelet_dt")) {
    const double dt = md.get_double("shaping_wavelet_dt");
    if (fabs(dt - target_dt) >
        1.0e-6 * max(1.0, max(fabs(dt), fabs(target_dt))))
      throw MsPASSError(caller + ": leaf shaping_wavelet_dt does not match "
                                 "the GID target_sample_interval",
                        ErrorSeverity::Invalid);
  }
}

void ValidateExternalTimeSeriesSampleInterval(const TimeSeries &d,
                                              const double target_dt,
                                              const string &caller) {
  if (fabs(d.dt() - target_dt) >
      1.0e-6 * max(1.0, max(fabs(d.dt()), fabs(target_dt))))
    throw MsPASSError(caller + ": external TimeSeries dt does not match "
                               "target_sample_interval",
                      ErrorSeverity::Invalid);
}

double FIRSelfOverlap(const vector<double> &fir, const int col0_i,
                      const int col0_j, const int ncols) {
  const int nf = static_cast<int>(fir.size());
  const int offset = col0_i - col0_j;
  const int p_start = max({0, -col0_i, -offset});
  const int p_end = min({nf, ncols - col0_i, nf - offset});
  const int n = p_end - p_start;
  if (n <= 0)
    return 0.0;
  return cblas_ddot(n, &(fir[p_start]), 1, &(fir[p_start + offset]), 1);
}

double FIRDataOverlap(const vector<double> &fir, const CoreSeismogram &target,
                      const int component, const int col0) {
  const int nf = static_cast<int>(fir.size());
  const int p_start = max(0, -col0);
  const int p_end = min(nf, static_cast<int>(target.npts()) - col0);
  const int n = p_end - p_start;
  if (n <= 0)
    return 0.0;
  return cblas_ddot(n, &(fir[p_start]), 1,
                    target.u.get_address(component, col0 + p_start), 3);
}

vector<double> SolveDenseSystem(const vector<vector<double>> &a,
                                const vector<double> &b,
                                const string &caller) {
  const int n = b.size();
  vector<double> result(n, 0.0);
  if (n <= 0)
    return result;
  vector<double> A(n * n, 0.0);
  vector<double> B(b);
  for (int row = 0; row < n; ++row) {
    for (int col = 0; col < n; ++col)
      A[col * n + row] = a[row][col];
  }
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
    B[row] = b[row];
    for (int col = 0; col < n; ++col)
      A[col * n + row] = a[row][col];
  }
  vector<int> ipiv(n, 0);
  n_lapack = n;
  dgesv(n_lapack, nrhs, &(A[0]), lda, &(ipiv[0]), &(B[0]), ldb, info);
  if (info == 0)
    result = B;
  else
    throw MsPASSError(caller +
                          ": dense spike-amplitude refit system is singular",
                      ErrorSeverity::Invalid);
  return result;
}

void RefitSpikeAmplitudes(list<ThreeCSpike> &spikes,
                          const CoreSeismogram &target,
                          const vector<double> &actual_o_fir,
                          const int actual_o_0, const double ridge_beta) {
  const int nspikes = spikes.size();
  if (nspikes <= 0)
    return;
  vector<ThreeCSpike *> spike_ptrs;
  spike_ptrs.reserve(nspikes);
  for (auto &spk : spikes)
    spike_ptrs.push_back(&spk);
  vector<vector<double>> gram(nspikes, vector<double>(nspikes, 0.0));
  for (int i = 0; i < nspikes; ++i) {
    int col0_i = spike_ptrs[i]->col - actual_o_0;
    for (int j = i; j < nspikes; ++j) {
      int col0_j = spike_ptrs[j]->col - actual_o_0;
      double gij = FIRSelfOverlap(actual_o_fir, col0_i, col0_j, target.npts());
      gram[i][j] = gij;
      gram[j][i] = gij;
    }
  }
  double maxdiag(0.0);
  for (int i = 0; i < nspikes; ++i)
    maxdiag = max(maxdiag, fabs(gram[i][i]));
  double damping = maxdiag * ridge_beta;
  for (int i = 0; i < nspikes; ++i)
    gram[i][i] += damping;
  for (int component = 0; component < 3; ++component) {
    vector<double> rhs(nspikes, 0.0);
    for (int i = 0; i < nspikes; ++i) {
      int col0 = spike_ptrs[i]->col - actual_o_0;
      rhs[i] = FIRDataOverlap(actual_o_fir, target, component, col0);
    }
    vector<double> amps =
        SolveDenseSystem(gram, rhs, "RefitSpikeAmplitudes");
    for (int i = 0; i < nspikes; ++i)
      spike_ptrs[i]->u[component] = amps[i];
  }
  for (auto &spk : spikes)
    spk.amp =
        sqrt(spk.u[0] * spk.u[0] + spk.u[1] * spk.u[1] + spk.u[2] * spk.u[2]);
}
} // namespace mspass::algorithms::deconvolution
