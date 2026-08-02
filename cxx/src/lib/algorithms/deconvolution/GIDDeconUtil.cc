#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
#include "gsl/gsl_cblas.h"
#include "mspass/utility/MsPASSError.h"
#include "misc/blas.h"
#include <algorithm>
#include <boost/any.hpp>
#include <cmath>
#include <cstdint>
#include <limits>
#include <sstream>
#include <typeinfo>
#include <utility>

namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::algorithms;
using namespace mspass::seismic;
using namespace mspass::utility;

namespace {
string metadata_type_name(const boost::any &val) { return demangled_name(val); }

MsPASSError metadata_get_context_error(const string &function_name,
                                       const string &key,
                                       const string &expected,
                                       const MetadataGetError &err) {
  return MsPASSError(function_name + ": Metadata key=" + key + " must be " +
                        expected + "; " + string(err.what()),
                    ErrorSeverity::Invalid);
}

inline double three_component_norm(const double x0, const double x1,
                                   const double x2) {
  return sqrt(x0 * x0 + x1 * x1 + x2 * x2);
}
} // namespace

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
  if ((sval == "group_sparse") || (sval == "group_lasso") ||
      (sval == "sparse_group_lasso"))
    return GROUP_SPARSE;
  throw MsPASSError(caller + ": unknown deconvolution_type=" + sval,
                    ErrorSeverity::Fatal);
}

string GIDDeconTypeName(const IterDeconType type) {
  switch (type) {
  case WATER_LEVEL:
    return "water_level";
  case LEAST_SQ:
    return "least_square";
  case MULTI_TAPER:
    return "multi_taper";
  case CNR:
    return "cnr";
  case NS_GID:
    return "ns_gid";
  case GROUP_SPARSE:
    return "group_sparse";
  }
  return "unknown";
}

double GetDoubleDefault(const Metadata &md, const string &key,
                        const double default_value) {
  if (md.is_defined(key))
    return GetDoubleRequired(md, key);
  return default_value;
}

double GetDoubleRequired(const Metadata &md, const string &key) {
  try {
    return md.get_double(key);
  } catch (const MetadataGetError &merr) {
    if (md.is_defined(key))
      throw metadata_get_context_error("GetDoubleRequired", key, "numeric",
                                       merr);
    throw;
  }
}

int GetIntDefault(const Metadata &md, const string &key,
                  const int default_value) {
  if (md.is_defined(key))
    return GetIntRequired(md, key);
  return default_value;
}

int GetIntRequired(const Metadata &md, const string &key) {
  try {
    return md.get_int(key);
  } catch (const MetadataGetError &merr) {
    if (md.is_defined(key))
      throw metadata_get_context_error("GetIntRequired", key, "integer-valued",
                                       merr);
    throw;
  }
}

long GetLongRequired(const Metadata &md, const string &key) {
  try {
    return md.get_long(key);
  } catch (const MetadataGetError &merr) {
    if (md.is_defined(key))
      throw metadata_get_context_error("GetLongRequired", key,
                                       "integer-valued", merr);
    throw;
  }
}

bool GetBoolDefault(const Metadata &md, const string &key,
                    const bool default_value) {
  if (md.is_defined(key)) {
    try {
      return md.get_bool(key);
    } catch (const MetadataGetError &merr) {
      throw metadata_get_context_error("GetBoolDefault", key, "boolean", merr);
    }
  }
  return default_value;
}

void ValidateProbability(const double p, const string &key,
                         const string &caller) {
  if (!std::isfinite(p) || p < 0.0 || p > 1.0)
    throw MsPASSError(caller + ": " + key + " must be in [0, 1]",
                      ErrorSeverity::Fatal);
}

void ValidatePositive(const double x, const string &key, const string &caller) {
  if (!std::isfinite(x) || x <= 0.0)
    throw MsPASSError(caller + ": " + key + " must be positive",
                      ErrorSeverity::Fatal);
}

void ValidateNonnegative(const double x, const string &key,
                         const string &caller) {
  if (!std::isfinite(x) || x < 0.0)
    throw MsPASSError(caller + ": " + key + " must be nonnegative",
                      ErrorSeverity::Fatal);
}

void ValidatePositiveInteger(const int x, const string &key,
                             const string &caller) {
  if (x <= 0)
    throw MsPASSError(caller + ": " + key + " must be positive",
                      ErrorSeverity::Fatal);
}

void ValidateThreeComponentIndex(const int component, const string &key,
                                 const string &caller) {
  if (component < 0 || component > 2)
    throw MsPASSError(caller + ": " + key + " must be 0, 1, or 2",
                      ErrorSeverity::Fatal);
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
                            key + " prefixed as " + prefixed_key +
                            "; actual type=" + metadata_type_name(val),
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

vector<double> ThreeCAmplitudes(const dmatrix &d) {
  vector<double> result;
  const int ncols = static_cast<int>(d.columns());
  result.reserve(ncols);
  for (int i = 0; i < ncols; ++i) {
    result.push_back(three_component_norm(d(0, i), d(1, i), d(2, i)));
  }
  return result;
}

double GroupSparseObjective(const CoreSeismogram &residual,
                            const list<ThreeCSpike> &spikes,
                            const double lambda) {
  double rss(0.0), penalty(0.0);
  const int nrows = static_cast<int>(residual.u.rows());
  const int ncols = static_cast<int>(residual.u.columns());
  for (int k = 0; k < nrows; ++k) {
    for (int j = 0; j < ncols; ++j) {
      const double e = residual.u(k, j);
      rss += e * e;
    }
  }
  for (const auto &spk : spikes) {
    penalty += three_component_norm(spk.u[0], spk.u[1], spk.u[2]);
  }
  return 0.5 * rss + lambda * penalty;
}

void ValidateGIDLeafWindow(const AntelopePf &mdleaf,
                           const TimeWindow &fftwin,
                           const string &leaf_name,
                           const string &base_error) {
  const double ts = GetDoubleRequired(mdleaf, "deconvolution_data_window_start");
  const double te = GetDoubleRequired(mdleaf, "deconvolution_data_window_end");
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
    throw MsPASSError(ss.str(), ErrorSeverity::Fatal);
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
      "ns_gid_external_wavelet_allowed",
      "wavelet_window_start",
      "wavelet_window_end",
      "group_sparse_lambda",
      "group_sparse_lambda_scale",
      "group_sparse_tolerance",
      "group_sparse_max_iterations",
      "group_sparse_active_threshold",
      "group_sparse_active_threshold_scale",
      "group_sparse_active_threshold_quantile"};
  static const vector<string> noise_window_keys{"noise_window_start",
                                                "noise_window_end"};
  for (auto const &key : gid_level_keys) {
    if (md.is_defined(key))
      throw MsPASSError(caller + ": GID-level parameter " + key +
                            " requires constructing a new GID engine; "
                            "changeparameter only changes the current leaf "
                            "inverse operator",
                        ErrorSeverity::Fatal);
  }
  if (!allow_noise_window_keys) {
    for (auto const &key : noise_window_keys) {
      if (md.is_defined(key))
        throw MsPASSError(caller + ": GID-level parameter " + key +
                              " requires constructing a new GID engine; "
                              "changeparameter only changes the current leaf "
                              "inverse operator",
                          ErrorSeverity::Fatal);
    }
  }
  if (md.is_defined("deconvolution_data_window_start")) {
    const double ts = GetDoubleRequired(md, "deconvolution_data_window_start");
    if (fabs(ts - fftwin.start) > 1.0e-10)
      throw MsPASSError(caller + ": leaf deconvolution_data_window_start does "
                                 "not match the GID deconvolution window",
                        ErrorSeverity::Fatal);
  }
  if (md.is_defined("deconvolution_data_window_end")) {
    const double te = GetDoubleRequired(md, "deconvolution_data_window_end");
    if (fabs(te - fftwin.end) > 1.0e-10)
      throw MsPASSError(caller + ": leaf deconvolution_data_window_end does "
                                 "not match the GID deconvolution window",
                        ErrorSeverity::Fatal);
  }
  if (md.is_defined("target_sample_interval")) {
    const double dt = GetDoubleRequired(md, "target_sample_interval");
    if (fabs(dt - target_dt) >
        1.0e-6 * max(1.0, max(fabs(dt), fabs(target_dt))))
      throw MsPASSError(caller + ": leaf target_sample_interval does not "
                                 "match the GID target_sample_interval",
                        ErrorSeverity::Fatal);
  }
  if (md.is_defined("shaping_wavelet_dt")) {
    const double dt = GetDoubleRequired(md, "shaping_wavelet_dt");
    if (fabs(dt - target_dt) >
        1.0e-6 * max(1.0, max(fabs(dt), fabs(target_dt))))
      throw MsPASSError(caller + ": leaf shaping_wavelet_dt does not match "
                                 "the GID target_sample_interval",
                        ErrorSeverity::Fatal);
  }
}

void ValidateExternalTimeSeriesSampleInterval(const TimeSeries &d,
                                              const double target_dt,
                                              const string &caller) {
  if (!std::isfinite(target_dt) || target_dt <= 0.0)
    throw MsPASSError(caller + ": target_sample_interval must be finite and "
                               "positive",
                      ErrorSeverity::Invalid);
  if (!std::isfinite(d.dt()) || d.dt() <= 0.0)
    throw MsPASSError(caller + ": external TimeSeries dt must be finite and "
                               "positive",
                      ErrorSeverity::Invalid);
  if (!std::isfinite(d.t0()) || !std::isfinite(d.endtime()))
    throw MsPASSError(caller + ": external TimeSeries t0 and endtime must "
                               "be finite",
                      ErrorSeverity::Invalid);
  if (fabs(d.dt() - target_dt) >
      1.0e-6 * max(1.0, max(fabs(d.dt()), fabs(target_dt))))
    throw MsPASSError(caller + ": external TimeSeries dt does not match "
                               "target_sample_interval",
                      ErrorSeverity::Invalid);
}

void ValidateExternalTimeSeriesTimeReference(
    const TimeSeries &d, const TimeReferenceType analysis_tref,
    const string &caller) {
  if (d.timetype() != analysis_tref)
    throw MsPASSError(caller + ": external TimeSeries TimeReferenceType does "
                               "not match the analysis data",
                      ErrorSeverity::Invalid);
}

namespace {
constexpr int64_t max_signed_int_fft_length = INT64_C(1) << 30;

void validate_blas_three_component_count(const int64_t npts,
                                         const string &caller) {
  if (npts <= 0 || npts > std::numeric_limits<int>::max() / 3)
    throw MsPASSError(caller + ": three-component sample count exceeds the "
                               "32-bit BLAS limit",
                      ErrorSeverity::Invalid);
}

void validate_common_grid_series(const BasicTimeSeries &d,
                                 const string &label,
                                 const string &caller) {
  if (d.npts() == 0)
    throw MsPASSError(caller + ": " + label + " has no samples",
                      ErrorSeverity::Invalid);
  if (!std::isfinite(d.dt()) || d.dt() <= 0.0)
    throw MsPASSError(caller + ": " + label + " dt must be finite and "
                               "positive",
                      ErrorSeverity::Invalid);
  if (!std::isfinite(d.t0()) || !std::isfinite(d.endtime()))
    throw MsPASSError(caller + ": " + label + " t0 and endtime must be "
                               "finite",
                      ErrorSeverity::Invalid);
  if (d.npts() > static_cast<size_t>(std::numeric_limits<int>::max()))
    throw MsPASSError(caller + ": " + label + " sample count exceeds the "
                               "supported signed-int grid limit",
                      ErrorSeverity::Invalid);
}

int checked_grid_offset(const double time, const double origin,
                        const double dt, const string &label,
                        const string &caller) {
  const long double q =
      (static_cast<long double>(time) - static_cast<long double>(origin)) /
      static_cast<long double>(dt);
  const long double int_limit =
      static_cast<long double>(std::numeric_limits<int>::max());
  if (!std::isfinite(q) || q < -int_limit || q > int_limit)
    throw MsPASSError(caller + ": " + label + " offset exceeds the "
                               "supported signed-int grid limit",
                      ErrorSeverity::Invalid);
  const long long offset = std::llround(q);
  /* The relative term accommodates absolute/UTC floating-point arithmetic,
   * but alignment is a sample-grid contract: it must never grow into a
   * material fraction of a sample for a large epoch offset. */
  const long double tolerance = std::min(
      1.0e-3L, 1.0e-6L * std::max(1.0L, std::fabs(q)));
  if (std::fabs(q - static_cast<long double>(offset)) > tolerance)
    throw MsPASSError(caller + ": " + label + " is not aligned to the "
                               "analysis sample grid",
                      ErrorSeverity::Invalid);
  if (offset < 0 || offset > std::numeric_limits<int>::max())
    throw MsPASSError(caller + ": " + label + " offset is outside the "
                               "common grid",
                      ErrorSeverity::Invalid);
  return static_cast<int>(offset);
}
} // namespace

int CheckedGIDLinearConvolutionNFFT(const int data_npts,
                                    const int wavelet_npts,
                                    const int noise_npts,
                                    const bool include_noise,
                                    const string &caller) {
  if (data_npts <= 0 || wavelet_npts <= 0 ||
      (include_noise && noise_npts <= 0))
    throw MsPASSError(caller + ": linear-convolution input lengths must be "
                               "positive",
                      ErrorSeverity::Invalid);
  const int64_t signal_linear = static_cast<int64_t>(data_npts) +
                                static_cast<int64_t>(wavelet_npts) - 1;
  int64_t required = signal_linear;
  if (include_noise) {
    const int64_t noise_linear = static_cast<int64_t>(noise_npts) +
                                 static_cast<int64_t>(wavelet_npts) - 1;
    required = max(required, noise_linear);
  }
  /* 2^30 is the largest power of two representable by a signed int. */
  if (required <= 0 || required > max_signed_int_fft_length)
    throw MsPASSError(caller + ": linear-convolution length cannot be "
                               "represented by the signed-int FFT API",
                      ErrorSeverity::Invalid);
  int64_t nfft = 1;
  while (nfft < required)
    nfft <<= 1;
  if (nfft > std::numeric_limits<int>::max())
    throw MsPASSError(caller + ": next power-of-two FFT length exceeds the "
                               "signed-int API",
                      ErrorSeverity::Invalid);
  return static_cast<int>(nfft);
}

int CheckedGIDWindowSampleCount(const TimeWindow &window, const double dt,
                                const string &caller) {
  if (!std::isfinite(window.start) || !std::isfinite(window.end) ||
      window.end < window.start || !std::isfinite(dt) || dt <= 0.0)
    throw MsPASSError(caller + ": window and sample interval must be finite "
                               "with nonnegative duration and positive dt",
                      ErrorSeverity::Invalid);
  const long double samples =
      (static_cast<long double>(window.end) -
       static_cast<long double>(window.start)) /
      static_cast<long double>(dt);
  const long double max_samples =
      static_cast<long double>(std::numeric_limits<int>::max()) - 1.0L;
  if (!std::isfinite(samples) || samples < 0.0L || samples > max_samples)
    throw MsPASSError(caller + ": window sample count exceeds the supported "
                               "signed-int limit",
                      ErrorSeverity::Invalid);
  const long long rounded = std::llround(samples);
  if (std::fabs(samples - static_cast<long double>(rounded)) > 1.0e-3L)
    throw MsPASSError(caller + ": window duration is not aligned to the "
                               "target sample interval",
                      ErrorSeverity::Invalid);
  const long long count = rounded + 1;
  validate_blas_three_component_count(count, caller);
  const int result = static_cast<int>(count);
  (void)CheckedGIDLinearConvolutionNFFT(result, 1, 1, false, caller);
  return result;
}

GIDCommonTimeGrid BuildGIDCommonTimeGrid(const BasicTimeSeries &analysis,
                                         const BasicTimeSeries &wavelet,
                                         const string &caller) {
  validate_common_grid_series(analysis, "analysis data", caller);
  validate_common_grid_series(wavelet, "wavelet", caller);
  if (analysis.timetype() != wavelet.timetype())
    throw MsPASSError(caller + ": wavelet TimeReferenceType does not match "
                               "the analysis data",
                      ErrorSeverity::Invalid);
  const double dt_tolerance =
      1.0e-6 * max(1.0, max(fabs(analysis.dt()), fabs(wavelet.dt())));
  if (fabs(analysis.dt() - wavelet.dt()) > dt_tolerance)
    throw MsPASSError(caller + ": wavelet dt does not match analysis data",
                      ErrorSeverity::Invalid);

  const double grid_t0 = min(analysis.t0(), wavelet.t0());
  const double grid_end = max(analysis.endtime(), wavelet.endtime());
  const long double span = static_cast<long double>(grid_end) -
                           static_cast<long double>(grid_t0);
  const long double samples = span / static_cast<long double>(analysis.dt());
  const long double max_count =
      static_cast<long double>(std::numeric_limits<int>::max()) - 1.0L;
  if (!std::isfinite(samples) || samples < 0.0L || samples > max_count)
    throw MsPASSError(caller + ": common analysis/wavelet grid exceeds the "
                               "supported signed-int sample limit",
                      ErrorSeverity::Invalid);
  const long long rounded_samples = std::llround(samples);
  /* See checked_grid_offset: a common grid cannot accept a fractional sample
   * merely because its absolute UTC offset is large. */
  const long double tolerance = std::min(
      1.0e-3L, 1.0e-6L * std::max(1.0L, std::fabs(samples)));
  if (std::fabs(samples - static_cast<long double>(rounded_samples)) >
      tolerance)
    throw MsPASSError(caller + ": analysis and wavelet endpoints are not "
                               "aligned to a common sample grid",
                      ErrorSeverity::Invalid);
  const long long count = rounded_samples + 1;
  if (count <= 0 || count > std::numeric_limits<int>::max())
    throw MsPASSError(caller + ": common analysis/wavelet grid sample count "
                               "is invalid or too large",
                      ErrorSeverity::Invalid);
  validate_blas_three_component_count(count, caller);
  /* The leaf receives two N-sample vectors on this common grid.  Reject the
   * request before any CoreSeismogram/vector allocation if its linear FFT
   * would overflow the signed-int downstream interfaces. */
  (void)CheckedGIDLinearConvolutionNFFT(
      static_cast<int>(count), static_cast<int>(count), 1, false, caller);

  GIDCommonTimeGrid result{grid_t0, static_cast<int>(count), 0, 0};
  result.analysis_offset =
      checked_grid_offset(analysis.t0(), grid_t0, analysis.dt(),
                          "analysis data", caller);
  result.wavelet_offset =
      checked_grid_offset(wavelet.t0(), grid_t0, analysis.dt(), "wavelet",
                          caller);
  const auto validate_endpoint = [&](const BasicTimeSeries &series,
                                     const int offset, const string &label) {
    const long double endpoint_index =
        (static_cast<long double>(series.endtime()) -
         static_cast<long double>(grid_t0)) /
        static_cast<long double>(analysis.dt());
    const long double expected_index =
        static_cast<long double>(offset) +
        static_cast<long double>(series.npts()) - 1.0L;
    if (!std::isfinite(endpoint_index) ||
        std::fabs(endpoint_index - expected_index) > 1.0e-3L)
      throw MsPASSError(caller + ": " + label + " endpoint is not aligned "
                                 "to the common analysis sample grid",
                        ErrorSeverity::Invalid);
  };
  validate_endpoint(analysis, result.analysis_offset, "analysis data");
  validate_endpoint(wavelet, result.wavelet_offset, "wavelet");
  if (result.analysis_offset + static_cast<long long>(analysis.npts()) >
          result.npts ||
      result.wavelet_offset + static_cast<long long>(wavelet.npts()) >
          result.npts)
    throw MsPASSError(caller + ": common grid does not contain both input "
                               "series",
                      ErrorSeverity::Invalid);
  return result;
}

bool GIDLagWeightPenaltyUsesDynamicKernel(const string &penalty_type) {
  return (penalty_type == "resolution_kernel") ||
         (penalty_type == "shaping_wavelet") ||
         GIDLagWeightPenaltyUsesAdaptiveMemory(penalty_type);
}

bool GIDLagWeightPenaltyUsesAdaptiveMemory(const string &penalty_type) {
  return penalty_type == "adaptive_memory";
}

namespace {
vector<double> kernel_coherence(const vector<double> &kernel,
                                const string &penalty_type,
                                const string &base_error) {
  if (kernel.empty())
    throw MsPASSError(base_error + penalty_type + " penalty kernel is empty",
                      ErrorSeverity::Invalid);
  double energy(0.0);
  for (auto x : kernel)
    energy += x * x;
  if (energy <= 0.0 || !std::isfinite(energy))
    throw MsPASSError(base_error + penalty_type +
                          " penalty kernel has zero or invalid energy",
                      ErrorSeverity::Invalid);

  const int max_radius = static_cast<int>(kernel.size()) - 1;
  vector<double> coherence(2 * max_radius + 1, 0.0);
  for (int delta = -max_radius; delta <= max_radius; ++delta) {
    double overlap(0.0);
    for (int i = 0; i < static_cast<int>(kernel.size()); ++i) {
      const int j = i + delta;
      if (j < 0 || j >= static_cast<int>(kernel.size()))
        continue;
      overlap += kernel[i] * kernel[j];
    }
    coherence[delta + max_radius] = fabs(overlap) / energy;
  }
  return coherence;
}

int coherence_radius(const vector<double> &coherence,
                     const double coherence_floor) {
  if (coherence.empty())
    return 0;
  const int max_radius = (static_cast<int>(coherence.size()) - 1) / 2;
  int left_radius(0), right_radius(0);
  while ((left_radius + 1) <= max_radius &&
         coherence[max_radius - left_radius - 1] >= coherence_floor)
    ++left_radius;
  while ((right_radius + 1) <= max_radius &&
         coherence[max_radius + right_radius + 1] >= coherence_floor)
    ++right_radius;
  return max(left_radius, right_radius);
}

int fwhm_radius(const vector<double> &coherence) {
  return coherence_radius(coherence, 0.5);
}
} // namespace

double EstimateThreeCColumnAmplitudeRMS(const CoreSeismogram &d) {
  if (d.dead() || d.npts() <= 0)
    return 0.0;
  const int npts = static_cast<int>(d.npts());
  double sumsq(0.0);
  for (int i = 0; i < npts; ++i) {
    for (int k = 0; k < 3; ++k)
      sumsq += d.u(k, i) * d.u(k, i);
  }
  return sqrt(sumsq / static_cast<double>(npts));
}

int SelectNoiseSignificantGIDCandidateIndex(
    const vector<double> &raw_amplitudes, const vector<double> &lag_weights,
    const double threshold) {
  if (raw_amplitudes.size() != lag_weights.size())
    throw MsPASSError("SelectNoiseSignificantGIDCandidateIndex: raw amplitude "
                      "and lag-weight vectors have different sizes",
                      ErrorSeverity::Invalid);
  if (!isfinite(threshold) || threshold <= 0.0)
    throw MsPASSError("SelectNoiseSignificantGIDCandidateIndex: threshold must "
                      "be finite and positive",
                      ErrorSeverity::Invalid);
  int selected(-1);
  double best_score(-1.0);
  for (size_t i = 0; i < raw_amplitudes.size(); ++i) {
    if (!isfinite(raw_amplitudes[i]) || !isfinite(lag_weights[i]) ||
        lag_weights[i] <= 0.0 || raw_amplitudes[i] < threshold)
      continue;
    const double score = raw_amplitudes[i] * lag_weights[i];
    if (score > best_score) {
      best_score = score;
      selected = static_cast<int>(i);
    }
  }
  return selected;
}

vector<int> OrderedNoiseSignificantGIDCandidates(
    const dmatrix &residual, const vector<double> &lag_weights,
    const vector<int> &active_lags, const double threshold) {
  const int n = residual.columns();
  vector<char> excluded(n, false);
  for (const int lag : active_lags)
    if (lag >= 0 && lag < n)
      excluded[lag] = true;
  vector<pair<double, int>> scored;
  scored.reserve(n);
  for (int j = 0; j < n; ++j) {
    if (excluded[j] || j >= static_cast<int>(lag_weights.size()) ||
        lag_weights[j] <= 0.0)
      continue;
    double amplitude_squared(0.0);
    for (int k = 0; k < min(3, static_cast<int>(residual.rows())); ++k)
      amplitude_squared += residual(k, j) * residual(k, j);
    const double amplitude = sqrt(max(0.0, amplitude_squared));
    if (isfinite(amplitude) && amplitude >= threshold)
      scored.emplace_back(amplitude * lag_weights[j], j);
  }
  stable_sort(scored.begin(), scored.end(),
              [](const auto &lhs, const auto &rhs) {
                return lhs.first > rhs.first;
              });
  vector<int> result;
  result.reserve(scored.size());
  for (const auto &candidate : scored)
    result.push_back(candidate.second);
  return result;
}

vector<double> BuildGIDLagWeightPenaltyFunctionFromKernel(
    const string &penalty_type, const double penalty_scale,
    const vector<double> &kernel, const int zero_lag_sample,
    const string &caller) {
  const string base_error(caller + ": ");
  if (!GIDLagWeightPenaltyUsesDynamicKernel(penalty_type))
    throw MsPASSError(base_error + "kernel-derived penalty requested for "
                                   "non-kernel penalty function=" +
                          penalty_type,
                      ErrorSeverity::Fatal);
  if (!std::isfinite(penalty_scale) || penalty_scale <= 0.0 ||
      penalty_scale > 1.0)
    throw MsPASSError(base_error +
                          "lag_weight_penalty_scale_factor must be in (0, 1]",
                      ErrorSeverity::Fatal);
  if (kernel.empty())
    throw MsPASSError(base_error + penalty_type + " penalty kernel is empty",
                      ErrorSeverity::Invalid);
  if (zero_lag_sample < 0 ||
      zero_lag_sample >= static_cast<int>(kernel.size()))
    throw MsPASSError(base_error + penalty_type +
                          " zero-lag sample is outside the penalty kernel",
                      ErrorSeverity::Invalid);

  vector<double> coherence(kernel_coherence(kernel, penalty_type, base_error));
  const int max_radius = static_cast<int>(kernel.size()) - 1;
  const int radius = fwhm_radius(coherence);

  vector<double> penalty;
  penalty.reserve(2 * radius + 1);
  for (int delta = -radius; delta <= radius; ++delta) {
    const double c = coherence[delta + max_radius];
    const double coherence_weight = c * c;
    double weight =
        1.0 - penalty_scale * coherence_weight;
    if (weight < 0.0)
      weight = 0.0;
    else if (weight > 1.0)
      weight = 1.0;
    penalty.push_back(weight);
  }
  return penalty;
}

GIDAdaptivePenaltyMetrics ApplyGIDAdaptiveMemoryPenalty(
    vector<double> &lag_weights, vector<double> &memory,
    vector<double> &retention, const vector<double> &kernel,
    const int zero_lag_sample, const int center_col,
    const double penalty_scale, const double candidate_amplitude,
    const double noise_amplitude, const string &caller) {
  GIDAdaptivePenaltyMetrics metrics;
  if (lag_weights.empty())
    return metrics;

  const string base_error(caller + ": ");
  if (!std::isfinite(penalty_scale) || penalty_scale <= 0.0 ||
      penalty_scale > 1.0)
    throw MsPASSError(base_error +
                          "lag_weight_penalty_scale_factor must be in (0, 1]",
                      ErrorSeverity::Fatal);
  if (zero_lag_sample < 0 ||
      zero_lag_sample >= static_cast<int>(kernel.size()))
    throw MsPASSError(base_error +
                          "adaptive penalty zero-lag sample is outside the "
                          "penalty kernel",
                      ErrorSeverity::Invalid);

  if (memory.size() != lag_weights.size())
    memory.assign(lag_weights.size(), 0.0);
  if (retention.size() != lag_weights.size())
    retention.assign(lag_weights.size(), 0.0);

  int valid_lags(0);
  for (auto w : lag_weights) {
    if (std::isfinite(w) && w > 0.0)
      ++valid_lags;
  }

  const double noise_floor =
      (std::isfinite(noise_amplitude) && noise_amplitude > 0.0)
          ? noise_amplitude
          : numeric_limits<double>::epsilon();
  metrics.noise_amplitude = noise_floor;
  double z(0.0);
  if (std::isfinite(candidate_amplitude) && candidate_amplitude > 0.0) {
    z = candidate_amplitude / noise_floor;
  }
  vector<double> coherence(
      kernel_coherence(kernel, "adaptive_memory", base_error));
  const int max_radius = static_cast<int>(kernel.size()) - 1;
  const double z2 = z * z;
  /* GID selects the maximum over all currently valid lags.  Even pure noise
   * therefore produces candidate amplitudes well above the RMS noise level.
   * The confidence must compare the accepted peak with a full-search
   * noise-only bound, not with an arbitrary single-lag sample.  Use the
   * Laurent-Massart chi-square tail bound with x=2*log(Nvalid) for a
   * three-component vector, normalized by the vector RMS used for noise_floor.
   * The extra log(Nvalid) factor controls false memory over repeated searches
   * without adding a user-tuned threshold. */
  const double search_log =
      2.0 * log(max(1.0, static_cast<double>(valid_lags)));
  const double search_energy =
      max(1.0, 1.0 + 2.0 * sqrt(search_log / 3.0) +
                         (2.0 / 3.0) * search_log);
  const double selection_adjusted_z2 = (z2 > 0.0) ? z2 / search_energy : 0.0;
  double confidence(0.0);
  if (selection_adjusted_z2 > 1.0) {
    confidence = std::isfinite(selection_adjusted_z2)
                     ? 1.0 - 1.0 / selection_adjusted_z2
                     : nextafter(1.0, 0.0);
  }
  if (!std::isfinite(confidence))
    confidence = 0.0;
  confidence = max(0.0, min(nextafter(1.0, 0.0), confidence));

  double coherence_energy_floor(0.25);
  if (confidence > 0.0)
    coherence_energy_floor = max(0.25, min(0.5, confidence));
  double coherence_floor = sqrt(coherence_energy_floor);
  coherence_floor =
      max(0.0, min(nextafter(1.0, 0.0), coherence_floor));
  const int radius = coherence_radius(coherence, coherence_floor);
  vector<pair<int, double>> footprint;
  footprint.reserve(2 * radius + 1);
  double footprint_energy_sum(0.0), footprint_energy_sumsq(0.0);
  for (int delta = -radius; delta <= radius; ++delta) {
    const int j = center_col + delta;
    if (j < 0 || j >= static_cast<int>(lag_weights.size()) ||
        lag_weights[j] <= 0.0)
      continue;
    const double c = coherence[delta + max_radius];
    const double coherence_weight = c * c;
    if (!std::isfinite(coherence_weight) || coherence_weight <= 0.0)
      continue;
    footprint.push_back(pair<int, double>(j, coherence_weight));
    footprint_energy_sum += coherence_weight;
    footprint_energy_sumsq += coherence_weight * coherence_weight;
  }
  metrics.effective_width = static_cast<int>(footprint.size());

  double specificity(0.0);
  if (valid_lags > 1 && footprint_energy_sum > 0.0 &&
      footprint_energy_sumsq > 0.0) {
    const double n_effective =
        max(1.0, (footprint_energy_sum * footprint_energy_sum) /
                     footprint_energy_sumsq);
    specificity = 1.0 - log(n_effective) / log(static_cast<double>(valid_lags));
  }
  specificity = max(0.0, min(nextafter(1.0, 0.0), specificity));
  const double immediate_strength = confidence;
  const double retention_strength = confidence * specificity;
  metrics.confidence = confidence;
  metrics.immediate_strength = immediate_strength;
  metrics.specificity = specificity;
  metrics.decay_factor = retention_strength;

  const double weight_floor = numeric_limits<double>::min();
  for (int j = 0; j < static_cast<int>(lag_weights.size()); ++j) {
    if (!std::isfinite(lag_weights[j]) || lag_weights[j] <= 0.0) {
      lag_weights[j] = 0.0;
      memory[j] = 0.0;
      retention[j] = 0.0;
      continue;
    }
    const double rho = max(0.0, min(nextafter(1.0, 0.0), retention[j]));
    memory[j] *= rho;
    retention[j] = (memory[j] > 0.0) ? rho : 0.0;
  }

  for (auto const &penalty_sample : footprint) {
    const int j = penalty_sample.first;
    const double coherence_weight = penalty_sample.second;
    double w = 1.0 - penalty_scale * immediate_strength * coherence_weight;
    w = max(weight_floor, min(1.0, w));
    const double old_memory = memory[j];
    const double added_memory = -log(w);
    const double updated_memory = old_memory + added_memory;
    memory[j] = updated_memory;
    retention[j] =
        (updated_memory > 0.0)
            ? (old_memory * retention[j] + added_memory * retention_strength) /
                  updated_memory
            : 0.0;
  }

  double sumsq(0.0), linf(0.0);
  for (int j = 0; j < static_cast<int>(lag_weights.size()); ++j) {
    if (lag_weights[j] <= 0.0) {
      lag_weights[j] = 0.0;
      continue;
    }
    const double updated_weight = exp(-memory[j]);
    lag_weights[j] =
        (std::isfinite(updated_weight) && updated_weight >= weight_floor)
            ? updated_weight
            : weight_floor;
    linf = max(linf, memory[j]);
    sumsq += memory[j] * memory[j];
  }
  metrics.memory_linf = linf;
  metrics.memory_l2 = sqrt(sumsq);
  return metrics;
}

vector<double> BuildGIDLagWeightPenaltyFunction(const Metadata &md,
                                                const string &caller) {
  const string base_error(caller + ": ");
  if (!md.is_defined("lag_weight_penalty_function"))
    throw MsPASSError(base_error +
                          "missing required parameter "
                          "lag_weight_penalty_function",
                      ErrorSeverity::Fatal);
  const string penalty_type = md.get_string("lag_weight_penalty_function");
  if (penalty_type == "none")
    return vector<double>{1.0};

  const double penalty_scale =
      md.is_defined("lag_weight_penalty_scale_factor")
          ? GetDoubleRequired(md, "lag_weight_penalty_scale_factor")
          : 1.0;
  if (!std::isfinite(penalty_scale) || penalty_scale <= 0.0 ||
      penalty_scale > 1.0)
    throw MsPASSError(base_error +
                          "lag_weight_penalty_scale_factor must be in (0, 1]",
                      ErrorSeverity::Fatal);

  if (!md.is_defined("lag_weight_function_width"))
    throw MsPASSError(base_error +
                          "missing required parameter "
                          "lag_weight_function_width",
                      ErrorSeverity::Fatal);
  int npenalty = GetIntRequired(md, "lag_weight_function_width");
  if (npenalty <= 0)
    throw MsPASSError(base_error + "lag_weight_function_width must be positive",
                      ErrorSeverity::Fatal);
  if ((npenalty % 2) == 0)
    ++npenalty;

  vector<double> penalty;
  penalty.reserve(npenalty);
  if (penalty_type == "boxcar") {
    const double weight = max(0.0, 1.0 - penalty_scale);
    for (int i = 0; i < npenalty; ++i)
      penalty.push_back(weight);
  } else if (penalty_type == "cosine_taper") {
    const double period = static_cast<double>(npenalty + 1);
    const double pi = acos(-1.0);
    for (int i = 0; i < npenalty; ++i) {
      double taper = 0.5 * (-cos(2.0 * pi *
                                  (static_cast<double>(i + 1)) / period));
      taper += 0.5;
      double weight = 1.0 - penalty_scale * taper;
      if (weight < 0.0)
        weight = 0.0;
      if (weight > 1.0)
        weight = 1.0;
      penalty.push_back(weight);
    }
  } else if (GIDLagWeightPenaltyUsesDynamicKernel(penalty_type)) {
    throw MsPASSError(
        base_error +
            "lag_weight_penalty_function=" + penalty_type +
            " requires a kernel context.  Use the kernel-aware helper.",
        ErrorSeverity::Fatal);
  } else {
    throw MsPASSError(base_error +
                          "illegal lag_weight_penalty_function=" +
                          penalty_type,
                      ErrorSeverity::Fatal);
  }
  return penalty;
}

void ApplyGIDLagWeightPenalty(vector<double> &lag_weights,
                              const vector<double> &penalty,
                              const int center_col) {
  if (lag_weights.empty() || penalty.empty())
    return;
  const int npenalty = static_cast<int>(penalty.size());
  const int first_col = center_col - npenalty / 2;
  for (int i = 0, j = first_col; i < npenalty; ++i, ++j) {
    if (j < 0 || j >= static_cast<int>(lag_weights.size()))
      continue;
    lag_weights[j] *= penalty[i];
    if (lag_weights[j] < 0.0)
      lag_weights[j] = 0.0;
    else if (lag_weights[j] > 1.0)
      lag_weights[j] = 1.0;
  }
}

TimeWindow ClipTimeWindowToSeries(const CoreTimeSeries &d,
                                  const TimeWindow &requested,
                                  const string &caller) {
  if (d.dead() || d.npts() <= 0)
    throw MsPASSError(caller + ": cannot clip a window to a dead or empty "
                                  "time series",
                      ErrorSeverity::Invalid);
  if (!std::isfinite(requested.start) || !std::isfinite(requested.end) ||
      requested.end <= requested.start)
    throw MsPASSError(caller + ": requested clip window is invalid",
                      ErrorSeverity::Invalid);
  const double clipped_start = max(requested.start, d.t0());
  const double clipped_end = min(requested.end, d.endtime());
  if (clipped_end <= clipped_start)
    throw MsPASSError(caller + ": requested clip window does not overlap "
                                  "time series",
                      ErrorSeverity::Invalid);
  return TimeWindow(clipped_start, clipped_end);
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

namespace {
double symmetric_condition_number(vector<vector<double>> a) {
  const int n = static_cast<int>(a.size());
  if (n <= 1)
    return 1.0;
  /* DSYEV is O(n^3), unlike a maximum-off-diagonal Jacobi iteration whose
   * repeated O(n^2) scans become prohibitive for long-window supports. */
  vector<double> packed(n * n, 0.0), eigenvalues(n, 0.0);
  for (int row = 0; row < n; ++row)
    for (int col = 0; col < n; ++col)
      packed[col * n + row] = a[row][col];
  char jobz = 'N', uplo = 'L';
  int n_lapack = n, lda = n, lwork = -1, info = 0;
  double workspace_query(0.0);
  dsyev(&jobz, &uplo, n_lapack, packed.data(), lda, eigenvalues.data(),
        &workspace_query, lwork, info);
  if (info != 0 || !isfinite(workspace_query) || workspace_query < 1.0)
    return numeric_limits<double>::infinity();
  lwork = max(3 * n - 1, static_cast<int>(ceil(workspace_query)));
  vector<double> workspace(lwork, 0.0);
  n_lapack = n;
  info = 0;
  dsyev(&jobz, &uplo, n_lapack, packed.data(), lda, eigenvalues.data(),
        workspace.data(), lwork, info);
  if (info != 0)
    return numeric_limits<double>::infinity();
  double largest(0.0), smallest(numeric_limits<double>::infinity());
  for (const auto eigenvalue : eigenvalues) {
    largest = max(largest, fabs(eigenvalue));
    smallest = min(smallest, fabs(eigenvalue));
  }
  if (!(largest > 0.0) || !(smallest > largest * 1.0e-12))
    return numeric_limits<double>::infinity();
  return largest / smallest;
}

double spike_residual_l2(const list<ThreeCSpike> &spikes,
                         const CoreSeismogram &target,
                         const vector<double> &fir, const int fir_zero) {
  vector<double> model(3 * target.npts(), 0.0);
  for (const auto &spk : spikes) {
    const int col0 = spk.col - fir_zero;
    for (int p = 0; p < static_cast<int>(fir.size()); ++p) {
      const int j = col0 + p;
      if (j >= 0 && j < target.npts())
        for (int k = 0; k < 3; ++k)
          model[k * target.npts() + j] += spk.u[k] * fir[p];
    }
  }
  long double sum(0.0);
  for (int k = 0; k < 3; ++k)
    for (int j = 0; j < target.npts(); ++j) {
      const long double d = target.u(k, j) - model[k * target.npts() + j];
      sum += d * d;
    }
  return isfinite(static_cast<double>(sum))
             ? sqrt(static_cast<double>(sum))
             : numeric_limits<double>::quiet_NaN();
}
} // namespace

void RefitSpikeAmplitudes(list<ThreeCSpike> &spikes,
                          const CoreSeismogram &target,
                          const vector<double> &actual_o_fir,
                          const int actual_o_0, const double ridge_beta,
                          SpikeRefitDiagnostics *diagnostics,
                          const double condition_limit,
                          const double condition_guard_relative_ridge) {
  const int nspikes = spikes.size();
  if (nspikes <= 0)
    return;
  const list<ThreeCSpike> pre_debias(spikes);
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
  /* Ordinary legacy/NS refits need only the ridge solve.  The eigensystem is
   * O(n^3) and is required solely for diagnostics or an explicit condition
   * guard request (group-sparse currently requests both). */
  const bool need_condition = diagnostics != nullptr || isfinite(condition_limit);
  const double condition = need_condition
                               ? symmetric_condition_number(gram)
                               : numeric_limits<double>::quiet_NaN();
  double maxdiag(0.0);
  for (int i = 0; i < nspikes; ++i)
    maxdiag = max(maxdiag, fabs(gram[i][i]));
  double relative_ridge = ridge_beta;
  const bool guarded = isfinite(condition_limit) && condition > condition_limit;
  if (guarded)
    relative_ridge = max(relative_ridge, condition_guard_relative_ridge);
  double damping = maxdiag * relative_ridge;
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
    spk.amp = three_component_norm(spk.u[0], spk.u[1], spk.u[2]);
  if (diagnostics != nullptr) {
    diagnostics->gram_condition_number = condition;
    diagnostics->relative_ridge_beta = relative_ridge;
    diagnostics->condition_guard_applied = guarded;
    diagnostics->residual_l2_pre =
        spike_residual_l2(pre_debias, target, actual_o_fir, actual_o_0);
    diagnostics->residual_l2_post =
        spike_residual_l2(spikes, target, actual_o_fir, actual_o_0);
    for (const auto &spk : pre_debias)
      diagnostics->maximum_amplitude_pre =
          max(diagnostics->maximum_amplitude_pre, spk.amp);
    for (const auto &spk : spikes)
      diagnostics->maximum_amplitude_post =
          max(diagnostics->maximum_amplitude_post, spk.amp);
    if (!isfinite(diagnostics->residual_l2_post) ||
        diagnostics->residual_l2_post > diagnostics->residual_l2_pre *
                                              (1.0 + 1.0e-10)) {
      spikes = pre_debias;
      diagnostics->fallback_to_pre_debias = true;
      diagnostics->fallback_reason = !isfinite(diagnostics->residual_l2_post)
                                         ? "nonfinite_refit"
                                         : "residual_increase";
      diagnostics->residual_l2_post = diagnostics->residual_l2_pre;
      diagnostics->maximum_amplitude_post =
          diagnostics->maximum_amplitude_pre;
    } else {
      diagnostics->fallback_reason = "none";
    }
  }
}

double VectorQuantile(vector<double> values, const double quantile) {
  if (values.empty())
    return 0.0;
  sort(values.begin(), values.end());
  const double q = min(1.0, max(0.0, quantile));
  const double pos = q * static_cast<double>(values.size() - 1);
  const int lo = static_cast<int>(floor(pos));
  const int hi = static_cast<int>(ceil(pos));
  if (lo == hi)
    return values[lo];
  const double frac = pos - static_cast<double>(lo);
  return values[lo] * (1.0 - frac) + values[hi] * frac;
}

GroupSparseDeconResult SolveGroupSparseDecon(
    const CoreSeismogram &target, const vector<double> &actual_o_fir,
    const int actual_o_0, const double lambda, const int max_iterations,
    const double tolerance, const double active_threshold,
    const double active_threshold_scale, const double active_threshold_quantile,
    const string &caller) {
  if (target.dead() || target.npts() <= 0)
    throw MsPASSError(caller + ": target data are dead or empty",
                      ErrorSeverity::Invalid);
  if (actual_o_fir.empty())
    throw MsPASSError(caller + ": actual output FIR kernel is empty",
                      ErrorSeverity::Invalid);
  if (!isfinite(lambda) || lambda < 0.0)
    throw MsPASSError(caller + ": group_sparse_lambda must be nonnegative",
                      ErrorSeverity::Fatal);
  ValidatePositiveInteger(max_iterations, "group_sparse_max_iterations",
                          caller);
  ValidatePositive(tolerance, "group_sparse_tolerance", caller);
  ValidateNonnegative(active_threshold, "group_sparse_active_threshold",
                      caller);
  ValidateNonnegative(active_threshold_scale,
                      "group_sparse_active_threshold_scale", caller);
  ValidateProbability(active_threshold_quantile,
                      "group_sparse_active_threshold_quantile", caller);

  const int npts = static_cast<int>(target.npts());
  const int ncoef = 3 * npts;
  const int nf = static_cast<int>(actual_o_fir.size());

  vector<char> valid(npts, false);
  for (int j = 0; j < npts; ++j) {
    /* A retained group must have the complete resolution response on both
     * sides of its physical (zero-lag) center.  actual_o_0 need not be at an
     * array endpoint: retaining it here preserves legal late arrivals while
     * rejecting the tail-only, clipped columns that otherwise leak a large
     * coefficient into the last samples. */
    const int col0 = j - actual_o_0;
    valid[j] = (col0 >= 0) && (col0 + nf <= npts);
  }

  double sumabs(0.0);
  for (auto x : actual_o_fir)
    sumabs += fabs(x);
  const double lipschitz = max(1.0e-12, sumabs * sumabs);
  const double step = 1.0 / lipschitz;

  vector<double> target_vec(ncoef, 0.0), x(ncoef, 0.0), xnew(ncoef, 0.0),
      model(ncoef, 0.0), residual(ncoef, 0.0), gradient(ncoef, 0.0);
  for (int j = 0; j < npts; ++j) {
    target_vec[j] = target.u(0, j);
    target_vec[npts + j] = target.u(1, j);
    target_vec[2 * npts + j] = target.u(2, j);
  }

  auto build_model = [&](const vector<double> &coef) {
    fill(model.begin(), model.end(), 0.0);
    const double *c0 = coef.data();
    const double *c1 = c0 + npts;
    const double *c2 = c1 + npts;
    double *m0 = model.data();
    double *m1 = m0 + npts;
    double *m2 = m1 + npts;
    for (int j = 0; j < npts; ++j) {
      if (!valid[j])
        continue;
      const int col0 = j - actual_o_0;
      const double a0 = c0[j];
      const double a1 = c1[j];
      const double a2 = c2[j];
      const int p_start = max(0, -col0);
      const int p_end = min(nf, npts - col0);
      for (int p = p_start; p < p_end; ++p) {
        const int sample = col0 + p;
        const double h = actual_o_fir[p];
        m0[sample] += h * a0;
        m1[sample] += h * a1;
        m2[sample] += h * a2;
      }
    }
    for (int i = 0; i < ncoef; ++i)
      residual[i] = model[i] - target_vec[i];
  };

  auto objective = [&](const vector<double> &coef) {
    build_model(coef);
    double rss(0.0), penalty(0.0);
    for (auto e : residual)
      rss += e * e;
    const double *c0 = coef.data();
    const double *c1 = c0 + npts;
    const double *c2 = c1 + npts;
    for (int j = 0; j < npts; ++j) {
      penalty += three_component_norm(c0[j], c1[j], c2[j]);
    }
    return 0.5 * rss + lambda * penalty;
  };

  GroupSparseDeconResult result;
  result.lambda = lambda;
  result.active_threshold_floor = active_threshold;
  result.active_threshold_scale = active_threshold_scale;
  result.active_threshold_quantile = active_threshold_quantile;
  result.objective_initial = objective(x);
  double prev_objective = result.objective_initial;
  for (int iter = 1; iter <= max_iterations; ++iter) {
    fill(gradient.begin(), gradient.end(), 0.0);
    double *g0 = gradient.data();
    double *g1 = g0 + npts;
    double *g2 = g1 + npts;
    const double *r0 = residual.data();
    const double *r1 = r0 + npts;
    const double *r2 = r1 + npts;
    for (int j = 0; j < npts; ++j) {
      if (!valid[j])
        continue;
      const int col0 = j - actual_o_0;
      double sum0(0.0), sum1(0.0), sum2(0.0);
      const int p_start = max(0, -col0);
      const int p_end = min(nf, npts - col0);
      for (int p = p_start; p < p_end; ++p) {
        const int sample = col0 + p;
        const double h = actual_o_fir[p];
        sum0 += h * r0[sample];
        sum1 += h * r1[sample];
        sum2 += h * r2[sample];
      }
      g0[j] = sum0;
      g1[j] = sum1;
      g2[j] = sum2;
    }

    const double shrink_threshold = lambda * step;
    fill(xnew.begin(), xnew.end(), 0.0);
    const double *x0 = x.data();
    const double *x1 = x0 + npts;
    const double *x2 = x1 + npts;
    const double *grad0 = gradient.data();
    const double *grad1 = grad0 + npts;
    const double *grad2 = grad1 + npts;
    double *xn0 = xnew.data();
    double *xn1 = xn0 + npts;
    double *xn2 = xn1 + npts;
    for (int j = 0; j < npts; ++j) {
      if (!valid[j])
        continue;
      const double z0 = x0[j] - step * grad0[j];
      const double z1 = x1[j] - step * grad1[j];
      const double z2 = x2[j] - step * grad2[j];
      const double znorm2 = z0 * z0 + z1 * z1 + z2 * z2;
      const double znorm = sqrt(znorm2);
      if (znorm <= shrink_threshold || znorm <= 0.0)
        continue;
      const double scale = 1.0 - shrink_threshold / znorm;
      xn0[j] = scale * z0;
      xn1[j] = scale * z1;
      xn2[j] = scale * z2;
    }

    const double current_objective = objective(xnew);
    result.iterations = iter;
    result.fractional_improvement_final =
        (prev_objective - current_objective) / max(1.0, prev_objective);
    x.swap(xnew);
    if (fabs(prev_objective - current_objective) <=
        tolerance * max(1.0, prev_objective)) {
      result.converged = true;
      prev_objective = current_objective;
      break;
    }
    prev_objective = current_objective;
  }

  vector<double> xnorm(npts, 0.0);
  vector<double> group_norms;
  group_norms.reserve(npts);
  const double *x0 = x.data();
  const double *x1 = x0 + npts;
  const double *x2 = x1 + npts;
  for (int j = 0; j < npts; ++j) {
    xnorm[j] = three_component_norm(x0[j], x1[j], x2[j]);
    if (valid[j])
      group_norms.push_back(xnorm[j]);
  }
  result.active_threshold_quantile_value =
      VectorQuantile(std::move(group_norms), active_threshold_quantile);
  result.active_threshold_used =
      max(active_threshold,
          active_threshold_scale * result.active_threshold_quantile_value);
  vector<double> xactive(ncoef, 0.0);
  double *xa0 = xactive.data();
  double *xa1 = xa0 + npts;
  double *xa2 = xa1 + npts;
  for (int j = 0; j < npts; ++j) {
    if (valid[j] && xnorm[j] > result.active_threshold_used) {
      result.spikes.emplace_back(j, x0[j], x1[j], x2[j]);
      xa0[j] = x0[j];
      xa1[j] = x1[j];
      xa2[j] = x2[j];
      ++result.active_groups;
    }
  }
  result.objective_final = objective(xactive);
  result.fractional_improvement_final =
      (result.objective_initial - result.objective_final) /
      max(1.0, result.objective_initial);
  result.residual = CoreSeismogram(target);
  const double *m0 = model.data();
  const double *m1 = m0 + npts;
  const double *m2 = m1 + npts;
  for (int j = 0; j < npts; ++j) {
    result.residual.u(0, j) = target_vec[j] - m0[j];
    result.residual.u(1, j) = target_vec[npts + j] - m1[j];
    result.residual.u(2, j) = target_vec[2 * npts + j] - m2[j];
  }
  return result;
}
} // namespace mspass::algorithms::deconvolution
