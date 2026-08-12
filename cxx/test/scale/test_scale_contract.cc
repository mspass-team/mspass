#include "mspass/algorithms/amplitudes.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <cmath>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

using mspass::algorithms::TimeWindow;
using mspass::algorithms::amplitudes::scale;
using mspass::algorithms::amplitudes::scale_ensemble;
using mspass::algorithms::amplitudes::ScalingMethod;
using mspass::seismic::Ensemble;
using mspass::seismic::Seismogram;
using mspass::seismic::TimeReferenceType;
using mspass::seismic::TimeSeries;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;

namespace {
void check(const bool condition, const std::string &message) {
  if (!condition)
    throw std::runtime_error(message);
}

bool same_number(const double lhs, const double rhs) {
  if (std::isnan(lhs) || std::isnan(rhs))
    return std::isnan(lhs) && std::isnan(rhs);
  if (std::isinf(lhs) || std::isinf(rhs))
    return lhs == rhs;
  return std::abs(lhs - rhs) <=
         1.0e-12 * std::max(1.0, std::max(std::abs(lhs), std::abs(rhs)));
}

TimeSeries make_timeseries(const std::vector<double> &samples,
                           const bool live = true) {
  TimeSeries result(samples.size());
  result.set_t0(0.0);
  result.set_dt(1.0);
  result.set_tref(TimeReferenceType::Relative);
  result.put("calib", 2.0);
  result.put("contract_marker", std::string("unchanged"));
  for (size_t i = 0; i < samples.size(); ++i)
    result.s[i] = samples[i];
  if (live)
    result.set_live();
  else
    result.kill();
  return result;
}

Seismogram make_seismogram(const std::vector<double> &samples,
                           const bool live = true) {
  Seismogram result(samples.size());
  result.set_t0(0.0);
  result.set_dt(1.0);
  result.set_tref(TimeReferenceType::Relative);
  result.put("calib", 2.0);
  result.put("contract_marker", std::string("unchanged"));
  for (size_t i = 0; i < samples.size(); ++i) {
    result.u(0, i) = samples[i];
    result.u(1, i) = 0.0;
    result.u(2, i) = 0.0;
  }
  if (live)
    result.set_live();
  else
    result.kill();
  return result;
}

void check_timeseries_equal(const TimeSeries &actual,
                            const TimeSeries &expected,
                            const std::string &context) {
  check(actual.live() == expected.live(), context + ": live state changed");
  check(actual.npts() == expected.npts(), context + ": npts changed");
  check(actual.elog.size() == expected.elog.size(), context + ": elog changed");
  check(actual.get_string("contract_marker") ==
            expected.get_string("contract_marker"),
        context + ": metadata changed");
  check(same_number(actual.get_double("calib"), expected.get_double("calib")),
        context + ": calib changed");
  for (size_t i = 0; i < actual.npts(); ++i)
    check(same_number(actual.s[i], expected.s[i]),
          context + ": sample changed");
}

void check_seismogram_equal(const Seismogram &actual,
                            const Seismogram &expected,
                            const std::string &context) {
  check(actual.live() == expected.live(), context + ": live state changed");
  check(actual.npts() == expected.npts(), context + ": npts changed");
  check(actual.elog.size() == expected.elog.size(), context + ": elog changed");
  check(actual.get_string("contract_marker") ==
            expected.get_string("contract_marker"),
        context + ": metadata changed");
  check(same_number(actual.get_double("calib"), expected.get_double("calib")),
        context + ": calib changed");
  for (size_t i = 0; i < actual.npts(); ++i)
    for (int component = 0; component < 3; ++component)
      check(same_number(actual.u(component, i), expected.u(component, i)),
            context + ": sample changed");
}

void expect_invalid_unchanged(TimeSeries &datum, const TimeWindow &window,
                              const std::string &context) {
  const TimeSeries before(datum);
  bool threw(false);
  try {
    scale(datum, ScalingMethod::Peak, 1.0, window);
  } catch (const MsPASSError &error) {
    threw = true;
    check(error.severity() == ErrorSeverity::Invalid,
          context + ": wrong exception severity");
  }
  check(threw, context + ": expected MsPASSError");
  check_timeseries_equal(datum, before, context);
}

void expect_invalid_unchanged(Seismogram &datum, const TimeWindow &window,
                              const std::string &context) {
  const Seismogram before(datum);
  bool threw(false);
  try {
    scale(datum, ScalingMethod::Peak, 1.0, window);
  } catch (const MsPASSError &error) {
    threw = true;
    check(error.severity() == ErrorSeverity::Invalid,
          context + ": wrong exception severity");
  }
  check(threw, context + ": expected MsPASSError");
  check_seismogram_equal(datum, before, context);
}

void test_atomic_windows() {
  TimeSeries interior(make_timeseries({100.0, 1.0, 2.0, 4.0, 3.0, 1.0}));
  const double interior_amplitude =
      scale(interior, ScalingMethod::Peak, 2.0, TimeWindow(2.0, 4.0));
  check(same_number(interior_amplitude, 4.0),
        "interior window used an exterior peak");
  check(same_number(interior.s[0], 50.0), "interior gain was not applied");
  check(same_number(interior.get_double("calib"), 4.0),
        "interior calib is wrong");

  TimeSeries clipped_left(make_timeseries({1.0, 2.0, 3.0, 4.0}));
  check(same_number(scale(clipped_left, ScalingMethod::Peak, 1.0,
                          TimeWindow(-5.0, 2.0)),
                    3.0),
        "left-clipped window is wrong");
  TimeSeries clipped_right(make_timeseries({1.0, 2.0, 3.0, 4.0}));
  check(same_number(scale(clipped_right, ScalingMethod::Peak, 1.0,
                          TimeWindow(2.0, 10.0)),
                    4.0),
        "right-clipped window is wrong");

  TimeSeries reversed(make_timeseries({100.0, 1.0, 2.0, 4.0}));
  check(same_number(
            scale(reversed, ScalingMethod::Peak, 1.0, TimeWindow(3.0, 2.0)),
            100.0),
        "reversed window did not select the full record");

  TimeSeries disjoint_ts(make_timeseries({1.0, 2.0, 3.0, 4.0}));
  expect_invalid_unchanged(disjoint_ts, TimeWindow(10.0, 12.0),
                           "TimeSeries disjoint window");
  TimeSeries zero_width_ts(make_timeseries({1.0, 2.0, 3.0, 4.0}));
  expect_invalid_unchanged(zero_width_ts, TimeWindow(2.0, 2.0),
                           "TimeSeries zero-width window");
  TimeSeries boundary_touch_ts(make_timeseries({1.0, 2.0, 3.0, 4.0}));
  expect_invalid_unchanged(boundary_touch_ts, TimeWindow(-2.0, 0.0),
                           "TimeSeries zero-width clipped intersection");

  Seismogram interior_seis(make_seismogram({100.0, 1.0, 2.0, 4.0, 3.0, 1.0}));
  check(same_number(scale(interior_seis, ScalingMethod::Peak, 2.0,
                          TimeWindow(2.0, 4.0)),
                    4.0),
        "Seismogram interior window used an exterior peak");
  check(same_number(interior_seis.u(0, 0), 50.0),
        "Seismogram common gain was not applied");
  check(same_number(interior_seis.get_double("calib"), 4.0),
        "Seismogram calib is wrong");
  Seismogram disjoint_seis(make_seismogram({1.0, 2.0, 3.0, 4.0}));
  expect_invalid_unchanged(disjoint_seis, TimeWindow(-4.0, -2.0),
                           "Seismogram disjoint window");
  Seismogram zero_width_seis(make_seismogram({1.0, 2.0, 3.0, 4.0}));
  expect_invalid_unchanged(zero_width_seis, TimeWindow(1.0, 1.0),
                           "Seismogram zero-width window");
  Seismogram boundary_touch_seis(make_seismogram({1.0, 2.0, 3.0, 4.0}));
  expect_invalid_unchanged(boundary_touch_seis, TimeWindow(3.0, 5.0),
                           "Seismogram zero-width clipped intersection");
}

template <typename Tdata>
Tdata make_member(const double amplitude, const bool live);

template <>
TimeSeries make_member<TimeSeries>(const double amplitude, const bool live) {
  return make_timeseries({amplitude}, live);
}

template <>
Seismogram make_member<Seismogram>(const double amplitude, const bool live) {
  return make_seismogram({amplitude}, live);
}

double first_sample(const TimeSeries &datum) { return datum.s[0]; }
double first_sample(const Seismogram &datum) { return datum.u(0, 0); }

template <typename Tdata>
void check_member_equal(const Tdata &actual, const Tdata &expected,
                        const std::string &context);

template <>
void check_member_equal<TimeSeries>(const TimeSeries &actual,
                                    const TimeSeries &expected,
                                    const std::string &context) {
  check_timeseries_equal(actual, expected, context);
}

template <>
void check_member_equal<Seismogram>(const Seismogram &actual,
                                    const Seismogram &expected,
                                    const std::string &context) {
  check_seismogram_equal(actual, expected, context);
}

template <typename Tdata>
void test_ensemble_statistic(const bool use_mean, const std::string &context) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();
  const std::vector<double> amplitudes{2.0, 4.0, 16.0, 128.0,
                                       0.0, nan, inf,  8.0};
  Ensemble<Tdata> ensemble;
  for (size_t i = 0; i < amplitudes.size(); ++i)
    ensemble.member.push_back(make_member<Tdata>(amplitudes[i], i != 7));
  const Tdata dead_before(ensemble.member.back());

  const double expected_amplitude = use_mean ? std::pow(16384.0, 0.25) : 8.0;
  const double returned =
      scale_ensemble(ensemble, ScalingMethod::Peak, 16.0, use_mean);
  check(same_number(returned, expected_amplitude),
        context + ": geometric statistic is wrong");
  const double gain = 16.0 / expected_amplitude;
  for (size_t i = 0; i < amplitudes.size() - 1; ++i) {
    check(same_number(first_sample(ensemble.member[i]), amplitudes[i] * gain),
          context + ": common gain was not applied to every live member");
    check(same_number(ensemble.member[i].get_double("calib"), 2.0 / gain),
          context + ": calib update is wrong");
  }
  check_member_equal(ensemble.member.back(), dead_before,
                     context + ": dead member changed");
}

template <typename Tdata>
void test_ensemble_no_eligible(const std::string &context) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();
  Ensemble<Tdata> ensemble;
  ensemble.member.push_back(make_member<Tdata>(0.0, true));
  ensemble.member.push_back(make_member<Tdata>(nan, true));
  ensemble.member.push_back(make_member<Tdata>(inf, true));
  ensemble.member.push_back(make_member<Tdata>(8.0, false));
  const std::vector<Tdata> before(ensemble.member);

  const double returned =
      scale_ensemble(ensemble, ScalingMethod::Peak, 8.0, true);
  check(returned == 0.0, context + ": no-eligible return is not zero");
  for (size_t i = 0; i < ensemble.member.size(); ++i)
    check_member_equal(ensemble.member[i], before[i],
                       context + ": no-eligible member changed");
}

template <typename Tdata>
void test_single_eligible(const bool use_mean, const std::string &context) {
  Ensemble<Tdata> ensemble;
  ensemble.member.push_back(make_member<Tdata>(4.0, true));
  ensemble.member.push_back(make_member<Tdata>(0.0, true));
  const double returned =
      scale_ensemble(ensemble, ScalingMethod::Peak, 8.0, use_mean);
  check(same_number(returned, 4.0), context + ": single statistic is wrong");
  check(same_number(first_sample(ensemble.member[0]), 8.0),
        context + ": eligible member gain is wrong");
  check(same_number(first_sample(ensemble.member[1]), 0.0),
        context + ": excluded member was not scaled consistently");
  check(same_number(ensemble.member[0].get_double("calib"), 1.0),
        context + ": eligible member calib is wrong");
  check(same_number(ensemble.member[1].get_double("calib"), 1.0),
        context + ": excluded member calib is wrong");
}
} // namespace

int main() {
  try {
    test_atomic_windows();
    test_ensemble_statistic<TimeSeries>(true, "TimeSeries mean");
    test_ensemble_statistic<TimeSeries>(false, "TimeSeries median");
    test_ensemble_statistic<Seismogram>(true, "Seismogram mean");
    test_ensemble_statistic<Seismogram>(false, "Seismogram median");
    test_ensemble_no_eligible<TimeSeries>("TimeSeries no eligible");
    test_ensemble_no_eligible<Seismogram>("Seismogram no eligible");
    test_single_eligible<TimeSeries>(true, "TimeSeries single eligible mean");
    test_single_eligible<TimeSeries>(false,
                                     "TimeSeries single eligible median");
    test_single_eligible<Seismogram>(true, "Seismogram single eligible mean");
    test_single_eligible<Seismogram>(false,
                                     "Seismogram single eligible median");
  } catch (const std::exception &error) {
    std::cerr << "test_scale_contract failed: " << error.what() << std::endl;
    return 1;
  }
  return 0;
}
