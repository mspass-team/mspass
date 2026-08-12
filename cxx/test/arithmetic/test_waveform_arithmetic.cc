#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

using mspass::seismic::Seismogram;
using mspass::seismic::TimeReferenceType;
using mspass::seismic::TimeSeries;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;

namespace {
int failure_count{0};

void check(const bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << message << '\n';
    ++failure_count;
  }
}

template <class Waveform> struct Samples;

template <> struct Samples<TimeSeries> {
  static constexpr std::size_t component_count{1};
  static double get(const TimeSeries &data, const std::size_t,
                    const std::size_t sample) {
    return data.s[sample];
  }
  static void set(TimeSeries &data, const std::size_t, const std::size_t sample,
                  const double value) {
    data.s[sample] = value;
  }
};

template <> struct Samples<Seismogram> {
  static constexpr std::size_t component_count{3};
  static double get(const Seismogram &data, const std::size_t component,
                    const std::size_t sample) {
    return data.u(component, sample);
  }
  static void set(Seismogram &data, const std::size_t component,
                  const std::size_t sample, const double value) {
    data.u(component, sample) = value;
  }
};

template <class Waveform>
Waveform make_waveform(const std::size_t sample_count, const double t0,
                       const double dt, const double base) {
  Waveform data(sample_count);
  data.set_t0(t0);
  data.set_dt(dt);
  data.set_tref(TimeReferenceType::Relative);
  data.set_live();
  data.put("sentinel", std::string("unchanged"));
  for (std::size_t component = 0;
       component < Samples<Waveform>::component_count; ++component)
    for (std::size_t sample = 0; sample < sample_count; ++sample)
      Samples<Waveform>::set(data, component, sample,
                             base + 10.0 * static_cast<double>(component) +
                                 static_cast<double>(sample));
  return data;
}

template <class Waveform> struct Snapshot {
  std::size_t sample_count;
  double t0;
  double dt;
  TimeReferenceType time_reference;
  bool live;
  int error_count;
  std::string sentinel;
  std::vector<double> samples;
};

template <class Waveform> Snapshot<Waveform> snapshot(const Waveform &data) {
  Snapshot<Waveform> result{data.npts(),
                            data.t0(),
                            data.dt(),
                            data.timetype(),
                            data.live(),
                            data.elog.size(),
                            data.get_string("sentinel"),
                            {}};
  result.samples.reserve(Samples<Waveform>::component_count * data.npts());
  for (std::size_t component = 0;
       component < Samples<Waveform>::component_count; ++component)
    for (std::size_t sample = 0; sample < data.npts(); ++sample)
      result.samples.push_back(Samples<Waveform>::get(data, component, sample));
  return result;
}

template <class Waveform>
void check_state(const Waveform &actual, const Snapshot<Waveform> &expected,
                 const std::string &context) {
  check(actual.npts() == expected.sample_count, context + ": npts changed");
  check(actual.t0() == expected.t0, context + ": t0 changed");
  check(actual.dt() == expected.dt, context + ": dt changed");
  check(actual.timetype() == expected.time_reference,
        context + ": time reference changed");
  check(actual.live() == expected.live, context + ": live state changed");
  check(actual.elog.size() == expected.error_count,
        context + ": error log changed");
  check(actual.get_string("sentinel") == expected.sentinel,
        context + ": metadata changed");
  std::size_t index{0};
  for (std::size_t component = 0;
       component < Samples<Waveform>::component_count; ++component)
    for (std::size_t sample = 0; sample < actual.npts(); ++sample, ++index)
      check(Samples<Waveform>::get(actual, component, sample) ==
                expected.samples[index],
            context + ": sample changed at component " +
                std::to_string(component) + ", index " +
                std::to_string(sample));
}

template <class Waveform>
void combine(Waveform &lhs, const Waveform &rhs, const bool add) {
  if (add)
    lhs += rhs;
  else
    lhs -= rhs;
}

template <class Waveform>
void verify_valid(const bool add, const double rhs_t0, const double rhs_dt,
                  const int expected_offset, const std::string &context,
                  const double lhs_dt = 1.0) {
  Waveform lhs = make_waveform<Waveform>(5, 0.0, lhs_dt, 100.0);
  const Waveform rhs = make_waveform<Waveform>(4, rhs_t0, rhs_dt, 10.0);
  Waveform expected(lhs);
  for (std::size_t rhs_index = 0; rhs_index < rhs.npts(); ++rhs_index) {
    const int lhs_index = expected_offset + static_cast<int>(rhs_index);
    if (lhs_index < 0 || lhs_index >= static_cast<int>(lhs.npts()))
      continue;
    for (std::size_t component = 0;
         component < Samples<Waveform>::component_count; ++component) {
      const double value = Samples<Waveform>::get(lhs, component, lhs_index) +
                           (add ? 1.0 : -1.0) * Samples<Waveform>::get(
                                                    rhs, component, rhs_index);
      Samples<Waveform>::set(expected, component, lhs_index, value);
    }
  }

  combine(lhs, rhs, add);
  check_state(lhs, snapshot(expected), context);
}

template <class Waveform>
void verify_rejected(const bool add, const double rhs_t0, const double rhs_dt,
                     const std::string &context, const double lhs_dt = 1.0) {
  Waveform lhs = make_waveform<Waveform>(5, 0.0, lhs_dt, 100.0);
  Waveform rhs = make_waveform<Waveform>(4, rhs_t0, rhs_dt, 10.0);
  rhs.elog.log_error("rhs", "must not merge on rejection",
                     ErrorSeverity::Complaint);
  const auto before = snapshot(lhs);
  bool threw_invalid{false};
  try {
    combine(lhs, rhs, add);
  } catch (const MsPASSError &error) {
    threw_invalid = error.severity() == ErrorSeverity::Invalid;
  }
  check(threw_invalid, context + ": did not throw MsPASSError Invalid");
  check_state(lhs, before, context + " rejection atomicity");
}

template <class Waveform>
void run_suite(const bool add, const std::string &type_name) {
  const std::string operation = add ? "+=" : "-=";
  const std::string prefix = type_name + " " + operation;

  verify_valid<Waveform>(add, 0.0, 1.0, 0, prefix + " equal grid");
  verify_valid<Waveform>(add, 2.0, 1.0, 2, prefix + " positive offset");
  verify_valid<Waveform>(add, -2.0, 1.0, -2, prefix + " negative offset");
  verify_valid<Waveform>(add, 5.0, 1.0, 5, prefix + " positive no overlap");
  verify_valid<Waveform>(add, -4.0, 1.0, -4, prefix + " negative no overlap");

  constexpr double tolerance = 1.0e-6;
  const double positive_offset_at_tolerance = 1.0 + tolerance;
  const double positive_offset_beyond = std::nextafter(
      positive_offset_at_tolerance, std::numeric_limits<double>::infinity());
  const double negative_offset_at_tolerance = -1.0 - tolerance;
  const double negative_offset_beyond = std::nextafter(
      negative_offset_at_tolerance, -std::numeric_limits<double>::infinity());
  check(std::abs(positive_offset_at_tolerance -
                 std::round(positive_offset_at_tolerance)) <= tolerance,
        "positive offset boundary fixture is invalid");
  check(std::abs(positive_offset_beyond - std::round(positive_offset_beyond)) >
            tolerance,
        "positive offset rejection fixture is invalid");
  check(std::abs(negative_offset_at_tolerance -
                 std::round(negative_offset_at_tolerance)) <= tolerance,
        "negative offset boundary fixture is invalid");
  check(std::abs(negative_offset_beyond - std::round(negative_offset_beyond)) >
            tolerance,
        "negative offset rejection fixture is invalid");
  verify_valid<Waveform>(add, positive_offset_at_tolerance, 1.0, 1,
                         prefix + " positive offset at tolerance");
  verify_valid<Waveform>(add, negative_offset_at_tolerance, 1.0, -1,
                         prefix + " negative offset at tolerance");
  verify_rejected<Waveform>(add, positive_offset_beyond, 1.0,
                            prefix + " positive offset beyond tolerance");
  verify_rejected<Waveform>(add, negative_offset_beyond, 1.0,
                            prefix + " negative offset beyond tolerance");

  constexpr double lhs_dt = 1.0e6;
  constexpr double dt_at_tolerance = lhs_dt - 1.0;
  const double dt_beyond = std::nextafter(dt_at_tolerance, 0.0);
  check(std::abs(lhs_dt - dt_at_tolerance) ==
            tolerance * std::max(std::abs(lhs_dt), std::abs(dt_at_tolerance)),
        "dt boundary fixture is invalid");
  check(std::abs(lhs_dt - dt_beyond) >
            tolerance * std::max(std::abs(lhs_dt), std::abs(dt_beyond)),
        "dt rejection fixture is invalid");
  verify_valid<Waveform>(add, 0.0, dt_at_tolerance, 0,
                         prefix + " dt at relative tolerance", lhs_dt);
  verify_rejected<Waveform>(add, 0.0, dt_beyond,
                            prefix + " dt beyond relative tolerance", lhs_dt);
}
} // namespace

int main() {
  run_suite<TimeSeries>(true, "TimeSeries");
  run_suite<TimeSeries>(false, "TimeSeries");
  run_suite<Seismogram>(true, "Seismogram");
  run_suite<Seismogram>(false, "Seismogram");
  return failure_count == 0 ? 0 : 1;
}
