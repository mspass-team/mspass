#include "mspass/algorithms/algorithms.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

using mspass::algorithms::agc;
using mspass::seismic::Seismogram;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;

void check(const bool condition, const std::string &message) {
  if (!condition)
    throw std::runtime_error(message);
}

void check_close(const double actual, const double expected,
                 const std::string &message) {
  const double scale = std::max({1.0, std::abs(actual), std::abs(expected)});
  check(std::abs(actual - expected) <= 1.0e-12 * scale, message);
}

Seismogram make_data(const std::size_t sample_count, const double dt = 0.5,
                     const double t0 = 0.0) {
  Seismogram data(sample_count);
  data.set_dt(dt);
  data.set_t0(t0);
  data.set_live();
  return data;
}

std::size_t expected_half_window(const Seismogram &data, const double twin) {
  const double requested = std::floor(std::round(twin / data.dt()) / 2.0);
  return static_cast<std::size_t>(
      std::min(requested, static_cast<double>((data.npts() - 1) / 2)));
}

void verify_against_formula(Seismogram data, const double twin) {
  const Seismogram original(data);
  const auto gain_function = agc(data, twin);
  const std::size_t half_window = expected_half_window(original, twin);

  check(gain_function.live(), "gain function is not live");
  check(gain_function.npts() == original.npts(),
        "gain function sample count mismatch");
  check(gain_function.s.size() == original.npts(),
        "gain function vector size mismatch");
  check_close(gain_function.t0(), original.t0(),
              "gain function start time mismatch");
  check_close(gain_function.dt(), original.dt(),
              "gain function sample interval mismatch");

  for (std::size_t i = 0; i < original.npts(); ++i) {
    const std::size_t first = i > half_window ? i - half_window : 0;
    const std::size_t last = std::min(original.npts() - 1, i + half_window);
    const std::size_t window_sample_count = last - first + 1;
    double energy = 0.0;
    for (std::size_t j = first; j <= last; ++j)
      for (std::size_t component = 0; component < 3; ++component)
        energy += original.u(component, j) * original.u(component, j);
    const double expected_gain =
        energy > 0.0 ? 1.0 / std::sqrt(energy / (3.0 * window_sample_count))
                     : 0.0;
    check_close(gain_function.s[i], expected_gain, "gain sample mismatch");
    for (std::size_t component = 0; component < 3; ++component)
      check_close(data.u(component, i),
                  expected_gain * original.u(component, i),
                  "output sample mismatch");
  }
}

void verify_rejected_without_mutation(Seismogram data, const double twin) {
  const Seismogram original(data);
  bool threw_invalid = false;
  try {
    agc(data, twin);
  } catch (const MsPASSError &error) {
    check(error.severity() == ErrorSeverity::Invalid,
          "AGC rejection used the wrong severity");
    threw_invalid = true;
  }
  check(threw_invalid, "AGC invalid input did not throw MsPASSError");
  check(data.npts() == original.npts(), "rejection changed npts");
  check(data.live() == original.live(), "rejection changed live state");
  if (std::isnan(original.dt()))
    check(std::isnan(data.dt()), "rejection changed NaN dt");
  else
    check(data.dt() == original.dt(), "rejection changed dt");
  check(data.t0() == original.t0(), "rejection changed t0");
  check(data.elog.size() == original.elog.size(), "rejection changed elog");
  for (std::size_t i = 0; i < original.npts(); ++i)
    for (std::size_t component = 0; component < 3; ++component)
      check(data.u(component, i) == original.u(component, i),
            "rejection changed a data sample");
}

int main() {
  auto constant = make_data(7, 0.5, 12.25);
  for (std::size_t i = 0; i < constant.npts(); ++i)
    for (std::size_t component = 0; component < 3; ++component)
      constant.u(component, i) = 1.0;
  verify_against_formula(constant, 2.6);

  auto all_zero = make_data(6);
  verify_against_formula(all_zero, 1.5);

  for (const std::size_t impulse_position : {std::size_t{0}, std::size_t{3}}) {
    auto impulse = make_data(7);
    impulse.u(1, impulse_position) = 3.0;
    verify_against_formula(impulse, 2.0);
  }

  auto signal_and_silence = make_data(9);
  signal_and_silence.u(0, 0) = 2.0;
  signal_and_silence.u(2, 1) = -1.0;
  verify_against_formula(signal_and_silence, 1.0);

  verify_against_formula(make_data(1, 0.25, -4.0), 0.01);
  auto two_samples = make_data(2, 0.25, 3.5);
  two_samples.u(0, 0) = 2.0;
  two_samples.u(1, 1) = 4.0;
  verify_against_formula(two_samples, 0.5);

  auto equal_window = make_data(5, 0.2);
  equal_window.u(0, 2) = 5.0;
  verify_against_formula(equal_window, equal_window.npts() * equal_window.dt());
  verify_against_formula(equal_window, 1000.0);

  auto valid = make_data(4);
  valid.u(0, 1) = 7.0;
  const std::vector<double> invalid_twin{
      0.0,
      -1.0,
      std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN(),
  };
  for (const double twin : invalid_twin)
    verify_rejected_without_mutation(valid, twin);

  const std::vector<double> invalid_dt{
      0.0,
      -1.0,
      std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN(),
  };
  for (const double dt : invalid_dt) {
    auto invalid = valid;
    invalid.set_dt(dt);
    verify_rejected_without_mutation(invalid, 1.0);
  }
  verify_rejected_without_mutation(make_data(0), 1.0);
}
