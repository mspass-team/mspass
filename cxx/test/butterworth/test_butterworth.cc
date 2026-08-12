#include <algorithm>
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "mspass/algorithms/Butterworth.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"

using mspass::algorithms::Butterworth;
using mspass::seismic::CoreSeismogram;
using mspass::seismic::CoreTimeSeries;
using mspass::seismic::Seismogram;
using mspass::seismic::TimeReferenceType;
using mspass::seismic::TimeSeries;
using mspass::utility::ErrorSeverity;
using mspass::utility::Metadata;
using mspass::utility::MsPASSError;

namespace {

void check(const bool condition, const std::string &message) {
  if (!condition)
    throw std::runtime_error(message);
}

bool close(const double lhs, const double rhs,
           const double tolerance = 1.0e-10) {
  return std::abs(lhs - rhs) <=
         tolerance * std::max({1.0, std::abs(lhs), std::abs(rhs)});
}

std::vector<double> two_sine(const double dt, const int npts, const double f1,
                             const double f2) {
  std::vector<double> result(npts);
  for (int i = 0; i < npts; ++i) {
    const double t = i * dt;
    result[i] = std::sin(2.0 * M_PI * f1 * t) + std::cos(2.0 * M_PI * f2 * t);
  }
  return result;
}

double spectral_amplitude(const std::vector<double> &data, const double dt,
                          const double frequency) {
  const int begin = data.size() / 4;
  const int end = 3 * data.size() / 4;
  double sine_projection(0.0), cosine_projection(0.0);
  for (int i = begin; i < end; ++i) {
    const double angle = 2.0 * M_PI * frequency * i * dt;
    sine_projection += data[i] * std::sin(angle);
    cosine_projection += data[i] * std::cos(angle);
  }
  const double scale = 2.0 / static_cast<double>(end - begin);
  return scale * std::hypot(sine_projection, cosine_projection);
}

void check_pass_and_stop(Butterworth filter, const double dt,
                         const double pass_frequency,
                         const double stop_frequency,
                         const std::string &description) {
  auto data = two_sine(dt, 10000, pass_frequency, stop_frequency);
  filter.apply(data);
  const double pass_amplitude = spectral_amplitude(data, dt, pass_frequency);
  const double stop_amplitude = spectral_amplitude(data, dt, stop_frequency);
  check(pass_amplitude > 0.75, description +
                                   " attenuated its pass-band sinusoid: " +
                                   std::to_string(pass_amplitude));
  check(stop_amplitude < 0.15, description +
                                   " retained its stop-band sinusoid: " +
                                   std::to_string(stop_amplitude));
}

void check_vectors(const std::vector<double> &lhs,
                   const std::vector<double> &rhs,
                   const std::string &description,
                   const double tolerance = 1.0e-10) {
  check(lhs.size() == rhs.size(), description + " size mismatch");
  for (size_t i = 0; i < lhs.size(); ++i)
    check(close(lhs[i], rhs[i], tolerance),
          description + " sample mismatch at " + std::to_string(i));
}

void check_filter_state(const Butterworth &filter, const double dt,
                        const double low_corner, const double high_corner,
                        const int low_poles, const int high_poles,
                        const std::string &filter_type, const bool zerophase,
                        const std::string &description) {
  check(close(filter.current_dt(), dt), description + " changed dt");
  check(close(filter.low_corner(), low_corner),
        description + " changed low corner");
  check(close(filter.high_corner(), high_corner),
        description + " changed high corner");
  check(filter.npoles_low() == low_poles, description + " changed low poles");
  check(filter.npoles_high() == high_poles,
        description + " changed high poles");
  check(filter.filter_type() == filter_type,
        description + " changed filter type");
  check(filter.is_zerophase() == zerophase,
        description + " changed zerophase state");
}

TimeSeries make_timeseries(const std::vector<double> &samples,
                           const double dt) {
  TimeSeries result(samples.size());
  result.s = samples;
  result.set_t0(0.0);
  result.set_dt(dt);
  result.set_tref(TimeReferenceType::Relative);
  result.set_live();
  return result;
}

Seismogram make_seismogram(const std::vector<double> &samples,
                           const double dt) {
  Seismogram result(samples.size());
  result.set_t0(0.0);
  result.set_dt(dt);
  result.set_tref(TimeReferenceType::Relative);
  result.set_live();
  for (size_t i = 0; i < samples.size(); ++i) {
    result.u(0, i) = samples[i];
    result.u(1, i) = 2.0 * samples[i];
    result.u(2, i) = -0.5 * samples[i];
  }
  return result;
}

std::vector<double> component(const Seismogram &data, const int k) {
  std::vector<double> result(data.npts());
  for (size_t i = 0; i < data.npts(); ++i)
    result[i] = data.u(k, i);
  return result;
}

void check_filter_equivalence(Butterworth lhs, Butterworth rhs, const double dt,
                              const std::string &description) {
  check(lhs.filter_type() == rhs.filter_type(),
        description + " filter type mismatch");
  check(lhs.npoles_low() == rhs.npoles_low(),
        description + " low-pole mismatch");
  check(lhs.npoles_high() == rhs.npoles_high(),
        description + " high-pole mismatch");
  check(close(lhs.low_corner(), rhs.low_corner()),
        description + " low-corner mismatch");
  check(close(lhs.high_corner(), rhs.high_corner()),
        description + " high-corner mismatch");
  auto lhs_data = two_sine(dt, 4000, 2.0, 15.0);
  auto rhs_data(lhs_data);
  lhs.apply(lhs_data);
  rhs.apply(rhs_data);
  check_vectors(lhs_data, rhs_data, description + " output");
}

Metadata corner_metadata(const std::string &type, const double dt) {
  Metadata md;
  md.put("sample_interval", dt);
  md.put("zerophase", true);
  md.put("filter_type", type);
  md.put("filter_definition_method", std::string("corner_pole"));
  if (type != "lowpass") {
    md.put("npoles_low", 4);
    md.put("corner_low", 1.0);
  }
  if (type != "highpass") {
    md.put("npoles_high", 4);
    md.put("corner_high", 5.0);
  }
  return md;
}

void test_configured_responses() {
  auto default_data = two_sine(1.0, 10000, 0.1, 0.5);
  const double default_stop_before = spectral_amplitude(default_data, 1.0, 0.5);
  Butterworth default_filter;
  default_filter.apply(default_data);
  check(spectral_amplitude(default_data, 1.0, 0.1) > 0.75,
        "default filter attenuated its pass band");
  check(spectral_amplitude(default_data, 1.0, 0.5) < 0.95 * default_stop_before,
        "default filter did not apply its configured high-cut state");
  check_pass_and_stop(Butterworth(true, false, true, 0, 0.0, 4, 5.0, 0.01),
                      0.01, 2.0, 15.0, "low-pass filter");
  check_pass_and_stop(Butterworth(true, true, false, 4, 1.0, 0, 0.0, 0.01),
                      0.01, 4.0, 0.2, "high-pass filter");
  check_pass_and_stop(Butterworth(true, true, true, 4, 1.0, 4, 5.0, 0.01), 0.01,
                      2.0, 0.2, "band-pass low edge");
  check_pass_and_stop(Butterworth(true, true, true, 4, 1.0, 4, 5.0, 0.01), 0.01,
                      2.0, 15.0, "band-pass high edge");
}

void test_metadata_constructors() {
  const double dt(0.01);
  check_filter_equivalence(Butterworth(corner_metadata("lowpass", dt)),
                           Butterworth(true, false, true, 0, 0.0, 4, 5.0, dt),
                           dt, "corner/pole low-pass metadata");
  check_filter_equivalence(Butterworth(corner_metadata("highpass", dt)),
                           Butterworth(true, true, false, 4, 1.0, 0, 0.0, dt),
                           dt, "corner/pole high-pass metadata");
  check_filter_equivalence(Butterworth(corner_metadata("bandpass", dt)),
                           Butterworth(true, true, true, 4, 1.0, 4, 5.0, dt),
                           dt, "corner/pole band-pass metadata");

  Metadata md;
  md.put("sample_interval", dt);
  md.put("zerophase", true);
  md.put("filter_type", std::string("bandpass"));
  md.put("filter_definition_method", std::string("stop_pass"));
  md.put("fstop_low", 0.5);
  md.put("astop_low", 0.01);
  md.put("fpass_low", 1.0);
  md.put("apass_low", 0.99);
  md.put("fpass_high", 5.0);
  md.put("apass_high", 0.99);
  md.put("fstop_high", 8.0);
  md.put("astop_high", 0.01);
  check_filter_equivalence(Butterworth(md),
                           Butterworth(true, true, true, 0.5, 0.01, 1.0, 0.99,
                                       5.0, 0.99, 8.0, 0.01, dt),
                           dt, "stop/pass band-pass metadata");
}

void test_timeseries_and_seismogram() {
  const double dt(0.01);
  const auto samples = two_sine(dt, 4000, 2.0, 15.0);
  TimeSeries ts = make_timeseries(samples, dt);
  Seismogram seis = make_seismogram(samples, dt);
  Butterworth ts_filter(true, true, true, 4, 1.0, 4, 5.0, dt);
  Butterworth seis_filter(ts_filter);
  ts_filter.apply(ts);
  seis_filter.apply(seis);
  check_vectors(ts.s, component(seis, 0), "TimeSeries/Seismogram component 0");
  auto component_one = component(seis, 1);
  auto component_two = component(seis, 2);
  for (size_t i = 0; i < ts.s.size(); ++i) {
    check(close(component_one[i], 2.0 * ts.s[i]),
          "Seismogram component 1 scale mismatch");
    check(close(component_two[i], -0.5 * ts.s[i]),
          "Seismogram component 2 scale mismatch");
  }
}

void test_reuse_across_sample_intervals() {
  const double dt1(0.01), dt2(0.02);
  Butterworth reusable(true, true, true, 4, 1.0, 4, 15.0, dt1);

  auto samples1 = two_sine(dt1, 4000, 3.0, 22.0);
  TimeSeries first = make_timeseries(samples1, dt1);
  TimeSeries first_expected(first);
  Butterworth fresh1(true, true, true, 4, 1.0, 4, 15.0, dt1);
  fresh1.apply(first_expected);
  reusable.apply(first);
  check_vectors(first.s, first_expected.s, "first reusable dt");

  auto samples2 = two_sine(dt2, 4000, 3.0, 22.0);
  Seismogram second = make_seismogram(samples2, dt2);
  Seismogram second_expected(second);
  Butterworth fresh2(true, true, true, 4, 1.0, 4, 15.0, dt2);
  fresh2.apply(second_expected);
  reusable.apply(second);
  for (int k = 0; k < 3; ++k)
    check_vectors(component(second, k), component(second_expected, k),
                  "reusable changed-dt Seismogram component");
  check(close(reusable.current_dt(), dt2), "reusable dt was not updated");
  check(close(reusable.low_corner(), 1.0), "reusable low corner changed");
  check(close(reusable.high_corner(), 15.0), "reusable high corner changed");

  TimeSeries third = make_timeseries(samples1, dt1);
  TimeSeries third_expected(third);
  Butterworth fresh3(true, true, true, 4, 1.0, 4, 15.0, dt1);
  fresh3.apply(third_expected);
  reusable.apply(third);
  check_vectors(third.s, third_expected.s, "reused original dt");
  check(close(reusable.current_dt(), dt1), "reusable dt did not return");
  check(close(reusable.low_corner(), 1.0), "reused low corner changed");
  check(close(reusable.high_corner(), 15.0), "reused high corner changed");
}

void test_unsafe_upper_corner_paths() {
  const double operator_dt(0.01), data_dt(0.03);
  const auto samples = two_sine(data_dt, 4000, 3.0, 12.0);
  Butterworth filter(true, true, true, 4, 1.0, 4, 20.0, operator_dt);
  Butterworth expected_filter(true, true, false, 4, 1.0, 0, 0.0, data_dt);
  TimeSeries actual = make_timeseries(samples, data_dt);
  TimeSeries expected(actual);
  expected_filter.apply(expected);
  filter.apply(actual);
  check_vectors(actual.s, expected.s, "unsafe TimeSeries fallback");
  check(actual.elog.size() == 1,
        "unsafe TimeSeries fallback did not log exactly once");
  check_filter_state(filter, operator_dt, 1.0, 20.0, 4, 4, "bandpass", true,
                     "unsafe TimeSeries fallback");

  Butterworth seis_filter(true, true, true, 4, 1.0, 4, 20.0, operator_dt);
  Seismogram actual_seis = make_seismogram(samples, data_dt);
  Seismogram expected_seis(actual_seis);
  Butterworth expected_seis_filter(true, true, false, 4, 1.0, 0, 0.0, data_dt);
  expected_seis_filter.apply(expected_seis);
  seis_filter.apply(actual_seis);
  for (int k = 0; k < 3; ++k)
    check_vectors(component(actual_seis, k), component(expected_seis, k),
                  "unsafe Seismogram fallback component");
  check(actual_seis.elog.size() == 1,
        "unsafe Seismogram fallback did not log exactly once");
  check_filter_state(seis_filter, operator_dt, 1.0, 20.0, 4, 4, "bandpass",
                     true, "unsafe Seismogram fallback");

  CoreTimeSeries core_ts(samples.size());
  core_ts.s = samples;
  core_ts.set_dt(data_dt);
  const auto core_ts_before = core_ts.s;
  Butterworth core_ts_filter(true, true, true, 4, 1.0, 4, 20.0, operator_dt);
  bool threw(false);
  try {
    core_ts_filter.apply(core_ts);
  } catch (const MsPASSError &error) {
    threw = true;
    check(error.severity() == ErrorSeverity::Invalid,
          "CoreTimeSeries rejection severity mismatch");
  }
  check(threw, "CoreTimeSeries unsafe corner was not rejected");
  check_vectors(core_ts.s, core_ts_before,
                "CoreTimeSeries rejection mutated data", 0.0);
  check_filter_state(core_ts_filter, operator_dt, 1.0, 20.0, 4, 4, "bandpass",
                     true, "CoreTimeSeries rejection");

  CoreSeismogram core_seis(samples.size());
  core_seis.set_dt(data_dt);
  for (size_t i = 0; i < samples.size(); ++i)
    for (int k = 0; k < 3; ++k)
      core_seis.u(k, i) = (k + 1) * samples[i];
  const auto core_seis_before = core_seis.u;
  Butterworth core_seis_filter(true, true, true, 4, 1.0, 4, 20.0, operator_dt);
  threw = false;
  try {
    core_seis_filter.apply(core_seis);
  } catch (const MsPASSError &error) {
    threw = true;
    check(error.severity() == ErrorSeverity::Invalid,
          "CoreSeismogram rejection severity mismatch");
  }
  check(threw, "CoreSeismogram unsafe corner was not rejected");
  for (size_t i = 0; i < core_seis.npts(); ++i)
    for (int k = 0; k < 3; ++k)
      check(core_seis.u(k, i) == core_seis_before(k, i),
            "CoreSeismogram rejection mutated data");
  check_filter_state(core_seis_filter, operator_dt, 1.0, 20.0, 4, 4, "bandpass",
                     true, "CoreSeismogram rejection");
}

void test_empty_inputs_are_noops() {
  const double operator_dt(0.01), empty_dt(0.1);
  Butterworth filter(true, true, true, 4, 1.0, 4, 5.0, operator_dt);
  std::vector<double> raw;
  filter.apply(raw);
  check(raw.empty(), "empty raw vector was changed");

  TimeSeries ts;
  ts.set_dt(empty_dt);
  filter.apply(ts);
  check(ts.npts() == 0, "empty TimeSeries was changed");
  check(ts.elog.size() == 0, "empty TimeSeries logged an error");
  check_filter_state(filter, operator_dt, 1.0, 5.0, 4, 4, "bandpass", true,
                     "empty TimeSeries");

  Seismogram seis;
  seis.set_dt(empty_dt);
  filter.apply(seis);
  check(seis.npts() == 0, "empty Seismogram was changed");
  check(seis.elog.size() == 0, "empty Seismogram logged an error");
  check_filter_state(filter, operator_dt, 1.0, 5.0, 4, 4, "bandpass", true,
                     "empty Seismogram");

  CoreTimeSeries core_ts;
  core_ts.set_dt(empty_dt);
  filter.apply(core_ts);
  check(core_ts.npts() == 0, "empty CoreTimeSeries was changed");
  check_filter_state(filter, operator_dt, 1.0, 5.0, 4, 4, "bandpass", true,
                     "empty CoreTimeSeries");

  CoreSeismogram core_seis;
  core_seis.set_dt(empty_dt);
  filter.apply(core_seis);
  check(core_seis.npts() == 0, "empty CoreSeismogram was changed");
  check_filter_state(filter, operator_dt, 1.0, 5.0, 4, 4, "bandpass", true,
                     "empty CoreSeismogram");
}

} // namespace

int main() {
  try {
    test_configured_responses();
    test_metadata_constructors();
    test_timeseries_and_seismogram();
    test_reuse_across_sample_intervals();
    test_unsafe_upper_corner_paths();
    test_empty_inputs_are_noops();
  } catch (const std::exception &error) {
    std::cerr << "Butterworth contract test failed: " << error.what()
              << std::endl;
    return 1;
  }
  std::cout << "Butterworth contract test passed" << std::endl;
  return 0;
}
