#include "mspass/algorithms/Taper.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include <cmath>
#include <iostream>
#include <string>

using mspass::algorithms::LinearTaper;
using mspass::seismic::Seismogram;
using mspass::seismic::TimeReferenceType;
using mspass::seismic::TimeSeries;

namespace {
int failure_count{0};

void check(const bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << message << '\n';
    ++failure_count;
  }
}

void check_close(const double actual, const double expected,
                 const std::string &message) {
  check(std::abs(actual - expected) <= 1.0e-12,
        message + ": expected " + std::to_string(expected) + ", got " +
            std::to_string(actual));
}

double head_weight(const double time) {
  constexpr double t0head = 2.0;
  constexpr double t1head = 5.0;
  if (time < t0head)
    return 0.0;
  if (time < t1head)
    return (time - t0head) / (t1head - t0head);
  return 1.0;
}

double tail_weight(const double time) {
  constexpr double t1tail = 6.0;
  constexpr double t0tail = 9.0;
  if (time < t1tail)
    return 1.0;
  if (time <= t0tail)
    return (t0tail - time) / (t0tail - t1tail);
  return 0.0;
}

TimeSeries make_timeseries() {
  TimeSeries waveform(11);
  waveform.set_t0(0.0);
  waveform.set_dt(1.0);
  waveform.set_tref(TimeReferenceType::Relative);
  waveform.set_live();
  for (size_t sample = 0; sample < waveform.npts(); ++sample)
    waveform.s[sample] = 1.0;
  return waveform;
}

Seismogram make_seismogram() {
  Seismogram waveform(11);
  waveform.set_t0(0.0);
  waveform.set_dt(1.0);
  waveform.set_tref(TimeReferenceType::Relative);
  waveform.set_live();
  for (size_t sample = 0; sample < waveform.npts(); ++sample)
    for (size_t component = 0; component < 3; ++component)
      waveform.u(component, sample) = static_cast<double>(component + 1);
  return waveform;
}
} // namespace

int main() {
  TimeSeries timeseries = make_timeseries();
  Seismogram seismogram = make_seismogram();
  LinearTaper taper(2.0, 5.0, 6.0, 9.0);

  check(taper.apply(timeseries) == 0, "TimeSeries apply should succeed");
  check(taper.apply(seismogram) == 0, "Seismogram apply should succeed");

  for (size_t sample = 0; sample < timeseries.npts(); ++sample) {
    const double time = timeseries.time(sample);
    const double expected = head_weight(time) * tail_weight(time);
    check_close(timeseries.s[sample], expected,
                "TimeSeries weight at sample " + std::to_string(sample));
    for (size_t component = 0; component < 3; ++component) {
      const double component_scale = static_cast<double>(component + 1);
      check_close(seismogram.u(component, sample), expected * component_scale,
                  "Seismogram component " + std::to_string(component) +
                      " at sample " + std::to_string(sample));
      check_close(seismogram.u(component, sample) / component_scale,
                  timeseries.s[sample],
                  "TimeSeries/Seismogram parity at sample " +
                      std::to_string(sample));
    }
  }

  check_close(timeseries.s[2], 0.0, "head t0 endpoint");
  check_close(timeseries.s[5], 1.0, "head t1 endpoint");
  check_close(timeseries.s[6], 1.0, "tail t1 endpoint");
  check_close(timeseries.s[9], 0.0, "tail t0 endpoint");

  return failure_count == 0 ? 0 : 1;
}
