#include "mspass/algorithms/amplitudes.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

using mspass::algorithms::amplitudes::PercAmplitude;
using mspass::seismic::CoreSeismogram;
using mspass::seismic::CoreTimeSeries;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;

void check(const bool condition, const std::string &message) {
  if (!condition)
    throw std::runtime_error(message);
}

template <typename Waveform>
void assert_invalid_percentile(const Waveform &waveform,
                               const double percentile) {
  bool threw_invalid_error = false;
  try {
    PercAmplitude(waveform, percentile);
  } catch (const MsPASSError &error) {
    check(error.severity() == ErrorSeverity::Invalid,
          "invalid percentile raised the wrong error severity");
    threw_invalid_error = true;
  }
  check(threw_invalid_error, "invalid percentile did not throw MsPASSError");
}

template <typename Waveform> void verify_percentiles(const Waveform &waveform) {
  check(PercAmplitude(waveform, 0.01) == 1.0, "0.01 percentile mismatch");
  check(PercAmplitude(waveform, 0.5) == 4.0, "0.5 percentile mismatch");
  check(PercAmplitude(waveform, 50.0) == 4.0, "50 percentile mismatch");
  check(PercAmplitude(waveform, 0.95) == 16.0, "0.95 percentile mismatch");
  check(PercAmplitude(waveform, 95.0) == 16.0, "95 percentile mismatch");
  check(PercAmplitude(waveform, 1.0) == 32.0, "1.0 percentile mismatch");
  check(PercAmplitude(waveform, 100.0) == 32.0, "100 percentile mismatch");

  const std::vector<double> invalid_percentiles{
      0.0,
      -1.0,
      100.1,
      std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN(),
  };
  for (const double percentile : invalid_percentiles)
    assert_invalid_percentile(waveform, percentile);
}

int main() {
  const std::vector<double> samples{-16.0, 1.0, -32.0, 4.0, -2.0, 8.0};

  CoreTimeSeries timeseries(samples.size());
  timeseries.set_live();
  timeseries.s = samples;

  CoreSeismogram seismogram(samples.size());
  seismogram.set_live();
  for (size_t sample = 0; sample < samples.size(); ++sample)
    seismogram.u(0, sample) = samples[sample];

  verify_percentiles(timeseries);
  verify_percentiles(seismogram);

  CoreTimeSeries dead_timeseries(timeseries);
  dead_timeseries.kill();
  CoreSeismogram dead_seismogram(seismogram);
  dead_seismogram.kill();
  check(PercAmplitude(dead_timeseries, 0.0) == 0.0,
        "dead TimeSeries did not return zero");
  check(PercAmplitude(dead_seismogram, 0.0) == 0.0,
        "dead Seismogram did not return zero");

  CoreTimeSeries empty_timeseries;
  empty_timeseries.set_live();
  CoreSeismogram empty_seismogram;
  empty_seismogram.set_live();
  check(PercAmplitude(empty_timeseries, 0.0) == 0.0,
        "empty TimeSeries did not return zero");
  check(PercAmplitude(empty_seismogram, 0.0) == 0.0,
        "empty Seismogram did not return zero");
}
