#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/Metadata.h"
#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <vector>

using mspass::algorithms::deconvolution::MTPowerSpectrumEngine;
using mspass::seismic::PowerSpectrum;
using mspass::seismic::TimeReferenceType;
using mspass::seismic::TimeSeries;
using mspass::utility::Metadata;
using std::vector;

void CheckCondition(const bool condition, const char *expression,
                    const char *file, const int line) {
  if (!condition) {
    std::ostringstream message;
    message << file << ":" << line << ": test check failed: " << expression;
    throw std::runtime_error(message.str());
  }
}
#define CHECK(...)                                                             \
  CheckCondition(static_cast<bool>((__VA_ARGS__)), #__VA_ARGS__, __FILE__,     \
                 __LINE__)

bool NearlyEqual(const double left, const double right) {
  const double scale = std::max({1.0, std::abs(left), std::abs(right)});
  return std::abs(left - right) <= 1.0e-12 * scale;
}

void TestFrequencyGrid(const int nfft) {
  const double dt = 0.2;
  const double expected_df = 1.0 / (static_cast<double>(nfft) * dt);
  MTPowerSpectrumEngine engine(nfft, 0.5, 1, nfft, dt);

  CHECK(engine.fftsize() == nfft);
  CHECK(engine.nf() == nfft / 2 + 1);
  CHECK(NearlyEqual(engine.df(), expected_df));

  const vector<double> frequencies(engine.frequencies());
  CHECK(frequencies.size() == static_cast<size_t>(engine.nf()));
  for (size_t i = 0; i < frequencies.size(); ++i)
    CHECK(NearlyEqual(frequencies[i], static_cast<double>(i) * expected_df));

  TimeSeries signal(nfft);
  signal.set_dt(dt);
  signal.set_t0(0.0);
  signal.set_tref(TimeReferenceType::Relative);
  signal.set_npts(nfft);
  signal.set_live();
  const double pi = std::acos(-1.0);
  for (int i = 0; i < nfft; ++i) {
    const double time = static_cast<double>(i) * dt;
    signal.s[i] = std::sin(2.0 * pi * expected_df * time);
  }

  const PowerSpectrum spectrum(engine.apply(signal));
  CHECK(spectrum.live());
  CHECK(NearlyEqual(spectrum.df(), expected_df));
  CHECK(spectrum.nf() == frequencies.size());
  const auto peak =
      std::max_element(spectrum.spectrum.begin(), spectrum.spectrum.end());
  CHECK(peak != spectrum.spectrum.end());
  const size_t peak_index =
      static_cast<size_t>(std::distance(spectrum.spectrum.begin(), peak));
  CHECK(peak_index == 1);
  CHECK(NearlyEqual(spectrum.frequency(static_cast<int>(peak_index)),
                    expected_df));
}

void TestPowerLookupEndpoint() {
  const double df = 0.25;
  const double f0 = 0.5;
  const vector<double> values{1.0, 4.0, 9.0, 16.0};
  const PowerSpectrum spectrum(Metadata(), values, df, "endpoint-test", f0,
                               0.125, 8);
  const double terminal = spectrum.frequency(spectrum.nf() - 1);

  CHECK(spectrum.power(terminal) == values.back());

  const double below = terminal - df / 4.0;
  const double expected_below =
      values.back() - (values.back() - values[values.size() - 2]) / 4.0;
  CHECK(NearlyEqual(spectrum.power(below), expected_below));

  const double immediately_below =
      std::nextafter(terminal, -std::numeric_limits<double>::infinity());
  CHECK(NearlyEqual(spectrum.power(immediately_below), values.back()));

  const double immediately_above =
      std::nextafter(terminal, std::numeric_limits<double>::infinity());
  CHECK(spectrum.power(immediately_above) == 0.0);
}

int main() {
  try {
    for (const int nfft : {5, 6, 7, 8})
      TestFrequencyGrid(nfft);
    TestPowerLookupEndpoint();
  } catch (const std::exception &error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
  return 0;
}
