#include "mspass/algorithms/amplitudes.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/utility/Metadata.h"
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using mspass::algorithms::amplitudes::BandwidthData;
using mspass::algorithms::amplitudes::BandwidthStatistics;
using mspass::algorithms::amplitudes::EstimateBandwidth;
using mspass::seismic::PowerSpectrum;
using mspass::utility::Metadata;

void require(const bool condition, const std::string &message) {
  if (!condition)
    throw std::runtime_error(message);
}

PowerSpectrum constant_spectrum(const std::size_t length) {
  return PowerSpectrum(Metadata(), std::vector<double>(length, 4.0), 1.0,
                       "constant normalized PSD", 0.0, 0.05, 20);
}

void test_normalized_snr_is_independent_of_storage_length() {
  const PowerSpectrum signal = constant_spectrum(11);
  const PowerSpectrum noise = constant_spectrum(6);
  const BandwidthData passband =
      EstimateBandwidth(1.0, signal, noise, 0.5, 0.5, 4.0, true);

  require(passband.low_edge_snr == 1.0,
          "identical normalized PSDs must have unit low-edge SNR");
  require(passband.high_edge_snr == 1.0,
          "identical normalized PSDs must have unit high-edge SNR");

  const Metadata stats = BandwidthStatistics(signal, noise, passband);
  require(stats.get_bool("stats_are_valid"),
          "the constant-spectrum passband statistics must be valid");
  for (const std::string key : {"median_snr", "maximum_snr", "minimum_snr",
                                "q1_4_snr", "q3_4_snr", "mean_snr"}) {
    require(stats.get_double(key) == 1.0,
            key + " must be exactly one for identical normalized PSDs");
  }
}

void test_storage_length_does_not_create_a_false_passband() {
  const PowerSpectrum signal = constant_spectrum(11);
  const PowerSpectrum noise = constant_spectrum(6);
  const BandwidthData result =
      EstimateBandwidth(1.0, signal, noise, 1.1, 0.5, 4.0, true);

  require(result.low_edge_f == 0.0 && result.high_edge_f == 0.0 &&
              result.f_range == 0.0,
          "unit SNR must not cross a threshold greater than one");
}

void test_nonpositive_low_edge_uses_bandwidth_error_sentinel() {
  BandwidthData bandwidth;
  bandwidth.f_range = 10.0;
  bandwidth.high_edge_f = 10.0;

  bandwidth.low_edge_f = -1.0;
  require(bandwidth.bandwidth() == 0.0,
          "a negative low edge must return the exact error sentinel");

  bandwidth.low_edge_f = 0.0;
  require(bandwidth.bandwidth() == 0.0,
          "a DC low edge must return the exact error sentinel");

  bandwidth.low_edge_f = 1.0;
  const double positive_bandwidth = bandwidth.bandwidth();
  require(std::isfinite(positive_bandwidth) &&
              std::abs(positive_bandwidth - 20.0) < 1.0e-12,
          "a positive low edge must retain the logarithmic bandwidth");
}
} // namespace

int main() {
  try {
    test_normalized_snr_is_independent_of_storage_length();
    test_storage_length_does_not_create_a_false_passband();
    test_nonpositive_low_edge_uses_bandwidth_error_sentinel();
  } catch (const std::exception &error) {
    std::cerr << "test_spectral_snr failed: " << error.what() << std::endl;
    return 1;
  }
  return 0;
}
