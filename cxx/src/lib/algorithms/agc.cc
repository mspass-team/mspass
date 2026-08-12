#include "mspass/algorithms/algorithms.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <sstream>

namespace mspass::algorithms {
using mspass::seismic::BasicTimeSeries;
using mspass::seismic::Seismogram;
using mspass::seismic::TimeSeries;
using mspass::utility::dmatrix;
using mspass::utility::ErrorSeverity;
using mspass::utility::Metadata;
using mspass::utility::MsPASSError;

TimeSeries agc(Seismogram &d, const double twin) {
  if (!std::isfinite(twin) || twin <= 0.0) {
    std::ostringstream message;
    message << "agc: twin must be finite and positive; received " << twin;
    throw MsPASSError(message.str(), ErrorSeverity::Invalid);
  }
  if (!std::isfinite(d.dt()) || d.dt() <= 0.0) {
    std::ostringstream message;
    message << "agc: input dt must be finite and positive; received " << d.dt();
    throw MsPASSError(message.str(), ErrorSeverity::Invalid);
  }
  const std::size_t sample_count = d.npts();
  if (sample_count == 0)
    throw MsPASSError("agc: input must contain at least one sample",
                      ErrorSeverity::Invalid);

  const double requested_half_window =
      std::floor(std::round(twin / d.dt()) / 2.0);
  const std::size_t maximum_half_window = (sample_count - 1) / 2;
  const std::size_t half_window = static_cast<std::size_t>(std::min(
      requested_half_window, static_cast<double>(maximum_half_window)));

  TimeSeries gain_function(dynamic_cast<BasicTimeSeries &>(d),
                           dynamic_cast<Metadata &>(d));
  dmatrix output(3, sample_count);

  for (std::size_t i = 0; i < sample_count; ++i) {
    const std::size_t first = i > half_window ? i - half_window : 0;
    const std::size_t last = std::min(sample_count - 1, i + half_window);
    const std::size_t window_sample_count = last - first + 1;

    double energy = 0.0;
    for (std::size_t j = first; j <= last; ++j) {
      for (std::size_t component = 0; component < 3; ++component) {
        const double sample = d.u(component, j);
        energy += sample * sample;
      }
    }

    const double gain =
        energy > 0.0 ? 1.0 / std::sqrt(energy / (3.0 * window_sample_count))
                     : 0.0;
    gain_function.s[i] = gain;
    for (std::size_t component = 0; component < 3; ++component)
      output(component, i) = gain * d.u(component, i);
  }

  d.u = output;
  gain_function.set_live();
  return gain_function;
}
} // namespace mspass::algorithms
