#ifndef MSPASS_SEISMIC_WAVEFORM_ARITHMETIC_H
#define MSPASS_SEISMIC_WAVEFORM_ARITHMETIC_H

#include "mspass/seismic/BasicTimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <sstream>
#include <string>

namespace mspass::seismic::detail {
struct ArithmeticOverlap {
  std::size_t lhs_begin;
  std::size_t rhs_begin;
  std::size_t count;
};
/* This is the fractional error allowed for a slipperly clock to differ from 
 a nominal sample rate.  This fixed constant may need to be adjusted.  It is a guess
 by glp from experience with older gsn data that had this issue.
*/
const double dt_fractional_error_tolerance(0.001); 
inline ArithmeticOverlap arithmetic_overlap(const BasicTimeSeries &lhs,
                                            const BasicTimeSeries &rhs,
                                            const char *caller) {
  using mspass::utility::ErrorSeverity;
  using mspass::utility::MsPASSError;

  if (lhs.timetype() != rhs.timetype())
    throw MsPASSError(std::string(caller) +
                          ": operands use inconsistent time references",
                      ErrorSeverity::Invalid);

  const double lhs_dt = lhs.dt();
  const double rhs_dt = rhs.dt();
  const double dt_tolerance =
      dt_fractional_error_tolerance * std::max(std::abs(lhs_dt), std::abs(rhs_dt));
  if (!std::isfinite(lhs_dt) || !std::isfinite(rhs_dt) ||
      !(std::abs(lhs_dt - rhs_dt) <= dt_tolerance)) {
    std::ostringstream message;
    message << caller << ": sample intervals do not match: lhs dt=" << lhs_dt
            << ", rhs dt=" << rhs_dt;
    throw MsPASSError(message.str(), ErrorSeverity::Invalid);
  }

  const double offset_samples = (rhs.t0() - lhs.t0()) / lhs_dt;
  const double rounded_offset = std::round(offset_samples);
  if (!(std::abs(offset_samples - rounded_offset) <= 0.5)) {
    std::ostringstream message;
    message << caller << ": start times are not aligned to the sample grid: "
            << "offset=" << offset_samples << " samples";
    throw MsPASSError(message.str(), ErrorSeverity::Invalid);
  }

  if (rounded_offset >= static_cast<double>(lhs.npts()) ||
      rounded_offset <= -static_cast<double>(rhs.npts()))
    return ArithmeticOverlap{0, 0, 0};

  if (rounded_offset >= 0.0) {
    const std::size_t lhs_begin = static_cast<std::size_t>(rounded_offset);
    return ArithmeticOverlap{lhs_begin, 0,
                             std::min(lhs.npts() - lhs_begin, rhs.npts())};
  }

  const std::size_t rhs_begin = static_cast<std::size_t>(-rounded_offset);
  return ArithmeticOverlap{0, rhs_begin,
                           std::min(lhs.npts(), rhs.npts() - rhs_begin)};
}
} // namespace mspass::seismic::detail

#endif
