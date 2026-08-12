#include "mspass/algorithms/amplitudes.h"
#include "misc/blas.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
#include <cmath>
#include <cstddef>

namespace {
double normalize_percentile(const double percentile) {
  if (!std::isfinite(percentile) || percentile <= 0.0 || percentile > 100.0) {
    std::stringstream ss;
    ss << "PercAmplitude: received percentile value=" << percentile << '\n'
       << "Must be a fraction in (0, 1] or a percentage in (1, 100]";
    throw mspass::utility::MsPASSError(ss.str(),
                                       mspass::utility::ErrorSeverity::Invalid);
  }
  return percentile > 1.0 ? percentile / 100.0 : percentile;
}

std::size_t percentile_index(const std::size_t sample_count,
                             const double percentile) {
  return static_cast<std::size_t>(
      std::floor(percentile * static_cast<double>(sample_count - 1)));
}
} // namespace

namespace mspass::algorithms::amplitudes {
using namespace std;
using namespace mspass::seismic;

/* Series of overloaded functions to measure peak amplitudes for
different types of seismic data objects.  These are used in
a generic algorithm defined in seispp.h */
double PeakAmplitude(const CoreTimeSeries &d) {
  if (d.dead() || ((d.npts()) <= 0))
    return (0.0);
  vector<double> work(d.s);
  vector<double>::iterator dptr, amp;
  /* We want maximum absolute value of the amplitude */
  for (dptr = work.begin(); dptr != work.end(); ++dptr)
    (*dptr) = fabs(*dptr);
  amp = max_element(work.begin(), work.end());
  return (*amp);
}
double PeakAmplitude(const CoreSeismogram &d) {
  if (d.dead() || ((d.npts() <= 0)))
    return (0.0);
  // This loop could use p->ns but this more more bulletproof.
  double ampval, ampvec;
  double *ptr;
  int j;
  ampvec = 0.0;
  for (j = 0; j < d.npts(); ++j) {
    ampval = 0.0;
    // Pointer arithmetic a bit brutal, but done
    // for speed to avoid 3 calls to operator ()
    ptr = d.u.get_address(0, j);
    ampval = (*ptr) * (*ptr);
    ++ptr;
    ampval += (*ptr) * (*ptr);
    ++ptr;
    ampval += (*ptr) * (*ptr);
    ampval = sqrt(ampval);
    if (ampval > ampvec)
      ampvec = ampval;
  }
  return (ampvec);
}
double RMSAmplitude(const CoreTimeSeries &d) {
  if (d.dead() || ((d.npts()) <= 0))
    return (0.0);
  double l2nrm = dnrm2(d.npts(), &(d.s[0]), 1);
  return sqrt(l2nrm * l2nrm / d.npts());
}
double RMSAmplitude(const CoreSeismogram &d) {
  /* rms is sum of squares so rms reduces to grand sum of squares of
  amplitudes on all 3 components.*/
  if (d.dead() || ((d.npts() <= 0)))
    return (0.0);
  double sumsq(0.0);
  /* This depends upon implementation detail for dmatrix u where the
  matrix is stored in contiguous block - beware of this implementation
  detail if matrix implementation changed. */
  double *ptr;
  ptr = d.u.get_address(0, 0);
  size_t n = 3 * d.npts();
  for (size_t k = 0; k < n; ++k, ++ptr)
    sumsq += (*ptr) * (*ptr);
  return sqrt(sumsq / d.npts());
}
double PercAmplitude(const CoreTimeSeries &d, const double percentile) {
  if (d.dead() || d.npts() == 0)
    return 0.0;
  const double percentile_fraction = normalize_percentile(percentile);
  vector<double> amps;
  amps = d.s;
  vector<double>::iterator ptr;
  for (ptr = amps.begin(); ptr != amps.end(); ++ptr)
    *ptr = fabs(*ptr);
  sort(amps.begin(), amps.end());
  size_t n = amps.size();
  size_t iperc = percentile_index(n, percentile_fraction);
  return amps[iperc];
}
double PercAmplitude(const CoreSeismogram &d, const double percentile) {
  if (d.dead() || d.npts() == 0)
    return 0.0;
  const double percentile_fraction = normalize_percentile(percentile);
  vector<double> amps;
  amps.reserve(d.npts());
  for (size_t i = 0; i < d.npts(); ++i) {
    double thisamp = dnrm2(3, d.u.get_address(0, i), 1);
    amps.push_back(thisamp);
  }
  sort(amps.begin(), amps.end());
  size_t n = amps.size();
  size_t iperc = percentile_index(n, percentile_fraction);
  return amps[iperc];
}
/* This pair could be made a template, but they are so simple
it is clearer to keep them here with the related functions */
double MADAmplitude(const CoreTimeSeries &d) { return PercAmplitude(d, 0.5); }
double MADAmplitude(const CoreSeismogram &d) { return PercAmplitude(d, 0.5); }
} // namespace mspass::algorithms::amplitudes
