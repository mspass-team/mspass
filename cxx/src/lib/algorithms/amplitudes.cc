#include "mspass/algorithms/amplitudes.h"
#include "misc/blas.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include <algorithm>
namespace mspass::algorithms::amplitudes {
using namespace std;
using namespace mspass::seismic;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;

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
double PercAmplitude(const CoreTimeSeries &d, const double perc) {
  double percfrac;
  if (perc > 100.0 || perc <= 0.0) {
    stringstream ss;
    ss << "PercAmplitude:  received perc value=" << perc << endl
       << "Must be a nonzero percentage from 1 to 100 or a fraction value less "
          "than 1"
       << endl;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  } else if (perc <= 1.0) {
    percfrac = perc;
  } else {
    // Land her for actual percentage values
    percfrac = perc / 100.0;
  }
  vector<double> amps;
  amps = d.s;
  vector<double>::iterator ptr;
  for (ptr = amps.begin(); ptr != amps.end(); ++ptr)
    *ptr = fabs(*ptr);
  sort(amps.begin(), amps.end());
  size_t n = amps.size();
  size_t iperc = static_cast<size_t>(percfrac * static_cast<double>(n));
  return amps[iperc];
}
double PercAmplitude(const CoreSeismogram &d, const double perc) {
  vector<double> amps;
  amps.reserve(d.npts());
  for (int i = 0; i < d.npts(); ++i) {
    double thisamp = dnrm2(3, d.u.get_address(0, i), 1);
    amps.push_back(thisamp);
  }
  sort(amps.begin(), amps.end());
  size_t n = amps.size();
  /* n-1 because C arrays start at 0 */
  size_t iperc = static_cast<size_t>(perc * static_cast<double>(n));
  /* Silently return 100% if iperc exceeds the range of amps*/
  if (iperc >= amps.size())
    iperc = amps.size() - 1;
  return amps[iperc];
}
/* This pair could be made a template, but they are so simple
it is clearer to keep them here with the related functions */
double MADAmplitude(const CoreTimeSeries &d) { return PercAmplitude(d, 0.5); }
double MADAmplitude(const CoreSeismogram &d) { return PercAmplitude(d, 0.5); }
} // namespace mspass::algorithms::amplitudes
