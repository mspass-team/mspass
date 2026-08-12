#include "mspass/seismic/CoreTimeSeries.h"
#include "misc/blas.h"
#include "mspass/seismic/keywords.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include "waveform_arithmetic.h"
#include <vector>
namespace mspass::seismic {
using namespace std;
using namespace mspass::utility;
//
// simple constructors for the CoreTimeSeries object are defined inline
// in seispp.h.
//
CoreTimeSeries::CoreTimeSeries() : BasicTimeSeries(), Metadata() {
  s.reserve(0);
  this->set_dt(1.0);
  this->set_t0(0.0);
  this->set_npts(0);
}
CoreTimeSeries::CoreTimeSeries(const size_t nsin)
    : BasicTimeSeries(), Metadata() {
  /* IMPORTANT:  this constructor assumes BasicTimeSeries initializes the
  equivalent of:
  set_dt(1.0)
  set_t0(0.0)
  set_tref(TimeReferenceType::Relative)
  this->kill() - i.e. marked dead

  It further assumes current api where set_npts allocates and initializes s
  to nsin zeros */
  this->CoreTimeSeries::set_npts(nsin);
}

CoreTimeSeries::CoreTimeSeries(const CoreTimeSeries &tsi)
    : BasicTimeSeries(tsi), Metadata(tsi) {
  if (mlive) {
    s = tsi.s;
  } else if (tsi.s.size() > 0) {
    /* This is needed to preserve the contents of data vector when something
    marks the data dead, but one wants to restore it later.  Classic example
    is an interactive trace editor.  Found mysterious errors can occur
    without this features. */
    s = tsi.s;
  }
  /* Do nothing if the parent s is empty as std::vector will be properly
  initialized*/
}

CoreTimeSeries::CoreTimeSeries(const BasicTimeSeries &bd, const Metadata &md)
    : BasicTimeSeries(bd), Metadata(md) {
  /* this assumes set_npts initializes the vector containers, s, to zeros
  AND that BasicTimeSeries constructor initializes ns (npts) to the value
  desired. */
  this->CoreTimeSeries::set_npts(this->nsamp);
}
// standard assignment operator
CoreTimeSeries &CoreTimeSeries::operator=(const CoreTimeSeries &tsi) {
  if (this != &tsi) {
    this->BasicTimeSeries::operator=(tsi);
    this->Metadata::operator=(tsi);
    s = tsi.s;
  }
  return (*this);
}
/*  Sum operator for CoreTimeSeries object */

CoreTimeSeries &CoreTimeSeries::operator+=(const CoreTimeSeries &data) {
  // Silently do nothing if data is marked dead
  if (data.dead())
    return (*this);
  const auto overlap =
      detail::arithmetic_overlap(*this, data, "CoreTimeSeries::operator+=");
  for (std::size_t i = 0; i < overlap.count; ++i)
    this->s[overlap.lhs_begin + i] += data.s[overlap.rhs_begin + i];
  return (*this);
}
/* IMPORTANT:  this code is absolutely identical to that for operator+=
except the += in the last loop becomes -=.  Any changes in operator+=
must have exactly the same change here (other than a message with a
tag to the function)*/
CoreTimeSeries &CoreTimeSeries::operator-=(const CoreTimeSeries &data) {
  // Silently do nothing if data is marked dead
  if (data.dead())
    return (*this);
  const auto overlap =
      detail::arithmetic_overlap(*this, data, "CoreTimeSeries::operator-=");
  for (std::size_t i = 0; i < overlap.count; ++i)
    this->s[overlap.lhs_begin + i] -= data.s[overlap.rhs_begin + i];
  return (*this);
}
const CoreTimeSeries
CoreTimeSeries::operator+(const CoreTimeSeries &other) const {
  CoreTimeSeries result(*this);
  result += other;
  return result;
}
const CoreTimeSeries
CoreTimeSeries::operator-(const CoreTimeSeries &other) const {
  CoreTimeSeries result(*this);
  result -= other;
  return result;
}
CoreTimeSeries &CoreTimeSeries::operator*=(const double scale) {
  dscal(this->npts(), scale, &(this->s[0]), 1);
  return *this;
}
void CoreTimeSeries::set_dt(const double sample_interval) {
  this->BasicTimeSeries::set_dt(sample_interval);
  /* This is the unique name - we always set it.
  Feb 2021 - changed to used const string value set in keywords.h*/
  this->put(SEISMICMD_dt, sample_interval);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("dt");
  for (aptr = aliases.begin(); aptr != aliases.end(); ++aptr) {
    if (this->is_defined(*aptr)) {
      this->put(*aptr, sample_interval);
    }
  }
}
void CoreTimeSeries::set_t0(const double t0in) {
  this->BasicTimeSeries::set_t0(t0in);
  /* This is the unique name - we always set it.
  Changed Feb 2021 to use const string value defined in keywords.h*/
  this->put(SEISMICMD_t0, t0in);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("t0");
  aliases.insert("time");
  for (aptr = aliases.begin(); aptr != aliases.end(); ++aptr) {
    if (this->is_defined(*aptr)) {
      this->put(*aptr, t0in);
    }
  }
}
void CoreTimeSeries::set_npts(const size_t npts) {
  this->BasicTimeSeries::set_npts(npts);
  /* This is the unique name - we always set it. Cast is necessary to
  avoid type mismatch in python for unsigned.
  Changed Feb 2021 to use key defined in in keywords.h*/
  this->put(SEISMICMD_npts, (long int)npts);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("nsamp");
  aliases.insert("wfdisc.nsamp");
  for (aptr = aliases.begin(); aptr != aliases.end(); ++aptr) {
    if (this->is_defined(*aptr)) {
      this->put(*aptr, (long int)npts);
    }
  }
  /* this method has the further complication that npts sets the size of the
  data buffer.  We clear it an initialize it to 0 to be consistent with
  how constructors handle this. */
  std::vector<double>().swap(this->s); //  Clear the memory allocation of s
  this->s.reserve(npts);
  for (size_t i = 0; i < npts; ++i)
    this->s.push_back(0.0);
}
void CoreTimeSeries::sync_npts() {
  if (nsamp != this->s.size()) {
    this->BasicTimeSeries::set_npts(this->s.size());
    /* This is the unique name - we always set it.  The weird
    cast is necessary to avoid type mismatch with unsigned
    Changed Feb 2021 to use key defined in keywords.h*/
    this->put(SEISMICMD_npts, (long int)nsamp);
    /* these are hard coded aliases for sample_interval */
    std::set<string> aliases;
    std::set<string>::iterator aptr;
    aliases.insert("nsamp");
    aliases.insert("wfdisc.nsamp");
    for (aptr = aliases.begin(); aptr != aliases.end(); ++aptr) {
      if (this->is_defined(*aptr)) {
        this->put(*aptr, (long int)nsamp);
      }
    }
  }
}

double CoreTimeSeries::operator[](size_t i) const {
  if (!mlive)
    throw MsPASSError(string("CoreTimeSeries operator[]: attempting to access "
                             "data marked as dead"),
                      ErrorSeverity::Invalid);
  if ((i < 0) || (i >= s.size())) {
    throw MsPASSError(string("CoreTimeSeries operator[]:  request for sample "
                             "outside range of data"),
                      ErrorSeverity::Invalid);
  }
  return (s[i]);
}

} // namespace mspass::seismic
