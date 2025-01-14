#include "mspass/seismic/TimeSeriesWGaps.h"
#include "mspass/seismic/DataGap.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/memory_constants.h"
#include <set>
#include <sstream>

namespace mspass::seismic {

using namespace mspass::utility;
using namespace mspass::utility::memory_constants;
using namespace mspass::seismic;
using namespace mspass::algorithms;

void TimeSeriesWGaps::ator(const double tshift) {
  /* dead traces should to totally ignored */
  if (this->dead())
    return;
  if (tref == TimeReferenceType::Relative)
    return;
  this->t0shift = tshift;
  this->mt0 -= tshift;
  // We have to shift all the gap windows definitions
  TimeWindow tw;
  std::set<TimeWindow, TimeWindowCmp> shifted_gaps;
  std::set<TimeWindow, TimeWindowCmp>::iterator this_gap;
  for (this_gap = gaps.begin(); this_gap != gaps.end(); ++this_gap) {
    tw.start = this_gap->start - tshift;
    tw.end = this_gap->end - tshift;
    shifted_gaps.insert(tw);
  }
  /* the section below is a replacement for the following simple
  construct that worked in the old seispp library that used a
  similar construct for gaps:
  this->gaps = shifted_gaps;
  Had to use the clear and loop below because g++ complained about operator=
  not being defined for the set container.   This loop is likely actually
  faster than having to call operator= anyway but why this fails is unknown. */
  this->gaps.clear();
  for (this_gap = shifted_gaps.begin(); this_gap != shifted_gaps.end();
       ++this_gap) {
    this->gaps.insert(*this_gap);
  }
  this->tref = TimeReferenceType::Relative;
  this->t0shift_is_valid = true;
}
void TimeSeriesWGaps::rtoa(const double tshift) {
  /* dead traces should to totally ignored */
  if (this->dead())
    return;
  if (tref == TimeReferenceType::UTC)
    return;
  this->mt0 += tshift;
  TimeWindow tw;
  std::set<TimeWindow, TimeWindowCmp> shifted_gaps;
  std::set<TimeWindow, TimeWindowCmp>::iterator this_gap;
  for (this_gap = gaps.begin(); this_gap != gaps.end(); ++this_gap) {
    tw.start = this_gap->start + tshift;
    tw.end = this_gap->end + tshift;
    shifted_gaps.insert(tw);
  }
  /* See related comment on why the line below will not compile */
  // this->gaps = shifted_gaps;
  this->gaps.clear();
  for (this_gap = shifted_gaps.begin(); this_gap != shifted_gaps.end();
       ++this_gap) {
    this->gaps.insert(*this_gap);
  }
  this->tref = TimeReferenceType::UTC;
  this->t0shift_is_valid = true;
}
void TimeSeriesWGaps::rtoa() {
  /* dead traces should to totally ignored */
  if (this->dead())
    return;
  const std::string errormess(
      "TimeSeriesWGaps::rtoa() t0shift for conversion is not defined.");
  /* This perhaps should create a complaint  message */
  if (tref == TimeReferenceType::UTC)
    return;
  /* A rather odd test for a nonzero.   We use 100 s assuming no active
   * source data would use a shift longer than that unless it really did
   * have an absolute time standard. Also assumes we'll never use data from
   * the first 2 minutes of 1960.*/
  if (t0shift_is_valid || (t0shift > 100.0)) {
    this->mt0 += this->t0shift;
    TimeWindow tw;
    std::set<TimeWindow, TimeWindowCmp> shifted_gaps;
    std::set<TimeWindow, TimeWindowCmp>::iterator this_gap;
    for (this_gap = gaps.begin(); this_gap != gaps.end(); ++this_gap) {
      tw.start = this_gap->start + t0shift;
      tw.end = this_gap->end + t0shift;
      shifted_gaps.insert(tw);
    }
    /* See related comment on why the line below will not compile */
    // this->gaps = shifted_gaps;
    this->gaps.clear();
    for (this_gap = shifted_gaps.begin(); this_gap != shifted_gaps.end();
         ++this_gap) {
      this->gaps.insert(*this_gap);
    }
    this->tref = TimeReferenceType::UTC;
    t0shift_is_valid = false;
  } else {
    this->kill();
    throw MsPASSError(errormess, ErrorSeverity::Invalid);
  }
}
void TimeSeriesWGaps::shift(const double dt) {
  /* This is the same test for valid t0shift used in BasicTimeSeries*/
  if (this->t0shift_is_valid || (this->t0shift > 100.0)) {
    double oldt0shift = this->t0shift;
    this->rtoa();
    this->ator(oldt0shift + dt);
  } else {
    this->kill();
    std::stringstream ss;
    ss << "TimeSeriesWGaps::shift:  internal shift attributes is marked invalid"
       << std::endl
       << "shift method should only be used on data originating with a UTC "
          "time standard"
       << std::endl;
    throw MsPASSError(ss.str(), ErrorSeverity::Invalid);
  }
}
void TimeSeriesWGaps::zero_gaps() {
  int i, istart, iend;
  std::set<TimeWindow, TimeWindowCmp>::iterator this_gap;

  for (this_gap = gaps.begin(); this_gap != gaps.end(); ++this_gap) {
    if (this_gap->end < this->mt0)
      continue;
    if (this_gap->start > this->endtime())
      continue;
    if (this_gap->start < this->mt0)
      istart = 0;
    else
      istart = round((this_gap->start - this->mt0) / this->mdt);
    if ((this_gap->end) > this->endtime())
      iend = this->nsamp - 1;
    else
      iend = round((this_gap->end - mt0) / this->mdt);
    for (i = istart; i <= iend; ++i)
      this->s[i] = 0.0;
  }
}

TimeSeriesWGaps &TimeSeriesWGaps::operator=(const TimeSeriesWGaps &parent) {
  if (this != (&parent)) {
    this->TimeSeries::operator=(parent);
    this->gaps = parent.gaps;
  }
  return *this;
}
size_t TimeSeriesWGaps::memory_use() const {
  size_t memory_estimate;
  memory_estimate = TimeSeries::memory_use();
  memory_estimate += DATA_GAP_AVERAGE_SIZE * gaps.size();
  return memory_estimate;
}

} // End namespace mspass::seismic
