#include "mspass/seismic/DataGap.h"
#include "mspass/algorithms/TimeWindow.h"

namespace mspass::seismic {

using namespace mspass::seismic;
using namespace mspass::algorithms;
DataGap::DataGap(const std::list<TimeWindow> &twlist) {
  for (auto twptr = twlist.begin(); twptr != twlist.end(); ++twptr)
    gaps.insert(*twptr);
}

void DataGap::add_gap(const TimeWindow tw) {
  auto insert_result = this->gaps.insert(tw);
  if (!insert_result.second) {
    /* We land here if tw overlaps or duplicates something already in
     * the gaps container.  first in this case contains an iterator to
     * the potential duplicate.  We have to resolve which to keep*/
    TimeWindow old_tw(*insert_result.first);
    TimeWindow new_tw;
    if (tw.start < old_tw.start)
      new_tw.start = tw.start;
    else
      new_tw.start = old_tw.start;
    if (tw.end > old_tw.end)
      new_tw.end = tw.end;
    else
      new_tw.end = old_tw.end;
    gaps.erase(insert_result.first);
    gaps.insert(new_tw);
  }
}
bool DataGap::is_gap(const double ttest) {
  const double dt(
      0.001); // used to define small time interval smaller than most sampling
  if (gaps.empty())
    return false;
  TimeWindow twin;
  twin.start = ttest - dt * 0.5;
  twin.end = ttest + dt * 0.5;
  if (gaps.find(twin) == gaps.end())
    return false;
  else
    return true;
}

bool DataGap::has_gap(const TimeWindow twin) {
  if (gaps.empty())
    return false;
  if (gaps.find(twin) == gaps.end())
    return (false);
  else
    return (true);
}
std::list<TimeWindow> DataGap::get_gaps() const {
  std::list<TimeWindow> result;
  for (auto sptr = this->gaps.begin(); sptr != this->gaps.end(); ++sptr)
    result.push_back(*sptr);
  return result;
}
DataGap DataGap::subset(const TimeWindow tw) const {
  /* This could be implemented with the set equal_range method
   * but these objects are expected to normally be very small
   * and it is a lot clearer what this algorithm does.
   * */
  DataGap result;
  std::list<TimeWindow> gaplist = this->get_gaps();
  for (auto twptr = gaplist.begin(); twptr != gaplist.end(); ++twptr) {
    if (((twptr->end) > tw.start) && ((twptr->start) < tw.end)) {
      result.add_gap(*twptr);
    }
  }
  return result;
}
/* std::set iterators are always effectively const and the const
 * cannot be cast away.  Hence, this algorithm is much more complex
 * than I thought it would be.  Have to make a copy of the gaps
 * container and edit the copy before then having to use the add_gap
 * method to put in the edited value.  If there were a more elegant
 * way to handle the const problem this would go away.  I think
 * another element is the custom compare operator for this set
 * container.  Anyway this works and because in all known situations
 * the gaps set will be small this inefficiency is probably not important.
 * */
void DataGap::translate_origin(double origin_time) {
  std::set<TimeWindow, TimeWindowCmp> translated_gaps;
  for (auto ptr = this->gaps.begin(); ptr != this->gaps.end(); ++ptr) {
    TimeWindow tw(*ptr);
    tw.start -= origin_time;
    tw.end -= origin_time;
    translated_gaps.insert(tw);
  }
  this->clear_gaps();
  for (auto ptr = translated_gaps.begin(); ptr != translated_gaps.end(); ++ptr)
    this->add_gap(*ptr);
}
DataGap &DataGap::operator=(const DataGap &parent) {
  if (this != (&parent)) {
    gaps = parent.gaps;
  }
  return *this;
}
DataGap &DataGap::operator+=(const DataGap &parent) {
  for (auto ptr = parent.gaps.begin(); ptr != parent.gaps.end(); ++ptr)
    this->add_gap(*ptr);
  return *this;
}
} // namespace mspass::seismic
