#include "mspass/seismic/DataGap.h"

namespace mspass::seismic{

using namespace mspass::seismic;
using namespace mspass::algorithms;
DataGap::DataGap(const std::list<TimeWindow>& twlist)
{
  for(auto twptr=twlist.begin();twptr!=twlist.end();++twptr)
     gaps.insert(*twptr);
}

bool DataGap::is_gap(const double ttest)
{
  const double dt(0.001);   // used to define small time interval smaller than most sampling
  if(gaps.empty())return false;
  TimeWindow twin;
  twin.start = ttest - dt*0.5;
  twin.end = ttest + dt*0.5;
  if(gaps.find(twin)==gaps.end())
    return false;
  else
    return true;
}

bool DataGap::has_gap(const TimeWindow twin)
{
  if(gaps.empty())return false;
  if(gaps.find(twin)==gaps.end())
    return(false);
  else
    return(true);
}

} // End namespace encapsulation
