#include "mspass/seismic/TimeSeriesWGaps.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/utility/ProcessingHistory.h"
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms;
using mspass::algorithms::TimeWindow;
using namespace mspass::utility;
TimeSeries make_ts(double t0, const int nsamp, const double fill)
{
  TimeSeries ts(nsamp);
  ts.set_t0(t0);
  ts.set_dt(0.01);
  ts.set_tref(TimeReferenceType::UTC);
  ts.set_live();
  for(size_t i=0;i<ts.s.size();++i)
  {
    ts.s[i] = fill;
  }
  return ts;
}
int main(int argc, char **argv)
{
  cout << "Testing splice that should succeed - no history"<<endl;
  TimeSeries d1 = make_ts(1000.0,1000,1.0);
  TimeSeries d2 = make_ts(d1.endtime()+d1.dt(),1000,1.0);
  std::vector<TimeSeries> segments;
  segments.push_back(d1);
  segments.push_back(d2);
  TimeSeriesWGaps spliced_data;
  spliced_data = splice_segments(segments,false);
  assert(spliced_data.npts() == 2000);
  assert(spliced_data.live());
  assert(!spliced_data.has_gap());
  cout << "Repeat but with history enabled"<<endl;
  d1.set_as_origin("test_splicing","1","111",AtomicType::TIMESERIES,true);
  d2.set_as_origin("test_splicing","2","222",AtomicType::TIMESERIES,true);
  segments.clear();
  segments.push_back(d1);
  segments.push_back(d2);
  spliced_data = splice_segments(segments,true);
  assert(spliced_data.npts() == 2000);
  assert(spliced_data.live());
  assert(!spliced_data.has_gap());
  /* This is a minimal test of preserving history, but adequate for the
  present*/
  assert(!spliced_data.is_empty());
  cout << "Testing two segments with a gap"<<endl;
  TimeSeries d3(d2);
  d3.set_t0(d1.endtime() + 2.0 + d1.dt());
  d3.set_as_origin("test_splicing","3","333",AtomicType::TIMESERIES,true);
  segments.clear();
  segments.push_back(d1);
  segments.push_back(d3);
  spliced_data = splice_segments(segments,true);
  assert(spliced_data.has_gap());
  assert(spliced_data.npts() == 2200);
  cout << "Trying reverse with 2 s overlap"<<endl;
  cout << "First create and run repair_overlaps and verify one segment of output"<<endl;
  d3.set_t0(d1.endtime() - 2.0 + d1.dt());
  segments.clear();
  segments.push_back(d1);
  segments.push_back(d3);
  segments = repair_overlaps(segments);
  spliced_data = splice_segments(segments,true);
  assert(!spliced_data.has_gap());
  assert(spliced_data.npts() == 1800);
  cout << "Repeat with a dead segments at start"<<endl;
  segments.clear();
  segments.push_back(d3);
  segments[0].kill();
  segments.push_back(d1);
  segments.push_back(d3);
  segments = repair_overlaps(segments);
  spliced_data = splice_segments(segments,true);
  assert(!spliced_data.has_gap());
  assert(spliced_data.npts() == 1800);
  cout << "Repeat with pure duplicate of d3"<<endl;
  segments.clear();
  segments.push_back(d1);
  segments.push_back(d3);
  segments.push_back(d3);
  segments = repair_overlaps(segments);
  spliced_data = splice_segments(segments,true);
  assert(!spliced_data.has_gap());
  assert(spliced_data.npts() == 1800);
  cout << "Running test with overlapping signals that do not match"<<endl;
  TimeSeries d4;
  d4 = make_ts(d2.endtime()-2.0,1000,-2.0);
  d4.set_as_origin("test_splicing","4","4444",AtomicType::TIMESERIES,true);
  segments.clear();
  segments.push_back(d1);
  segments.push_back(d2);
  segments.push_back(d4);
  /* This should automatically kill the central segment.  splice should then
  define that as a gap */
  segments = repair_overlaps(segments);
  assert(segments[0].live());
  assert(segments[1].dead());
  assert(segments[2].live());
  spliced_data = splice_segments(segments,true);
  assert(spliced_data.has_gap());
  assert(spliced_data.npts() == 2799);
  double t;
  t=segments[0].endtime() + 5.0;
  assert(spliced_data.is_gap(t));
}
