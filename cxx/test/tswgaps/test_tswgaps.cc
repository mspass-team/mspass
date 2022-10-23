#include "mspass/seismic/TimeSeriesWGaps.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/algorithms/TimeWindow.h"
using namespace std;
using namespace mspass::seismic;
using mspass::algorithms::TimeWindow;
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
  cout << "Testing default constructor"<<endl;
  TimeSeriesWGaps nulldata;
  assert(!nulldata.has_gap());
  cout << "Testing constructor from TimeSeries"<<endl;
  TimeSeries d1 = make_ts(1000.0,1000,1.0);
  TimeSeriesWGaps d1g(d1);
  assert(d1g.npts() == 1000);
  assert(d1g.s[0] == 1.0);
  assert(!d1g.has_gap());
  cout << "Testing add_gap and is_gap methods"<<endl;
  TimeWindow g1(1001.0,1002.0);
  d1g.add_gap(g1);
  assert(d1g.has_gap());
  assert(d1g.is_gap(1001.1));
  assert(!d1g.is_gap(1000.5));
  assert(!d1g.is_gap(1003.0));
  TimeWindow gt1(1001.1,1005.0),gt2(999.0,1001.1),gt3(999.0,1000.1),gt4(1002.2,4000.0);
  assert(d1g.has_gap(gt1));
  assert(d1g.has_gap(gt2));
  assert(!d1g.has_gap(gt3));
  assert(!d1g.has_gap(gt4));
  cout << "Testing copy constructor"<<endl;
  TimeSeriesWGaps d2g(d1g);
  assert(d2g.has_gap());
  assert(d2g.is_gap(1001.1));
  assert(!d2g.is_gap(1000.5));
  assert(!d2g.is_gap(1003.0));
  /* We don't repeat window version tests as window as single time tests
  what we need for copy */
  cout << "Testing clear_gaps method"<<endl;
  d2g.clear_gaps();
  assert(!d2g.has_gap());
  cout << "Testing operator ="<<endl;
  d2g = d1g;
  assert(d2g.has_gap());
  assert(d2g.is_gap(1001.1));
  assert(!d2g.is_gap(1000.5));
  assert(!d2g.is_gap(1003.0));
  cout << "Testing zero_gaps method"<<endl;
  d2g.zero_gaps();
  int i;
  i = d2g.sample_number(1001.5);
  assert(d2g.s[i] == 0.0);
  assert(d2g.s[0] == 1.0);
  assert(d2g.s[999] == 1.0);
  cout << "Testing adding second gap"<<endl;
  d2g = d1g;
  TimeWindow gap2(1003.0,1003.2);
  d2g.add_gap(gap2);
  assert(d2g.has_gap());
  assert(d2g.is_gap(1001.1));
  assert(d2g.is_gap(1003.1));
  cout << "Testing if ator and rtoa"<<endl;
  d2g.ator(d2g.t0());
  assert(d2g.has_gap());
  assert(d2g.is_gap(1.1));
  assert(d2g.is_gap(3.1));
  d2g.rtoa();
  assert(d2g.has_gap());
  assert(d2g.is_gap(1001.1));
  assert(d2g.is_gap(1003.1));
  cout << "Testing shift method"<<endl;
  d2g.ator(d2g.t0());
  d2g.shift(1.0);
  assert(d2g.has_gap());
  assert(d2g.is_gap(0.1));
  assert(d2g.is_gap(2.1));
}
