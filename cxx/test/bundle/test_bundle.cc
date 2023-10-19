#include <float.h>
#include <string>
#include <vector>
#include "mspass/seismic/keywords.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/algorithms/algorithms.h"
using namespace std;
using namespace mspass::algorithms;
using namespace mspass::seismic;
using namespace mspass::utility;
/* Creates a time series with specified npts filled with constant, fill, value*/
TimeSeries make_testdatum(const string net, const string sta,const string chan,
  const string loc,const double dt, const double t0,const int npts,
  const double fill)
{
  TimeSeries d(npts);
  for(size_t i=0;i<npts;++i)d.s[i]=fill;
  d.set_t0(t0);
  d.set_dt(dt);
  d.set_live();
  d.set_tref(TimeReferenceType::UTC);
  d.force_t0_shift(0.0);
  d.put(SEISMICMD_net,net);
  d.put(SEISMICMD_sta,sta);
  d.put(SEISMICMD_chan,chan);
  d.put(SEISMICMD_loc,loc);
  string comp;
  comp.assign(chan,2,1);
  if(comp=="Z")
  {
    d.put(SEISMICMD_hang,0.0);
    d.put(SEISMICMD_vang,0.0);
  }
  /* For this test we don't worry about real issue of HH1 and HH2 */
  else if(comp=="N")
  {
    d.put(SEISMICMD_hang,0.0);
    d.put(SEISMICMD_vang,90.0);
  }
  else
  {
    d.put(SEISMICMD_hang,90.0);
    d.put(SEISMICMD_vang,90.0);
  }
  return d;
}
void print_input_pattern(const LoggingEnsemble<TimeSeries>& e)
{
  string sta,chan,loc,net;
  cout <<"net sta chan loc npts fill_value"<<endl;
  for(auto d=e.member.begin();d!=e.member.end();++d)
  {
    if(d->is_defined(SEISMICMD_net))
    {
      cout << d->get_string(SEISMICMD_net)<<" ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    if(d->is_defined(SEISMICMD_sta))
    {
      cout << d->get_string(SEISMICMD_sta)<<" ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    if(d->is_defined("chan"))
    {
      cout << d->get_string("chan")<<" ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    if(d->is_defined(SEISMICMD_loc))
    {
      loc=d->get_string(SEISMICMD_loc);
      if(loc=="")
        cout << "EMPTY ";
      else
        cout << loc << " ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    cout << d->s[0]<< endl;
  }
}
void print_output(const LoggingEnsemble<Seismogram>& e)
{
  string sta,chan,loc,net;
  cout <<"net sta chan loc npts fill_values"<<endl;
  int i(0);
  for(auto d=e.member.begin();d!=e.member.end();++d,++i)
  {
    /* Exception kill but insert dead Seismogram objects so
    we flag them in this print function */
    if(d->dead())
    {
      cout << "Ensemble member "<<i<<" was killed"<<endl;
    }
    if(d->is_defined(SEISMICMD_net))
    {
      cout << d->get_string(SEISMICMD_net)<<" ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    if(d->is_defined(SEISMICMD_sta))
    {
      cout << d->get_string(SEISMICMD_sta)<<" ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    if(d->is_defined(SEISMICMD_loc))
    {
      loc=d->get_string(SEISMICMD_loc);
      if(loc!="")
        cout << d->get_string(SEISMICMD_loc)<<" ";
      else
        cout << "EMPTY" << " ";
    }
    else
    {
      cout << "Undefined"<<" ";
    }
    if(d->live())
    {
      for(auto k=0;k<3;++k)
      {
        cout << d->u(k,0) << " ";
      }
    }
    else
    {
      cout << "NULL - killed"<<endl;
    }
    cout <<endl;
  }
}
bool compare_stas(const LoggingEnsemble<Seismogram>& d,const vector<string>& pattern)
{
  size_t i(0);
  for(auto dptr=d.member.begin();dptr!=d.member.end();++dptr,++i)
  {
    string sta=dptr->get_string(SEISMICMD_sta);
    if(sta!=pattern[i]) return false;
  }
  return true;
}
/* used in exception handler tests */
int count_live(const LoggingEnsemble<Seismogram>& d)
{
  int count=0;
  for(auto dptr=d.member.begin();dptr!=d.member.end();++dptr)
  {
    if(dptr->live()) ++count;
  }
  return count;
}
void print_ensemble_errors(const LoggingEnsemble<Seismogram>& d)
{
  int count=0;
  for(auto dptr=d.member.begin();dptr!=d.member.end();++dptr,++count)
  {
    if(dptr->dead())
    {
      cout << "Member number "<< count << " marked dead with this error message"<<endl;
      list<LogData> errors=dptr->elog.get_error_log();
      for(auto e=errors.begin();e!=errors.end();++e)
      {
        /* Just print algorithm and message */
        cout << e->algorithm << " generated this message:  "<<e->message<<endl;
      }
    }
  }
}
int main(int argc, char **argv)
{
  cout << "Starting test_bundle program"<<endl;
  LoggingEnsemble<TimeSeries> ens0, ens1, ens2, ens3, ens4;
  LoggingEnsemble<Seismogram> ens3c;  // always holds output of bundle
  /* We will fill ens0 with a pattern we will mostly reuse.  Data are
  intentionally in the wrong order to test sorting */
  vector<string> sta;
  vector<string> net;
  vector<string> loc;
  vector<string>chan;
  net.push_back(string("AA"));
  net.push_back(string("XY"));
  sta.push_back(string("ABC"));
  sta.push_back(string("DEF"));
  sta.push_back(string("BBLONG"));
  chan.push_back(string("HHN"));
  chan.push_back(string("HHZ"));
  chan.push_back(string("HHE"));
  loc.push_back(string("00"));
  loc.push_back(string(""));
  const double dt1(0.01);
  const double t01(10000.0);
  const int npts1(100);
  int n(0);
  for(auto i=0;i<chan.size();++i)
  {
    for(auto j=0;j<net.size();++j)
    {
      for(auto k=0;k<loc.size();++k)
      {
        for(auto l=0;l<sta.size();++l)
        {
          double fill;
          ++n;
          fill=static_cast<double>(n);
          TimeSeries d;
          d=make_testdatum(net[j],sta[l],chan[i],loc[k],dt1,t01,npts1,fill);
          ens0.member.push_back(d);
        }
      }
    }
  }
  vector<string> pattern;  // First set of tests should have sat in this order
  pattern.push_back("DEF");
  pattern.push_back("DEF");
  pattern.push_back("BBLONG");
  pattern.push_back("BBLONG");
  pattern.push_back("ABC");
  pattern.push_back("ABC");
  pattern.push_back("DEF");
  pattern.push_back("DEF");
  pattern.push_back("BBLONG");
  pattern.push_back("BBLONG");
  pattern.push_back("ABC");
  pattern.push_back("ABC");
  cout << "Input ensemble summary"<<endl;
  print_input_pattern(ens0);
  cout << "Running best case test for bundle_seed_data"<<endl;
  ens1=ens0;
  ens3c=bundle_seed_data(ens1);
  print_output(ens3c);
  assert(compare_stas(ens3c,pattern));
  int nlive;
  nlive=count_live(ens3c);
  assert(nlive==12);
  ens2=ens0;
  cout << "Trying an ensemble with some net values undefined"<<endl;
  for(auto d=ens2.member.begin();d!=ens2.member.end();++d)
  {
    string netname;
    netname=d->get_string(SEISMICMD_net);
    if(netname=="AA") d->erase(SEISMICMD_net);
  }
  ens3c=bundle_seed_data(ens2);
  print_output(ens3c);
  assert(compare_stas(ens3c,pattern));
  nlive=count_live(ens3c);
  assert(nlive==12);
  ens3=ens0;
  cout << "Trying similar test with some loc values undefined"<<endl;
  for(auto d=ens3.member.begin();d!=ens3.member.end();++d)
  {
    string name;
    name=d->get_string(SEISMICMD_loc);
    if(name=="00") d->erase(SEISMICMD_loc);
  }
  ens3c=bundle_seed_data(ens3);
  print_output(ens3c);
  assert(compare_stas(ens3c,pattern));
  nlive=count_live(ens3c);
  assert(nlive==12);
  cout << "Test handling of pure duplicates"<<endl;
  ens2=ens0;
  ens2.member.push_back(ens0.member[4]);
  ens2.member.push_back(ens0.member[8]);
  ens3c=bundle_seed_data(ens2);
  print_output(ens3c);
  assert(compare_stas(ens3c,pattern));
  nlive=count_live(ens3c);
  assert(nlive==12);
  cout << "Test handling of incomplete bundles"<<endl;
  ens2=ens0;
  ens2.member.erase(ens2.member.begin());
  ens2.member.erase(ens2.member.end());
  ens3c=bundle_seed_data(ens2);
  print_output(ens3c);
  assert(compare_stas(ens3c,pattern));
  cout << "Testing handling of irregular start and end time"<<endl
    << "Note this is really a test of the CoreTimeSeries constructor used by bundle_seed_data"
    <<endl;
  ens4=ens0;
  double oldt0=ens4.member[0].t0();
  ens4.member[0].set_t0(oldt0+0.2);
  // Assume all members have commons start time - true unless something is changed in test
  ens4.member[5].set_t0(oldt0-0.3);
  ens3c=bundle_seed_data(ens4);
  assert(compare_stas(ens3c,pattern));
  cout << "Output member start and end times relative to base time shift"<<endl;
  for(auto d=ens3c.member.begin();d!=ens3c.member.end();++d)
  {
    d->ator(10000.0);
    cout << d->t0()<<" "<<d->endtime()<<endl;
  }
  assert(fabs(ens3c.member[9].endtime()-0.68)/0.68 < FLT_EPSILON);
  assert(fabs(ens3c.member[10].t0() - 0.2)/0.2 < FLT_EPSILON);
  assert(fabs(ens3c.member[10].endtime()-0.98)/0.98 < FLT_EPSILON);
  cout << "Testing exception handlers for unrepairable data"<<endl;
  ens4=ens0;
  oldt0=ens4.member[0].t0();
  /* this will make a member without an overlap that will cause an exception
  to be thrown */
  ens4.member[0].set_t0(oldt0+1000.0);
  /* This will generate an irregular sample rate error */
  ens4.member[5].set_dt(0.5);
  ens3c=bundle_seed_data(ens4);
  cout << "test Ensemble output size="<<ens3c.member.size()<<endl;
  nlive=count_live(ens3c);
  cout << "This should say number marked live is 10"<<endl;
  cout << "Number marked live="<<nlive<<endl;
  cout << "This should show two different errors and define last two members as marked dead"<<endl;
  print_ensemble_errors(ens3c);
  assert(nlive==10);
  cout << "Testing sort function isolation"<<endl;
  ens2=ens1;
  /* Could not figure out a good assert to validate that the following work.
  The steps after will create errors if it didn't do what it should though.*/
  seed_ensemble_sort(ens2);
  cout << "Testing BundleSEEDGroup"<<endl<<"First a test that should work"<<endl;
  Seismogram s;
  s=BundleSEEDGroup(ens2.member,0,2);
  assert(s.live());
  cout << "Success - trying an improper bundle.  This one should be killed"<<endl;
  s=BundleSEEDGroup(ens2.member,0,5);
  assert(s.dead());
  cout << "Error message posted"<<endl;
  list<LogData> errors=s.elog.get_error_log();
  for(auto e=errors.begin();e!=errors.end();++e) cout << *e<<endl;
}
