#include <iostream>
#include "mspass/utility/ErrorLogger.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/seismic/Taper.h"
using namespace mspass;
/* We use this print the results as a matrix on completion.*/
dmatrix vov2dmatrix(vector<vector<double>>& d)
{
  int rows=d.size();
  int columns=d[0].size();
  dmatrix result(rows,columns);
  for(int j=0;j<columns;++j)
  {
    for(int i=0;i<rows;++i) result(i,j)=d[i][j];
  }
  return result;
}
void print_error_log(const ErrorLogger& e)
{
  if(e.size()>0)
  {
    list<LogData> errs=e.get_error_log();
    list<LogData>::iterator iptr;
    for(iptr=errs.begin();iptr!=errs.end();++iptr)
    {
      cout << iptr->algorithm <<":"<< iptr->message<<endl;
    }
  }
}
int main(int argc, char **argv)
{
  int i,k;
  vector<vector<double>> tsout;
  vector<dmatrix> seisout;
  cout << "test_taper starting - building working data objects"<<endl;
  TimeSeries ts;
  ts.t0=0;
  ts.dt=1.0;
  ts.tref=TimeReferenceType::Relative;
  ts.ns=200;
  ts.live=true;
  ts.s.reserve(200);
  for(i=0;i<200;++i)ts.s.push_back(1.0);
  CoreSeismogram dtmp(200);
  Seismogram seis0(dtmp,string("test"));
  seis0.t0=0.0;
  seis0.dt=1.0;
  seis0.tref=TimeReferenceType::Relative;
  seis0.ns=200;
  for(i=0;i<200;++i)
    for(k=0;k<3;++k) seis0.u(k,i)=(double)(k+1);
  cout << "Setup finished - Starting tests of tapers"<<endl
    << "Trying a front mute linear taper"<<endl;
  LinearTaper tfront(4.0,14.0,500.0,450.0);
  TimeSeries ts1(ts);
  Seismogram seis1(seis0);
  int iret;
  iret=tfront.apply(ts1);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts1.elog);
  iret=tfront.apply(seis1);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis1.elog);
  tsout.push_back(ts1.s);
  seisout.push_back(seis1.u);
  cout << "Trying linear tail mute taper"<<endl;
  LinearTaper tback(-20.0,-30.0,150.0,180.0);
  TimeSeries ts2(ts);
  Seismogram seis2(seis0);
  iret=tback.apply(ts2);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts2.elog);
  iret=tback.apply(seis2);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis2.elog);
  tsout.push_back(ts2.s);
  seisout.push_back(seis2.u);
  cout << "Trying full linear taper"<<endl;
  LinearTaper tfull(10.0,25.0,150.0,180.0);
  TimeSeries ts3(ts);
  Seismogram seis3(seis0);
  iret=tfull.apply(ts3);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts3.elog);
  iret=tfull.apply(seis3);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis3.elog);
  tsout.push_back(ts3.s);
  seisout.push_back(seis3.u);
  cout << "Intentional error to test error logging functions"<<endl;
  seis3.t0=10000.0;
  ts3.t0=10000.0;
  cout << "Both of the following should show an informational error"<<endl;
  iret=tfull.apply(ts3);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts3.elog);
  iret=tfull.apply(seis3);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis3.elog);
  cout << "Starting similar tests for cosine taper"<<endl;

  CosineTaper tcfront(4.0,14.0,500.0,450.0);
  ts1=ts;
  seis1=seis0;
  cout << "Trying cosine front end taper"<<endl;
  iret=tcfront.apply(ts1);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts1.elog);
  iret=tcfront.apply(seis1);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis1.elog);
  tsout.push_back(ts1.s);
  seisout.push_back(seis1.u);
  cout << "Trying cosine tail mute taper"<<endl;
  CosineTaper tcback(-20.0,-30.0,150.0,180.0);
  ts2=ts;
  seis2=seis0;
  iret=tcback.apply(ts2);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts2.elog);
  iret=tcback.apply(seis2);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis2.elog);
  tsout.push_back(ts2.s);
  seisout.push_back(seis2.u);
  cout << "Trying full CosineTaper taper"<<endl;
  CosineTaper tcfull(10.0,25.0,150.0,180.0);
  ts3=ts;
  seis3=seis0;
  iret=tcfull.apply(ts3);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts3.elog);
  iret=tcfull.apply(seis3);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis3.elog);
  tsout.push_back(ts3.s);
  seisout.push_back(seis3.u);
  cout << "Intentional error to test error logging functions"<<endl;
  seis3.t0=10000.0;
  ts3.t0=10000.0;
  cout << "Both of the following should show an informational error"<<endl;
  iret=tcfull.apply(ts3);
  cout << "TimeSeries apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(ts3.elog);
  iret=tcfull.apply(seis3);
  cout << "Seismogram apply method completed returning "<<iret<<endl;
  if(iret!=0) print_error_log(seis3.elog);
  dmatrix allts=vov2dmatrix(tsout);
  cout << "dump of all TimeSeries tests"<<endl;
  cout << allts;
  vector<dmatrix>::iterator sptr;
  for(sptr=seisout.begin(),i=0;sptr!=seisout.end();++sptr,++i)
  {
    dmatrix dtmp;
    dtmp=tr(*sptr);
    cout << "Data from test number "<<i<<endl;
    cout << dtmp;
  }
}
