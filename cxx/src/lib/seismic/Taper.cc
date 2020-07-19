#include "mspass/seismic/Taper.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
using namespace std;
using namespace mspass;
namespace mspass{
LinearTaper::LinearTaper()
{
  head=false;
  tail=false;
  all=false;
}
LinearTaper::LinearTaper(const double t0h,const double t1h,
            const double t1t,const double t0t)
{
  t0head=t0h;
  t1head=t1h;
  t0tail=t0t;
  t1tail=t1t;
  if(t1head>t0head)
    head=true;
  else
    head=false;
  if(t1tail<t0tail)
    tail=true;
  else
    tail=false;
  if(head&&tail)
    all=true;
  else
    all=false;
  if(!(head || tail))
  {
    stringstream ss;
    ss << "LinearTaper constructor:  illegal input.  No valid taper parameters"<<endl
       << "Input defines front end ramp defined as 0 at "<<t0h<<" rising to 1 at "<<t1h<<endl
       << "Tail end ramp defined with 1 at "<<t1t<<" dropping to 0 at "<<t0t<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
}
int LinearTaper::apply(TimeSeries& d)
{
  double rampslope;
  if(head)
  {
    if(d.endtime()<t0head)
    {
      stringstream ss;
      ss<<"LinearTaper::apply:  inconsistent head taper parameters"<<endl
         << "Data endtime="<<d.endtime()<<" which is earlier than start of head taper="
         << t0head<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("LinearTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.t0();t<t0head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts()) d.s[is]=0.0;
    }
    rampslope=1.0/(t1head-t0head);
    for(t=t0head;t<t1head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=rampslope*(t-t0head);
        d.s[is]*=wt;
      }
    }
  }
  if(tail)
  {
    if(d.t0()>t0tail)
    {
      stringstream ss;
      ss<<"LinearTaper::apply:  inconsistent tail taper parameters"<<endl
        <<"Data start time="<<d.t0()<<" is after the end of the tail taper = "
        <<t0tail<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("LinearTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.endtime();t>=t0tail;t-=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts()) d.s[is]=0.0;
    }
    rampslope=1.0/(t0tail-t1tail);
    for(t=t0tail;t>=t1tail;t-=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=rampslope*(t0tail-t);
        d.s[is]*=wt;
      }
    }
  }
  return 0;
}
int LinearTaper::apply( Seismogram& d)
{
  int k;
  double rampslope;
  if(head)
  {
    if(d.endtime()<t0head)
    {
      stringstream ss;
      ss<<"LinearTaper::apply:  inconsistent head taper parameters"<<endl
         << "Data endtime="<<d.endtime()<<" which is earlier than start of head taper="
         << t0head<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("LinearTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.t0();t<t0head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts())
      {
        for(k=0;k<3;++k)d.u(k,is)=0.0;
      }
    }
    rampslope=1.0/(t1head-t0head);
    for(t=t0head;t<t1head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=rampslope*(t-t0head);
        for(k=0;k<3;++k)d.u(k,is)*=wt;
      }
    }
  }
  if(tail)
  {
    if(d.t0()>t0tail)
    {
      stringstream ss;
      ss<<"LinearTaper::apply:  inconsistent tail taper parameters"<<endl
        <<"Data start time="<<d.t0()<<" is after the end of the tail taper = "
        <<t0tail<<endl<<"Data vector was not altered"<<endl;
      MsPASSError merr(ss.str(),ErrorSeverity::Complaint);
      d.elog.log_error("LinearTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.endtime();t>=t0tail;t-=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts())
      {
        for(k=0;k<3;++k) d.u(k,is)=0.0;
      }
    }
    rampslope=1.0/(t0tail-t1tail);
    for(t=t0tail;t>=t1tail;t-=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=rampslope*(t-t1tail);
        for(k=0;k<3;++k)d.u(k,is)*=wt;
      }
    }
  }
  return 0;
}
/* this pair of functions return weight from 0 to 1
for head and tail cosine tapers.   Would make the code more efficient to put
these inline, but this will be easier to maintain at a tiny cost */
double headcos(double t0,double t1,double t)
{
  /* No range changing necesary in this context - trust t0<=t<=t1*/
  double T=(t1-t0)*2;
  double x;
  x=2.0*M_PI*(t-t0)/T;
  x-=M_PI;
  double wt=(cos(x)+1.0)/2.0;
  return(wt);
}
double tailcos(double t0,double t1,double t)
{
  /* No range changing necesary in this context - trust t1<=t<=t0*/
  double T=(t0-t1)*2;
  double x;
  x=2.0*M_PI*(t-t1)/T;
  double wt=(cos(x)+1.0)/2.0;
  return(wt);
}
CosineTaper::CosineTaper()
{
  head=false;
  tail=false;
  all=false;
}
CosineTaper::CosineTaper(const double t0h,const double t1h,
            const double t1t,const double t0t)
{
  t0head=t0h;
  t1head=t1h;
  t0tail=t0t;
  t1tail=t1t;
  if(t1head>t0head) head=true;
  if(t1tail<t0tail) tail=true;
  if(head&&tail)
    all=true;
  else
    all=false;
  if(!(head || tail))
  {
    stringstream ss;
    ss << "CosineTaper constructor:  illegal input.  No valid taper parameters"<<endl
       << "Input defines front end ramp defined as 0 at "<<t0h<<" rising to 1 at "<<t1h<<endl
       << "Tail end ramp defined with 1 at "<<t1t<<" dropping to 0 at "<<t0t<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
}
int CosineTaper::apply( TimeSeries& d)
{
  if(head)
  {
    if(d.endtime()<t0head)
    {
      stringstream ss;
      ss<<"CosineTaper::apply:  inconsistent head taper parameters"<<endl
         << "Data endtime="<<d.endtime()<<" which is earlier than start of head taper="
         << t0head<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("CosineTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.t0();t<t0head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts()) d.s[is]=0.0;
    }
    for(t=t0head;t<t1head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=headcos(t0head,t1head,t);
        d.s[is]*=wt;
      }
    }
  }
  if(tail)
  {
    if(d.t0()>t0tail)
    {
      stringstream ss;
      ss<<"CosineTaper::apply:  inconsistent tail taper parameters"<<endl
        <<"Data start time="<<d.t0()<<" is after the end of the tail taper = "
        <<t0tail<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("CosineTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.endtime();t>=t0tail;t-=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts())
      {
        d.s[is]=0.0;
      }
    }
    for(t=t1tail;t<t0tail;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=tailcos(t0tail,t1tail,t);
        d.s[is]*=wt;
      }
    }
  }
  return 0;
}
int CosineTaper::apply( Seismogram& d)
{
  if(head)
  {
    if(d.endtime()<t0head)
    {
      stringstream ss;
      ss<<"CosineTaper::apply:  inconsistent head taper parameters"<<endl
         << "Data endtime="<<d.endtime()<<" which is earlier than start of head taper="
         << t0head<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("CosineTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.t0();t<t0head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts())
      {
        for(int k=0;k<3;++k)d.u(k,is)=0.0;
      }
    }
    for(t=t0head;t<t1head;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=headcos(t0head,t1head,t);
        for(int k=0;k<3;++k)d.u(k,is)*=wt;
      }
    }
  }
  if(tail)
  {
    if(d.t0()>t0tail)
    {
      stringstream ss;
      ss<<"CosineTaper::apply:  inconsistent tail taper parameters"<<endl
        <<"Data start time="<<d.t0()<<" is after the end of the tail taper = "
        <<t0tail<<endl<<"Data vector was not altered"<<endl;
      d.elog.log_error("CosineTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    int is;
    double t,wt;
    for(t=d.endtime();t>=t0tail;t-=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0 && is<d.npts())
      {
        for(int k=0;k<3;++k) d.u(k,is)=0.0;
      }
    }
    for(t=t1tail;t<t0tail;t+=d.dt())
    {
      is=d.sample_number(t);
      if(is>=0)
      {
        wt=tailcos(t0tail,t1tail,t);
        for(int k=0;k<3;++k)d.u(k,is)*=wt;
      }
    }
  }
  return 0;
}
VectorTaper::VectorTaper()
{
  head=false;
  tail=false;
  all=false;
}
VectorTaper::VectorTaper(const vector<double> tp) : taper(tp)
{
  /* Really only all needs to be set true here, but need to initialize
  others anyway as good practice.*/
  head=true;
  tail=true;
  all=true;
}
int VectorTaper::apply( TimeSeries& d)
{
  if(all)
  {
    if(d.npts()!=taper.size())
    {
      stringstream ss;
      ss<<"VectorTaper apply method:  size mismatch with data"<<endl
        <<"operator taper size="<<taper.size()<<" but data vector length="<<d.npts()
        <<endl
        <<"This operator requires these lengths to match"<<endl;
      d.elog.log_error("VectorTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    for(int i=0;i<d.npts();++i) d.s[i] *= taper[i];
    return 0;
  }
  else
    return -1;
}
int VectorTaper::apply( Seismogram& d)
{
  if(all)
  {
    if(d.npts()!=taper.size())
    {
      stringstream ss;
      ss<<"VectorTaper apply method:  size mismatch with data"<<endl
        <<"operator taper size="<<taper.size()<<" but data vector length="<<d.npts()
        <<endl
        <<"This operator requires these lengths to match"<<endl;
      d.elog.log_error("VectorTaper",ss.str(),ErrorSeverity::Complaint);
      return -1;
    }
    for(int i=0;i<d.npts();++i)
    {
      double wt=taper[i];
      for(int k=0;k<3;++k)
      {
        d.u(k,i)*=wt;
      }
    }
    return 0;
  }
  else
    return -1;
}

}  // End namespace
