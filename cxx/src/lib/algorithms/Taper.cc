#include "mspass/algorithms/Taper.h"
namespace mspass::algorithms
{
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;

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

/* Top mute implementation start here */
TopMute::TopMute()
{
  taper=NULL;
}
TopMute::~TopMute()
{
}
TopMute::TopMute(const double t0, const double t1, const std::string type)
{
  const string base_error("TopMute parameterized constructor:  ");
  if(t1<=t0)
  {
    stringstream ss;
    ss<<base_error<<"Zero time (t0) must be less than end of mute time (t1)"<<endl
       << "Constructor was passed t0="<<t0<<" and t1="<<t1<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
  try{
    if(type=="linear")
    {
      taper=std::make_shared<LinearTaper>(t0,t1,t0+999999.0,t1+999999.0);
      taper->disable_tail();
    }
    else if(type=="cosine")
    {
      taper=std::make_shared<CosineTaper>(t0,t1,t0+999999.0,t1+999999.0);
      taper->disable_tail();
    }
    else
    {
      stringstream ss;
      ss << base_error<<"Unrecognized type argument="<<type<<endl
         << "Current options are:  linear OR cosine"<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
  }catch(...){throw;};
}
TopMute::TopMute(const TopMute& parent) : taper(parent.taper)
{
}
TopMute& TopMute::operator=(const TopMute& parent)
{
  if(&parent != this)
  {
    this->taper = parent.taper;
  }
  return *this;
}
int TopMute::apply(mspass::seismic::TimeSeries& d)
{
  try{
    int iret;
    iret=this->taper->apply(d);
    return iret;
  }catch(...){throw;};
}
int TopMute::apply(mspass::seismic::Seismogram& d)
{
  try{
    int iret;
    iret=this->taper->apply(d);
    return iret;
  }catch(...){throw;};
}
/* Some sources say the approach used in the algorithm is evil, but
I don't see a better solution.  We use the property of dynamic_cast
of a pointer returning NULL if the cast fails because the type is wrong.
We then just walk through the possibilities and throw an exception if none
of them work.
*/
string TopMute::taper_type() const
{
  BasicTaper *rawptr;
  rawptr = this->taper.get();
  if(dynamic_cast<const LinearTaper*>(rawptr))
    return string("linear");
  else if(dynamic_cast<const CosineTaper*>(rawptr))
    return string("cosine");
  else
    /* note there is a VectorTaper child of BasicTaper but it is not supported
    by TopMute - only allows linear and cosine - so if that happened somehow
    we throw this exception in that case too. */
    throw MsPASSError("TopMute::taper_type:  Internal taper dynamic cast does not resolve;  this should not happen and is a bug",
      ErrorSeverity::Fatal);
}
}  // End namespace
