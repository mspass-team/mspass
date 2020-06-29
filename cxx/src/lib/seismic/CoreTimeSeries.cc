#include <vector>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/Metadata.h"
namespace mspass
{
using namespace std;
using namespace mspass;
//
// simple constructors for the CoreTimeSeries object are defined inline
// in seispp.h.
//
CoreTimeSeries::CoreTimeSeries() : BasicTimeSeries(), Metadata()
{
    s.reserve(0);
}
CoreTimeSeries::CoreTimeSeries(size_t nsin) : BasicTimeSeries(), Metadata()
{
  s.reserve(nsin);
  /* This assumes current api where set_npts allocates and initializes s
  to nsin zeros */
  this->set_npts(nsin);
}


CoreTimeSeries::CoreTimeSeries(const CoreTimeSeries& tsi) :
    BasicTimeSeries(tsi),
    Metadata(tsi)
{
    if(mlive)
    {
        s=tsi.s;
    }
    else if(tsi.s.size()>0)
    {
      /* This is needed to preserve the contents of data vector when something
      marks the data dead, but one wants to restore it later.  Classic example
      is an interactive trace editor.  Found mysterious errors can occur
      without this features. */
        s=tsi.s;
    }
    /* Do nothing if the parent s is empty as std::vector will be properly
    initialized*/
}

CoreTimeSeries::CoreTimeSeries(const BasicTimeSeries& bd,const Metadata& md)
    : BasicTimeSeries(bd), Metadata(md)
{
  /* this assumes set_npts initializes the vector containers, s, to zeros
  AND that BasicTimeSeries constructor initializes ns (npts) to the value
  desired. */
  this->set_npts(this->nsamp);
}
// standard assignment operator
CoreTimeSeries& CoreTimeSeries::operator=(const CoreTimeSeries& tsi)
{
    if(this!=&tsi)
    {
        this->BasicTimeSeries::operator=(tsi);
        this->Metadata::operator=(tsi);
        s=tsi.s;
    }
    return(*this);
}
/*  Sum operator for CoreTimeSeries object */

CoreTimeSeries& CoreTimeSeries::operator+=(const CoreTimeSeries& data)
{
    size_t i,i0,iend;
    size_t j,j0=0;
    // Sun's compiler complains about const objects without this.
    CoreTimeSeries& d=const_cast<CoreTimeSeries&>(data);
    // Silently do nothing if d is marked dead
    if(!d.mlive) return(*this);
    // Silently do nothing if d does not overlap with data to contain sum
    if( (d.endtime()<mt0)
            || (d.mt0>(this->endtime())) ) return(*this);
    if(d.tref!=(this->tref))
        throw MsPASSError("CoreTimeSeries += operator cannot handle data with inconsistent time base\n",
                          ErrorSeverity::Invalid);
    //
    // First we have to determine range fo sum for d into this
    //
    i0=d.sample_number(this->mt0);
    if(i0<0)
    {
        j=-i0;
        i0=0;
    }
    iend=d.sample_number(this->endtime());
    if(iend>(d.s.size()-1))
    {
        iend=d.s.size()-1;
    }
    //
    // IMPORTANT:  This algorithm simply assumes zero_gaps has been called
    // and/or d was checked for gaps befor calling this operatr.
    // It will produce garbage for most raw gap (sample level) marking schemes
    //
    for(i=i0,j=j0; i<=iend; ++i,++j)
        this->s[j]+=d.s[i];
    return(*this);
}

void CoreTimeSeries::set_dt(const double sample_interval)
{
  this->BasicTimeSeries::set_dt(sample_interval);
  /* This is the unique name - we always set it. */
  this->put("delta",sample_interval);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("dt");
  for(aptr=aliases.begin();aptr!=aliases.end();++aptr)
  {
    if(this->is_defined(*aptr))
    {
      this->put(*aptr,sample_interval);
    }
  }
}
void CoreTimeSeries::set_t0(const double t0in)
{
  this->BasicTimeSeries::set_t0(t0in);
  /* This is the unique name - we always set it. */
  this->put("starttime",t0in);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("t0");
  aliases.insert("time");
  for(aptr=aliases.begin();aptr!=aliases.end();++aptr)
  {
    if(this->is_defined(*aptr))
    {
      this->put(*aptr,t0in);
    }
  }
}
void CoreTimeSeries::set_npts(const size_t npts)
{
  this->BasicTimeSeries::set_npts(npts);
  /* This is the unique name - we always set it. Cast is necessary to
  avoid type mismatch in python for unsigned*/
  this->put("npts",(long int)npts);
  /* these are hard coded aliases for sample_interval */
  std::set<string> aliases;
  std::set<string>::iterator aptr;
  aliases.insert("nsamp");
  aliases.insert("wfdisc.nsamp");
  for(aptr=aliases.begin();aptr!=aliases.end();++aptr)
  {
    if(this->is_defined(*aptr))
    {
      this->put(*aptr,npts);
    }
  }
  /* this method has the further complication that npts sets the size of the
  data buffer.  We clear it an initialize it to 0 to be consistent with
  how constructors handle this. */
  this->s.clear();
  for(size_t i=0;i<npts;++i)this->s.push_back(0.0);
}

double CoreTimeSeries::operator[](size_t i) const
{
    if(!mlive)
        throw MsPASSError(string("CoreTimeSeries operator[]: attempting to access data marked as dead"),
                          ErrorSeverity::Invalid);
    if( (i<0) || (i>=s.size()) )
    {
        throw MsPASSError(
            string("CoreTimeSeries operator[]:  request for sample outside range of data"),
            ErrorSeverity::Invalid);
    }
    return(s[i]);
}

} // End MsPASS namespace declaration
