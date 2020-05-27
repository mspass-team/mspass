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
    // This seems to be necessary at least for Sun's compiler
    for(size_t i=0; i<nsin; ++i)s.push_back(0.0);
}


CoreTimeSeries::CoreTimeSeries(const CoreTimeSeries& tsi) :
    BasicTimeSeries(tsi),
    Metadata(tsi)
{
    if(live)
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
    size_t i;
    this->s.reserve(this->ns);   // ns should be set by BasicTimeSeries constructor
    for(i=0; i<this->ns; ++i)
        this->s.push_back(0.0);
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
    if(!d.live) return(*this);
    // Silently do nothing if d does not overlap with data to contain sum
    if( (d.endtime()<t0)
            || (d.t0>(this->endtime())) ) return(*this);
    if(d.tref!=(this->tref))
        throw MsPASSError("CoreTimeSeries += operator cannot handle data with inconsistent time base\n",
                          ErrorSeverity::Invalid);
    //
    // First we have to determine range fo sum for d into this
    //
    i0=d.sample_number(this->t0);
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
double CoreTimeSeries::operator[](size_t i) const
{
    if(!live)
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
