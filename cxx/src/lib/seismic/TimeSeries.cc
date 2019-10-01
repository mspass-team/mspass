#include <vector>
#include "mspass/MsPASSError.h"
#include "seismic/TimeSeries.h"
#include "mspass/Metadata.h"
namespace mspass
{
using namespace std;
using namespace mspass;
//
// simple constructors for the TimeSeries object are defined inline
// in seispp.h.
//
TimeSeries::TimeSeries() : BasicTimeSeries(), Metadata()
{
    s.reserve(0);
}
TimeSeries::TimeSeries(int nsin) : BasicTimeSeries(), Metadata()
{
    s.reserve(nsin);
    // This seems to be necessary at least for Sun's compiler
    for(int i=0; i<nsin; ++i)s.push_back(0.0);
}

/*
TimeSeries::TimeSeries(const TimeSeries& tsi) :
		BasicTimeSeries(dynamic_cast<const BasicTimeSeries&>(tsi)),
		Metadata(dynamic_cast<const Metadata&>(tsi))
*/
TimeSeries::TimeSeries(const TimeSeries& tsi) :
    BasicTimeSeries(tsi),
    Metadata(tsi)
{
    if(live)
    {
        s=tsi.s;
    }
}

TimeSeries::TimeSeries(const BasicTimeSeries& bd,const Metadata& md)
    : BasicTimeSeries(bd), Metadata(md)
{
    int i;
    this->s.reserve(this->ns);   // ns should be set by BasicTimeSeries constructor
    for(i=0; i<this->ns; ++i)
        this->s.push_back(0.0);
}
// standard assignment operator
TimeSeries& TimeSeries::operator=(const TimeSeries& tsi)
{
    if(this!=&tsi)
    {
        this->BasicTimeSeries::operator=(tsi);
        this->Metadata::operator=(tsi);
        if(tsi.live)
        {
            s=tsi.s;
        }
    }
    return(*this);
}
/*  Sum operator for TimeSeries object */

void TimeSeries::operator+=(const TimeSeries& data)
{
    int i,i0,iend;
    int j,j0=0;
    // Sun's compiler complains about const objects without this.
    TimeSeries& d=const_cast<TimeSeries&>(data);
    // Silently do nothing if d is marked dead
    if(!d.live) return;
    // Silently do nothing if d does not overlap with data to contain sum
    if( (d.endtime()<t0)
            || (d.t0>(this->endtime())) ) return;
    if(d.tref!=(this->tref))
        throw MsPASSError("TimeSeries += operator cannot handle data with inconsistent time base\n",
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
    if(iend>(d.ns-1))
    {
        iend=d.ns-1;
    }
    //
    // IMPORTANT:  This algorithm simply assumes zero_gaps has been called
    // and/or d was checked for gaps befor calling this operatr.
    // It will produce garbage for most raw gap (sample level) marking schemes
    //
    for(i=i0,j=j0; i<iend; ++i,++j)
        this->s[j]+=d.s[i];
}
double TimeSeries::operator[](int i) const
{
    if(!live)
        throw MsPASSError(string("TimeSeries operator[]: attempting to access data marked as dead"),
                          ErrorSeverity::Invalid);
    if( (i<0) || (i>=ns) )
    {
        throw MsPASSError(
            string("TimeSeries operator[]:  request for sample outside range of data"),
            ErrorSeverity::Invalid);
    }
    return(s[i]);
}

} // End MsPASS namespace declaration
