/* This file contains member functions for a BasicTimeSeries object.*/
#include <ostream>
#include "mspass/seismic/BasicTimeSeries.h"
#include "mspass/utility/MsPASSError.h"
namespace mspass::seismic {
using namespace std;
using namespace mspass::utility;

void BasicTimeSeries::ator(double tshift)
{
    /* dead traces should to totally ignored */
    if(!(this->mlive)) return;
    if(tref==TimeReferenceType::Relative) return;
    t0shift=tshift;
    mt0 -= tshift;
    tref=TimeReferenceType::Relative;
    t0shift_is_valid=true;
}
// inverse of ator -- note minus becomes plus
// everything else is nearly identical
void BasicTimeSeries::rtoa()
{
    /* dead traces should to totally ignored */
    if(!(this->mlive)) return;
    const string base_error("BasicTimeSeries::rtoa() t0shift for conversion is not defined.");
    if(tref==TimeReferenceType::UTC) return;
    /* A rather odd test for a nonzero.   We use 100 s assuming no active
     * source data would use a shift longer than that unless it really did
     * have an UTC time standard. Also assumes we'll never use data from
     * the first 2 minutes of 1960.*/
    if(t0shift_is_valid || (t0shift>100.0) )
    {
        mt0 += t0shift;
        tref=TimeReferenceType::UTC;
        t0shift_is_valid=false;
	t0shift=0.0;
    }
    else
        throw MsPASSError(base_error + "time shift to return to UTC time is not defined",ErrorSeverity::Invalid);
}

BasicTimeSeries::BasicTimeSeries()
{
    mt0=0.0;
    tref=TimeReferenceType::Relative;
    mlive=false;
    mdt=1.0;
    nsamp=0;
    t0shift=0.0;
    t0shift_is_valid=false;
}
BasicTimeSeries::BasicTimeSeries(const BasicTimeSeries& tsin)
{
    mt0=tsin.mt0;
    tref=tsin.tref;
    mlive=tsin.mlive;
    mdt=tsin.mdt;
    nsamp=tsin.nsamp;
    t0shift=tsin.t0shift;
    t0shift_is_valid=tsin.t0shift_is_valid;
}
BasicTimeSeries& BasicTimeSeries::operator=(const BasicTimeSeries& parent)
{
    if (this!=&parent)
    {
        mt0=parent.mt0;
        tref=parent.tref;
        mlive=parent.mlive;
        mdt=parent.mdt;
        nsamp=parent.nsamp;
        t0shift=parent.t0shift;
        t0shift_is_valid=parent.t0shift_is_valid;
    }
    return *this;
}
void BasicTimeSeries::shift(double dt)
{
    try {
        double oldt0shift=t0shift;
        this->rtoa();
        this->ator(oldt0shift+dt);
    } catch(...) {
        throw;
    };
}
double BasicTimeSeries::time_reference() const
{
    const string base_error("BasicTimeSeries::time_reference method: ");
    if(tref==TimeReferenceType::UTC)
        throw MsPASSError(base_error
                          + "data have UTC time set so requesting the reference"
                          + " time make no sense - likely a coding error",
                          ErrorSeverity::Fatal);
    if(t0shift_is_valid)
        return(t0shift);
    else
        throw MsPASSError(base_error
                          + "cannot return time reference as it is marked invalid",
                          ErrorSeverity::Invalid);
}
}  // end mspass namespace encapsulation
