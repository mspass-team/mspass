#include "mspass/seismic/TimeSeries.h"
using namespace mspass;
namespace mspass
{
TimeSeries::TimeSeries(const CoreTimeSeries& d, const std::string oid)
    : CoreTimeSeries(d)
{
    try{
        this->set_id(oid);
    }catch(...){throw;};
}
/* this is kind of a weird construct because the pieces are assembled
out of the regular order of an object created by inheritance.  I hope
that does not cause problems. */
TimeSeries::TimeSeries(const BasicTimeSeries& b, const Metadata& m,
  const ProcessingHistory& his,const vector<double>& d)
    : CoreTimeSeries(b,m),ProcessingHistory(his)
{
  this->s=d;
}
TimeSeries& TimeSeries::operator=(const TimeSeries& parent)
{
    if(this!=(&parent))
    {
        this->Metadata::operator=(parent);
        this->CoreTimeSeries::operator=(parent);
        this->ProcessingHistory::operator=(parent);
    }
    return *this;
}
}// end mspass namespace
