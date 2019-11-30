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
TimeSeries& TimeSeries::operator=(const TimeSeries& parent)
{
    if(this!=(&parent))
    {
        this->Metadata::operator=(parent);
        this->CoreTimeSeries::operator=(parent);
        this->MsPASSCoreTS::operator=(parent);
    }
    return *this;
}
}// end mspass namespace
