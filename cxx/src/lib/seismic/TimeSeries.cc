#include "TimeSeries.h"
using namespace mspass;
namespace mspass
{
TimeSeries& TimeSeries::operator=(const TimeSeries& parent)
{
    if(this!=(&parent))
    {
        this->Metadata::operator=(parent);
        this->CoreTimeSeries::operator=(parent);
        this->MdPASSCoreTS::operator=(parent);
    }
}
}// end mspass namespace
