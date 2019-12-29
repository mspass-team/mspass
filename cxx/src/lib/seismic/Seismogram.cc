#include "mspass/seismic/Seismogram.h"
using namespace mspass;
namespace mspass
{
Seismogram::Seismogram(const CoreSeismogram& d, const std::string oid)
    : CoreSeismogram(d)
{
    try{
        this->set_id(oid);
    }catch(...){throw;};
}
Seismogram::Seismogram(const BasicTimeSeries& b, const Metadata& m,
        const ErrorLogger elf)
{
    /* Have to use this construct instead of : and a pair of 
       copy constructors for Metadata and BasicSeismogram.   Compiler
       complains they are not a direct or virtual base for Seismogram.  */
    this->BasicTimeSeries::operator=(b);
    this->Metadata::operator=(m);
    elog=elf;
    this->set_id("INVALID");
}
Seismogram& Seismogram::operator=(const Seismogram& parent)
{
    if(this!=(&parent))
    {
        this->CoreSeismogram::operator=(parent);
        this->MsPASSCoreTS::operator=(parent);
    }
    return *this;
}
}// end mspass namespace
