#include "Seismogram.h"
using namespace mspass;
namespace mspass
{
Seismogram& Seismogram::operator=(const Seismogram& parent)
{
    if(this!=(&parent))
    {
        this->Metadata::operator=(parent);
        this->CoreSeismogram::operator=(parent);
        this->MdPASSCoreTS::operator=(parent);
    }
}
}// end mspass namespace
