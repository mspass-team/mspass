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
Seismogram::Seismogram(const Metadata& md)
	: CoreSeismogram(md,true),MsPASSCoreTS()
{
  /* We use oid_string to hold the ObjectID stored as a hex string.
   * In mspass this is best done in the calling python function
   * that already has hooks for mongo.   If that field is not 
   * defined we silently set the id invalid.   
   */
  try{
    string oids=this->get_string("oid_string");
    this->set_id(oids);
  }catch(MsPASSError& merr)
  {
    this->set_id("invalid");
  }
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
