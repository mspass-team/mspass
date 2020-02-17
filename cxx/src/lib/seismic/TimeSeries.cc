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
TimeSeries::TimeSeries(const BasicTimeSeries& b, const Metadata& m,
        const ErrorLogger elf)
{
    /* Have to use this construct instead of : and a pair of
       copy constructors for Metadata and BasicTimeSeries.   Compiler
       complains they are not a direct or virtual base for TimeSeries.  */
    this->BasicTimeSeries::operator=(b);
    this->Metadata::operator=(m);
    elog=elf;
    this->set_id("INVALID");
}
/* this is kind of a weird construct because the pieces are assembled
out of the regular order of an object created by inheritance.  I hope
that does not cause problems. */
TimeSeries::TimeSeries(const BasicTimeSeries& b, const Metadata& m,
  const MsPASSCoreTS& mcts,const vector<double>& d)
     : MsPASSCoreTS(mcts)
{
  /* This seems necessary due to ambiguities of multiple inheritance. */
  this->BasicTimeSeries::operator=(b);
  this->Metadata::operator=(m);
  this->s=d;
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
