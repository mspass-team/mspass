#include "mspass/seismic/TimeSeries.h"
namespace mspass::seismic
{
using namespace std;
using namespace mspass::utility;

TimeSeries::TimeSeries(const CoreTimeSeries& d, const std::string alg)
    : CoreTimeSeries(d),ProcessingHistory(),elog()
{
  /* Not sure this is a good idea, but will give each instance
  created by this constructor a uuid.*/
  string id=this->newid();
  this->ProcessingHistory::set_as_origin(alg,id,id,AtomicType::SEISMOGRAM,false);
  this->ProcessingHistory::set_jobname(string("test"));
  this->ProcessingHistory::set_jobid(string("test"));
}
/* this is kind of a weird construct because the pieces are assembled
out of the regular order of an object created by inheritance.  I hope
that does not cause problems. */
TimeSeries::TimeSeries(const BasicTimeSeries& b, const Metadata& m,
  const ErrorLogger& elg, const ProcessingHistory& his,const vector<double>& d)
    : ProcessingHistory(his),elog(elg)
{
  this->BasicTimeSeries::operator=(b);
  this->Metadata::operator=(m);
  this->s=d;
}
TimeSeries& TimeSeries::operator=(const TimeSeries& parent)
{
    if(this!=(&parent))
    {
        this->elog=parent.elog;
        this->CoreTimeSeries::operator=(parent);
        this->ProcessingHistory::operator=(parent);
    }
    return *this;
}
void TimeSeries::load_history(const ProcessingHistory& h)
{
  this->ProcessingHistory::operator=(h);
}
}// end mspass namespace
