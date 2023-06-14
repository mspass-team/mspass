#include "mspass/seismic/keywords.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/memory_constants.h"
namespace mspass::seismic
{
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;

TimeSeries::TimeSeries(const CoreTimeSeries& d, const std::string alg)
    : CoreTimeSeries(d),ProcessingHistory()
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
  const ProcessingHistory& his,const vector<double>& d)
    : CoreTimeSeries(b,m),ProcessingHistory(his)
{
  this->s=d;
}
TimeSeries::TimeSeries(const Metadata& md) : ProcessingHistory()
{
    mlive=false;
    try {
        this->Metadata::operator=(md);
        /* Names used are from mspass defintions as of Jan 2020.
        We don't need to call the set methods for these attributes as they
        would add the overhead of setting delta, startime, and npts to the
        same value passed. */
        this->mdt = this->get_double(SEISMICMD_dt);
        this->mt0 = this->get_double(SEISMICMD_t0);
        if(this->is_defined(SEISMICMD_time_standard))
        {
          if(this->get_string(SEISMICMD_time_standard) == "UTC")
            this->set_tref(TimeReferenceType::UTC);
          else
          {
            this->set_tref(TimeReferenceType::Relative);
            this->elog.log_error("TimeSeries Metadata constructor",
              SEISMICMD_time_standard+" attribute is not defined - set to Relative",
              ErrorSeverity::Complaint);
          }
        }
        if(this->time_is_relative())
        {
          /* It is not an error if a t0 shift is not defined and we are
          in relative time. That is the norm for active source data. */
          if(this->is_defined(SEISMICMD_t0_shift))
          {
            double t0shift=this->get_double(SEISMICMD_t0_shift);
            this->force_t0_shift(t0shift);
          }
        }
        /* this default construct is needed to handle miniseed data in
        MsPASS.  It perhaps should generate a log message a information
        but for now we do this silently.   since the constructor returns
        a result marked dead in all cases a default of 0 is sensible.*/
        long int ns;
        if(md.is_defined(SEISMICMD_npts))
        {
          ns = md.get_long(SEISMICMD_npts);
        }
        else
        {
          ns = 0;
        }
        /* this CoreTimeSeries method sets the npts attribute and
        initializes the s buffer to all zeros */
        this->set_npts(ns);
    }catch(...) {throw;};
}
TimeSeries& TimeSeries::operator=(const TimeSeries& parent)
{
    if(this!=(&parent))
    {
        this->CoreTimeSeries::operator=(parent);
        this->ProcessingHistory::operator=(parent);
    }
    return *this;
}
void TimeSeries::load_history(const ProcessingHistory& h)
{
  this->ProcessingHistory::operator=(h);
}
size_t TimeSeries::memory_use() const
{
  size_t memory_estimate;
  memory_estimate = sizeof(TimeSeries);
  memory_estimate += sizeof(double)*this->npts();
  /* We can only estimate the size of the Metadata container.
  These constants are defined in memory_constants.h */
  memory_estimate += memory_constants::MD_AVERAGE_SIZE*this->md.size();
  memory_estimate += memory_constants::KEY_AVERAGE_SIZE*this->changed_or_set.size();
  /* Similar for history and elog containers */
  memory_estimate += memory_constants::HISTORYDATA_AVERAGE_SIZE*this->nodes.size();
  memory_estimate += memory_constants::ELOG_AVERAGE_SIZE*this->elog.size();
  return memory_estimate;
}
}// end mspass namespace
