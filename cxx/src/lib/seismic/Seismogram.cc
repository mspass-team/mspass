#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/ProcessingHistory.h"
namespace mspass::seismic
{
using namespace std;
using namespace mspass::utility;

Seismogram::Seismogram(const CoreSeismogram& d)
    : CoreSeismogram(d),ProcessingHistory()
{
}
Seismogram::Seismogram(const CoreSeismogram& d, const string alg)
    : CoreSeismogram(d),ProcessingHistory()
{
  /* Not sure this is a good idea, but will give each instance
  created by this constructor a uuid.*/
  string id=this->newid();
  this->ProcessingHistory::set_as_origin(alg,id,id,AtomicType::SEISMOGRAM,false);
  this->ProcessingHistory::set_jobname(string("test"));
  this->ProcessingHistory::set_jobid(string("test"));
}
Seismogram::Seismogram(const BasicTimeSeries& b, const Metadata& m,
  const ProcessingHistory& his,const bool card, const bool ortho,
  const dmatrix& tm, const dmatrix& uin)
{
  /* for reasons I couldn't figure out these couldn't appear in the copy
  constructor chain following the : above.   Compiler complained about
  these not being a direct base.  Sure there is a way to fix that, but
  the difference in calling operator= like here is next to nothing.*/
  BasicTimeSeries bts=dynamic_cast<BasicTimeSeries&>(*this);
  bts=BasicTimeSeries::operator=(b);
  Metadata mdthis=dynamic_cast<Metadata&>(*this);
  mdthis=Metadata::operator=(m);
  ProcessingHistory histhis=dynamic_cast<ProcessingHistory&>(*this);
  histhis=ProcessingHistory::operator=(his);
  components_are_cardinal=card;
  components_are_orthogonal=ortho;
  int i,j;
  for(i=0;i<3;++i)
    for(j=0;j<3;++j) tmatrix[i][j]=tm(i,j);
  this->u=uin;
}
Seismogram::Seismogram(const Metadata& md, const string jobname,
    const string jobid, const string readername,const string algid)
	: CoreSeismogram(md,true),ProcessingHistory()
{
  const string algname("SeismogramMDConstructor");
  this->set_jobname(jobname);
  this->set_jobid(jobid);

  /* We try to read uuid from the metadata used in creation with the
  CoreSeismogram constructor.   If it isn't found we generate it from
  the random number generator and post a complaint.
   */
  string thisid;
  try{
    string thisid=this->get_string("uuid");
    this->set_id(thisid);
  }catch(MsPASSError& merr)
  {
    /* this sets the id to a random number based uuid */
    thisid=this->newid();
    this->elog.log_error(algname,"uuid not defined.\nSet by constructor to"
          +thisid,ErrorSeverity::Complaint);
  }
  bool mark_as_raw;
  try{
    mark_as_raw=this->get_bool("rawdata");
  } catch(MsPASSError& merr)
  {
    this->elog.log_error(algname,"rawdata boolean not found - default to false",
      ErrorSeverity::Complaint);
    mark_as_raw=false;
  }
  this->ProcessingHistory::set_as_origin(readername,algid,thisid,
        AtomicType::SEISMOGRAM,mark_as_raw);
}
Seismogram& Seismogram::operator=(const Seismogram& parent)
{
    if(this!=(&parent))
    {
        this->CoreSeismogram::operator=(parent);
        this->ProcessingHistory::operator=(parent);
    }
    return *this;
}
void Seismogram::load_history(const ProcessingHistory& h)
{
  this->ProcessingHistory::operator=(h);
}
}// end mspass namespace
