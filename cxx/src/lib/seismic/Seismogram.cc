#include "mspass/seismic/Seismogram.h"
#include "mspass/utility/ProcessingHistory.h"
using namespace mspass;
namespace mspass
{
Seismogram::Seismogram(const CoreSeismogram& d)
    : CoreSeismogram(d),ProcessingHistory()
{
}
Seismogram::Seismogram(const CoreSeismogram& d, const string alg)
    : CoreSeismogram(d),ProcessingHistory()
{
  this->set_id();
  ProcessingHistoryRecord rec;
  rec.status=ProcessingStatus::ORIGIN;
  rec.algorithm=alg;
  rec.instance="0";
  rec.id=this->id_string();
  this->ProcessingHistory::set_as_origin(rec);
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
  bts=mspass::BasicTimeSeries::operator=(b);
  Metadata mdthis=dynamic_cast<Metadata&>(*this);
  mdthis=mspass::Metadata::operator=(m);
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
  this->set_jobname(jobname);
  this->set_jobid(jobid);

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
    /* this sets the id to a random number based uuid */
    this->set_id();
  }
  ProcessingHistoryRecord rec;
  rec.status=ProcessingStatus::ORIGIN;
  rec.algorithm=readername;
  rec.instance="0";
  rec.id=algid;
  history_list.push_back(rec);
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
}// end mspass namespace
