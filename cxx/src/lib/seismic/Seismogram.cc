#include <string>
#include "mspass/seismic/keywords.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass::seismic
{
using namespace std;
using namespace mspass::utility;
Seismogram::Seismogram(const size_t nsamples)
   : CoreSeismogram(nsamples),ProcessingHistory()
{
/* Note this constructor body needs no content.  Just a wrapper for CoreSeismogram */
}
/* For some weird reason we can't call the parallel constructors for
CoreSeismogram.  Instead we have to call the constructors for the base class.*/
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
Seismogram::Seismogram(const BasicTimeSeries& bts, const Metadata& md)
  : CoreSeismogram(md,false),ProcessingHistory()
{
  /* the contents of BasicTimeSeries passed will override anything set from
  Metadata in this section.  Note also the very important use of this
  for the putters to assure proper resolution of the virtual methods */
  this->kill();   //set dead because buffer has invalid data
  this->set_t0(bts.t0());
  this->set_tref(bts.timetype());
  this->set_npts(bts.npts());
  /* The handling of these is kind of awkward in the current api.  Good to
  hide it here.   Note in current implementation force_t0_shift sets the
  shift as valid (what shifted tests).  I am slightly worried there are
  cases where the else block could create an inconsistency with active
  source data if this method were used to convert raw data.  Careful if
  this is used in that context where the time reference is defined
  implicitly as shot time with no tie to an absolute time reference */
  if(bts.shifted())
  {
    double t0shift;
    t0shift=bts.time_reference();
    this->force_t0_shift(t0shift);
  }
  else
  {
    this->force_t0_shift(0.0);
  }
}
/* Note that the : notation listing base classes only works if
CoreSeismogram has public virtual.  Without that declaration in the .h
this would generate compiler errors complaining that x is not a direct
base of CoreSeismogram.   For some mysterious, probably related reason,
I couldn't get the dmatrix u to be allowed in the copy construct
sequence - following the :.  Minor performance hit duplicating a
default construction of u before calling operator= on the last line of
this constructor.*/
Seismogram::Seismogram(const BasicTimeSeries& b, const Metadata& m,
  const ProcessingHistory& his,const bool card, const bool ortho,
  const dmatrix& tm, const dmatrix& uin)
  : ProcessingHistory(his)
{
  this->BasicTimeSeries::operator=(b);
  this->Metadata::operator=(m);
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
    string thisid=this->get_string(SEISMICMD_uuid);
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
    mark_as_raw=this->get_bool(SEISMICMD_rawdata);
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
