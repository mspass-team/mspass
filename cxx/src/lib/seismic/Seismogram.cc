#include "mspass/seismic/Seismogram.h"
using namespace mspass;
namespace mspass
{
Seismogram::Seismogram(const CoreSeismogram& d, const std::string oid)
    : CoreSeismogram(d),MsPASSCoreTS()
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
Seismogram::Seismogram(const BasicTimeSeries& b, const Metadata& m,
  const MsPASSCoreTS& corets,const bool card, const bool ortho,
  const dmatrix& tm, const dmatrix& uin)
    : CoreSeismogram(),MsPASSCoreTS(corets)
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
Seismogram::Seismogram(const Metadata& md)
	: CoreSeismogram(md,true),MsPASSCoreTS()
{
  /* We use oid_string to hold the ObjectID stored as a hex string.
   * In mspass this is best done in the calling python function
   * that already has hooks for mongo.   If that field is not
   * defined we silently set the id invalid.
   */
  try{
    string oids=this->get_string("wf_id");
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
