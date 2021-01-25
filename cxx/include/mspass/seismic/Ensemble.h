#ifndef _MSPASS_ENSEMBLE_H_
#define _MSPASS_ENSEMBLE_H_
#include <vector>
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass::seismic{
template <typename Tdata> class Ensemble : public mspass::utility::Metadata
{
public:
  /*! \brief Container holding data objects.

  An ensemble is defined as a collection of data objects linked by
  some property that is normally defined by data sharing a comomon
  data-value pair.  The data are stored here in a vector container.
  We make this vector public because the assumption is the member
  objects can be large and copying with a getter would be a serious
  inefficiently.
  */
  std::vector<Tdata> member;
  /*! Default constructor. */
  Ensemble(){};
  /*! \brief Reserve space but build empty ensemble.

  Often we know what the size of an ensemble is before we start
  building it.  This constructor reserve space but marks all members
  dead.

  \param - expected number of members.
  */
  Ensemble(const size_t n){member.reserve(n);};
  /*! Partial constructor to clone metadata and set aside n slots but no data*/
  Ensemble(const mspass::utility::Metadata& md,const size_t n) : mspass::utility::Metadata(md)
  {
      member.reserve(n);
  };
  /*! Standard copy constructor. */
  Ensemble(const Ensemble& parent) : mspass::utility::Metadata(dynamic_cast<const mspass::utility::Metadata&>(parent)),
    member(parent.member){};
  /*! Standard assignment operator. */
  Ensemble& operator=(const Ensemble& parent)
  {
    if(this!=(&parent))
    {
      this->Metadata::operator=(parent);
      member=parent.member;
    }
    return *this;
  };
  /* \brief Indexing operator.

  This is the indexing operator used to extract an ensemble member
  with a (simpler) construct like x=e[i] as opposed to x=e.member[i].
  The later is possible because we (intionally) make the member Container
  public, but this operator can be used to write code that follows common
  guidelines to not make data public.   It will be used for the python
  interface.

  \param n  is the member to be extracted and returned.
  */
  Tdata& operator[](const size_t n) const
  try{
    return(this->member[n]);
  }catch(...){throw;};
  /*! \brief updates ensemble header (Metadata).

  Sometime it is helpful to change a group of header attributes stored
  in the Metadata component.   Rather than do a bunch of puts this uses
  operator += to update the header.  Note this operator uses the property
  that any existing attributes with the same key as one in the updates
  set will be replaced with the update.

  \param newmd contains new Metadata to use for updates.

  */
  void update_metadata(const mspass::utility::Metadata& newmd)
  try{
    mspass::utility::Metadata *md;
    md=dynamic_cast<mspass::utility::Metadata*>(this);
    (*md) += newmd;
  }catch(...){throw;};
  /*! \brief copy ensemble metadata to all members.

    An ensemble has global metadata, but each member is required to have
    a metadata component.  This method takes the ensemble metadata and
    copies it to each of the member objects.   The operation will overwrite
    previous key:value pairs in a member that are also present in the
    ensemble metadata.
    */
  void sync_metadata()
  {
      size_t i;
      for(i=0;i<this->member.size();++i)
      {
          mspass::utility::Metadata *mdmember=&(this->member[i]);
          (*mdmember)+=dynamic_cast<mspass::utility::Metadata&>(*this);
      }
  };
  /*! \brief copy ensemble metadata to all members except for the ones excluded.
    */
  void sync_metadata(std::vector<std::string> exclude)
  {
    mspass::utility::Metadata sync_md(*this);
    for (size_t i = 0; i < exclude.size(); i++)
    {
      if(sync_md.is_defined(exclude[i])) {
          sync_md.erase(exclude[i]);
      }
    }
    for (size_t i = 0; i < this->member.size(); ++i)
    {
      mspass::utility::Metadata *mdmember = &(this->member[i]);
      (*mdmember) += sync_md;
    }
  };
};
/*! Useful alias for Ensemble<TimeSeries> */
typedef Ensemble<TimeSeries> TimeSeriesEnsemble;
/*! Useful alias for Ensemble<Seismogram> */
typedef Ensemble<Seismogram> ThreeComponentEnsemble;
/*! Procedure to set inputs vector defined by an Ensemble object.

Ensembles are the generalization in MsPaSS of a "gather" as used in
seismic reflection processing.  An Ensemble is just a vector of data objects.
This procedure assumes the member of the ensemble have ProcessingHistory
as a base class so they can access the history mechanism datta in MsPaSS.
The algorithm simply copies all history data from each live member to
the inputs vector of ProcessingHistoryRecord.

Do not use this procedure if the algorithm receiving the Ensemble can
ignore members of the Ensemble and mark them dead.  e.g. a robust
stacker might use a trimmed mean that discards some data without an
explicit kill.   Use the procedure that works on the atomic object, Tdata,
instead in such a situation.

\param rec is the output ProcessingHistoryRecord where history will be
  copied to inputs.  (note the operation is append.  If inputs has
  previous content the new data will be appended)
\param d is Ensemble which is to be defined as input.
*/
/* Disable temporarily - needs a revision to match new history approach
template <typename Tdata>
  size_t set_inputs(ProcessingHistoryRecord& rec, const mspass::seismic::Ensemble<Tdata>& d)
{
  try{
    for(size_t i=0;i<d.member.size();++i)
    {
      // Ignore any data not defined as live
      if(d.member[i].live())
      {
        list<ProcessingHistoryRecord> h(d.member[i].history());
        // do nothing if the container is empty
        if(h.size()>0)rec.inputs.push_back(h);
      }
    }
    return rec.inputs.size();
  }catch(...){throw;};
};
*/
}  // End mspass::seismic namespace encapsulation
#endif  //  End guard
