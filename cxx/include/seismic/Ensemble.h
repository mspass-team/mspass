#ifndef _MSPASS_ENSEMBLE_H_
#define _MSPASS_ENSEMBLE_H_
#include <vector>
#include "seismic/TimeSeries.h"
#include "seismic/Seismogram.h"
namespace mspass{
template <typename Tdata> class Ensemble : public Metadata
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
  vector<Tdata> member;
  /*! Default constructor. */
  Ensemble(){};
  /*! \brief Reserve space but build empty ensemble.

  Often we know what the size of an ensemble is before we start
  building it.  This constructor reserve space but marks all members
  dead.

  \param - expected number of members.
  */
  Ensemble(const int n){member.reserve(n);};
  /*! Partial constructor to clone metadata and set aside n slots but no data*/
  Ensemble(const Metadata& md,const int n) : Metadata(md)
  {
      member.reserve(n);
  };
  /*! Standard copy constructor. */
  Ensemble(const Ensemble& parent) : Metadata(dynamic_cast<Metadata&>(parent)),
    member(parent.member){};
  /*! Standard assignment operator. */
  Ensemble& operator=(const Ensemble& parent)
  {
    if(this!=(&parent))
    {
      this->Metadata::operator=(parent);
      member=parent.member;
    }
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
  Tdata& operator[](const int n) const
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

  \return number of attributes updated (size of newmd)
  */
  int update_metadata(const Metadata& newmd)
  try{
    Metadata *md;
    md=dynamic_cast<Metadata*>(this);
    (*md) += newmd;
  }catch(...){throw;};
};
/*! \brief  Returns a gather of Seismograms in an arrival time reference fram.

 An arrival time reference means that the time is set to relative and 
 zero is defined as an arrival time extracted from the metadata area of
 each member object.

\exception SeisppError for errors in extracting required information from metadata area.

\param din  is input gather
\param key is the metadata key used to find the arrival time to use as a reference.
\param tw is a TimeWindow object that defines the window of data to extract around
    the desired arrival time.
**/
/*
typedef mspass::Ensemble<Seismogram> ThreeComponentEnsemble;
std::shared_ptr<ThreeComponentEnsemble> ArrivalTimeReference
  (ThreeComponentEnsemble& din,std::string key, TimeWindow tw);
  */
}  // End mspass namespace encapsulation
#endif  //  End guard
