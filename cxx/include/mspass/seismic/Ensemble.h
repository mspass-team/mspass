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


/*! \brief Template class that extends Ensemble to include an error log and live tests.

This class extends the Ensemble class defined in MsPASS for bundling
collections of data that can be pushed to an std::vector container.  The
class is actually very generic, but mspass defines it for TimeSeries and
Seismogram objects.   This class is a necessary extension for an algorithm
that take ensembles in and emits a different ensemble.  In that situation
we need a way to indicate the entire ensemble is invalid and not be to used
and post error messages that give an explanation of why the output is invalid.
This class provides that mechanism by adding a mspass::utility::ErrorLogger
and a (private) boolean defining if the ensemble has valid data.  That makes
the API to a LoggingEnsemble a hybrid with the atomic Seismogram and
TimeSeries objects (adds the dna of ErrorLogger and live/dead concept).

A secondary fundamental reason this class can be needed is to parallelize
algorithms that emit an ensemble (regardless of inputs).  The live/dead
tests and error logger make the handlers consistent with the atomic objects
of mspass.
*/
template <typename T> class LoggingEnsemble : public Ensemble<T>
{
public:
  /*! Standard mspass container for holding error logs. */
  mspass::utility::ErrorLogger elog;
  /*! Default constructor.   Initializes all pieces with default constructor
  and marks the ensemble dead.*/
  LoggingEnsemble(): Ensemble<T>(), elog()
  {
    ensemble_is_live=false;
  };

  /* The next two constructors are used to provide a parallel qpi to
  CoreEnsemble - there might be another way to do this in the C++ code or
  in pybind11 wrappers but this works*/
  /*! Reserve slots but otherwise create and empty ensemble marked dead.

  This constructor is a minor variant from the default constructor that
  calls reserve to set aside n slotes for the data.   Use this one when
  you know the size of the ensemble you want to create.  Note after the
  ensemble, like the default constructor, is marked initially as dead.
  The user must call the set_live method of this ensemble after loading
  data or downstream processors may drop the data. */
  LoggingEnsemble(const size_t n) : Ensemble<T>(n),elog()
  {
    ensemble_is_live=false;
  }
  /*! Construct a framework for the ensemble with metadata.

  This constructor loads the metadata received into the ensmeble area
  and reserves n slots to be added.   Be warned it marks the ensemble dead.
  Once valid data are loaded the user should call the set_live method for
  the ensemble to prevent the data from being ignored downstream.*/
  LoggingEnsemble(const mspass::utility::Metadata& md,const size_t n)
    : Ensemble<T>(md,n),elog()
  {
    /* Might be advised to set this true, but since an object created by
    this method has only slots but no data validate would return false.*/
    ensemble_is_live=false;
  }
  /*! Standard copy constructor.   */
  LoggingEnsemble(const LoggingEnsemble<T>& parent)
          : Ensemble<T>(parent),elog(parent.elog)
  {
    ensemble_is_live=parent.ensemble_is_live;
  };
  /*! Clone from a base class Ensemble.  Initializes error null and sets live. */
  LoggingEnsemble(const Ensemble<T>& parent)
          : Ensemble<T>(parent),elog()
 {
   ensemble_is_live=true;
 };
  /*! Markt the entire ensemble bad. */
  void kill(){ensemble_is_live=false;};
  /*! Getter to test if the ensemble has any valid data. */
  bool live(){return ensemble_is_live;};
  /*! Complement to live method - returns true if there are no valid data members. */
  bool dead(){return !ensemble_is_live;};
  /*! Force, with care, the ensemble to be marked live.

  This extension of CoreEnsemble adds a boolean that is used to test if
  an entire ensemble should be treated as dead or alive.  It first runs the
  validate method.  If validate is false it refuses to set the state
  live and returns false.  If validate returns true it will return true.
   */
  bool set_live(){
    if(this->validate())
    {
      ensemble_is_live=true;
      return true;
    }
    else
      return false;
  };
  /*! Check to see if ensemble has any live data.

  In processing once can occasionally (not rare but not common either)
  end up with enemble full of data marked dead.  This is a convenience
  method to check all members.  If it finds any live member it will immediately
  return true (ok).  If after a search of the entire ensemble no live members
  are found it will return false AND then mark the entire ensemble bad. */
  bool validate();
  /*! Standard assignment operator. */
  LoggingEnsemble<T>& operator=(const LoggingEnsemble<T>& parent)
  {
    if(&parent != this)
    {
      Ensemble<T> *baseptr
                =dynamic_cast<Ensemble<T>>(this);
      baseptr->operator=(parent);
      elog=parent.elog;
      ensemble_is_live=parent.ensemble_is_live;
    }
    return *this;
  };
private:
  bool ensemble_is_live;
};

template <typename T> bool LoggingEnsemble<T>::validate()
{
  for(auto dptr=this->member.begin();dptr!=this->member.end();++dptr)
  {
    if(dptr->live()) return true;
  }
  return false;
}

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
