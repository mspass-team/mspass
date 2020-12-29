#ifndef _TIMESERIES_H_
#define _TIMESERIES_H_
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/ProcessingHistory.h"
namespace mspass::seismic{
  /*! \brief Implemntation of TimeSeries for MsPASS.

  This is the working version of a three-component TimeSeries object used
  in the MsPASS framework.   It extends CoreTimeSeries by adding
  common MsPASS components (ProcessingHistory).  It may evolve with additional
  special features.  */
class TimeSeries : virtual public mspass::seismic::CoreTimeSeries,
   public mspass::utility::ProcessingHistory
{
public:
  /*! Default constructor.   Only runs subclass default constructors. */
  TimeSeries() : mspass::seismic::CoreTimeSeries(),
     mspass::utility::ProcessingHistory()
  {};
  /*! Bare bones constructor allocates space and little else.

  Sometimes it is helpful to construct a skeleton that can be fleshed out
  manually.   This constructor allocates an nsamples vector and
  initializes it to all zeros.   The data are marked dead
  because the assumption is the caller will fill out commonly needed basic
  Metadata, load some kind of sample data into the data vector, and then
  call the set_live method when that process is completed.   That kind of
  manipulation is most common in preparing simulation or test data where
  the common tags on real data do not exist and need to be defined manually
  for the simulatioon or test.   The history section is initialized with
  the default constructor, which currently means it is empty.   If a simulation
  or test requires a history origin the user must load it manaually.

  \param nsamples is the number of samples needed for storing the data vector.
    */
  TimeSeries(const size_t nsamples) : mspass::seismic::CoreTimeSeries(nsamples),
      mspass::utility::ProcessingHistory()
  {};
  /*! Partially construct from components.

      There are times one wants to use the Metadata area as a template to
      flesh out a CoreTimeSeries as what might be called skin and bones:  skin is
      Metadata and bones as BasicTimeSeries data.   This constructor initializes
      those two base classes but does not fully a valid data vector.  It only
      attempts to fetch the number of points expected for the data vector using
      the npts metadata (integer) key (i.e. it sets npts to md.get_int("npts")).
      It then creates the data vector of that length and initialzies it to all zeros.

      This constructor is largely a wrapper for the CoreTimeSeries version
      with the same signature but it also initalizes the ProcessingHistory
      to null (calling the default constructor)*/
  TimeSeries(const BasicTimeSeries& bts,const Metadata& md)
      : mspass::seismic::CoreTimeSeries(bts,md),
         mspass::utility::ProcessingHistory()
  {};

  /*!  \brief Construct from lower level CoreTimeSeries.

  In MsPASS CoreTimeSeries has the primary functions that define the
  concept of a a single channel seismogram.   TimeSeries implements
  mspass specific features needed to mesh with the mspass processing system.
  This constructor clones only the CoreTimeSeries components and initializes
  the ProcessingHistory with the default constructor leaving the history
  in an empty state.   Users should call methods in ProcessingHistory to
  initiate a valid history chain.   Processing can continue if left in
  that state, but the history chain will have an undefined origin.  Job
  information will also be lost if not initialized (see BasicProcessingHistory)

  \param d is the data to be copied to create the new TimeSeries object.  Note
   that means a deep copy wherein the data vector is copied.
   */
  TimeSeries(const mspass::seismic::CoreTimeSeries& d)
    : mspass::seismic::CoreTimeSeries(d),mspass::utility::ProcessingHistory(){};
  /*! Contruct from a core time series and initialize history as origin.

  This constructor is a variant of a similar one built only from a
  CoreTimeSeries.  This constuctor is intended to be used mainly on
  simulation data created by some mechanism (e.g. a python procedure).
  It is more rigid that the simple one arg constructor as it will
  create a top level history record.   The record will mark the result
  as an origin with an id set as a uuid created by a random number
  generator (boost implementation).  The jobname and jobid are
  frozen as "test". Use a different constructor and/or reset job info
  if more flexibility is desired.  Use of this constructor
  is recommended only for test python programs that do not need to
  interact with MongoDB.

  \param is core data to be cloned
  \param alg is the algorithm name to set for the origin history record.
  */
  TimeSeries(const mspass::seismic::CoreTimeSeries& d, const std::string alg);
/*! Special constructor for pickle interface.

The pickle interface required by spark presented problems for MsPASS.  The
complicated data objects of TimeSeries and TimeSeries have to be serialized
in pieces.   This constructor is only used in the function called
indirectly by pickle.load.   It essentially contains a TimeSeries dismembered
into the pieces that can be the serialized independently.   The
parameters are each associated with one of those required pieces and
are simply copied to build a valid TimeSeries object in the pickle.load
function */
  TimeSeries(const mspass::seismic::BasicTimeSeries& b,const mspass::utility::Metadata& m,
          const mspass::utility::ProcessingHistory& mcts, const std::vector<double>& d);
  /*! Standard copy constructor. */
  TimeSeries(const TimeSeries& parent)
    : mspass::seismic::CoreTimeSeries(parent), mspass::utility::ProcessingHistory(parent){};
  /*! Standard assignment operator. */
  TimeSeries& operator=(const TimeSeries& parent);
  TimeSeries& operator+=(const TimeSeries& d)
  {
    dynamic_cast<CoreTimeSeries&>(*this)+=dynamic_cast<const CoreTimeSeries&>(d);
    return(*this);
  };
  TimeSeries& operator*=(const double scale)
  {
    dynamic_cast<CoreTimeSeries&>(*this) *= scale;
    return *this;
  };
  TimeSeries& operator-=(const TimeSeries& d)
  {
    dynamic_cast<CoreTimeSeries&>(*this)-=dynamic_cast<const CoreTimeSeries&>(d);
    return(*this);
  };
  void load_history(const mspass::utility::ProcessingHistory& h);
};
}//END mspass::seismic namespace
#endif
