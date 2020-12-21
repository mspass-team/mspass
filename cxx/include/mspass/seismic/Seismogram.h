#ifndef _SEISMOGRAM_H_
#define _SEISMOGRAM_H_
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/utility/ProcessingHistory.h"
#include "mspass/utility/ErrorLogger.h"

namespace mspass::seismic{
/*! \brief Implemntation of Seismogram for MsPASS.

This is the working version of a three-component seismogram object used
in the MsPASS framework.   It extends CoreSeismogram by adding
ProcessingHistory.   */
class Seismogram : virtual public mspass::seismic::CoreSeismogram,
   public mspass::utility::ProcessingHistory
{
public:
  /*! Error logging object.

  In MsPASS we use an error logger attached to atomic data objects to
  provide a mechanism to move nonfatal errors along with data.  This allows
  errors to be posted in a parallel environment and not be lost of jumbled
  up by simultaneous writes by multiple threads to the same buffer
  (stdio functions, for example, aren't thread safe and have that issue)
  */
  mspass::utility::ErrorLogger elog;
  /*! Default constructor.   Only runs subclass default constructors. */
  Seismogram() : mspass::seismic::CoreSeismogram(),mspass::utility::ProcessingHistory(){};
  /*! Bare bones constructor allocates space and little else.

  Sometimes it is helpful to construct a skeleton that can be fleshed out
  manually.   This constructor allocates a 3xnsamples array and sets the
  matrix to all zeros.  It also sets the orientation information to cardinal
  and defines the data in relative time units.  The data are marked dead
  because the assumption is the caller will fill out commonly needed basic
  Metadata, load some kind of sample data into the data matrix, and then
  call the set_live method when that process is completed.   That kind of
  manipulation is most common in preparing simulation or test data where
  the common tags on real data do not exist and need to be defined manually
  for the simulatioon or test.   The history section is initialized with
  the default constructor, which currently means it is empty.   If a simulation
  or test requires a history origin the user must load it manaually.

  \param nsamples is the number of 3c vector samples to iniitalize the
    object with (creates a 3xnsamples matrix).
    */
  Seismogram(const size_t nsamples);
  /*! \brief Construct from lower level CoreSeismogram.

  In MsPASS CoreSeismogram has the primary functions that define the
  concept of a three-component seismogram.   Seismogram implements
  mspass specific features needed to mesh with the mspass processing system.
  This constructor clones onlyh the CoreSeismogram components and initializes
  the ProcessingHistory with the default constructor leaves the history
  in an empty state.   Users should call methods in ProcessingHistory to
  initiate a valid history chain.   Processing can continue if left in
  that state, but the history chain will have an undefined origin.  Job
  information will also be lost if not initialized (see BasicProcessingHistory)

  \param d is the data to be copied to create the new Seismogram object
   */
  Seismogram(const mspass::seismic::CoreSeismogram& d);
  /*! Contruct from a core seismogram and initialize history as origin.

  This constructor is a variant of a similar one built only from a
  CoreSeismogram.  This constuctor is intended to be used mainly on
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
  Seismogram(const mspass::seismic::CoreSeismogram& d, const std::string alg);
  /*! \brief Construct from all pieces.

This constructor build a Seismogram object from all the pieces that define
this highest level object in mspass.   This constructor is planned to
be hidden from python programmers and not exposed with pybind11 wrappers
because is has some potentially undesirable side effects if not used
carefully.  The primary purpose of this constructor is for serialization
and deserializaton in spark with pickle.  The pickle interface is
purely in C for this function so again python programmers don't need
to see this constructor.  The parameter names are obvious because
they are associated one for one with the objects they are used to
construct.  The only exceptions are card and ortho which are booleans
used to set the internal booleans components_are_cardinal and
components_are_orthogonal respectively (two internals users should not
mess with).   tm is also a dmatrix representation the tmatrix stored
internally as a 2d C array, but we use the dmatrix to mesh
with serialization.
*/
  Seismogram(const mspass::seismic::BasicTimeSeries& b, const mspass::utility::Metadata& m,
    const mspass::utility::ErrorLogger& elg,
     const mspass::utility::ProcessingHistory& his,
      const bool card, const bool ortho,
        const mspass::utility::dmatrix& tm, const mspass::utility::dmatrix& uin);
  /*! Constructor driven by a Metadata object.

  The flexibilityof Metadata makes it helpful at times to build a Seismogram
  object driven by Metadata key:value pairs.   This implementation always uses
  file io to read the actual data and clone the Metadata.   ProcessingHistory
  is initialized to define the result as raw data.  The id field is set from
  Metadata using the special (frozen) key "wf_id" that is assumed to hold the
  string representation of an ObjectID of the waveform read.

  Note all but the Metadata argument are defaulted, but setting the jobname
  and jobid is strongly recommended to avoid ambiguity - these define a unique
  run of a particular workflow.


  \param md is the Metadata that is used to drive the constructor.
  \param jobname is the jobname (defaults to test, but should be changed)
  \param readername is the algorithm name assigned to the top level history
     record.   Defaults to "load3C"
  */
  Seismogram(const Metadata& md,const std::string jobname=std::string("test"),
    const std::string jobid=std::string("UNDEFINED"),
      const std::string readername=std::string("load3C"),
        const std::string algid=std::string("0"));

  /*! \brief Construct from core components creating a buffer initialized to zeros.

  This constructor can be thought of as a variant of the one with this signature:
  CoreSeismogram(const Metadata& md,const bool load_data);
  that is a component of the lower level class CoreSeismogram.  The reason is that the algorithm
  calls that constructor has this construct as its first line:
  Seismogram(const BasicTimeSeries& bts, const Metadata& md)
     : CoreSeismogram(md,false)
  it then overrides any data that constructor loads from Metadata with contents
  from the BasicTimeSeries base class.   In the process it allocates the space for the
  data matrix and initialzies it to zeros.  Finally, the top line also contains
  a call to the default constructor for ProcessingHistory, which currently
  creates an empty history record.

  Important point:  because the data buffer contains nuill data the object
  is marked dead.  When valid data is loaded into the data container the
  user should call the set_live() method to prevent processors from ignoring it.

  This method should only be used as a component to build a Seismogram
  from pieces that provide a convenient mechanism of assembly.  Use this
  one only if you have a sound understanding of all the pieces or you can
  easily get a data object with inconsistencies.

  \param bts - BasicTimeSeries data used as described above.
  \param md - Metadata used loaded with data and used as described above.
  */
  Seismogram(const mspass::seismic::BasicTimeSeries& bts,
                         const mspass::utility::Metadata& md);
  /*! Standard copy constructor. */
  Seismogram(const Seismogram& parent)
    : mspass::seismic::CoreSeismogram(parent), mspass::utility::ProcessingHistory(parent)
  {};
  /*! Standard assignment operator. */
  Seismogram& operator=(const Seismogram& parent);
  /*! \brief Load just the ProcessingHistory data from another data source.

  Some algorithms don't handle processing history.   In those situations it
  can prove helpful to manage ProcessingHistory separately and load the
  history data through this method.

  \param h is the ProcessingHistory data to copy into this Seismogram.
  */
  void load_history(const mspass::utility::ProcessingHistory& h);
};
}//END mspass::seismic namespace
#endif
