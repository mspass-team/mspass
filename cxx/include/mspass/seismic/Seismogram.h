#ifndef _SEISMOGRAM_H_
#define _SEISMOGRAM_H_
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/utility/ProcessingHistory.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_serialize.hpp>
namespace mspass{
/*! \brief Implemntation of Seismogram for MsPASS.

This is the working version of a three-component seismogram object used
in the MsPASS framework.   It extends CoreSeismogram by adding
ProcessingHistory.   */
class Seismogram : public mspass::CoreSeismogram,
   public mspass::ProcessingHistory
{
public:
  ErrorLogger elog;
  /*! Default constructor.   Only runs subclass default constructors. */
  Seismogram() : mspass::CoreSeismogram(),mspass::ProcessingHistory(){};
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
  Seismogram(const mspass::CoreSeismogram& d);
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
  Seismogram(const mspass::CoreSeismogram& d, const std::string alg);
  /*! \brief Construct from all pieces.

This constructor build a Seismogram object from all the pieces that define
this highest level object in mspass.   This constructor is planned to
be hidden from python programmers and not exposed with pybind11 wrappers
because is has some potentially undesirable side effects if now used
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
  Seismogram(const mspass::BasicTimeSeries& b, const mspass::Metadata& m,
    const mspass::ProcessingHistory& his,const bool card, const bool ortho,
      const mspass::dmatrix& tm, const mspass::dmatrix& uin);
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
  Seismogram(const Metadata& md,const string jobname=string("test"),
    const string jobid=string("UNDEFINED"),
      const string readername=string("load3C"),
        const string algid=string("0"));
  /*! Standard copy constructor. */
  Seismogram(const Seismogram& parent);
  /*! Standard assignment operator. */
  Seismogram& operator=(const Seismogram& parent);
  /*! Return sring representation of the unique id for this object. */
  std::string id_string() const
  {
    return id;
  };
  /*! Set id from a string - commonly MongoDB objectid string*/
  void set_id(const std::string newid)
  {
    id=newid;
  };
  /*! Set id from a random number generator - normal for transient data.*/
  void set_id()
  {
    boost::uuids::random_generator gen;
    boost::uuids::uuid uuidval;
    uuidval=gen();
    id=boost::uuids::to_string(uuidval);
  };
  /*! Return true if the id set is a MongoDB ObjectID string representation. */
  bool is_objectid()
  {
    /* Use a magic test that an object id string is 12 bytes.  An alternative
    would be a boolean or enum for id type, but for simplicity we'll just
    use this magic number.  A bit fragile if mongochanges the definition.*/
    if(id.size()==12)
      return true;
    else
      return false;
  };
private:
  std::string id;
};
}//END mspass namespace
#endif
