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
class Seismogram : public mspass::seismic::CoreSeismogram,
   public mspass::utility::ProcessingHistory
{
public:
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
  /*! \brief Partial constructor from base classes.

  This is a partial constructor useful sometimes for building a Seismogram
  object from scratch such as in a simultion.  It clones the Metadata and
  uses attributes in BasicTimeSeries to build a framework that data can
  be added into.  It initialzies the data array to npts stored in the
  BasicTimeSeries passed to the constructor.

  \param bts is the BasicTimeSeries that overrides any md data.
  \param md is the Metadata cloned to make the partially constructed object.
  */
  Seismogram(const mspass::seismic::BasicTimeSeries& bts,
      const mspass::utility::Metadata& md);


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

/*! \brief Construct from Metadata definition that includes data path.
 *
 A Metadata object is sufficiently general that it can contain enough
 information to contruct an object from attributes contained in it.
 This constuctor uses that approach, with the actual loading of data
 being an option (on by default).   In mspass this is constructor is
 used to load data with Metadata constructed from MongoDB and then
 using the path created from two parameters (dir and dfile used as
 in css3.0 wfdisc) to read data.   The API is general but the
 implementation in mspass is very rigid.   It blindly assumes the
 data being read are binary doubles in the right byte order and
 ordered in the native order for dmatrix (Fortran order).  i.e.
 the constuctor does a raw fread of ns*3 doubles into the internal
 array used in the dmatrix implementation.

 A second element of the Metadata that is special for MsPASS is the
 handling of the transformation matrix by this constructor.   In MsPASS
 the transformation matrix is stored as a python object in MongoDB.
 This constructor aims to fetch that entity with the key 'tmatrix'.
 To be more robust and simpler to use with data not loaded from mongodb
 we default tmatrix to assume the data are in standard coordinates.  That is,
 if the key tmatrix is not defined in Metadata passed as arg0, the
 constructor assumes it should set the transformation matrix to an identity.
 Use set_transformation_matrix if that assumption is wrong for your data.

 \param md is the Metadata used for the construction.

 \param load_data if true (default) a file name is constructed from
 dir+"/"+dfile, the file is openned, fseek is called to foff,
 data are read with fread, and the file is closed.  If false a dmatrix
 for u is still created of size 3xns, but the matrix is only initialized
 to all zeros.

 \exception  Will throw a MsPASSError if required metadata are missing.
 */
   Seismogram(const Metadata& md, bool load_data)
     : mspass::seismic::CoreSeismogram(md,load_data), mspass::utility::ProcessingHistory()
   {};
  /*! Standard copy constructor. */
  Seismogram(const Seismogram& parent)
    : mspass::seismic::CoreSeismogram(parent), mspass::utility::ProcessingHistory(parent)
  {};
  virtual ~Seismogram(){};
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
