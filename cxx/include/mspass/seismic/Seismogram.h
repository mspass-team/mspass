#ifndef _SEISMOGRAM_H_
#define _SEISMOGRAM_H_
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/utility/MsPASSCoreTS.h"
namespace mspass{
/*! \brief Implemntation of Seismogram for MsPASS.

This is the working version of a three-component seismogram object used
in the MsPASS framework.   It extends CoreSeismogram by adding
common MsPASS components (MsPASSCORETS).  It may evolve with additional
special features.  */
class Seismogram : public mspass::CoreSeismogram, public mspass::MsPASSCoreTS
{
public:
  /*! Default constructor.   Only runs subclass default constructors. */
  Seismogram() : mspass::CoreSeismogram(),mspass::MsPASSCoreTS(){};
  /*! Partial copy constructor.

   Most of this class is defined by the CoreSeismogram class, but at present for
   mspass extension we need the objectid for mongdb.  This passes the object id
   as a string of hex digits.  

   \param d is the main CoreSeismogram to be copied.
   \param oid is the objectid specified as a hex string.
   */
  Seismogram(const mspass::CoreSeismogram& d, const std::string oid);
  /*! Extended partial copy constructor. 

  A Seismogram object is created from several pieces.   It can be 
  useful at times to create a partial clone that copies everything 
  but the actual data.   This version clones all components that are
  not data.  Note whenever this constructor is called the object id
  will automatically be invalid since by definition the object 
  created is not stored in the MongoDB database.

  \param b - BasicSeismogram component to use to construct data.
  \param m - Metadata componet to use to construct data (no test are 
    made to verify any attributes stored here are consistent with b.
  \param e - ErrorLogger content.  If these data are derived from a 
    parent that has an error log (ErrorLogger) that may not be empty 
    it can be useful to copy the log.   This argument has a default
    that passes an empty ErrorLog object.   The idea is calling this
    constructor with only two parameters will not copy the error log.
    */
  Seismogram(const mspass::BasicTimeSeries& b,const mspass::Metadata& m, 
          const ErrorLogger elf=ErrorLogger());
/*! \brief Construct from Metadata and read data from file.
   *
   This constuctor creates a Seismogram object with attributes loaded
   from Metadata AND data loaded from a file described by Metadata
   attributes dir and dfile.   Most of the work is done in the
   CoreSeismogram constructor with the same signature.  See documentation
   there for details.   This constuctor creates an empty error log
   and tries to extract the ObjectID string from the Metadata and
   set the id.   If that fails it sets the id invalid.  When the
   Metadata object passed is created from MongoDB queries the id
   should alway be defined. Invalid will only happen if this constructor
   is used for some other purpose and the Id is not set.

   Note CoreSeismogram has a load_data boolean which is always defined
   true when used by this constructor.   Thus, the parallel constructor
   for CoreSeismogram to this one has only a const Metadata as an argument.

   \param md is the Metadata object used to drive the constructor.

   */
  Seismogram(const Metadata& md);
  /*! Standard copy constructor. */
  Seismogram(const Seismogram& parent)
    : mspass::CoreSeismogram(parent), mspass::MsPASSCoreTS(parent){};
  /*! Standard assignment operator. */
  Seismogram& operator=(const Seismogram& parent);
};
}//END mspass namespace
#endif
