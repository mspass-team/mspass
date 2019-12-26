#ifndef _TIMESERIES_H_
#define _TIMESERIES_H_
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/utility/MsPASSCoreTS.h"
namespace mspass{
  /*! \brief Implemntation of TimeSeries for MsPASS.

  This is the working version of a three-component TimeSeries object used
  in the MsPASS framework.   It extends CoreTimeSeries by adding
  common MsPASS components (MsPASSCORETS).  It may evolve with additional
  special features.  */
class TimeSeries : public CoreTimeSeries, public MsPASSCoreTS
{
public:
  /*! Default constructor.   Only runs subclass default constructors. */
  TimeSeries() : CoreTimeSeries(),MsPASSCoreTS(){};
  /*! Partial copy constructor.

   Most of this class is defined by the CoreTimeSeries class, but at present for
   mspass extension we need the objectid for mongdb.  This passes the object id
   as a string of hex digits.  

   \param d is the main CoreTimeSeries to be copied.
   \param oid is the objectid specified as a hex string.
   */
  TimeSeries(const mspass::CoreTimeSeries& d, const std::string oid);
  /*! Extended partial copy constructor. 

  A TimeSeries object is created from several pieces.   It can be 
  useful at times to create a partial clone that copies everything 
  but the actual data.   This version clones all components that are
  not data.  Note whenever this constructor is called the object id
  will automatically be invalid since by definition the object 
  created is not stored in the MongoDB database.

  \param b - BasicTimeSeries component to use to construct data.
  \param m - Metadata componet to use to construct data (no test are 
    made to verify any attributes stored here are consistent with b.
  \param e - ErrorLogger content.  If these data are derived from a 
    parent that has an error log (ErrorLogger) that may not be empty 
    it can be useful to copy the log.   This argument has a default
    that passes an empty ErrorLog object.   The idea is calling this
    constructor with only two parameters will not copy the error log.
    */
  TimeSeries(const BasicTimeSeries& b,const Metadata& m, 
          const ErrorLogger elf=ErrorLogger());
  /*! Standard copy constructor. */
  TimeSeries(const TimeSeries& parent)
    : CoreTimeSeries(parent), MsPASSCoreTS(parent){};
  /*! Standard assignment operator. */
  TimeSeries& operator=(const TimeSeries& parent);
};
}//END mspass namespace
#endif
