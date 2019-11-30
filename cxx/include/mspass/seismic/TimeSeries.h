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
  /*! Standard copy constructor. */
  TimeSeries(const TimeSeries& parent)
    : CoreTimeSeries(parent), MsPASSCoreTS(parent){};
  /*! Standard assignment operator. */
  TimeSeries& operator=(const TimeSeries& parent);
};
}//END mspass namespace
#endif
