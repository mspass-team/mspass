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
  /*! Standard copy constructor. */
  TimeSeries(const TimeSeries& parent)
    : CoreTimeSeries(parent), MsPASSCoreTS(parent){};
  /*! Standard assignment operator. */
  TimeSeries& operator=(const TimeSeries& parent);
};
}//END mspass namespace
#endif
