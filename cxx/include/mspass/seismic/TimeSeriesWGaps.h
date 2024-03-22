#ifndef _MSPASS_SEISMIC_TSWGAPS_H_
#define _MSPASS_SEISMIC_TSWGAPS_H_
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/DataGap.h"
namespace mspass::seismic{
class TimeSeriesWGaps : public TimeSeries, public DataGap
{
public:
  /*! \brief Constructor.

  Will need a set of conastructors.   Requires some thought as how to
  set gap is an issue. */
  TimeSeriesWGaps():TimeSeries(),DataGap(){};
  /*! Partial copy constructor from a plain TimeSeries.

    Sometimes we need to build the skeleton of a gappy TimeSeries from
    a regular TimeSeries.  This does that and builds makes a copy of the
    TimeSeries and creates an empty container that defines the gaps. */
  TimeSeriesWGaps(const TimeSeries& parent) : TimeSeries(parent),DataGap(){};
  /*! Copy constructor. */
  TimeSeriesWGaps(const TimeSeriesWGaps& parent)
      : TimeSeries(dynamic_cast<const TimeSeries&>(parent)),
              DataGap(dynamic_cast<const DataGap&>(parent)){};;
  TimeSeriesWGaps(const TimeSeries& tsp, const DataGap& dgp) 
	  : TimeSeries(tsp), DataGap(dgp) {};
  TimeSeriesWGaps& operator=(const TimeSeriesWGaps& parent);
  virtual ~TimeSeriesWGaps(){};
  /*!
  Absolute to relative time conversion.
  Sometimes we want to convert data from absolute time (epoch times)
  to a relative time standard.  Examples are conversions to travel
  time using an event origin time or shifting to an arrival time
  reference frame.  This operation simply switches the tref
  variable and alters t0 by tshift.

  This method overrides the one in BasicTimeSeries to correctly reference gaps.
  \param tshift - time shift applied to data before switching data to relative time mode.
  **/
  	void ator(const double tshift);
  /*!  Relative to absolute time conversion.
   Sometimes we want to convert data from relative time to
   to an absolute time standard.  An example would be converting
   segy shot data to something that could be processed like earthquake
   data in a css3.0 database.
   This operation simply switches the tref
   variable and alters t0 by tshift.

     This method overrides the one in BasicTimeSeries to correctly reference gaps.
  \param tshift - time shift applied to data before switching data to absolute time mode.

  NOTE:  This method is maintained only for backward compatibility.   May be depricated
     in favor of method that uses internally stored private shift variable.
  **/
  	void rtoa(const double tshift);
  /*! Relative to absolute time conversion.
   Sometimes we want to convert data from relative time to
   to an absolute time standard.  An example would be converting
   segy shot data to something that could be processed like earthquake
   data in a css3.0 database.

   This method returns data previously converted to relative back to absolute using the
   internally stored time shift attribute.


     This method overrides the one in BasicTimeSeries to correctly reference gaps.*/
    void rtoa();
  /*! Shift the reference time.

    Sometimes we need to shift the reference time t0.  An example is a moveout correction.
    This method shifts the reference time by dt.   Note a positive dt means data aligned to
    zero will be shifted left because relative time is t-t0.
    */
    void shift(const double dt);
/*! Force all data inside data gaps to zero.
**/
  void zero_gaps();
  /*! Return an estimate of the memmory use by the data in this object.

  Memory consumed by a TimeSeriesWGaps object is needed to implement the
  __sizeof__ method in python that dask/spark use to manage memory.  Without
  that feature we had memory fault issues.  Note the estimate this
  method returns should not be expected to be exact.  The MsPASS implementation
  or any alternative implementation avoids an exact calculation because it
  requries an (expensive) traversal of multiple map containers.
  */
  size_t memory_use() const;
};
} //end mspass::seismic namespace
#endif //end guard
