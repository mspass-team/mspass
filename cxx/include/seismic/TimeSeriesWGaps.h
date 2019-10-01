#include "TimeSeries.h"
#include "DataGap.h"
class TimeSeriesWGaps : public TimeSeries, public DataGap
{
public:
  /*! \brief Constructor.

  Will need a set of conastructors.   Requires some thought as how to
  set gap is an issue. */
  TimeSeriesWGaps(args)
  /*! Copy constructor. */
  TimeSeriesWGaps(const TimeSeriesWGaps& parent);
  TimeSeriesWGaps& operator=(const TimeSeriesWGaps& parent);
  ~TimeSeriesWGaps();
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

};
