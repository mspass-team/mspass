#ifndef _BASICTIMESERIES_H_
#define _BASICTIMESERIES_H_
#include <math.h>
namespace mspass{
/*! \brief Type of time standard for time series data.

Time series data have two common standards.  Absolute time means the
time is an epoch time.  Relative means time is some other arbitrary
reference.  An example is an arrival time reference frame where all
data are set with time zero defined by a set of arrival time picks.
**/
enum class TimeReferenceType {
	UTC, /*!< Use an absolute (usually UTC) time base - previously absolute in SEISPP. */
	Relative /*! Time is relative to some other standard like shot time */
};

/*! \brief Base class for time series objects.

This is a mostly abstract class defining data and methods shared by all
data objects that are time series.  To this library time series means
data sampled on a 1d, uniform grid defined by a sample rate, start time,
and number of samples.  Derived types can be scalar, vector, complex, or
any data that is uniformly sampled.

\author Gary L. Pavlis
**/
class BasicTimeSeries
{
public:
/*!
Boolean defining if a data object has valid data or is to be ignored.
Data processing often requires data to be marked bad but keep the original
data around in case an error was made.  This boolean allows this capability.
**/
	bool live;
/*!
Sample interval.
**/
	double dt;
/*!
Data start time.  That is the time of the first sample of data.
**/
	double t0;
/*!
Number of data samples in this data object.
**/
	int ns;
/*!
Time reference standard for this data object.  Defined by enum Time_Reference
this currently is only one of two things.  When set as "UTC" the time
standard is an epoch time.  When set as "relative" time has no relationship
to any external standard but are relative to some arbitrary reference that must
ascertained by the algorithm by some other means (in seispp this is normally
done through a metadata object).  A classic example is multichannel data where
channels have a time relative to a shot time.
**/
	TimeReferenceType tref;
/*!
Default constructor. Does essentially nothing since a BasicTimeSeries
object has no data.  Does initialize data to avoid run time checkers
bitching about unitialized data, but values are meaningless when this
constructor is called.
**/
	BasicTimeSeries();
/*!
Standard copy constructor.
**/
	BasicTimeSeries(const BasicTimeSeries&);
/*! \brief Virtual destructor.

  A base class with virtual members like this requires this
  incantation to avoid some odd conflicts.  This particular one
  was added to make the boost::serialization code work properly.
  The geeky details for why this is necessary can be found in
  Scott Meyers book "Effective C++" */
  virtual ~BasicTimeSeries(){};
/*!
Get the time of sample i.
It is common to need to ask for the time of a given sample.
This standardizes this common operation in an obvious way.
//\param i - sample number to compute time for.
**/
	double time(const int i)const noexcept
        {
            return(t0+dt*static_cast<double>(i));
        };
/*!
Inverse of time function.  That is,  it returns the integer position
of a given time t within a time series.  The returned number is
not tested for validity compared to the data range.  This is the
callers responsibility as this is a common error condition that
should not require the overhead of an exception.
**/
	int sample_number(double t)const noexcept
        {
            return(round((t-t0)/dt));
        };
/*!
Returns the end time (time associated with last data sample)
of this data object.
**/
	double endtime()const noexcept
        {
            return(t0+dt*static_cast<double>(ns-1));
        };
/*! Return true if a time shift has been applied to the data.
 * Never true if data were never in an absolute time frame (i.e.UTC)*/
	bool shifted() const
	{
		return t0shift_is_valid;
	};
/*! Return the reference time.

  We distinguish relative and UTC time by a time shift constant
  stored with the object.   This returns the time shift to return
  data to an epoch time.

  \throw SeisppError object if the request is not rational.  That is this
  request only makes sense if the data began with an absolute time and was
  converted with the ator method.   Some cross checks are made for consistency
  that can throw an error in this condition. */
  double time_reference() const;
/*! \brief Force a t0 shift value on data.
 *
 * This is largely an interface routine for constructors that need to 
 * handle data in relative time that are derived from an absolute 
 * base.  It can also be used to fake processing routines that demand
 * data be in absolute time when the original data were not.  It was
 * added for MsPASS to support reads and writes to MongoDB where we
 * want to be able to read and write data that had been previously 
 * time shifted (e.g. ArrivalTimeReference).
 *
 * \param t is the time shift to force 
 * */
	void force_t0_shift(const double t)
	{
		this->t0shift=t;
		t0shift_is_valid=true;
	};
/*!
Absolute to relative time conversion.
Sometimes we want to convert data from absolute time (epoch times)
to a relative time standard.  Examples are conversions to travel
time using an event origin time or shifting to an arrival time
reference frame.  This operation simply switches the tref
variable and alters t0 by tshift.
\param tshift - time shift applied to data before switching data to relative time mode.
**/
	virtual void ator(const double tshift);
/*!  Relative to absolute time conversion.
 Sometimes we want to convert data from relative time to
 to an absolute time standard.  An example would be converting
 segy shot data to something that could be processed like earthquake
 data in a css3.0 database.
 This operation simply switches the tref
 variable and alters t0 by tshift.
\param tshift - time shift applied to data before switching data to absolute time mode.

NOTE:  This method is maintained only for backward compatibility.   May be depricated
   in favor of method that uses internally stored private shift variable.
**/
	virtual void rtoa(const double tshift);
/*! Relative to absolute time conversion.
 Sometimes we want to convert data from relative time to
 to an UTC time standard.  An example would be converting
 segy shot data to something that could be processed like earthquake
 data in a css3.0 database.

 This method returns data previously converted to relative back to UTC using the
 internally stored time shift attribute. */
  virtual void rtoa();
/*! Shift the reference time.

  Sometimes we need to shift the reference time t0.  An example is a moveout correction.
  This method shifts the reference time by dt.   Note a positive dt means data aligned to
  zero will be shifted left because relative time is t-t0.
  */
  virtual void shift(const double dt);
/*! Standard assignment operator. */
  BasicTimeSeries& operator=(const BasicTimeSeries& parent);

private:
    /* We actually test for t0shift two ways.  If this is true we always accept it.
     * If false we check for nonzero t0shift and override if necessary.
     * */
    bool t0shift_is_valid;
    /*When ator or rtoa are called this variable defines the conversion back
     * and forth.  The shift method should be used to change it. */
    double t0shift;
};
}
#endif   // End guard
