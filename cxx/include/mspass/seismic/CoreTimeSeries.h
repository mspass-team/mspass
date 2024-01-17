#ifndef _MSPASS_CORETIMESERIES_H_
#define _MSPASS_CORETIMESERIES_H_
#include <vector>
#include "mspass/seismic/BasicTimeSeries.h"
#include "mspass/utility/Metadata.h"

namespace mspass::seismic{
/*! \brief Scalar time series data object.

This data object extends BasicTimeSeries mainly by adding a vector of
scalar data.  It uses a Metadata object to contain auxiliary parameters
that aren't essential to define the data object, but which are necessary
for some algorithms.
\author Gary L. Pavlis
**/
class CoreTimeSeries: public mspass::seismic::BasicTimeSeries ,
             public mspass::utility::Metadata
{
public:
/*!
Actual data stored as an STL vector container.
Note the STL guarantees the data elements of vector container are
contiguous in memory like FORTRAN vectors.  As a result things
like the BLAS can be used with data object by using a syntax
like this: if d is a CoreTimeSeries object, the address of the first sample of
the data is &(d.s[0]).
**/
	std::vector<double>s;
/*!
Default constructor.  Initializes object data to zeros and sets the
initial STL vector size to 0 length.
**/
	CoreTimeSeries();
/*!
Similar to the default constructor but creates a vector of data
with nsin samples and initializes all samples to 0.0.
This vector can safely be accessed with the vector index
operator (i.e. operator []).  A corollary is that push_back
or push_front applied to this vector will alter it's length
so use this only if the size of the data to fill the object is
already known.
**/
	CoreTimeSeries(const size_t nsin);
  virtual ~CoreTimeSeries(){};
/*! Partially construct from components.

There are times one wants to use the Metadata area as a template to
flesh out a CoreTimeSeries as what might be called skin and bones:  skin is
Metadata and bones as BasicTimeSeries data.   This constructor initializes
those two base classes but does not fully a valid data vector.  It only
attempts to fetch the number of points expected for the data vector using
the npts metadata (integer) key (i.e. it sets npts to md.get_int("npts")).
It then creates the data vector of that length and initialzies it to all zeros.*/
  CoreTimeSeries(const BasicTimeSeries& bts,const Metadata& md);
/*!
Standard copy constructor.
**/
	CoreTimeSeries(const CoreTimeSeries&);
	/* These overload virtual methods in BasicTimeSeries. */
	/*! \brief Set the sample interval.

  This method is complicated by the need to sync the changed value with
	Metadata.   That is further complicated by the need to support aliases
	for the keys used to defined dt in Metadata.   That is handled by
	first setting the internal dt value and then going through a fixed list
	of valid alias keys for dt.  Any that exist are changed.   If
	none were previously defined the unique name (see documentation) is
	added to Metadata.

	\param sample_interval is the new data sample interval to be used.
	*/
  void set_dt(const double sample_interval);
	/*! \brief Set the number of samples attribute for data.

	This method is complicated by the need to sync the changed value with
	Metadata.   That is further complicated by the need to support aliases
	for the keys used to defined npts in Metadata.   That is handled by
	first setting the internal npts value (actually ns) and then going through a fixed list
	of valid alias keys for npts.  Any that exist are changed.   If
	none were previously defined the unique name (see documentation) is
	added to Metadata.

	This attribute has an additional complication compared to other setter
	that are overrides from BasicTimeSeries.   That is, the number of points
	define the data buffer size to hold the sample data.   To guarantee
	the buffer size and the internal remain consistent this method clears
	any existing content of the vector s and initializes npts points to 0.0.
	Note this means if one is using this to assemble a data object in pieces
	you MUST call this method before loading any data or it will be cleared
	and you will mysteriously find the data are all zeros.

	\param npts is the new number of points to set.
	*/
	void set_npts(const size_t npts);
	/*! \brief Sync the number of samples attribute with actual data size.

	This method syncs the npts attribute with the actual size of the vector s.
	It also syncs aliases in the same way as the set_npts method.

	*/
	void sync_npts();
	/*! \brief Set the data start time.

	This method is complicated by the need to sync the changed value with
	Metadata.   That is further complicated by the need to support aliases
	for the keys used to defined npts in Metadata.   That is handled by
	first setting the internal t0 value and then going through a fixed list
	of valid alias keys for it.  Any that exist are changed.   If
	none were previously defined the unique name (see documentation) is
	added to Metadata.

	This is a dangerous method to use on real data as it can mess up the time
	if not handled correctly.   It should be used only when that sharp knife is
	needed such as in assembling data outside of constructors in a test program.

	\param t0in is the new data sample interval to be used.
	*/
	void set_t0(const double t0in);
/* Depricated in 2020 API change - no longer needed with new BasicTimeSeries.
Returns the end time (time associated with last data sample)
of this data object.

*/
/*
	double endtime()const noexcept
        {
            return(mt0+mdt*static_cast<double>(s.size()-1));
        };
*/
/*!
Standard assignment operator.
**/
	CoreTimeSeries& operator=(const CoreTimeSeries& parent);
/*! \brief Summation operator.

Summing data from signals of irregular length requires handling potential
mismatches in size and overlap.  This behaves the way a += operator should
logically behave in that situation.  That is, because the lhs is where
the sum is being accumulated, the size is always controlled by the left hand
side of the operator.  Any portions of the right hand side that are outside
the t0 to endtime() of the left hand side are silently discarded.   If the
start time of the right hand side is greater than t0 or the endtime is less
than endtime of the lhs there will be discontinuties in the sum there
the ends of the rhs are inside the range of the lhs.

\param d is other signal to add to this.
\exception MsPASSError can be thrown if lhs and rhs do not have matching
time standards.
**/
	CoreTimeSeries& operator+=(const CoreTimeSeries& d);
	/*! Addition operator.

	This operator is implemented in a standard way utilizing operator+=.
	For data with irregular start and end times that has an important
	consequence;  the operator is not communative. i.e given x an y
	z=x+y will not yield the same result as z=y+x.
	*/
	const CoreTimeSeries operator+(const CoreTimeSeries& other) const;

/*! Multiply data by a scalar. */
	CoreTimeSeries& operator*=(const double);
	/*! \brief Subtraction operator.

	Differencing data from signals of irregular length requires handling potential
	mismatches in size and overlap.  This behaves the way a -= operator should
	logically behave in that situation.  That is, because the lhs is where
	the sum is being accumulated, the size is always controlled by the left hand
	side of the operator.  Any portions of the right hand side that are outside
	the t0 to endtime() of the left hand side are silently discarded.   If the
	start time of the right hand side is greater than t0 or the endtime is less
	than endtime of the lhs there will be discontinuties in the sum there
	the ends of the rhs are inside the range of the lhs.

	\param d is other signal to subract from this.
	\exception MsPASSError can be thrown if lhs and rhs do not have matching
	time standards.
	**/
	CoreTimeSeries& operator-=(const CoreTimeSeries& d);
	/*! Subtraction operator.

	This operator is implemented in a standard way utilizing operator-=.
	For data with irregular start and end times that has an important
	consequence;  the operator is not communative. i.e given x an y
	z=x-y will not yield the same result as z=-(y-x).
	*/
	const CoreTimeSeries operator-(const CoreTimeSeries& other) const;
/*!
Extract a sample from data vector with range checking.
Because the data vector is public in this interface
this operator is simply an alterative interface to this->s[sample].

\exception SeisppError exception if the requested sample is outside
   the range of the data.  Note this includes an implicit "outside"
   defined when the contents are marked dead.

\param sample is the integer sample number of data desired.
**/
	double operator[](size_t const sample) const;
};
}  // End mspass::seismic namespace
#endif //end guard
