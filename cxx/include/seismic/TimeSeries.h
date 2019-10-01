#ifndef _MSPASS_TIMESERIES_H_
#define _MSPASS_TIMESERIES_H_
#include <vector>
#include "seismic/BasicTimeSeries.h"
#include "mspass/Metadata.h"

namespace mspass{
/*! \brief Scalar time series data object.

This data object extends BasicTimeSeries mainly by adding a vector of
scalar data.  It uses a Metadata object to contain auxiliary parameters
that aren't essential to define the data object, but which are necessary
for some algorithms.
\author Gary L. Pavlis
**/
class TimeSeries: public mspass::BasicTimeSeries , public mspass::Metadata
{
public:
/*!
Actual data stored as an STL vector container.
Note the STL guarantees the data elements of vector container are
contiguous in memory like FORTRAN vectors.  As a result things
like the BLAS can be used with data object by using a syntax
like this: if d is a TimeSeries object, the address of the first sample of
the data is &(d.s[0]).
**/
	vector<double>s;
/*!
Default constructor.  Initializes object data to zeros and sets the
initial STL vector size to 0 length.
**/
	TimeSeries();
/*!
Similar to the default constructor but creates a vector of data
with nsin samples and initializes all samples to 0.0.
This vector can safely be accessed with the vector index
operator (i.e. operator []).  A corollary is that push_back
or push_front applied to this vector will alter it's length
so use this only if the size of the data to fill the object is
already known.
**/
	TimeSeries(int nsin);
/*! Construct from components. */
        TimeSeries(const BasicTimeSeries& bts,const Metadata& md);
/*!
Standard copy constructor.
**/
	TimeSeries(const TimeSeries&);
/*!
Standard assignment operator.
**/
	TimeSeries& operator=(const TimeSeries&);
/*!
Summation operator.  Simple version of stack.  Aligns data before
summing.
**/
	void operator+=(const TimeSeries& d);

/*!
Extract a sample from data vector with range checking.
Because the data vector is public in this interface
this operator is simply an alterative interface to this->s[sample].

\exception SeisppError exception if the requested sample is outside
   the range of the data.  Note this includes an implicit "outside"
   defined when the contents are marked dead.

\param sample is the integer sample number of data desired.
**/
	double operator[](int const sample) const;
};
}  // End mspass namespace
#endif //end guard
