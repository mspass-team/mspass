#include <set>
#include "TimeWindow.h"
/*! \brief Function object used for weak comparison to order TimeWindow objects.

// TimeWindow objects are used, among other things, to define real
// or processed induced data gaps.
// The set container requires a weak ordering function like to correctly
// determine if a time is inside a particular time window.
//\author Gary L. Pavlis
**/
class TimeWindowCmp
{
public:
        bool operator()(const TimeWindow ti1,const TimeWindow ti2) const
        {return(ti1.end<ti2.start);};
};
class DataGap
{
public:
/*!
Checks if a sample defined by an integer offset value is a data gap.
Calls like seis.is_gap(is) return true if sample is is a data gap.
It also returns true if i is outside the range of the data.
(i.e. less than 0 or >= ns).
//\param is - sample number to test.
**/
      bool is_gap(const int is);  query by sample number
/*!
Checks if data at time ttest is a gap or valid data.
This function is like the overloaded version with an int argument except
it uses a time instead of sample number for the query.
\param ttest - time to be tested.
**/
      bool is_gap(const double ttest);  query by time
/*!
Checks if a given data segment has a gap.
For efficiency it is often useful to ask if a whole segment of data is
free of gaps.  Most time series algorithms cannot process through data
gaps so normal practice would be to drop data with any gaps in a
requested time segment.
\return true if time segment has any data gaps
\param  twin time window of data to test defined by a TimeWindow object
**/
      bool is_gap(const TimeWindow twin);
/*!
Global test to see if data has any gaps defined.
Gap processing is expensive and we need this simple method to
test to see if the associated object has any gaps defined.
\return true if the associated object has any gaps defined.
**/
      bool has_gap(){return(!gaps.empty());};
/*!
Adds a gap to the gap definitions for this data object.
Sometimes an algorithm detects or needs to create a gap (e.g. a mute,
or a constructor).
This function provides a common mechanism to define such a gap in the data.
**/
      void add_gap(const TimeWindow tw){gaps.insert(tw);};
      /*! \brief Clear gaps.

It is sometimes necessary to clear gap definitions.
This is particularly important when a descendent of this class
is cloned and then morphed into something else.
*/
      void clear_gaps(){if(!gaps.empty())gaps.clear();};
/*! \brief virtual method for zeroing data gaps.

Any object using this object needs to implement this method */
      virtual void zero_gaps()=0;
protected:
  /*! \brief Holds data gap definitions.
  We use an STL set object to define data gaps for any time series
  object derived from this base class.  The set is keyed by a TimeWindow
  which allows a simple, fast way to define a time range with invalid
  data. */
  set<TimeWindow,TimeWindowCmp> gaps;
};
