#ifndef _MSPASS_SEISMIC_DATAGAP_H_
#define _MSPASS_SEISMIC_DATAGAP_H_
#include <set>
#include <list>
#include "mspass/algorithms/TimeWindow.h"

namespace mspass::seismic{
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
        bool operator()(const mspass::algorithms::TimeWindow ti1,
                const mspass::algorithms::TimeWindow ti2) const
        {return(ti1.end<ti2.start);};
};

class DataGap
{
public:
    /*! Default construtor.  Does nothing but create empty gap container. */
    DataGap(){};
    /*! Construct with an initial list of TimeWindows defining gaps. */
    DataGap(const std::list<mspass::algorithms::TimeWindow>& twlist);
    virtual ~DataGap(){};
/*!
Checks if data at time ttest is a gap or valid data.
This function is like the overloaded version with an int argument except
it uses a time instead of sample number for the query.
\param ttest - time to be tested.
**/
      bool is_gap(const double ttest);  //query by time
/*!
Checks if a given data segment has a gap.
For efficiency it is often useful to ask if a whole segment of data is
free of gaps.  Most time series algorithms cannot process through data
gaps so normal practice would be to drop data with any gaps in a
requested time segment.
\return true if time segment has any data gaps
\param  twin time window of data to test defined by a TimeWindow object
**/
      bool has_gap(const mspass::algorithms::TimeWindow twin);
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
      void add_gap(const mspass::algorithms::TimeWindow tw){gaps.insert(tw);};
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
  std::set<mspass::algorithms::TimeWindow,mspass::algorithms::TimeWindowCmp> gaps;
};
}  // End mspass::seismic namespace
#endif //end guard
