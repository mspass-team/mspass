#include "TimeSeries.h"
#include "DataGap.h"
class SeismogramWGaps : public Seismogram, public DataGap
{
public:
  /*! \brief Constructor.

  Will need a set of conastructors.   Requires some thought as how to
  set gap is an issue. */
  SeismogramWGaps(args)
  /*! Copy constructor. */
  SeismogramWGaps(const SeismogramWGaps& parent);
  SeismogramWGaps& operator=(const SeismogramWGaps& parent);
  ~SeismogramWGaps();

/*! Force all data inside data gaps to zero.
**/
  void zero_gaps();
};
