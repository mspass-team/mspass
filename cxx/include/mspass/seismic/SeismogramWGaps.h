#ifndef _MSPASS_SEISMIC_SEISWGAPS_H_
#define _MSPASS_SEISMIC_SEISWGAPS_H_
#include "mspass/seismic/DataGap.h"
#include "mspass/seismic/Seismogram.h"

namespace mspass::seismic {
class SeismogramWGaps : public mspass::seismic::Seismogram,
                        public mspass::seismic::DataGap {
public:
  /*! \brief Constructor.

  Will need a set of conastructors.   Requires some thought as how to
  set gap is an issue. */
  SeismogramWGaps();
  /*! Copy constructor. */
  SeismogramWGaps(const SeismogramWGaps &parent);
  SeismogramWGaps &operator=(const SeismogramWGaps &parent);
  ~SeismogramWGaps();

  /*! Force all data inside data gaps to zero.
   **/
  void zero_gaps();
};
} // namespace mspass::seismic
#endif // end guard
