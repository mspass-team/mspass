#ifndef _SEISMOGRAM_H_
#define _SEISMOGRAM_H_
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/utility/MsPASSCoreTS.h"
namespace mspass{
/*! \brief Implemntation of Seismogram for MsPASS.

This is the working version of a three-component seismogram object used
in the MsPASS framework.   It extends CoreSeismogram by adding
common MsPASS components (MsPASSCORETS).  It may evolve with additional
special features.  */
class Seismogram : public mspass::CoreSeismogram, public mspass::MsPASSCoreTS
{
public:
  /*! Default constructor.   Only runs subclass default constructors. */
  Seismogram() : mspass::CoreSeismogram(),mspass::MsPASSCoreTS(){};
  /*! Standard copy constructor. */
  Seismogram(const Seismogram& parent)
    : mspass::CoreSeismogram(parent), mspass::MsPASSCoreTS(parent){};
  /*! Standard assignment operator. */
  Seismogram& operator=(const Seismogram& parent);
};
}//END mspass namespace
#endif
