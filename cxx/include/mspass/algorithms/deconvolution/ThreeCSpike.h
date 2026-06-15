#ifndef __THREE_C_SPIKE_H__
#define __THREE_C_SPIKE_H__
#include "mspass/utility/dmatrix.h"

namespace mspass::algorithms::deconvolution {
enum IterDeconType {
  WATER_LEVEL,
  LEAST_SQ,
  MULTI_TAPER,
  CNR,
  NS_GID,
  GROUP_SPARSE
};

/*! \brief Sparse three-component spike used by iterative deconvolution. */
class ThreeCSpike {
public:
  /*! Column index where this spike is placed in a three-component matrix. */
  int col;
  /*! Three-component spike amplitude. */
  double u[3];
  /*! Cached L2 norm of u. */
  double amp;
  /*! Construct a spike from the vector sample at column k. */
  ThreeCSpike(mspass::utility::dmatrix &d, int k);
  /*! Construct a spike from explicit three-component amplitudes. */
  ThreeCSpike(int k, const double u0, const double u1, const double u2);
  /*! Copy constructor. */
  ThreeCSpike(const ThreeCSpike &parent);
  /*! Assignment operator required for STL containers. */
  ThreeCSpike &operator=(const ThreeCSpike &parent);
};
} // namespace mspass::algorithms::deconvolution
#endif
