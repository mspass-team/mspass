/* This class is defined in this file because it is intimately linked to
that operator.  We put the C++ code here to make it easier to find. */
#include "mspass/algorithms/deconvolution/GeneralIterDecon.h"
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::utility;

ThreeCSpike::ThreeCSpike(dmatrix &d, int k) {
  try {
    int i;
    for (i = 0; i < 3; ++i)
      u[i] = d(i, k);
    col = k;
    for (i = 0, amp = 0.0; i < 3; ++i)
      amp += u[i] * u[i];
    amp = sqrt(amp);
  } catch (...) {
    throw;
  };
}
ThreeCSpike::ThreeCSpike(const ThreeCSpike &parent) {
  col = parent.col;
  amp = parent.amp;
  for (int k = 0; k < 3; ++k)
    u[k] = parent.u[k];
}
ThreeCSpike &ThreeCSpike::operator=(const ThreeCSpike &parent) {
  if (this != (&parent)) {
    col = parent.col;
    amp = parent.amp;
    for (int k = 0; k < 3; ++k)
      u[k] = parent.u[k];
  }
  return *this;
}
} // namespace mspass::algorithms::deconvolution
