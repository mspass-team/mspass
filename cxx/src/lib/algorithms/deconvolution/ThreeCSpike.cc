#include "mspass/algorithms/deconvolution/ThreeCSpike.h"
#include <cmath>
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
ThreeCSpike::ThreeCSpike(int k, const double u0, const double u1,
                         const double u2) {
  col = k;
  u[0] = u0;
  u[1] = u1;
  u[2] = u2;
  amp = sqrt(u0 * u0 + u1 * u1 + u2 * u2);
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
