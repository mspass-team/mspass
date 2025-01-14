#include "mspass/utility/MsPASSError.h"
#include <string>
#include <vector>
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::utility;

/*! \brief Circular buffer procedure.

In the Fourier world circular vectors are an important thing
to deal with because the fft is intimately connected with circlular
things.   This routine can be used, for example, to time shift the
time domain version of a signal after it was processed with an fft.
The shifting here is done for a time domain vector.  the same operation
can be achieved in the frequency by the standard linear phase shift theorem.

\param d - is the input vector to be shifted
\param i0 - is the wrap point.   On exit sample i0 of d will be sample 0.
  Note that negative i0 puts the wrap point at i0 points from the left.
  (Warning - this is C convention sample number)

\return - time shifted signal (same length as input d)
*/
vector<double> circular_shift(const vector<double> &d, const int i0) {
  /* a few basic sanity checks are useful to allow broader use */
  const string base_error("circular_shift procedure:  ");
  int nd(d.size());
  if (nd <= 0)
    throw MsPASSError(base_error + "Received empty data vector",
                      ErrorSeverity::Invalid);
  /* This one can be corrected - a negative shift is equivalent to this formula
   */
  int i0used;
  if (i0 < 0)
    i0used = nd + i0;
  else
    i0used = i0;
  /* It is an error if i0 is still negative as it means we asked for a shift
   * larger than nd.  This could be handled, but to me it would scream a coding
   * error */
  if (i0used < 0)
    throw MsPASSError(base_error +
                          "Large negative shift exceeds length of data vector",
                      ErrorSeverity::Invalid);
  if (i0used >= nd)
    throw MsPASSError(base_error +
                          "Large positive shift exceeds length of data vector",
                      ErrorSeverity::Invalid);
  vector<double> result;
  result.reserve(nd);
  int k, kk;
  if (i0 < 0)
    kk = nd + i0;
  else
    kk = i0;
  for (k = 0; k < nd; ++k, ++kk) {
    kk %= nd;
    result.push_back(d[kk]);
  }
  return result;
}
} // namespace mspass::algorithms::deconvolution
