#ifndef __MSPASS_GENERAL_ITER_DECON_COMPAT_H__
#define __MSPASS_GENERAL_ITER_DECON_COMPAT_H__

#include "mspass/algorithms/deconvolution/TimeDomainGIDDecon.h"

namespace mspass::algorithms::deconvolution {
/*!
 * \brief Deprecated compatibility alias for TimeDomainGIDDecon.
 *
 * GeneralIterDecon was replaced by TimeDomainGIDDecon on the generalized
 * iterative deconvolution branch.  This alias preserves C++ source
 * compatibility for code that still includes the old header.  New code should
 * include TimeDomainGIDDecon.h and use TimeDomainGIDDecon directly.
 */
using GeneralIterDecon [[deprecated("Use TimeDomainGIDDecon instead")]] =
    TimeDomainGIDDecon;
} // namespace mspass::algorithms::deconvolution

#endif
