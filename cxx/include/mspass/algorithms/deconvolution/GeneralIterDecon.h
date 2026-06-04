#ifndef __MSPASS_GENERAL_ITER_DECON_COMPAT_H__
#define __MSPASS_GENERAL_ITER_DECON_COMPAT_H__

#include "mspass/algorithms/deconvolution/TimeDomainGIDDecon.h"

namespace mspass::algorithms::deconvolution {
/*!
 * \brief Deprecated compatibility alias for TimeDomainGIDDecon.
 *
 * GeneralIterDecon was replaced by TimeDomainGIDDecon on the generalized
 * iterative deconvolution branch.  This alias keeps old include paths and
 * non-copy uses source-compatible.  The old copy-constructor behavior is not
 * preserved because TimeDomainGIDDecon owns processor state that is not safely
 * copyable.  New code should include TimeDomainGIDDecon.h and use
 * TimeDomainGIDDecon directly.
 */
using GeneralIterDecon [[deprecated("Use TimeDomainGIDDecon instead")]] =
    TimeDomainGIDDecon;
} // namespace mspass::algorithms::deconvolution

#endif
