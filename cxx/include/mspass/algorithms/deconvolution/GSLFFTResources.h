#ifndef MSPASS_ALGORITHMS_DECONVOLUTION_GSLFFTRESOURCES_H
#define MSPASS_ALGORITHMS_DECONVOLUTION_GSLFFTRESOURCES_H

#include "mspass/utility/MsPASSError.h"
#include <gsl/gsl_fft_complex.h>
#include <memory>
#include <new>
#include <string>

namespace mspass::algorithms::deconvolution::detail {

struct GSLFFTWavetableDeleter {
  void operator()(gsl_fft_complex_wavetable *p) const noexcept {
    if (p != nullptr)
      gsl_fft_complex_wavetable_free(p);
  }
};

struct GSLFFTWorkspaceDeleter {
  void operator()(gsl_fft_complex_workspace *p) const noexcept {
    if (p != nullptr)
      gsl_fft_complex_workspace_free(p);
  }
};

using GSLFFTWavetablePtr =
    std::unique_ptr<gsl_fft_complex_wavetable, GSLFFTWavetableDeleter>;
using GSLFFTWorkspacePtr =
    std::unique_ptr<gsl_fft_complex_workspace, GSLFFTWorkspaceDeleter>;

struct GSLFFTResources {
  GSLFFTWavetablePtr wavetable;
  GSLFFTWorkspacePtr workspace;
};

/* Allocate the two GSL objects as one transaction.  The unique pointers make
 * failure of the workspace allocation release the already-created wavetable
 * before bad_alloc escapes. */
inline GSLFFTResources AllocateGSLFFTResources(const int nfft,
                                               const std::string &caller) {
  if (nfft <= 0)
    throw mspass::utility::MsPASSError(
        caller + ": fft length must be positive",
        mspass::utility::ErrorSeverity::Fatal);
  GSLFFTWavetablePtr wavetable(gsl_fft_complex_wavetable_alloc(nfft));
  if (!wavetable)
    throw std::bad_alloc();
  GSLFFTWorkspacePtr workspace(gsl_fft_complex_workspace_alloc(nfft));
  if (!workspace)
    throw std::bad_alloc();
  return {std::move(wavetable), std::move(workspace)};
}

} // namespace mspass::algorithms::deconvolution::detail

#endif
