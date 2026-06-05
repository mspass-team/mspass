#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/Metadata.h"
#include <cmath>
#include <math.h>
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::algorithms;
using namespace mspass::utility;

int ComputeTaperLength(const Metadata &md) {
  try {
    const string caller("ComputeTaperLength");
    double ts, te, dt;
    ts = md.get<double>("deconvolution_data_window_start");
    te = md.get<double>("deconvolution_data_window_end");
    dt = md.get<double>("target_sample_interval");
    ValidateWindowDuration(TimeWindow(ts, te), "deconvolution window", caller);
    if (!std::isfinite(dt) || dt <= 0.0)
      throw MsPASSError(caller + ": target_sample_interval must be positive",
                        ErrorSeverity::Fatal);
    const int n = round((te - ts) / dt) + 1;
    if (n < 2)
      throw MsPASSError(caller +
                            ": deconvolution window must contain at least two "
                        "samples",
                        ErrorSeverity::Fatal);
    return n;
  } catch (...) {
    throw;
  };
}
} // namespace mspass::algorithms::deconvolution
