#include <cassert>
#include <cmath>
#include <iostream>
#include <limits>
#include <vector>

#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"

using namespace std;
using namespace mspass::algorithms::deconvolution;

int main() {
  cout << "Testing GID adaptive-memory penalty saturation" << endl;

  vector<double> lag_weights(1, 1.0);
  vector<double> memory(1, 1000.0);
  vector<double> retention(1, nextafter(1.0, 0.0));
  const vector<double> kernel(1, 1.0);

  GIDAdaptivePenaltyMetrics metrics(ApplyGIDAdaptiveMemoryPenalty(
      lag_weights, memory, retention, kernel, 0, 0, 1.0, 1.0e6, 1.0,
      "test_gid_penalty"));

  assert(metrics.memory_linf > 900.0);
  assert(memory[0] > 900.0);
  assert(retention[0] > 0.0);
  assert(std::isfinite(lag_weights[0]));
  assert(lag_weights[0] > 0.0);
  assert(lag_weights[0] <= numeric_limits<double>::min());

  metrics = ApplyGIDAdaptiveMemoryPenalty(lag_weights, memory, retention, kernel,
                                          0, 0, 1.0, 1.0e6, 1.0,
                                          "test_gid_penalty");

  assert(metrics.memory_linf > 900.0);
  assert(memory[0] > 900.0);
  assert(retention[0] > 0.0);
  assert(std::isfinite(lag_weights[0]));
  assert(lag_weights[0] > 0.0);
  assert(lag_weights[0] <= numeric_limits<double>::min());

  /* A raw-significant candidate may be heavily downweighted by the lag
   * penalty.  The old global argmax of raw*weight would select index 1 and
   * then incorrectly reject it for raw significance.  NS-GID must first
   * restrict the candidate set by raw significance, then apply the penalty. */
  const vector<double> raw_amplitudes{0.90, 0.70};
  const vector<double> selection_weights{0.10, 1.00};
  const double threshold(0.80);
  const int old_selection = 1;
  assert(raw_amplitudes[old_selection] * selection_weights[old_selection] >
         raw_amplitudes[0] * selection_weights[0]);
  assert(raw_amplitudes[old_selection] < threshold);
  const int selected = SelectNoiseSignificantGIDCandidateIndex(
      raw_amplitudes, selection_weights, threshold);
  assert(selected == 0);
  assert(raw_amplitudes[selected] >= threshold);

  /* A final amplitude refit may invalidate a provisional candidate stop.
   * Both TD and FD engines use this shared resolution policy. */
  assert(ResolveNSGIDFinalStopReason("candidate_not_significant", true) ==
         "post_refit_significant_candidate_remaining");
  assert(ResolveNSGIDFinalStopReason("candidate_not_significant", false) ==
         "candidate_not_significant");

  cout << "GID adaptive-memory penalty saturation test passed" << endl;
  return 0;
}
