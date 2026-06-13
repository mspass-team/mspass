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

  cout << "GID adaptive-memory penalty saturation test passed" << endl;
  return 0;
}
