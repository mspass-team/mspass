#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/GIDDeconUtil.h"
namespace mspass::algorithms::deconvolution {
using namespace std;
using namespace mspass::utility;

ScalarDecon::ScalarDecon(const Metadata &md) : shapingwavelet(md) {
  try {
    /* This has to be defined or the shapingwavlet constructor will
     * fail in the current implementation */
    int nfft = GetIntRequired(md, "operator_nfft");
    wavelet.reserve(nfft);
    data.reserve(nfft);
    result.reserve(nfft);
  } catch (...) {
    throw;
  };
}
ScalarDecon::ScalarDecon(const std::vector<double> &d,
                         const std::vector<double> &w)
    : data(d), wavelet(w) {
  result.reserve(data.size());
}
ScalarDecon::ScalarDecon(const ScalarDecon &parent)
    : data(parent.data), wavelet(parent.wavelet), result(parent.result),
      shapingwavelet(parent.shapingwavelet) {}
ScalarDecon &ScalarDecon::operator=(const ScalarDecon &parent) {
  if (this != &parent) {
    wavelet = parent.wavelet;
    data = parent.data;
    result = parent.result;
  }
  return *this;
}
int ScalarDecon::load(const vector<double> &w, const vector<double> &d) {
  wavelet = w;
  data = d;
  result.clear();
  return 0;
}
/* this an next method are normally called back to back.  To assure
stability we must have both call the clear method for result.   Could
do that in only loadwavelet, but that could produce funny bugs downsteam
so we always clear for stability at a small cost */
int ScalarDecon::loaddata(const vector<double> &d) {
  data = d;
  result.clear();
  return 0;
}
int ScalarDecon::loadwavelet(const vector<double> &w) {
  wavelet = w;
  result.clear();
  return 0;
}
void ScalarDecon::changeparameter(const Metadata &md) {
  shapingwavelet = ShapingWavelet(md);
}
void ScalarDecon::change_shaping_wavelet(const ShapingWavelet &nsw) {
  shapingwavelet = nsw;
}
Metadata ScalarDecon::BasicQCMetrics(const string &operator_name,
                                     const bool processed) {
  Metadata md;
  md.put("decon_operator", operator_name);
  md.put("decon_processed", processed);
  md.put("decon_input_loaded", !data.empty() && !wavelet.empty());
  md.put("decon_data_npts", static_cast<int>(data.size()));
  md.put("decon_wavelet_npts", static_cast<int>(wavelet.size()));
  md.put("decon_output_npts", static_cast<int>(result.size()));
  md.put("decon_shaping_wavelet_type", shapingwavelet.type());
  md.put("decon_shaping_wavelet_dt", shapingwavelet.sample_interval());
  md.put("decon_shaping_wavelet_nfft", shapingwavelet.size());
  return md;
}
} // namespace mspass::algorithms::deconvolution
