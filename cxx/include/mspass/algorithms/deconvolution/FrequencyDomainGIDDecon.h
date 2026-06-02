#ifndef __FREQUENCY_DOMAIN_GID_DECON__
#define __FREQUENCY_DOMAIN_GID_DECON__
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/algorithms/deconvolution/TimeDomainGIDDecon.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include <list>
#include <vector>

namespace mspass::algorithms::deconvolution {
class FrequencyDomainGIDDecon : public ScalarDecon {
public:
  FrequencyDomainGIDDecon(const mspass::utility::AntelopePf &md);
  FrequencyDomainGIDDecon(const FrequencyDomainGIDDecon &parent) = delete;
  FrequencyDomainGIDDecon &operator=(const FrequencyDomainGIDDecon &parent) =
      delete;
  ~FrequencyDomainGIDDecon();
  void changeparameter(const mspass::utility::Metadata &md);
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin);
  int loadnoise(const mspass::seismic::CoreSeismogram &d,
                mspass::algorithms::TimeWindow nwin);
  int load(const mspass::seismic::CoreSeismogram &d,
           mspass::algorithms::TimeWindow dwin,
           mspass::algorithms::TimeWindow nwin);
  void process();
  mspass::seismic::CoreSeismogram getresult();
  mspass::seismic::CoreTimeSeries ideal_output();
  mspass::seismic::CoreTimeSeries actual_output();
  mspass::seismic::CoreTimeSeries inverse_wavelet();
  mspass::seismic::CoreTimeSeries inverse_wavelet(double t0parent);
  mspass::utility::Metadata QCMetrics();

private:
  mspass::seismic::CoreSeismogram d_all, d_decon, r, n;
  mspass::algorithms::TimeWindow dwin, nwin, fftwin;
  int ndwin, nnwin, noise_component;
  int actual_o_0, iter_count, iter_max;
  double residual_ratio_floor, residual_improvement_floor;
  double resid_l2_initial, resid_l2_prev, resid_l2_final;
  double resid_linf_initial, resid_linf_final;
  IterDeconType decon_type;
  ScalarDecon *preprocessor;
  CNRDeconEngine *cnrprocessor;
  mspass::seismic::TimeSeries current_wavelet;
  std::vector<double> actual_o_fir;
  std::list<ThreeCSpike> spikes;

  void initialize_inverse_operator();
  void rescale_spike(ThreeCSpike &spk);
  void update_residual_matrix(const ThreeCSpike &spk);
};
} // namespace mspass::algorithms::deconvolution
#endif
