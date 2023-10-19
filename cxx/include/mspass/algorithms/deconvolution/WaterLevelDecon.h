#ifndef __SIMPLE_WATER_LEVEL_DECON_H__
#define __SIMPLE_WATER_LEVEL_DECON_H__
#include <vector>
#include "mspass/utility/Metadata.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
namespace mspass::algorithms::deconvolution{
class WaterLevelDecon : public FFTDeconOperator, public ScalarDecon
{
public:
    WaterLevelDecon(const WaterLevelDecon &parent);
    WaterLevelDecon(const mspass::utility::Metadata &md);
    WaterLevelDecon(const mspass::utility::Metadata &md,const std::vector<double> &wavelet,const std::vector<double> &data);
    void changeparameter(const mspass::utility::Metadata &md);
    void process();
    /*! \brief Return the actual output of the deconvolution operator.

    The actual output is defined as w^-1*w and is compable to resolution
    kernels in linear inverse theory.   Although not required we would
    normally expect this function to be peaked at 0.   Offsets from 0
    would imply a bias. */
    mspass::seismic::CoreTimeSeries actual_output();
    /*! \brief Return a FIR respresentation of the inverse filter.

    An inverse filter has an impulse response.  For some wavelets this
    can be respresented by a FIR filter with finite numbers of coefficients.
    Since this is a Fourier method the best we can do is return the inverse
    fft of the regularized operator.   The output usually needs to be
    phase shifted to be most useful.   For typical seismic source wavelets
    that are approximately minimum phase the shift can be small, but for
    zero phase input it should be approximately half the window size.
    This method also has an optional argument for t0parent.   Because
    this processor was written to be agnostic about a time standard
    it implicitly assumes time 0 is sample 0 of the input waveforms.
    If the original data have a nonzero start time this should be
    passed as t0parent or the output will contain a time shift of t0parent.
    Note that tshift and t0parent do very different things.  tshift
    is used to apply circular phase shift to the output (e.g. a shift
    of 10 samples causes the last 10 samples in the wavelet to be wrapped
    to the first 10 samples).   t0parent only changes the time standard
    so the output has t0 -= parent.t0.

    Output wavelet is always circular shifted with 0 lag at center.

    \param t0parent - time zero of parent seismograms (see above).

    \throw SeisppError is thrown if the tshift value is more than
     half the length of the data (nfft*dt).  Reason is justified above.
    */
    mspass::seismic::CoreTimeSeries inverse_wavelet(const double t0parent=0.0);
    /*! \brief Return default FIR represesentation of the inverse filter.

    This is an overloaded version of the parameterized method.   It is
    equivalent to this->inverse_wavelet(0.0,0.0);
    */
    mspass::seismic::CoreTimeSeries inverse_wavelet() ;
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    mspass::utility::Metadata QCMetrics();
private:
    int read_metadata(const mspass::utility::Metadata &md);
    int apply();
    double wlv;
    /* QC metrics.   */
    /* This is the fraction of frequencies below the water level */
    double regularization_fraction;
};
}
#endif
