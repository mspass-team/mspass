#ifndef __PAVLIS_MULTITAPER_DECON_H__
#define __PAVLIS_MULTITAPER_DECON_H__
#include <vector>
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/deconvolution/ComplexArray.h"
#include "mspass/deconvolution/ScalarDecon.h"
#include "mspass/deconvolution/FFTDeconOperator.h"
#include "mspass/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
namespace mspass{
class MultiTaperSpecDivDecon: public ScalarDecon, public FFTDeconOperator
{
public:

    MultiTaperSpecDivDecon(const mspass::Metadata &md,const std::vector<double> &noise,
                          const std::vector<double> &wavelet,const std::vector<double> &data);
    MultiTaperSpecDivDecon(const mspass::Metadata &md);
    MultiTaperSpecDivDecon(const MultiTaperSpecDivDecon &parent);
    ~MultiTaperSpecDivDecon() {};
    void changeparameter(const mspass::Metadata &md) {
        this->read_metadata(md,true);
    };
    /*! \brief Load a section of preevent noise.

    The multitaper algorithm requires a preevent noise it uses to compute a
    frequency dependent regularization.   This method loads the noise data
    but does not initiate a computation.  It should be called before calling
    the ScalarDecon::load method which will initiate a computation of the
    result. */
    int loadnoise(const std::vector<double> &noise);
    /*! \brief load all data components.

    This method should be called immediately befor process.  It loads the
    wavelet and data to which teh deconvolution is to be applied.

    \param w is expected to contain the wavelet data
    \paarm e is the data to be deconvolved with w.
    \param n is the vector of noise used for regularization in this method..*/
    int load(const std::vector<double>& w, const std::vector<double>& d,
             const std::vector<double>& n);
    void process();
    /*! \brif Return the actual output of the deconvolution operator.

    The actual output is defined as w^-1*w and is compable to resolution
    kernels in linear inverse theory.   Although not required we would
    normally expect this function to be peaked at 0.   Offsets from 0
    would imply a bias. */
    mspass::CoreTimeSeries actual_output();
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

    */
    mspass::CoreTimeSeries inverse_wavelet(const double t0parent=0.0);
    /*! \brief Return default FIR represesentation of the inverse filter.

    This is an overloaded version of the parameterized method.   It is
    equivalent to this->inverse_wavelet(0.0,0.0);
    */
    mspass::CoreTimeSeries inverse_wavelet() ;
    /*! Return the ensemble for number of tapers inverse wavelets. */
    std::vector<mspass::CoreTimeSeries> all_inverse_wavelets(const double t0parent=0.0);
    /*! Return the ensemble of rf estimates */
    std::vector<mspass::CoreTimeSeries> all_rfestimates(const double t0parent=0.0);
    /*! Return the ensemble of actual outputs */
    std::vector<mspass::CoreTimeSeries> all_actual_outputs(const double t0parent=0.0);
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    mspass::Metadata QCMetrics();
    int get_taperlen() {
        return taperlen;
    };
    int get_number_tapers() {
        return nseq;
    };
    double get_time_bandwidth_product() {
        return nw;
    };
private:
    /*! Private method called by constructors to load parameters.   */
    int read_metadata(const mspass::Metadata &md,bool refresh);
    /* Returns a tapered data in container of ComplexArray objects*/
    std::vector<mspass::ComplexArray> taper_data(const std::vector<double>& signal);
    std::vector<double> noise;
    double nw,damp;
    int nseq;  // number of tapers
    unsigned int taperlen;
    mspass::dmatrix tapers;
    /* inverse wavelet in the frequency domain */
    std::vector<mspass::ComplexArray> winv;
    /* We also cache the actual output fft because the cost is
     * small compared to need to recompute it when requested.
     * This is a feature added for the GID method that adds
     * an inefficiency for straight application */
    std::vector<mspass::ComplexArray> ao_fft;
    /* This contains the rf estimates from each of the tapers.   They
    are averaged to produce final result, but we keep them for the
    option of bootstrap errors. */
    std::vector<mspass::ComplexArray> rfestimates;
    int apply();
};
}
#endif
