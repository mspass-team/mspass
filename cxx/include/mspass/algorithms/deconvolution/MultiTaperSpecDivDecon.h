#ifndef __PAVLIS_MULTITAPER_DECON_H__
#define __PAVLIS_MULTITAPER_DECON_H__
#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/vector.hpp>
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
#include "mspass/algorithms/deconvolution/ScalarDecon.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
namespace mspass::algorithms::deconvolution{
class MultiTaperSpecDivDecon: public FFTDeconOperator, public ScalarDecon
{
public:
    /*! Default constructor.   Do not use - only for declarations */
    MultiTaperSpecDivDecon() : FFTDeconOperator(),ScalarDecon(){};
    MultiTaperSpecDivDecon(const mspass::utility::Metadata &md,const std::vector<double> &noise,
                          const std::vector<double> &wavelet,const std::vector<double> &data);
    MultiTaperSpecDivDecon(const mspass::utility::Metadata &md);
    MultiTaperSpecDivDecon(const MultiTaperSpecDivDecon &parent);
    ~MultiTaperSpecDivDecon() {};
    void changeparameter(const mspass::utility::Metadata &md) {
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

    */
    mspass::seismic::CoreTimeSeries inverse_wavelet(const double t0parent=0.0);
    /*! \brief Return default FIR represesentation of the inverse filter.

    This is an overloaded version of the parameterized method.   It is
    equivalent to this->inverse_wavelet(0.0,0.0);
    */
    mspass::seismic::CoreTimeSeries inverse_wavelet() ;
    /*! Return the ensemble for number of tapers inverse wavelets. */
    std::vector<mspass::seismic::CoreTimeSeries> all_inverse_wavelets(const double t0parent=0.0);
    /*! Return the ensemble of rf estimates */
    std::vector<mspass::seismic::CoreTimeSeries> all_rfestimates(const double t0parent=0.0);
    /*! Return the ensemble of actual outputs */
    std::vector<mspass::seismic::CoreTimeSeries> all_actual_outputs(const double t0parent=0.0);
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    mspass::utility::Metadata QCMetrics();
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
    std::vector<double> noise;
    double nw,damp;
    int nseq;  // number of tapers
    unsigned int taperlen;
    mspass::utility::dmatrix tapers;
    /* With this algorithm we have to keep frequeny domain 
     * representations of the inverse for each taper.  The 
     * xcor version of this alorithm doesn't need that because 
     * the averaging is different. */
    std::vector<ComplexArray> winv_taper;
    /* We also cache the actual output fft because the cost is
     * small compared to need to recompute it when requested.
     * This is a feature added for the GID method that adds
     * an inefficiency for straight application */
    std::vector<ComplexArray> ao_fft;
    /* This contains the rf estimates from each of the tapers.   They
    are averaged to produce final result, but we keep them for the
    option of bootstrap errors. */
    std::vector<ComplexArray> rfestimates;
    /* Private methods */
    /* Returns a tapered data in container of ComplexArray objects*/
    std::vector<ComplexArray> taper_data(const std::vector<double>& signal);
    /*! Private method called by constructors to load parameters.   */
    int read_metadata(const mspass::utility::Metadata &md,bool refresh);
    int apply();
    friend boost::serialization::access;
    template<class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar & boost::serialization::base_object<ScalarDecon>(*this);
        ar & boost::serialization::base_object<FFTDeconOperator>(*this);
        ar & noise;
        ar & nw;
        ar & damp;
        ar & nseq;
        ar & taperlen;
        ar & tapers;
        ar & winv_taper;
        ar & ao_fft;
        ar & rfestimates;
    }
};
}
#endif
