#ifndef __SIMPLE_DECON_H__
#define __SIMPLE_DECON_H__

#include <vector>
#include "mspass/utility/Metadata.h"
#include "mspass/deconvolution/BasicDeconOperator.h"
#include "mspass/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/CoreTimeSeries.h"
namespace mspass{
/*! \brief Base class decon operator for single station 3C decon (receiver functions).

A class of algorithms exist for computing so called receiver functions.
Simple for this application means a method that is applied to a single
station's data and computed in a scalar time series sense.   Thus the
interface assumes we want to always at least load an estimate of the source
wavelet for deconvolution and the data to which it is to be applied.

The design of this interface was made more complicated by a design goal
to allow application of different conventional methods as the first step
in the generalized iterative method.  The generalized method can select
one of the children of this base class.
*/
class ScalarDecon: public BasicDeconOperator
{
public:
    ScalarDecon():shapingwavelet() {};
    ScalarDecon(const mspass::Metadata& md);
    ScalarDecon(const std::vector<double>& d, const std::vector<double>& w);
    ScalarDecon(const ScalarDecon& parent);
    int load(const std::vector<double> &wavelet,const std::vector<double> &data);
    virtual void process()=0;
    ~ScalarDecon() {};
    ScalarDecon& operator=(const ScalarDecon& parent);
    std::vector<double> getresult() {
        return result;
    };
    /* This method does nothing, but needs to be defined to avoid
     * gcc compile errors in programs using children of this class.*/
    void changeparameter(const mspass::Metadata &md);
    /*! Change the shaping wavelet that will be applied to output.
     *
     The suite of algorithms here use the concept of a shaping wavelet
     thoughout.  The shaping wavelet for most applications should have
     a zero phase impulse response.  This method changes the
     wavelet set with the operator. */
    void change_shaping_wavelet(const ShapingWavelet& nsw);
    /* \brief Return the ideal output of the deconvolution operator.

    All deconvolution operators have a implicit or explicit ideal output
    signal. e.g. for a spiking Wiener filter it is a delta function with or
    without a lag.  For a shaping wavelt it is the time domain version of the
    wavelet. */
    mspass::CoreTimeSeries ideal_output() {
        return this->shapingwavelet.impulse_response();
    };
    /*! \brif Return the actual output of the deconvolution operator.

    The actual output is defined as w^-1*w and is compable to resolution
    kernels in linear inverse theory.   Although not required we would
    normally expect this function to be peaked at 0.   Offsets from 0
    would imply a bias. */
    virtual mspass::CoreTimeSeries actual_output()=0;

    /*! \brief Return a FIR represention of the inverse filter.

    After any deconvolution is computed one can sometimes produce a finite
    impulse response (FIR) respresentation of the inverse filter.  */
    virtual mspass::CoreTimeSeries inverse_wavelet() = 0;
    virtual mspass::CoreTimeSeries inverse_wavelet(double) = 0;
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    virtual mspass::Metadata QCMetrics()=0;
protected:
    std::vector<double> data;
    std::vector<double> wavelet;
    std::vector<double> result;
    mspass::ShapingWavelet shapingwavelet;
};
}
#endif
