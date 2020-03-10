#ifndef __SIMPLE_DECON_H__
#define __SIMPLE_DECON_H__

#include <vector>
#include "mspass/utility/Metadata.h"
#include "mspass/deconvolution/BasicDeconOperator.h"
#include "mspass/deconvolution/ShapingWavelet.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
namespace mspass{
/*! \brief Absract base class for algorithms handling full 3C data.
*/
class Base3CDecon : public BasicDeconOperator
{
public:
    Base3CDecon() {};
    virtual ~Base3CDecon() {};
    virtual void changeparameter(const mspass::Metadata &md){};
    virtual void loaddata(mspass::Seismogram& d,const int comp)=0;
    virtual void loadwavelet(const mspass::TimeSeries& w)=0;
    /* \brief Return the ideal output of the deconvolution operator.

    All deconvolution operators have a implicit or explicit ideal output
    signal. e.g. for a spiking Wiener filter it is a delta function with or
    without a lag.  For a shaping wavelt it is the time domain version of the
    wavelet. */
    virtual mspass::Seismogram process()=0;
    /*! \brif Return the actual output of the deconvolution operator.

    The actual output is defined as w^-1*w and is compable to resolution
    kernels in linear inverse theory.   Although not required we would
    normally expect this function to be peaked at 0.   Offsets from 0
    would imply a bias. */
    virtual mspass::TimeSeries actual_output()=0;

    /*! \brief Return a FIR represention of the inverse filter.

    After any deconvolution is computed one can sometimes produce a finite
    impulse response (FIR) respresentation of the inverse filter.  */
    virtual mspass::TimeSeries inverse_wavelet() = 0;
    virtual mspass::TimeSeries inverse_wavelet(double) = 0;
    /*! \brief Return appropriate quality measures.

    Each operator commonly has different was to measure the quality of the
    result.  This method should return these in a generic Metadata object. */
    virtual mspass::Metadata QCMetrics()=0;
};
}
#endif
