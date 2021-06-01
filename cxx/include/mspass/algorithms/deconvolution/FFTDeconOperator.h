#ifndef __FFT_DECON_OPERATOR_H__
#define __FFT_DECON_OPERATOR_H__
#include <string>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_fft_complex.h>
#include "mspass/utility/Metadata.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
namespace mspass::algorithms::deconvolution{
/*! \brief Object to hold components needed in all fft based decon algorithms.

The fft based algorithms implemented here us the GNU Scientific Library
prime factorization fft algorithm.  Those methods require initialization
given length of the fft to load and store the factorization data.  This
object holds these for all such methods and recomputes them only when
needed for efficiency.  */
class FFTDeconOperator
{
public:
    FFTDeconOperator();
    FFTDeconOperator(const mspass::utility::Metadata& md);
    FFTDeconOperator(const FFTDeconOperator& parent);
    ~FFTDeconOperator();
    FFTDeconOperator& operator=(const FFTDeconOperator& parent);
    void changeparameter(const mspass::utility::Metadata& md);
    void change_size(const int nfft_new);
    void change_shift(const int shift) {
        sample_shift=shift;
    };
    int get_size(){return nfft;};
    int get_shift(){return sample_shift;};
    int operator_size() {
        return static_cast<int>(nfft);
    };
    int operator_shift() {
        return sample_shift;
    };
    double df(const double dt){
        double period;
        period=static_cast<double>(nfft)*dt;
        return 1.0/period;
    };
    /*! \brief Return inverse wavelet for Fourier methods.

    This is a helper to be used by the inverse_wavelet method for all
    Fourier based deconvolution methods.   It avoids repetitious code that
    would be required otherwise.  inverse_wavelet methods are only
    wrappers for this generic method.  See documentation for inverse_wavelet
    for description of tshift and t0parent.  */
    mspass::seismic::CoreTimeSeries FourierInverse(const ComplexArray& winv, const ComplexArray& sw,
   	const double dt, const double t0parent);

protected:
    int nfft;
    int sample_shift;
    gsl_fft_complex_wavetable *wavetable;
    gsl_fft_complex_workspace *workspace;
    ComplexArray winv;
};

/* This helper is best referenced here */

/*! \brief Circular buffer procedure.

 In the Fourier world circular vectors are an important thing
 to deal with because the fft is intimately connected with circlular
 things.   This routine can be used, for example, to time shift the
 time domain version of a signal after it was processed with an fft.

 \param d - is the input vector to be shifted
 \param i0 - is the wrap point.   On exit sample i0 of d will be sample 0.
   (Warning - this is C convention sample number)
*/

std::vector<double>  circular_shift(const std::vector<double>& d,const int i0);
/*! Derive fft length from a time window.

All deconvlution methods using an fft need to define nfft based on the
length of the working time series.   This procedure returns the size from
an input window and sample interval. */
int ComputeFFTLength(const mspass::algorithms::TimeWindow w, const double dt);
/*! Derive fft length using parameters in a metadata object.

This procedure is basically a higher level version of the function of
the same name with a time window and sample interval argument.
This procedure extracts these using three parameter keys to extract
the real numbers form md:  deconvolution_data_window_start,
decon_window_end, and target dt. */
int ComputeFFTLength(const mspass::utility::Metadata& md);
/*! Returns next power of 2 larger than n.
 *
 * Some FFT implementations require the size of the input data vector be a power
 * of 2.  This routine can be used to define a buffer size satisfying that constraint.*/
extern "C" {
    unsigned int nextPowerOf2(unsigned int n);
}
}
#endif
