#include <math.h>
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/CoreTimeSeries.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

FFTDeconOperator::FFTDeconOperator()
{
    nfft=0;
    sample_shift=0;
    wavetable=NULL;
    workspace=NULL;
}
FFTDeconOperator::FFTDeconOperator(const Metadata& md)
{
  try {
    const string base_error("FFTDeconOperator Metadata constructor:  ");
    int nfftpf=md.get_int("operator_nfft");
    /* We force a power of 2 algorithm for efficiency and always round up*/
    this->nfft=nextPowerOf2(nfftpf);
	/* We compute the sample shift from the window start time and dt.  This assures
	 * the output will be phase shifted so zero lag is at the zero position of the
	 * array.   Necessary because operators using this object internally only return
	 * a raw vector of samples not a child of BasicTimeSeries. */
	  double dt_to_use=md.get_double("target_sample_interval");
	  double dwinstart=md.get_double("deconvolution_data_window_start");
	  int i0=round(dwinstart/dt_to_use);
	     /* sample shift is positive for a negative i0 */
	  this->sample_shift = (-i0);
    if(this->sample_shift<0)
            throw MsPASSError(base_error
                + "illegal sample_shift parameter - must be ge 0",
                ErrorSeverity::Invalid);
    if((this->sample_shift)>nfft)
	    throw MsPASSError(base_error
	    	+ "Computed shift parameter exceeds length of fft\n"
		    + "Deconvolution data window parameters are probably nonsense",
         ErrorSeverity::Invalid);
    wavetable = gsl_fft_complex_wavetable_alloc (nfft);
    workspace = gsl_fft_complex_workspace_alloc (nfft);
  } catch(...) {
        throw;
    };
}
FFTDeconOperator::FFTDeconOperator(const FFTDeconOperator& parent)
{
    nfft=parent.nfft;
    sample_shift=parent.sample_shift;
    /* copies need their own work space */
    wavetable = gsl_fft_complex_wavetable_alloc (nfft);
    workspace = gsl_fft_complex_workspace_alloc (nfft);
}
FFTDeconOperator::~FFTDeconOperator()
{
    if(wavetable!=NULL) gsl_fft_complex_wavetable_free (wavetable);
    if(workspace!=NULL) gsl_fft_complex_workspace_free (workspace);
}
FFTDeconOperator& FFTDeconOperator::operator=(const FFTDeconOperator& parent)
{
    if(this != &parent)
    {
        nfft=parent.nfft;
        sample_shift=parent.sample_shift;
        /* copies need their own work space */
        wavetable = gsl_fft_complex_wavetable_alloc (nfft);
        workspace = gsl_fft_complex_workspace_alloc (nfft);
    }
    return *this;
}
void FFTDeconOperator::changeparameter(const Metadata& md)
{
    try {
        size_t nfft_test=md.get_int("operator_nfft");
        if(nfft_test != nfft)
        {
            nfft=nfft_test;
            if(wavetable!=NULL) gsl_fft_complex_wavetable_free (wavetable);
            if(workspace!=NULL) gsl_fft_complex_workspace_free (workspace);
            wavetable = gsl_fft_complex_wavetable_alloc (nfft);
            workspace = gsl_fft_complex_workspace_alloc (nfft);
        }
        sample_shift=md.get_int("sample_shift");
        if(sample_shift<0)
            throw MsPASSError(string("FFTDeconOperator::changeparameter:  ")
                    + "illegal sample_shift parameter - must be ge 0",
                  ErrorSeverity::Invalid);
    } catch(...) {
        throw;
    };
}
void FFTDeconOperator::change_size(const int n)
{
    try {
        if(nfft!=0)
        {
            if(wavetable!=NULL) gsl_fft_complex_wavetable_free (wavetable);
            if(workspace!=NULL) gsl_fft_complex_workspace_free (workspace);
        }
        nfft=n;
        wavetable = gsl_fft_complex_wavetable_alloc (nfft);
        workspace = gsl_fft_complex_workspace_alloc (nfft);
    } catch(...) {
        throw;
    };
}
/* Helper method to avoid repetitious code in Fourier methods.   Computes the
 inverse FIR filter for the inverse_wavelet methods of fourier decon operators.

 winv - fourier coefficients of inverse wavelet
 sw - fourier coefficients of shaping wavelet that is to be applied to winv
 dt - data sample interval (number of points is derived form winv)
 t0parent is as described in ScalarDecon::inverse_wavelet defintions.
 */

CoreTimeSeries FFTDeconOperator::FourierInverse(const ComplexArray& winv, const ComplexArray& sw,
   const double dt, const double t0parent)
{
  try{
    const string base_error("FFTDeconOperator::FourierInverse:  ");
    /* Compute wrap point for circular shift operator - note negative
    because a positive t0 is requires wrapping at a point i0 to the left
    of the original zero point*/
    /* Testing with matlab prototypes showed Fourier inverse filters
     * showed they created valid results only if the filter used a
     * circular shift of nfft/2.   This sets that as a requirement here. */
    int ntest;
    ntest=winv.size();
    if(ntest != nfft) throw MsPASSError(base_error
      + "wavelet inverse fourier array size mismatch with operator",
      ErrorSeverity::Invalid);
    if(sw.size() != nfft) throw MsPASSError(base_error
      + "shaping wavelet fourier array size mismatch with operator",
       ErrorSeverity::Invalid);
    ComplexArray winv_work(winv);
    /* This applies the shaping wavelet*/
    winv_work *= sw;
    gsl_fft_complex_inverse(winv_work.ptr(),1,nfft,wavetable,workspace);
    CoreTimeSeries result;
    result.set_t0(t0parent);
    result.set_dt(dt);
    result.set_live();
    /* Note this new api method initializes s all zeros so we need only set
    the values not use push back below */
    result.set_npts(nfft);
    result.set_tref(TimeReferenceType::Relative);
    for(int k=0; k<winv_work.size(); ++k) result.s[k]=winv_work[k].real();
    return result;
  }catch(...){throw;};
}

/* helpers*/
int ComputeFFTLength(const TimeWindow w, const double dt)
{
    int nsamples,nfft;
    nsamples=static_cast<int>(((w.end-w.start)/dt))+1;
    nfft=nextPowerOf2(nsamples);
    return nfft;
}
/* Newbies note this works because of the fundamental concept of
overloading in C++ */
int ComputeFFTLength(const Metadata& md)
{
    try {
        double ts,te,dt;
        ts=md.get<double>("deconvolution_data_window_start");
        te=md.get<double>("deconvolution_data_window_end");
        TimeWindow w(ts,te);
        dt=md.get<double>("target_sample_interval");
        int nfft;
        nfft=ComputeFFTLength(w,dt);
        if(nfft<2)
            throw MsPASSError(string("FFTDeconOperator ComputeFFTLength procedure:  ")
                      + "Computed fft length is less than 2 - check window parameters",
                    ErrorSeverity::Invalid);
        return nfft;
    } catch(MetadataGetError& mde) {
        throw mde;
    };
}
}  //End namespace
