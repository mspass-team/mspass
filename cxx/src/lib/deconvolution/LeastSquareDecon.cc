#include "mspass/utility/MsPASSError.h"
#include "mspass/deconvolution/LeastSquareDecon.h"
#include "mspass/deconvolution/MultiTaperXcorDecon.h"
using namespace std;
using namespace mspass;
namespace mspass{
LeastSquareDecon::LeastSquareDecon(const LeastSquareDecon &parent)
    : FFTDeconOperator(parent)
{
    damp=parent.damp;
}
int LeastSquareDecon::read_metadata(const Metadata &md)
{
    try {
        const string base_error("SimpleLeastTaperDecon::read_metadata method: ");
        int nfft_from_win=ComputeFFTLength(md);
        //window based nfft always overrides that extracted directly from md */
        if(nfft_from_win!=nfft)
        {
            this->change_size(nfft_from_win);
        }
        damp=md.get_double("damping_factor");
        /* Note this depends on nfft inheritance from FFTDeconOperator.
        * That is a bit error prone with changes*/
        shapingwavelet=ShapingWavelet(md,nfft);
        return 0;
    } catch(...) {
        throw;
    };
}
/* This constructor is little more than a call to read_metadata for this
operator */
LeastSquareDecon::LeastSquareDecon(const Metadata &md)
    : FFTDeconOperator(md)
{
    try {
        this->read_metadata(md);
    } catch(...) {
        throw;
    }
}
/* This method is really an alias for read_metadata for this operator */
void LeastSquareDecon::changeparameter(const Metadata &md)
{
    try {
        this->read_metadata(md);
    } catch(...) {
        throw;
    };
}
LeastSquareDecon::LeastSquareDecon(const Metadata &md,
        const vector<double> &w,const vector<double> &d)
{
    try {
        this->read_metadata(md);
    } catch(...) {
        throw;
    };
    wavelet=w;
    data=d;
}
void LeastSquareDecon::process()
{

    const string base_error("LeastSquareDecon::process:  ");
    //apply fft to the input trace data
    ComplexArray d_fft(nfft,&(data[0]));
    gsl_fft_complex_forward(d_fft.ptr(), 1, nfft, wavetable, workspace);

    //apply fft to wavelet
    ComplexArray b_fft(nfft,&(wavelet[0]));
    gsl_fft_complex_forward(b_fft.ptr(), 1, nfft, wavetable, workspace);

    //deconvolution: RF=conj(B).*D./(conj(B).*B+damp)
    b_fft.conj();
    ComplexArray rf_fft,conj_b_fft(b_fft);
    b_fft.conj();

    double b_rms=b_fft.rms();
    ComplexArray denom(conj_b_fft*b_fft);
    double theta(b_rms*damp);
    for(int k=0;k<nfft;++k)
    {
      double *ptr;
      ptr=denom.ptr(k);
      /* ptr points to the real part - an oddity of this interface */
      *ptr += theta;
    }
    rf_fft=(conj_b_fft*d_fft)/denom;
    //rf_fft=(conj_b_fft*d_fft)/(conj_b_fft*b_fft+b_rms*damp);
    /* Compute the frequency domain version of the inverse wavelet now
    and save it the object for efficiency. */
    //winv=conj_b_fft/(conj_b_fft*b_fft+b_rms*damp);
    winv=conj_b_fft/denom;

    //apply shaping wavelet but only to rf estimate - actual output and
    //inverse_wavelet methods apply it to when needed there for efficiency
    rf_fft=(*shapingwavelet.wavelet())*rf_fft;

    //ifft gets result
    gsl_fft_complex_inverse(rf_fft.ptr(), 1, nfft, wavetable, workspace);
    if(sample_shift>0)
    {
        for(int k=sample_shift; k>0; k--)
            result.push_back(rf_fft[nfft-k].real());
        for(int k=0; k<data.size()-sample_shift-1; k++)
            result.push_back(rf_fft[k].real());
    }
    else if(sample_shift==0)
    {
        for(int k=0; k<data.size(); k++)
            result.push_back(rf_fft[k].real());
    }
    else
    {
        throw MsPASSError(base_error
              + "Coding error - trying to use an illegal negative time shift parameter",
            ErrorSeverity::Fatal);
    }
}
CoreTimeSeries LeastSquareDecon::actual_output()
{
    try {
        ComplexArray W(nfft,&(wavelet[0]));
        gsl_fft_complex_forward(W.ptr(),1,nfft,wavetable,workspace);
        ComplexArray ao_fft;
        ao_fft=winv*W;
        /* We always apply the shaping wavelet - this perhaps should be optional
        but probably better done with a none option for the shaping wavelet */
        ao_fft=(*shapingwavelet.wavelet())*ao_fft;
        gsl_fft_complex_inverse(ao_fft.ptr(),1,nfft,wavetable,workspace);
        vector<double> ao;
        ao.reserve(nfft);
        for(int k=0; k<ao_fft.size(); ++k) ao.push_back(ao_fft[k].real());
        /* We always shift this wavelet to the center of the data vector.
        We handle the time through the CoreTimeSeries object. */
        int i0=nfft/2;
        ao=circular_shift(ao,i0);
        CoreTimeSeries result(nfft);
        /* Getting dt from here is unquestionably a flaw in the api, but will
        retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
        double dt=this->shapingwavelet.sample_interval();
        result.t0= (-dt*((double)i0));
        result.dt=dt;
        result.live=true;
        result.tref=TimeReferenceType::Relative;
        result.s=ao;
        result.ns=nfft;
        return result;
    } catch(...) {
        throw;
    };
}

CoreTimeSeries LeastSquareDecon::inverse_wavelet(const double t0parent)
{
    try {
      /* Getting dt from here is unquestionably a flaw in the api, but will
 *         retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
      double dt=this->shapingwavelet.sample_interval();
      return (this->FourierInverse(this->winv,
                *shapingwavelet.wavelet(),dt,t0parent));
    } catch(...) {
        throw;
    };
}

CoreTimeSeries LeastSquareDecon::inverse_wavelet()
{
  try{
    return this->inverse_wavelet(0.0);
  }catch(...){throw;};
}
Metadata LeastSquareDecon::QCMetrics()
{
    cerr << "LeastSquareDecon::QCMetrics not yet implemented"<<endl;
    return Metadata();
}
} //End namespace
