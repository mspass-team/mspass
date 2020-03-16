#include <cfloat> // Needed for DBL_EPSILON
#include "mspass/deconvolution/WaterLevelDecon.h"
using namespace std;
using namespace mspass;
namespace mspass{
WaterLevelDecon::WaterLevelDecon(const WaterLevelDecon &parent)
    : FFTDeconOperator(parent)
{
    wlv=parent.wlv;
}
int WaterLevelDecon::read_metadata(const Metadata &md)
{
    try {
        const string base_error("SimpleLeastTaperDecon::read_metadata method: ");
        int nfft_from_win=ComputeFFTLength(md);
        //window based nfft always overrides that extracted directly from md */
        if(nfft_from_win!=nfft)
        {
            this->change_size(nfft_from_win);
        }
        wlv=md.get_double("water_level");
        shapingwavelet=ShapingWavelet(md,nfft);
        return 0;
    } catch(...) {
        throw;
    };
}
/* This constructor is little more than a call to read_metadata for this
opeator */
WaterLevelDecon::WaterLevelDecon(const Metadata &md)
    : FFTDeconOperator(md)
{
    try {
        this->read_metadata(md);
    } catch(...) {
        throw;
    }
}
/* This method is really an alias for read_metadata for this operator */
void WaterLevelDecon::changeparameter(const Metadata &md)
{
    try {
        this->read_metadata(md);
    } catch(...) {
        throw;
    };
}
WaterLevelDecon::WaterLevelDecon(const Metadata &md,
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
void WaterLevelDecon::process()
{

    //apply fft to the input trace data
    ComplexArray d_fft(nfft,&(data[0]));
    gsl_fft_complex_forward(d_fft.ptr(), 1, nfft, wavetable, workspace);

    //apply fft to wavelet
    ComplexArray b_fft(nfft,&(wavelet[0]));
    gsl_fft_complex_forward(b_fft.ptr(), 1, nfft, wavetable, workspace);

    double b_rms=b_fft.rms();
    if(b_rms==0.0) throw MsPASSError("WaterLevelDecon::process():  wavelet data vector is all zeros");

    //water level - count the number of points below water level
    int nunderwater(0);
    for(int i=0; i<nfft; i++)
    {
	/* We have to avoid hard zeros because the formula below will
	 * yield an NaN when that happens from 0/0 */
	double bamp;
	bamp=abs(b_fft[i]);
      /*WARNING:  this is dependent on implementation detail of ComplexArray
      that defines the data as a FortranComplex64 - real,imag pairs. */
        if(bamp<b_rms*wlv)
        {
	   /* Use 64 bit epsilon as the floor for defining a pure zero */
	    if(bamp/b_rms<DBL_EPSILON)
	    {
	       *b_fft.ptr(i)=b_rms*wlv;
	       *(b_fft.ptr(i)+1)=b_rms*wlv;
	    }
	    else
	    {
              //real part
              *b_fft.ptr(i)=(*b_fft.ptr(i)/abs(b_fft[i]))*b_rms*wlv;
              //imag part
              *(b_fft.ptr(i)+1)=(*(b_fft.ptr(i)+1)/abs(b_fft[i]))*b_rms*wlv;
	    }
            ++nunderwater;
        }
    }
    regularization_fraction= ((double)nunderwater)/((double)nfft);
    //deconvolution: RF=D./(B+wlv)
    ComplexArray rf_fft;
    rf_fft=d_fft/b_fft;
    /* Make numerator for inverse from zero lag spike */
    double *d0=new double[nfft];
    for(int k=0;k<nfft;++k) d0[k]=0.0;
    d0[0]=1.0;
    ComplexArray delta0(nfft,d0);
    delete [] d0;
    gsl_fft_complex_forward(delta0.ptr(),1,nfft,wavetable,workspace);
    winv=delta0/b_fft;

    //apply shaping wavelet to rf estimate
    rf_fft=(*shapingwavelet.wavelet())*rf_fft;

    //ifft gets result
    gsl_fft_complex_inverse(rf_fft.ptr(), 1, nfft, wavetable, workspace);
    if(sample_shift>0)
    {
        for(int k=sample_shift; k>0; k--)
            result.push_back(rf_fft[nfft-k].real());
        for(unsigned int k=0; k<data.size()-sample_shift; k++)
            result.push_back(rf_fft[k].real());
    }
    else
    {
        for(unsigned int k=0; k<data.size(); k++)
            result.push_back(rf_fft[k].real());
    }
}
CoreTimeSeries WaterLevelDecon::actual_output()
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
        for(unsigned int k=0; k<ao_fft.size(); ++k) ao.push_back(ao_fft[k].real());
        /* We always shift this wavelet to the center of the data vector.
        We handle the time through the CoreTimeSeries object. */
        int i0=nfft/2;
        ao=circular_shift(ao,i0);
        CoreTimeSeries result(nfft);
        /* Getting dt from here is unquestionably a flaw in the api, but will
        retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
        double dt=this->shapingwavelet.sample_interval();
        /* t0 is time of sample zero - hence normally negative*/
        result.t0=dt*(-(double)i0);
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

CoreTimeSeries WaterLevelDecon::inverse_wavelet(const double t0parent)
{
    try {
      /* Getting dt from here is unquestionably a flaw in the api, but will
 *         retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
	double dt=this->shapingwavelet.sample_interval();
	return (this->FFTDeconOperator::FourierInverse(this->winv,
		*shapingwavelet.wavelet(),dt,t0parent));
    } catch(...) {
        throw;
    };
}
CoreTimeSeries WaterLevelDecon::inverse_wavelet()
{
  try{
    return this->inverse_wavelet(0.0);
  }catch(...){throw;};
}
Metadata WaterLevelDecon::QCMetrics()
{
    try {
        Metadata md;
        md.put("underwater_fraction",regularization_fraction);
        return md;
    } catch(...) {
        throw;
    };
}
}//End namespace
