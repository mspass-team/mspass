#include <vector>
#include <string>
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/deconvolution/MultiTaperXcorDecon.h"
#include "mspass/deconvolution/dpss.h"
#include "mspass/deconvolution/common_multitaper.h"
using namespace std;
using namespace mspass;
namespace mspass{
MultiTaperXcorDecon::MultiTaperXcorDecon(const Metadata &md)
    : FFTDeconOperator(md)
{

    try {
        this->read_metadata(md,false);
    } catch(...) {
        throw;
    };
    /* assume tapers matrix is created in read_metadata.   We call reserve
    on the three stl vector containers for efficiency. */
    data.reserve(nfft);
    wavelet.reserve(nfft);
    noise.reserve(nfft);
}

MultiTaperXcorDecon::MultiTaperXcorDecon(const MultiTaperXcorDecon &parent)
    : FFTDeconOperator(parent), tapers(parent.tapers)
{
    /* wavelet and data vectors are copied in ScalarDecon copy constructor.
    This method needs a noise vector so we have explicitly copy it here. */
    noise=parent.noise;
    /* ditto for shaping wavelet vector */
    shapingwavelet=parent.shapingwavelet;
    /* multitaper parameters to copy */
    nw=parent.nw;
    taperlen=parent.taperlen;
    damp=parent.damp;
}
int MultiTaperXcorDecon::read_metadata(const Metadata &md,bool refresh)
{
    try {
        const string base_error("MultiTaperXcorDecon::read_metadata method: ");
        int i,j,ii;
        /* We use these temporaries to test for changes when we are not
        initializing */
        int nfft_old,nseq_old, tl_old;
        double nw_old;
        if(refresh)
        {
            nfft_old=nfft;
            nseq_old=nseq;
            tl_old=taperlen;
            nw_old=nw;
        }
        taperlen=ComputeTaperLength(md);
        int nfft_from_win=ComputeFFTLength(md);
        //window based nfft always overrides that extracted directly from md */
        if(nfft_from_win!=nfft)
        {
            this->change_size(nfft_from_win);
        }
        damp=md.get_double("damping_factor");
        nw=md.get_double("time_bandwidth_product");
        /* Wang originally had this as nw*2-2 but Park and Levin say
        the maximum is nw*2-1 which we use here.  P&L papers all use mw=2.5
        with K(seql here) of 3 */
        //seql=md.get_int("lower_dpss");
        nseq=md.get_int("number_tapers");
        if(nseq>(2*nw-1) || (nseq<1))
        {
            cerr << base_error << "(WARNING) Illegal value for number_of tapers parameter="<<nseq
                 << endl << "Resetting to maximum of 2*(time_bandwidth_product)=" ;
            nseq=static_cast<int>(2.0*nw);
            cerr << nseq<<endl;
        }
        int seql=nseq-1;
        /* taperlen must be less than or equal nfft */
        /* old - this can not happen with algorithm change
        if(taperlen>nfft)
            throw MsPASSError(base_error
        			+"illegal taper_length parameter.\ntaper_length must be less than or equal nfft computed from decon time window");
                                      */
        /* This is a bit ugly, but the finite set of parameters that change make
        this the best approach I (glp) can see */
        bool parameters_changed(false);
        if(refresh)
        {
            if( (nfft!=nfft_old) || (nseq!=nseq_old) || (taperlen!=tl_old)
                    || (nw!=nw_old)) parameters_changed=true;
        }
        /* Odd negative logic here.   Idea is to always call this section
        with a constructor, but bypass it when called in refresh mode
        if we don't need to recompute the slepian functions */
        if( (!refresh) || parameters_changed)
        {
            double *work(NULL);
            work=new double[nseq*taperlen];
            /* This procedure allows selection of slepian tapers over a range
            from seql to sequ.   We alway swan the first nseq values so
            set them as follows */
            seql=0;
            int sequ=nseq-1;
            dpss_calc(taperlen, nw, seql, sequ, work);
            /* The tapers are stored in row order in work.  We preserve that
            here but use the dmatrix to store the values */
            tapers=dmatrix(nseq,taperlen);
            for(i=0,ii=0; i<nseq; ++i)
            {
                for(j=0; j<taperlen; ++j)
                {
                    tapers(i,j)=work[ii];
                    ++ii;
                }
            }
            delete [] work;
            shapingwavelet=ShapingWavelet(md,nfft);
        }
        //DEBUG
        //cerr<< "Exiting constructor - damp="<<damp<<endl;
        return 0;
    } catch(...) {
        throw;
    };
}
int MultiTaperXcorDecon::loadnoise(const vector<double> &n)
{
    /* For this implementation we insist n be the same length
     * as d (assumed taperlen) to avoid constant recomputing slepians. */
    if(n.size() == taperlen)
        noise=n;
    else
    {
        int nn=n.size();
        int k;
        noise.clear();
        for(k=0; k<nfft; ++k) noise.push_back(0.0);
        /* This zero padds noise on right when input series length
         * is short.   If ns is long we always take the leading portion */
        if(nn>taperlen) nn=taperlen;
        for(k=0; k<nn; ++k) noise[k]=n[k];
    }
    return 0;
}
int MultiTaperXcorDecon::load(const vector<double>& w, const vector<double>& d,
                                const vector<double>& n)
{
    try {
        int lnr=this->loadnoise(n);
        int ldr;
        ldr=this->ScalarDecon::load(w,d);
        return(lnr+ldr);
    } catch(...) {
        throw;
    };
}
MultiTaperXcorDecon::MultiTaperXcorDecon(const Metadata &md,
        const vector<double> &n,const vector<double> &w,const vector<double> &d)
{
    try {
        this->read_metadata(md,false);
    } catch(...)
    {
        throw;
    }
    wavelet=w;
    data=d;
    noise=n;
}
vector<ComplexArray> MultiTaperXcorDecon::taper_data(const vector<double>& signal)
{
    const string base_error("taper_data procedure:  ");
    /* We put in this sanity check */
    if(signal.size()>nfft) throw MsPASSError(base_error
                + "Illegal input parameters.  Vector of data received is larger than the fft buffer space allocated",
                ErrorSeverity::Invalid);
    /* The tapered data are stored in this vector of arrays */
    int i,j;
    vector<ComplexArray> tdata;
    int ntapers=tapers.rows();
    tdata.reserve(ntapers);
    for(i=0; i<ntapers; ++i)
    {
        double *work=new double[nfft];
        /* This will assure part of vector between end of
         * data and nfft is zero padded */
        for(j=0; j<nfft; ++j) work[j]=0.0;
        for(j=0; j<data.size(); ++j)
        {
            work[j]=tapers(i,j)*signal[j];
        }
        /* Force zero pads always */
        ComplexArray cwork(nfft,work);
        tdata.push_back(cwork);
    }
    return tdata;
}
void MultiTaperXcorDecon::process()
{
    const string base_error("MultiTaperXcorDecon::process():  ");
    /* WARNING about this algorithm. At present there is nothing to stop
    a coding error of calling the algorithm with inconsistent signal and
    noise data vectors. */
    if(noise.size()<=0)
    {
      throw MsPASSError(base_error+"noise data is empty.",ErrorSeverity::Invalid);
    }
    /* The tapered data are stored in this vector of arrays */
    int i,j;
    vector<ComplexArray> tdata;
    tdata=taper_data(data);
    /* Apply fft to each tapered data vector */
    for(i=0; i<nseq; ++i)
    {
        gsl_fft_complex_forward(tdata[i].ptr(),1,nfft,wavetable,workspace);
    }
    /* Now we need to do the same for the wavelet data */
    vector<ComplexArray> wdata;
    wdata=taper_data(wavelet);
    for(i=0; i<nseq; ++i)
    {
        gsl_fft_complex_forward(wdata[i].ptr(),1,nfft,wavetable,workspace);
    }
    /* And the noise data - although with noise we quickly turn to power spectrum */
    vector<ComplexArray> ndata;
    ndata=taper_data(noise);
    for(i=0; i<nseq; ++i)
    {
        gsl_fft_complex_forward(ndata[i].ptr(),1,nfft,wavetable,workspace);
    }
    vector<double> noise_spectrum(ndata[0].abs());
    for(i=1; i<nseq; ++i)
    {
        vector<double> nwork(ndata[i].abs());
        for(j=0; j<ndata.size(); ++j)
        {
            noise_spectrum[j] += nwork[j];
        }
    }
    /* normalize and add damping */
    vector<double>::iterator nptr;
    double scale=damp/(static_cast<double>(nseq));
    //DEBUG
    //cerr << "scale computed from damp="<<damp<<" and nseq="<<nseq<<endl;
    //cerr << "Noise spectrum"<<endl;
    for(nptr=noise_spectrum.begin(); nptr!=noise_spectrum.end(); ++nptr)
    {
        (*nptr) *= scale;
        //DEBUG
        //cerr << (*nptr)<<endl;
    }
    /* We put noise_spectrum data into ndata array with this constructor.  We
    then just add ComplexArray vectors in the decon calculation below */
    ComplexArray fdamp(noise_spectrum.size(),noise_spectrum);
    /* Now we compute the deconvolved data using formulas in Park and Levin 2000

    The ComplexArray object implements operator* as equivalent to .* in matlab.
    i.e. it is NOT a dot product but a sample by sample multiply as normal for
    convolution in the frequency domain.  Here we use that an the += operator
    to form a sum.  This is complicated by the fact tha conj will alter the
    data when called so we have to make copies whenever we call conj */
    ComplexArray numerator(wdata[0]);
    numerator.conj();
    /* Keep this term to define winv. Made inverse below */
    winv=numerator;
    /* Similarly we always compute this for actual output return */
    ao_fft=numerator*wdata[0];
    numerator=numerator*tdata[0]; // initialized with first vector
    for(i=1; i<nseq; ++i)
    {
        ComplexArray work(wdata[i]);
        work.conj();
        winv += work;
        numerator += work*tdata[i];
        ao_fft += (work*wdata[i]);
    }
    ComplexArray denominator(wdata[0]);
    denominator.conj();
    denominator=denominator*wdata[0];
    for(i=1; i<nseq; ++i)
    {
        ComplexArray work(wdata[i]);
        work.conj();
        work = work*wdata[i];
        denominator += work;
    }
    /* Assume fdamp here has already been scaled by damp.  Note Park and
    Levin's formula has the noise power spectrum inside the brackets of the sum
    but a basic theorem of summations is that a constant in a sum of N things is
    N times the constant.   We assume damp will absorb the detail of scaling
    by N=number of tapers. */
    denominator += fdamp;
    ComplexArray rf_fft=numerator/denominator;
    winv=winv/denominator;
    ao_fft=ao_fft/denominator;
    //DEBUG
    /*
    cerr << "Raw RF spectrum"<<endl;
    double dfreq=this->df(0.01);  //dt for test data
    vector<double> specwork(rf_fft.abs());
    for(i=0;i<nfft/2;++i)
    {
        cerr << dfreq*static_cast<double>(i)<<" "<<specwork[i]<<endl;
    }
    */
    /* this applies the shaping wavelet.  NOTE we intentionally do NOT
         * apply this to winv and ao_fft as a bow to some efficiency */
    rf_fft=(*shapingwavelet.wavelet())*rf_fft;
    //DEBUG
    /*
    cerr << "RF spectrum after shaping wavelet applied"<<endl;
    specwork=rf_fft.abs();
    for(i=0;i<nfft/2;++i)
    {
        cerr << dfreq*static_cast<double>(i)<<" "<<specwork[i]<<endl;
    }
    */
    /* Next compute inverse fft, save real part, and apply the time shift.
    The time shift formula assumes wrapping in the form used by the gsl
    algorithm. */
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
            ErrorSeverity::Invalid);
    }
}
CoreTimeSeries MultiTaperXcorDecon::actual_output()
{
    try {
        /* The ao_fft array contains the fft of the actual output wavelet.
         * We do need to appy the shaping wavelet for consistency before
         * converting it to the time domain.*/
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

CoreTimeSeries MultiTaperXcorDecon::inverse_wavelet(const double t0parent)
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
CoreTimeSeries MultiTaperXcorDecon::inverse_wavelet()
{
  try{
    return this->inverse_wavelet(0.0);
  }catch(...){throw;};
}

Metadata MultiTaperXcorDecon::QCMetrics()
{
    try {
        throw MsPASSError("MultiTaperXcorDecon::QCMetrics method not yet implemented",
         ErrorSeverity::Invalid);
    } catch(...) {
        throw;
    };
}
} //End namespace
