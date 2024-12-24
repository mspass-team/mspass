#include <cfloat>
#include <vector>
#include <string>
#include "misc/blas.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/utility.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/algorithms/deconvolution/MultiTaperSpecDivDecon.h"
#include "mspass/algorithms/deconvolution/dpss.h"
/* this include is local to this directory*/
#include "mspass/algorithms/deconvolution/common_multitaper.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using mspass::algorithms::amplitudes::normalize;

MultiTaperSpecDivDecon::MultiTaperSpecDivDecon(const Metadata &md)
    : ScalarDecon(md), FFTDeconOperator(md)
{

    try {
        this->read_metadata(md,false);
    } catch(...) {
        throw;
    };
    /* assume tapers matrix is created in read_metadata.   We call reserve
    on the three stl vector containers for efficiency. */
    ScalarDecon::data.reserve(nfft);
    ScalarDecon::wavelet.reserve(nfft);
    noise.reserve(nfft);
}

MultiTaperSpecDivDecon::MultiTaperSpecDivDecon(const MultiTaperSpecDivDecon &parent)
    : ScalarDecon(parent), FFTDeconOperator(parent), tapers(parent.tapers)
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
int MultiTaperSpecDivDecon::read_metadata(const Metadata &md,bool refresh)
{
    try {
        const string base_error("MultiTaperSpecDivDecon::read_metadata method: ");
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
	int nseqtest=static_cast<int>(2.0*nw);
        if(nseq>nseqtest || (nseq<1))
        {
            cerr << base_error << "(WARNING) Illegal value for number_tapers parameter="<<nseq
                 << endl << "Resetting to maximum of 2*(time_bandwidth_product)="
		 << nseqtest<<endl;
            nseq=nseqtest;
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
            here but use the dmatrix to store the values as transpose*/
            tapers=dmatrix(nseq,taperlen);
            //vector<double> norms;
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
        return 0;
    } catch(...) {
        throw;
    };
}
int MultiTaperSpecDivDecon::loadnoise(const vector<double> &n)
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
int MultiTaperSpecDivDecon::load(const vector<double>& w, const vector<double>& d,
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
MultiTaperSpecDivDecon::MultiTaperSpecDivDecon(const Metadata &md,
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
vector<ComplexArray> MultiTaperSpecDivDecon::taper_data(const vector<double>& signal)
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
    vector<double> work;
    work.reserve(nfft);
    for(j=0; j<nfft; ++j) work.push_back(0.0);
    for(i=0; i<ntapers; ++i)
    {
        /* This will assure part of vector between end of
         * data and nfft is zero padded */
        for(j=0; j<this->tapers.columns(); ++j)
        {
            work[j]=tapers(i,j)*signal[j];
        }
        /* Force zero pads always */
        ComplexArray cwork(nfft,work);
        tdata.push_back(cwork);
    }
    return tdata;
}
void MultiTaperSpecDivDecon::process()
{
  const string base_error("MultiTaperSpecDivDecon::process():  ");
  try{
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
        for(j=0; j<noise_spectrum.size(); ++j)
        {
            noise_spectrum[j] += nwork[j];
        }
    }
    /* normalize and add damping */
    vector<double>::iterator nptr;
    /* This makes the scaling indepndent of the choise for tiem bandwidth product*/
    double scale=damp/(static_cast<double>(nseq));
    for(nptr=noise_spectrum.begin(); nptr!=noise_spectrum.end(); ++nptr)
    {
        (*nptr) *= scale;
    }
    /* We compute a RF estimate for each taper independently usinga  variant of
    the water level method.  The variant is that the level is frequency dependent
    defined by sacled noise level.
    */
    /* We need this for amplitude scaling - depend on Parseval's theorem*/
    double wnrm=dnrm2(wavelet.size(),&(wavelet[0]),1);
    vector<ComplexArray> denominator;
    for(i=0;i<nseq;++i)
    {
      ComplexArray work(wdata[i]);
      /* This is kind of messy as the noise_spectrum is a vector of real
      numbers while the wdata vector is a complex fft outputs in fortran
      style.  We use two different indices, but that is a tad dangerous
      UNLESS constructor guarantees nfft is ndata.size()*2 doubles*/
      int number_regularized(0);
      for(j=0;j<nfft;++j)
      {
        /* We do this with pointers.  It makes the code more obscure, but
        works efficiently.  Note this assumes the ComplexArray implementation
        uses FortranComplex64*/
        double *z=work.ptr(j);
        double re=(*z);
        double im=(*(z+1));
        double amp=sqrt( re*re +im*im);
	/* this normalization assumes noise_spectrum is amplitude NOT
	 * power spectrum values */
        if(amp<noise_spectrum[j])
        {
	  double wlscal;
	  /* Avoid divide by zero if amp is tiny */
	  if(fabs(amp)/wnrm<DBL_EPSILON)
	  {
            (*z)=noise_spectrum[j];
	    (*(z+1))=noise_spectrum[j];
	  }
	  else
	  {
            wlscal=noise_spectrum[j]/amp;
            (*z)*=wlscal;
            (*(z+1))*=wlscal;
	  }
          ++number_regularized;
        }
      }
      denominator.push_back(work);
    }
    /* Probably should save these in private area for this estimator*/
    //vector<ComplexArray> rfestimates;
    /* Must clear this and winv containers or they accumulate */
    rfestimates.clear();
    for(i=0;i<nseq;++i)
    {
      ComplexArray work(tdata[i]);
      work=work/denominator[i];
      rfestimates.push_back(work);
    }
    /* Now we want to compute the inverse filter.
    For consistency with related methods we'll store the frequency domain
    values, BUT these now become essentially a matrix - actually stored
    as a vector of vectors */
    //ComplexArray winv;
    winv.clear();
    ao_fft.clear();
    double *d0=new double[nfft];
    for(int k=0;k<nfft;++k) d0[k]=0.0;
    d0[0]=1.0;
    ComplexArray delta0(nfft,d0);
    delete [] d0;
    gsl_fft_complex_forward(delta0.ptr(),1,nfft,wavetable,workspace);
    for(i=0;i<nseq;++i)
    {
      ComplexArray work(delta0);
      work=work/denominator[i];
      winv.push_back(work);
    }
    for(i=0; i<nseq; ++i)
    {
        ComplexArray work(wdata[i]);
	work=work/denominator[i];
        ao_fft.push_back(work);
    }

    /* To mesh with the API of other methods we now compute the average
    rf estimate.  We compute this as a simple average. */
    result.clear();
    for(j=0;j<nfft;++j)result.push_back(0.0);
    vector<double> wtmp;
    for(i=0;i<nseq;++i)
    {
      ComplexArray work(rfestimates[i]);
      /* We always apply the shaping wavelet to the rf estimate.  We do it
      here before averaging. */
      work=(*shapingwavelet.wavelet())*work;
      gsl_fft_complex_inverse(work.ptr(), 1, nfft, wavetable, workspace);
      for(j=0;j<nfft;++j)
      {
        result[j]+=work[j].real();
      }
    }
    double nrmscl=1.0/((double)nseq);
    for(j=0;j<nfft;++j) result[j]*=nrmscl;
    /* Finally do a circular shift if requested. */
    if(sample_shift>0)
        result=circular_shift(result,-sample_shift);
  }catch(...){throw;};
}
CoreTimeSeries MultiTaperSpecDivDecon::actual_output()
{
    try {
      int i,k;
      vector<double> ao;
      ao.reserve(nfft);
      for(k=0;k<nfft;++k)ao.push_back(0.0);
      for(i=0;i<nseq;++i)
      {
        ComplexArray work(ao_fft[i]);
        work=(*shapingwavelet.wavelet())*work;
        gsl_fft_complex_inverse(work.ptr(),1,nfft,wavetable,workspace);
        for(k=0;k<nfft;++k) ao[k]+=work[k].real();
      }
      double nrmscl=1.0/((double)nseq);
      for(k=0;k<nfft;++k) ao[k] *= nrmscl;
      /* We always shift this wavelet to the center of the data vector.
      We handle the time through the CoreTimeSeries object. */
      int i0=nfft/2;
      ao=circular_shift(ao,i0);
      ao = normalize<double>(ao);
      CoreTimeSeries result(nfft);
      /* Getting dt from here is unquestionably a flaw in the api, but will
      retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
      double dt=this->shapingwavelet.sample_interval();
      /* t0 is time of sample zero - hence normally negative*/
      /* Old API
      result.t0=dt*(-(double)i0);
      result.dt=dt;
      result.live=true;
      result.tref=TimeReferenceType::Relative;
      result.s=ao;
      result.ns=nfft;
      */

      result.set_t0(dt*(-(double)i0));
      result.set_dt(dt);
      result.set_live();
      result.set_npts(nfft);
      result.set_tref(TimeReferenceType::Relative);
      for(k=0;k<nfft;++k)result.s[k]=ao[k];
      return result;
    } catch(...) {
        throw;
    };
}

CoreTimeSeries MultiTaperSpecDivDecon::inverse_wavelet(const double t0parent)
{
    try {
      /* Getting dt from here is unquestionably a flaw in the api, but will
 *         retain for now.   Perhaps should a copy of dt in the ScalarDecon object. */
	   double dt=this->shapingwavelet.sample_interval();
     /*algorithm assumes this data vector in result is initialized to nfft zeos */
     CoreTimeSeries result(this->nfft);
     for(int i=0;i<nseq;++i)
     {
       CoreTimeSeries work(this->FFTDeconOperator::FourierInverse(this->winv[i],
                  *shapingwavelet.wavelet(),dt,t0parent));
       if(i==0)
	   result=work;
       else
           result+=work;
     }
     double nrmscal=1.0/((double)nseq);
     for(int k=0;k<result.s.size();++k) result.s[k]*=nrmscal;
     return result;
     }
     catch(...) {
            throw;
     };
}
CoreTimeSeries MultiTaperSpecDivDecon::inverse_wavelet()
{
  try{
    return this->inverse_wavelet(0.0);
  }catch(...){throw;};
}
std::vector<CoreTimeSeries> MultiTaperSpecDivDecon::all_inverse_wavelets
    (const double t0parent)
{
    try{
        std::vector<CoreTimeSeries> all;
        all.reserve(nseq);
        double dt=this->shapingwavelet.sample_interval();
        for(int i=0;i<nseq;++i)
        {
            CoreTimeSeries work(this->FFTDeconOperator::FourierInverse
               (this->winv[i],*shapingwavelet.wavelet(),dt,t0parent));
            all.push_back(work);
        }
        return all;
    }catch(...){throw;};
}
std::vector<CoreTimeSeries> MultiTaperSpecDivDecon::all_rfestimates
    (const double t0parent)
{
    try{
        std::vector<CoreTimeSeries> all;
        all.reserve(nseq);
        double dt=this->shapingwavelet.sample_interval();
        for(int i=0;i<nseq;++i)
        {
            /* Althought this method of FFTDeconOperator was originally
             * written to return inverse wavelet, it can work in this
             * contest too*/
            CoreTimeSeries work(this->FFTDeconOperator::FourierInverse
               (this->rfestimates[i],*shapingwavelet.wavelet(),dt,t0parent));
            all.push_back(work);
        }
        return all;
    }catch(...){throw;};
}
std::vector<CoreTimeSeries> MultiTaperSpecDivDecon::all_actual_outputs
    (const double t0parent)
{
    try{
        std::vector<CoreTimeSeries> all;
        all.reserve(nseq);
        double dt=this->shapingwavelet.sample_interval();
        for(int i=0;i<nseq;++i)
        {
            /* Althought this method of FFTDeconOperator was originally
             * written to return inverse wavelet, it can work in this
             * contest too*/
            CoreTimeSeries work(this->FFTDeconOperator::FourierInverse
               (this->ao_fft[i],*shapingwavelet.wavelet(),dt,t0parent));
            all.push_back(work);
        }
        return all;
    }catch(...){throw;};
}

Metadata MultiTaperSpecDivDecon::QCMetrics()
{
  /* Return only an empty Metadata container.  Done as it is
  easier to maintain the code letting python do this work.
  This also anticipates new metrics being added which would be
  easier in python.*/
  Metadata md;
  return md;
}
} //End namespace
