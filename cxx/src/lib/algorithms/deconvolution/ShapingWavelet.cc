#include <string>
#include <math.h>
#include <gsl/gsl_errno.h>
#include <gsl/gsl_fft_complex.h>
#include "misc/blas.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/algorithms/deconvolution/ShapingWavelet.h"
#include "mspass/algorithms/deconvolution/wavelet.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

ShapingWavelet::ShapingWavelet(const Metadata& md, int nfftin)
{
    const string base_error("ShapingWavelet object constructor:  ");
    try {
        /* We use this to allow a nfft to be set in md.   A bit error prone
         * we add a special error handler. */
        if(nfftin>0)
            this->nfft=nfftin;
        else
        {
            try {
                nfft=md.get_int("operator_nfft");
            } catch(MetadataGetError& mderr)
            {
                throw MsPASSError(base_error
                + "Called constructor with nfft=0 but parameter with key=operator_nfft is not in parameter file",
                 ErrorSeverity::Invalid);
            }
        }
        /* these are workspaces used by gnu's fft algorithm.  Other parts
         * of this library cache them for efficiency, but this one we
         * compute ever time the object is created and then discard it. */
        gsl_fft_complex_wavetable *wavetable;
        gsl_fft_complex_workspace *workspace;
        wavetable = gsl_fft_complex_wavetable_alloc (nfft);
        workspace = gsl_fft_complex_workspace_alloc (nfft);
        string wavelettype=md.get_string("shaping_wavelet_type");
        wavelet_name=wavelettype;
        dt=md.get_double("shaping_wavelet_dt");
        double *r;
        if(wavelettype=="gaussian")
        {
            float fpeak=md.get_double("shaping_wavelet_frequency");
            //construct wavelet and fft
            r=gaussian(fpeak,(float)dt,nfft);
            w=ComplexArray(nfft,r);
            gsl_fft_complex_forward(w.ptr(), 1, nfft, wavetable, workspace);
            delete [] r;
        }
        else if(wavelettype=="ricker")
        {
            float fpeak=md.get_double("shaping_wavelet_frequency");
            //construct wavelet and fft
            r=rickerwavelet(fpeak,(float)dt,nfft);
//DEBUG
//cerr << "Ricker shaping wavelet"<<endl;
//for(int k=0;k<nfft;++k) cerr << r[k]<<endl;
            w=ComplexArray(nfft,r);
            gsl_fft_complex_forward(w.ptr(), 1, nfft, wavetable, workspace);
            delete [] r;
        }
        /*   This option requires a package to compute zero phase wavelets of
        some specified type and bandwidth.  Previous used antelope filters which
        colides with open source goal of mspass.   Another implementation of
        TimeInvariantFilter and this could be restored.
        */
        /*
        else if(wavelettype=="filter_response")
        {
            string filterparam=md.get_string("filter");
            TimeInvariantFilter filt(filterparam);
            TimeSeries dtmp(nfft);
            dtmp.ns=nfft;
            dtmp.dt=dt;
            dtmp.live=true;
            dtmp.t0=(-dt*static_cast<double>(nfft/2));
            dtmp.tref=relative;
            int i;
            i=dtmp.sample_number(0.0);
            dtmp.s[i]=1.0;  // Delta function at 0
            filt.zerophase(dtmp);
	    // We need to shift the filter response now back to
 	   // zero to avoid time shifts in output
            dtmp.s=circular_shift(dtmp.s,nfft/2);
            w=ComplexArray(dtmp.s.size(), &(dtmp.s[0]));
            gsl_fft_complex_forward(w.ptr(), 1, nfft, wavetable, workspace);
        }
        */
        else if((wavelettype=="slepian") || (wavelettype=="Slepian") )
        {
          double tbp=md.get_double("time_bandwidth_product");
          double target_pulse_width=md.get_double("target_pulse_width");
          /* Sanity check on pulse width */
          if(target_pulse_width>(nfft/4) || (target_pulse_width<tbp))
          {
            stringstream ss;
            ss << "ShapingWavelet Metadata constructor:  bad input parameters for Slepian shaping wavelet"
              <<endl
              << "Specified target_pulse_width of "<<target_pulse_width<<" samples"<<endl
              << "FFT buffer size="<<nfft<<" and time bandwidth product="<<tbp<<endl
              << "Pulse width should be small fraction of buffer size but ge than tbp"<<endl;
            throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
          }
          double c=tbp/target_pulse_width;
          int nwsize=round(c*(static_cast<double>(nfft)));
          double *wtmp=slepian0(tbp,nwsize);
          double *work=new double[nfft];
          for(int k=0;k<nfft;++k)work[k]=0.0;
          dcopy(nwsize,wtmp,1,work,1);
          delete [] wtmp;
          w=ComplexArray(nfft,work);
          gsl_fft_complex_forward(w.ptr(), 1, nfft, wavetable, workspace);
          delete [] work;
        }
        else if(wavelettype=="none")
        {
            /* Prototype code issued an error in this condition, but we accept it
            here as an option defined by none.  We could do this by putting
	          all ones in the w array but using a delta function at zero lage
	          avoids scaling issues for little cost - this assumes this
	          object is created only occassionally and not millions of times */
	        double *r=new double[nfft];
	        for(int k=0;k<nfft;++k) r[k]=0.0;
	        r[0]=1.0;
          w=ComplexArray(nfft,r);
	        delete [] r;
        }
        else
        {
            throw MsPASSError(base_error
                  + "illegal value for shaping_wavelet_type="+wavelettype,
                  ErrorSeverity::Invalid);
        }
        gsl_fft_complex_wavetable_free (wavetable);
        gsl_fft_complex_workspace_free (workspace);
        df=1.0/(dt*((double)nfft));
    } catch(MsPASSError& err)
    {
        cerr <<base_error<<"Something threw an unhandled MsPASSError with this message:"<<endl;
        err.log_error();
        cerr << "This is a nonfatal bug that needs to be fixed"<<endl;
        throw err;
    } catch(...)
    {
        throw;
    }
}
ShapingWavelet::ShapingWavelet(const ShapingWavelet& parent) : w(parent.w)
{
    dt=parent.dt;
    df=parent.df;
    wavelet_name=parent.wavelet_name;
}
ShapingWavelet::ShapingWavelet(CoreTimeSeries d, int nfft)
{
    const string base_error("ShapingWavelet TimeSeries constructor:  ");
    wavelet_name=string("data");
    /* Silently handle this default condition that allows calling the
     * constructor without the nfft argument */
    if(nfft<=0) nfft=d.npts();
    dt=d.dt();
    df=1.0/(dt*((double)nfft));
    /* This is prone to an off by one error */
    double t0;
    if(nfft%2)
        t0=-(double)(nfft/2) + 1;
    else
        t0=-(double)(nfft/2);
    t0 *= dt;
    /* A basic sanity check on d */
    if(d.time_is_UTC())
      throw MsPASSError(base_error
                + "Shaping wavelet must be defined in relative time centered on 0",
                 ErrorSeverity::Invalid);
    if( (d.endtime()<t0) || (d.t0()>(-t0)) )
        throw MsPASSError(base_error + "Input wavelet time span is illegal\n"
            + "Wavelet must be centered on 0 reference time",
            ErrorSeverity::Invalid);
    /* Create a work vector an initialize it to zeros to make insertion
     * algorithm easier */
    vector<double> dwork;
    dwork.reserve(nfft);
    int i;
    for(i=0; i<nfft; ++i) dwork.push_back(0.0);
    /* We loop over
     * we skip through d vector until we insert */
    for(i=0; i<d.npts(); ++i)
    {
        double t;
        t=t0+dt*((double)i);
        int iw=d.sample_number(t);
        if(t>d.endtime()) break;
        if( (iw>=0) && (iw<nfft)) dwork[i]=d.s[iw];
    }
    gsl_fft_complex_wavetable *wavetable;
    gsl_fft_complex_workspace *workspace;
    wavetable = gsl_fft_complex_wavetable_alloc (nfft);
    workspace = gsl_fft_complex_workspace_alloc (nfft);
    w=ComplexArray(nfft,&(dwork[0]));
    gsl_fft_complex_forward(w.ptr(), 1, nfft, wavetable, workspace);
    gsl_fft_complex_wavetable_free (wavetable);
    gsl_fft_complex_workspace_free (workspace);
}
ShapingWavelet& ShapingWavelet::operator=(const ShapingWavelet& parent)
{
    if(this != &parent)
    {
        w=parent.w;
        dt=parent.dt;
        df=parent.df;
        wavelet_name=parent.wavelet_name;
    }
    return *this;
}
CoreTimeSeries ShapingWavelet::impulse_response()
{
    try {
        int nfft=w.size();
        gsl_fft_complex_wavetable *wavetable;
        gsl_fft_complex_workspace *workspace;
        wavetable = gsl_fft_complex_wavetable_alloc (nfft);
        workspace = gsl_fft_complex_workspace_alloc (nfft);
        /* We need to copy the current shaping wavelet or the inverse fft
         * will make it invalid */
        ComplexArray iwf(w);
        gsl_fft_complex_inverse(iwf.ptr(), 1, nfft, wavetable, workspace);
        gsl_fft_complex_wavetable_free (wavetable);
        gsl_fft_complex_workspace_free (workspace);
        CoreTimeSeries result(nfft);
        /* old API
        result.tref=TimeReferenceType::Relative;
        result.ns=nfft;
        result.dt=dt;
        result.t0 = dt*(-(double)nfft/2);
        result.live=true;
        */

        result.set_tref(TimeReferenceType::Relative);
        result.set_npts(nfft);
        result.set_dt(dt);
        result.set_t0(dt*(-(double)nfft/2));
        result.set_live();
        /* Unfold the fft output */
        int k,shift;
        shift=nfft/2;
        // Need this because constructor fills initially with nfft zeros and
        // we use this approach to unfold the fft output
        result.s.clear();
	      for(k=0;k<nfft;++k) result.s.push_back(iwf[k].real());
	      result.s=circular_shift(result.s,shift);
        return result;
    } catch(...) {
        throw;
    };
}
} //End namespace
