#include "mspass/deconvolution/CNR3CDecon.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/algorithms/algorithms.h"
using namespace std;
using namespace mspass;
namespace mspass{
CNR3CDecon::CNR3CDecon() : shapingwavelet()
{
  damp=0.0;
  taper_data=false;
  taper=NULL;
}
CNR3CDecon::CNR3CDecon(const AntelopePf& pf) : shapingwavelet(pf)
{
  this->read_parameters(pf);
}
void CNR3CDecon::change_parameters(const AntelopePf& pf)
{
  if(taper!=NULL) delete taper;
  this->read_parameters(pf);
}
void CNR3CDecon::read_parameters(const AntelopePf& pf)
{
  try{
    this->damp=pf.get_double("damping_factor");
    this->snr_regularization_floor=pf.get_double("snr_regularization_floor");
    this->operator_dt=pf.get_double("target_sample_interval");
    double ts,te;
    ts=pf.get_double("deconvolution_data_window_start");
    te=pf.get_double("deconvolution_data_window_end");
    this->processing_window=TimeWindow(ts,te);
    this->winlength=round((te-ts)/operator_dt)+1;
    ts=pf.get_double("noise_window_start");
    te=pf.get_double("noise_window_end");
    this->noise_window=TimeWindow(ts,te);
    int noise_winlength=round((te-ts)/operator_dt)+1;
    double tbp=pf.get_double("time_bandwidth_product");
    long ntapers=pf.get_long("number_tapers");
    this->specengine=MTPowerSpectrumEngine(noise_winlength,tbp,ntapers);
    string sval;
    sval=pf.get_string("taper_type");
    if(sval=="linear")
    {
      double f0,f1,t1,t0;
      AntelopePf pfbranch=pf.get_branch("LinearTaper");
      f0=pfbranch.get_double("front0");
      f1=pfbranch.get_double("front1");
      t1=pfbranch.get_double("tail1");
      t0=pfbranch.get_double("tail0");
      taper=new LinearTaper(f0,f1,t1,t0);
      taper_data=true;
    }
    else if(sval=="cosine")
    {
      double f0,f1,t1,t0;
      AntelopePf pfbranch=pf.get_branch("CosineTaper");
      f0=pfbranch.get_double("front0");
      f1=pfbranch.get_double("front1");
      t1=pfbranch.get_double("tail1");
      t0=pfbranch.get_double("tail0");
      taper=new CosineTaper(f0,f1,t1,t0);
      taper_data=true;
    }
    else if(sval=="vector")
    {
      AntelopePf pfbranch=pf.get_branch("VectorTaper");
      vector<double> tdataread;
      list<string> tdl;
      tdl=pfbranch.get_tbl("taper_data");
      tdataread.reserve(tdl.size());
      list<string>::iterator tptr;
      for(tptr=tdl.begin();tptr!=tdl.end();++tptr)
      {
        double val;
        sscanf(tptr->c_str(),"%lf",&val);
        tdataread.push_back(val);
      }
      taper=new VectorTaper(tdataread);
      taper_data=true;
    }
    else
    {
      taper=NULL;
      taper_data=false;
    }

  }catch(...){throw;};
}
CNR3CDecon::~CNR3CDecon()
{
  if(taper!=NULL) delete taper;
}
/* Small helper to test for common possible input data issues.
If return is nonzero errors were encountered.   Can be retrieved
from elog of d */
int CNR3CDecon::TestSeismogramInput(Seismogram& d,int wcomp)
{
  /* Fractional error allowed in sample interval */
  const double DTSKEW(0.0001);
  const string base_error("TestSeismogramInput:  ");
  int error_count(0);
  if(d.tref!=TimeReferenceType::Relative)
  {
    stringstream ss;
    ss<<base_error<<"Data received are using UTC standard; must be Relative"<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    ++error_count;
  }
  /* 9999 is a magic number used for external wavelet input */
  if((wcomp<0 || wcomp>2) && (wcomp!=-9999))
  {
    stringstream ss;
    ss<<base_error<<"Illegal component ="<<wcomp<<" specified for wavelet"<<endl
      << "Must be 0,1, or 2"<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    ++error_count;
  }
  if((abs(d.dt-operator_dt)/operator_dt)>DTSKEW)
  {
    stringstream ss;
    ss<<base_error<<"Mismatched sample intervals.  "<<
    "Each operator instantance requires a fixed sample interval"<<endl
    <<"operator sample interval="<<operator_dt<<" but data sample interval="
    <<d.dt<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    ++error_count;
  }
  return error_count;
}
void CNR3CDecon::loaddata(Seismogram& d, const int wcomp)
{
  try{
    int errcount;
    errcount=TestSeismogramInput(d,wcomp);
    if(errcount>0)
    {
      stringstream ss;
      ss<<"CNR3CDecon::loaddata:  "<<errcount<<" errors were detected in this call"
        <<endl<<"Check ErrorLog for input Seismogram has detailed error messages"<<endl
        << "Operator does not contain valid data for processing"<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
    if(d.ns==winlength)
    {
      decondata=d;
    }
    else
    {
      /* This can throw an exception, but we let the overall catch for the
      method handle these errors.  Caller will need to not process data
      that create an exception.*/
      decondata=WindowData3C(d,processing_window);
    }
    /* This weird construct is required because ExtractComponent returns a
    CoreSeismogram object that needs additional info to be promoted to a
    Seismogram*/
    wavelet=TimeSeries(ExtractComponent(decondata,wcomp),"Invalid");
  }catch(...){throw;};
}
void CNR3CDecon::loaddata(Seismogram& d, const TimeSeries& w)
{
  try{
    int errcount;
    /* The -9999 is a magic number used to signal the test is
    coming from this variant*/
    errcount=TestSeismogramInput(d,-9999);
    if(errcount>0)
    {
      stringstream ss;
      ss<<"CNR3CDecon::loaddata:  "<<errcount<<" errors were detected in this call"
        <<endl<<"Check ErrorLog for input Seismogram has detailed error messages"<<endl
        << "Operator does not contain valid data for processing"<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
    if(d.ns==winlength)
    {
      decondata=d;
    }
    else
    {
      /* This can throw an exception, but we let the overall catch for the
      method handle these errors.  Caller will need to not process data
      that create an exception.*/
      decondata=WindowData3C(d,processing_window);
    }
    if(w.ns==winlength)
      wavelet=w;
    else
      wavelet=TimeSeries(WindowData(w,processing_window),"Invalid");
  }catch(...){throw;};
}
void CNR3CDecon::loadnoise(Seismogram& n)
{
  try{
    /* If the noise data length is larger than the operator we
    truncate it.  If less we zero pad and post a warning error to n.elog.*/
    Seismogram work(n);
    if(n.ns>FFTDeconOperator::nfft)
    {
      TimeWindow twork(n.time(0),n.time(FFTDeconOperator::nfft-1));
      work=WindowData3C(n,twork);
    }
    else if(n.ns<FFTDeconOperator::nfft)
    {
      work.u=dmatrix(3,FFTDeconOperator::nfft);
      work.u.zero();
      for(int i=0;i<n.ns;++i)
        for(int k=0;k<3;++k) work.u(k,i)=n.u(k,i);
      stringstream ss;
      ss << "CNR3CDecon::loadnoise:  noise data window was shorter than expected"<<endl
        << "Operator wants a window of length at least "<<FFTDeconOperator::nfft
        << "samples"<<endl
        << "Input data has length of only "<<n.ns<<endl
        << "Zero padding to compute power spectrum on operator frequency grid"
        <<endl;
      n.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    }
    /* We always compute noise as total of three component power spectra
    normalized by number of components - sum of squares */
    TimeSeries tswork;
    for(int k=0;k<3;++k)
    {
      tswork=TimeSeries(ExtractComponent(work,k),"Invalid");
      if(k==0)
        this->psnoise = this->specengine.apply(tswork);
      else
        this->psnoise += this->specengine.apply(tswork);
    }
    double scl=1.0/3.0;
    for(int i=0;i<this->psnoise.nf();++i)this->psnoise.spectrum[i]*=scl;
  }catch(...){throw;};
}
void CNR3CDecon::loadnoise(const PowerSpectrum& d)
{
  const string base_error("CNR3CDecon::loadnoise from PowerSpectrum object:  ");
  try{
    int nd=d.nf();
    if(nd!=(FFTDeconOperator::nfft/2))
    {
      stringstream ss;
      ss<<base_error<<"Size mismatch"<<endl
        << "PowerSpectrum object number of frequencies="<<nd<<endl
        << "Operator requires size="<<FFTDeconOperator::nfft<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Complaint);
    }
    const double DFFACTION(0.001);
    double operator_df=1.0/((this->operator_dt)*((double)FFTDeconOperator::nfft));
    if( abs(d.df - operator_df)/operator_df > DFFACTION)
    {
      stringstream ss;
      ss<<base_error<<"Frequency mismatch"<<endl
        << "PowerSpectrum object Rayleigh bin size="<<d.df<<" Hz"<<endl
        << "Operator frequency bin size="<<operator_df<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Complaint);
    }
    psnoise=d;
  }catch(...){throw;};
}
Seismogram CNR3CDecon::process()
{
  const string base_error("CNR3CDecon::process method:  ");
  int i,j,k;
  try{
    TimeSeries work(wavelet);
    /* First we compute the wavelet inverse as it is used to compute
    the solution for all three components.   We could gain some efficiency
    by assuming wavelet had already been tapered, but doing it here makes
    the algorithm much clearer. */
    taper->apply(work);
    vector<double> wvec;
    wvec.reserve(FFTDeconOperator::nfft);
    /* Assume load method assures wavelet.ns <=nfft*/
    for(i=0;i<work.ns;++i) wvec.push_back(work.s[i]);
    for(i=work.ns;i<FFTDeconOperator::nfft;++i) wvec.push_back(0.0);
    ComplexArray cwvec(FFTDeconOperator::nfft,wvec);
    gsl_fft_complex_forward(cwvec.ptr(),1,FFTDeconOperator::nfft,
          wavetable,workspace);
    /* This computes the (regularized) denominator for the decon operator*/
    double df,fNy;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    fNy=df*static_cast<double>(FFTDeconOperator::nfft/2);
    wavelet_snr.clear();
    for(j=0;j<FFTDeconOperator::nfft;++j)
    {
      double *z=cwvec.ptr(j);
      double re=(*z);
      double im=(*(z+1));
      double amp=sqrt( re*re +im*im);
      double f;
      f=df*static_cast<double>(j);
      if(f>fNy) f=2.0*fNy-f;  // Fold frequency axis
      double namp=psnoise.amplitude(f);
      double snr=amp/namp;
      wavelet_snr.push_back(snr);
      if(snr<snr_regularization_floor)
      {
        double scale=sqrt(amp*amp+damp*damp*namp*namp);
        *z *= scale;
        *(z+1) *= scale;
      }
    }
    double *d0=new double[FFTDeconOperator::nfft];
    for(int k=0;k<FFTDeconOperator::nfft;++k) d0[k]=0.0;
    d0[0]=1.0;
    ComplexArray delta0(FFTDeconOperator::nfft,d0);
    delete [] d0;
    gsl_fft_complex_forward(delta0.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
    winv=delta0/cwvec;
    Seismogram rfest(decondata);
    /* This is the proper mspass way to preserve history */
    rfest.append_chain("process_sequence","CNR3CDecon");
    if(rfest.ns!=FFTDeconOperator::nfft) rfest.u=dmatrix(3,FFTDeconOperator::nfft);
    for(k=0;k<3;++k)
    {
      work=TimeSeries(ExtractComponent(decondata,k),"Invalid");
      wvec.clear();
      int ntocopy=FFTDeconOperator::nfft;
      if(ntocopy>work.ns) ntocopy=work.ns;
      for(j=0;j<ntocopy;++j) wvec.push_back(work.s[j]);
      for(j=ntocopy+1;j<FFTDeconOperator::nfft;++j)
                   wvec.push_back(FFTDeconOperator::nfft);
      ComplexArray numerator(FFTDeconOperator::nfft,wvec);
      gsl_fft_complex_forward(numerator.ptr(),1,FFTDeconOperator::nfft,
            wavetable,workspace);
      ComplexArray rftmp=numerator/cwvec;
      rftmp=(*shapingwavelet.wavelet())*rftmp;
      gsl_fft_complex_inverse(rftmp.ptr(), 1, FFTDeconOperator::nfft,
          wavetable, workspace);
      // left off here - needs circulalr shift
      wvec.clear();
      for(j=0;j<FFTDeconOperator::nfft;++j) wvec.push_back(rftmp[j].real());
      if(FFTDeconOperator::sample_shift!=0)
        wvec=circular_shift(wvec,-FFTDeconOperator::sample_shift);
      for(j=0;j<FFTDeconOperator::nfft;++j)rfest.u(k,j)=wvec[j];
    }
    return rfest;
  }catch(...){throw;};
}
TimeSeries CNR3CDecon::ideal_output()
{
  try{
    /* We simply return the sahaping wavelet.  Only complexity here is the
    need to create the higher level TimeSeries object. */
    cerr << "Not yet implemented"<<endl;
  }catch(...){throw;};
}
TimeSeries CNR3CDecon::actual_output()
{
  try {
      ComplexArray W(FFTDeconOperator::nfft,&(wavelet.s[0]));
      gsl_fft_complex_forward(W.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
      ComplexArray ao_fft;
      ao_fft=winv*W;
      /* We always apply the shaping wavelet - this perhaps should be optional
      but probably better done with a none option for the shaping wavelet */
      ao_fft=(*shapingwavelet.wavelet())*ao_fft;
      gsl_fft_complex_inverse(ao_fft.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
      vector<double> ao;
      ao.reserve(FFTDeconOperator::nfft);
      for(int k=0; k<ao_fft.size(); ++k) ao.push_back(ao_fft[k].real());
      /* We always shift this wavelet to the center of the data vector.
      We handle the time through the CoreTimeSeries object. */
      int i0=FFTDeconOperator::nfft/2;
      ao=circular_shift(ao,i0);
      TimeSeries result(wavelet);  // Use this to clone metadata and elog from wavelet
      /* Force these even though they are likely already defined as
      in the parent wavelet TimeSeries. */
      result.t0=operator_dt*(-(double)i0);
      result.dt=this->operator_dt;
      result.live=true;
      result.tref=TimeReferenceType::Relative;
      result.s=ao;
      result.ns=FFTDeconOperator::nfft;
      return result;
  } catch(...) {
      throw;
  };
}
TimeSeries CNR3CDecon::inverse_wavelet(double tshift)
{
  try {
    /* Using the time shift of wavelet.t0 may be a bad idea here.  Will
    need to sort that out in debugging behaviour*/
    CoreTimeSeries invcore(this->FFTDeconOperator::FourierInverse(this->winv,
        *shapingwavelet.wavelet(),operator_dt,wavelet.t0));
    TimeSeries result(invcore,"Invalid");
    /* Copy the error log from wavelet and post some information parameters
    to metadata */
    result.elog=wavelet.elog;
    result.put("waveform_type","deconvolution_inverse_wavelet");
    result.put("decon_type","CNR3CDecon");
  } catch(...) {
      throw;
  };
}
Metadata CNR3CDecon::QCMetrics()
{
  cerr << "CNR3CDecon::QCMEtrics not yet implemented - this is a placeholder";
  return Metadata();
}
} //end namespace
