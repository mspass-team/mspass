#include <string.h>  //needed for memcpy
#include <algorithm>
#include "mspass/utility/MsPASSError.h"
#include "mspass/deconvolution/CNR3CDecon.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/algorithms/algorithms.h"
using namespace std;
using namespace mspass;
/* This enum is file scope to intentionally exclude it from python wrappers.
It is used internally to define the algorithm the processor is to run.
I (glp) chose that approach over the inheritance approach used in the scalar
methods as an artistic choice.   It is a matter of opinion which approach
is better.  This makes one symbol do multiple things with changes done in
the parameter setup as opposed to having to select the right symbolic name
to construct.  Anyway, this enum defines algorithms that can be chosen for
processing.
*/
enum class CNR3C_algorithms{
    generalized_water_level,
    colored_noise_damping
};
namespace mspass{
CNR3CDecon::CNR3CDecon() : FFTDeconOperator(),specengine(),
  shapingwavelet(),psnoise(),psnoise_data(),decondata(),wavelet()
{
  algorithm=CNR3C_algorithms::undefined;
  operator_dt=1.0;
  winlength=0;
  processing_window=TimeWindow(0.0,1.0);
  noise_window=TimeWindow(0.0,1.0);
  damp=0.0;
  noise_floor=0.0001;
  snr_regularization_floor=1.5;
  taper_data=false;
  for(int k=0;k<3;++k)
  {
    signal_bandwidth_fraction[k]=0.0;
    peak_snr[k]=0.0;
  }
}
CNR3CDecon::CNR3CDecon(const AntelopePf& pf)
   : FFTDeconOperator(pf),specengine(),shapingwavelet(pf),psnoise(),
      psnoise_data(),decondata(),wavelet()
{
  this->read_parameters(pf);
}
/* Note this method assumes BasicMetadata is actually an AntelopePf.*/
void CNR3CDecon::change_parameters(const BasicMetadata& basemd)
{
    try{
      AntelopePf pf=dynamic_cast<const AntelopePf&>(basemd);
      this->read_parameters(pf);
    }catch(...){throw;};
}
void CNR3CDecon::read_parameters(const AntelopePf& pf)
{
  try{
    string stmp;
    stmp=pf.get_string("algorithm");
    if(stmp=="generalized_water_level")
    {
      algorithm=CNR3C_algorithms::generalized_water_level;
    }
    else if(stmp=="colored_noise_damping")
    {
      algorithm=CNR3C_algorithms::colored_noise_damping;
    }
    else
    {
      throw MsPASSError("CNR3CDecon::read_parameters:  invalid value for parameter algorithm="
        + stmp,ErrorSeverity::Invalid);
    }
    this->damp=pf.get_double("damping_factor");
    /* Note this paramter is used for both the damping method and the
    generalized_water_level */
    this->noise_floor=pf.get_double("noise_floor");
    this->snr_regularization_floor=pf.get_double("snr_regularization_floor");
    this->operator_dt=pf.get_double("target_sample_interval");
    double ts,te;
    ts=pf.get_double("deconvolution_data_window_start");
    te=pf.get_double("deconvolution_data_window_end");
    this->processing_window=TimeWindow(ts,te);
    this->winlength=round((te-ts)/operator_dt)+1;
    /* In this algorithm we are very careful to avoid circular convolution
    artifacts that I (glp) suspect may be a problem in some frequency domain
    implementations of rf deconvolution.   Here we set the length of the fft
    (nfft) to a minimum of 3 times the window size.   That allows 1 window
    of padding around both ends of the waveform being deconvolved.  Circular
    shift is used to put the result back in a rational time base. */
    int minwinsize=3*(this->winlength);
    /* This complicated set of tests to set nfft is needed to mesh with
     * ShapingWavelet constructor and FFTDeconOperator api constraints created by
     * use in other classes in this directory that also use these */
    int nfftneeded=nextPowerOf2(minwinsize);
    int nfftpf=pf.get<int>("operator_nfft");
    if(nfftneeded!=nfftpf)
    {
      FFTDeconOperator::change_size(nfftneeded);
      AntelopePf pfcopy(pf);
      pfcopy.put("operator_nfft",nfftneeded);
      this->shapingwavelet=ShapingWavelet(pfcopy);
    }
    FFTDeconOperator::change_size(nextPowerOf2(minwinsize));
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
      AntelopePf pfb=pf.get_branch("LinearTaper");
      AntelopePf pfbranch=pfb.get_branch("wavelet_taper");
      f0=pfbranch.get_double("front0");
      f1=pfbranch.get_double("front1");
      t1=pfbranch.get_double("tail1");
      t0=pfbranch.get_double("tail0");
      wavelet_taper=shared_ptr<LinearTaper>(new LinearTaper(f0,f1,t1,t0));
      pfbranch=pfb.get_branch("data_taper");
      f0=pfbranch.get_double("front0");
      f1=pfbranch.get_double("front1");
      t1=pfbranch.get_double("tail1");
      t0=pfbranch.get_double("tail0");
      data_taper=shared_ptr<LinearTaper>(new LinearTaper(f0,f1,t1,t0));
      taper_data=true;
    }
    else if(sval=="cosine")
    {
      double f0,f1,t1,t0;
      AntelopePf pfb=pf.get_branch("CosineTaper");
      AntelopePf pfbranch=pfb.get_branch("wavelet_taper");
      f0=pfbranch.get_double("front0");
      f1=pfbranch.get_double("front1");
      t1=pfbranch.get_double("tail1");
      t0=pfbranch.get_double("tail0");
      wavelet_taper=shared_ptr<CosineTaper>(new CosineTaper(f0,f1,t1,t0));
      pfbranch=pfb.get_branch("data_taper");
      f0=pfbranch.get_double("front0");
      f1=pfbranch.get_double("front1");
      t1=pfbranch.get_double("tail1");
      t0=pfbranch.get_double("tail0");
      data_taper=shared_ptr<CosineTaper>(new CosineTaper(f0,f1,t1,t0));
      taper_data=true;
    }
    else if(sval=="vector")
    {
      AntelopePf pfbranch=pf.get_branch("VectorTaper");
      vector<double> tdataread;
      list<string> tdl;
      tdl=pfbranch.get_tbl("wavelet_taper_vector");
      tdataread.reserve(tdl.size());
      list<string>::iterator tptr;
      for(tptr=tdl.begin();tptr!=tdl.end();++tptr)
      {
        double val;
        sscanf(tptr->c_str(),"%lf",&val);
        tdataread.push_back(val);
      }
      wavelet_taper=shared_ptr<VectorTaper>(new VectorTaper(tdataread));
      tdataread.clear();
      tdl.clear();
      tdl=pfbranch.get_tbl("data_taper_vector");
      tdataread.reserve(tdl.size());
      for(tptr=tdl.begin();tptr!=tdl.end();++tptr)
      {
        double val;
        sscanf(tptr->c_str(),"%lf",&val);
        tdataread.push_back(val);
      }
      data_taper=shared_ptr<VectorTaper>(new VectorTaper(tdataread));
      taper_data=true;
    }
    else
    {
      //wavelet_taper=NULL;
      //data_taper=NULL;
      taper_data=false;
    }

  }catch(...){throw;};
}
CNR3CDecon::CNR3CDecon(const CNR3CDecon& parent) :
  processing_window(parent.processing_window),
  noise_window(parent.noise_window),
  specengine(parent.specengine),
  shapingwavelet(parent.shapingwavelet),
  psnoise(parent.psnoise),
  psnoise_data(parent.psnoise_data),
  decondata(parent.decondata),
  wavelet(parent.wavelet),
  wavelet_taper(parent.wavelet_taper),
  data_taper(parent.data_taper),
  ao_fft(parent.ao_fft),
  wavelet_snr(parent.wavelet_snr)
{
  algorithm=parent.algorithm;
  taper_data=parent.taper_data;
  operator_dt=parent.operator_dt;
  winlength=parent.winlength;
  damp=parent.damp;
  noise_floor=parent.noise_floor;
  snr_regularization_floor=parent.snr_regularization_floor;
  band_snr_floor=parent.band_snr_floor;
  regularization_bandwidth_fraction=parent.regularization_bandwidth_fraction;
  for(int k=0;k<3;++k)
  {
    signal_bandwidth_fraction[k]=parent.signal_bandwidth_fraction[k];
    peak_snr[k]=parent.peak_snr[k];
  }
}
CNR3CDecon& CNR3CDecon::operator=(const CNR3CDecon& parent)
{
  if(this!=(&parent))
  {
    algorithm=parent.algorithm;
    processing_window=parent.processing_window;
    noise_window=parent.noise_window;
    specengine=parent.specengine;
    psnoise=parent.psnoise;
    psnoise_data=parent.psnoise_data;
    decondata=parent.decondata;
    wavelet=parent.wavelet;
    shapingwavelet=parent.shapingwavelet;
    ao_fft=parent.ao_fft;
    wavelet_snr=parent.wavelet_snr;
    taper_data=parent.taper_data;
    operator_dt=parent.operator_dt;
    winlength=parent.winlength;
    damp=parent.damp;
    noise_floor=parent.noise_floor;
    snr_regularization_floor=parent.snr_regularization_floor;
    band_snr_floor=parent.band_snr_floor;
    regularization_bandwidth_fraction=parent.regularization_bandwidth_fraction;
    for(int k=0;k<3;++k)
    {
      signal_bandwidth_fraction[k]=parent.signal_bandwidth_fraction[k];
      peak_snr[k]=parent.peak_snr[k];
    }
    wavelet_taper=parent.wavelet_taper;
    data_taper=parent.data_taper;

  }
  return *this;
}
CNR3CDecon::~CNR3CDecon()
{
  //if(wavelet_taper!=NULL) delete wavelet_taper;
  //if(data_taper!=NULL) delete data_taper;
}
/* Small helper to test for common possible input data issues.
If return is nonzero errors were encountered.   Can be retrieved
from elog of d */
int CNR3CDecon::TestSeismogramInput(Seismogram& d,const int wcomp,const bool loadnoise)
{
  /* Fractional error allowed in sample interval */
  const double DTSKEW(0.0001);
  const string base_error("TestSeismogramInput:  ");
  int error_count(0);
  if(d.time_is_relative())
  {
    stringstream ss;
    ss<<base_error<<"Data received are using UTC standard; must be Relative"<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Invalid);
    ++error_count;
  }
  /* 9999 is a magic number used for external wavelet input */
  if((wcomp<0 || wcomp>2) && (wcomp!=-9999))
  {
    stringstream ss;
    ss<<base_error<<"Illegal component ="<<wcomp<<" specified for wavelet"<<endl
      << "Must be 0,1, or 2"<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Invalid);
    ++error_count;
  }
  if((abs(d.dt()-operator_dt)/operator_dt)>DTSKEW)
  {
    stringstream ss;
    ss<<base_error<<"Mismatched sample intervals.  "<<
    "Each operator instantance requires a fixed sample interval"<<endl
    <<"operator sample interval="<<operator_dt<<" but data sample interval="
    <<d.dt()<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    ++error_count;
  }
  if(this->processing_window.start<d.t0() || this->processing_window.end>d.endtime())
  {
    stringstream ss;
    ss<<base_error<<"Data time window mistmatch."<<endl
	    <<"Data span relative time range ="<<d.t0()<<" to "<<d.endtime()<<endl
	    <<"Processing window range of "<<this->processing_window.start
      	    << " to " <<this->processing_window.start<<" is not inside data range"<<endl;
    d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Invalid);
    ++error_count;
  }
  if(loadnoise)
  {
    if(this->noise_window.start<d.t0() || this->noise_window.end>d.endtime())
    {
      stringstream ss;
      ss<<base_error<<"Noise time window mistmatch."<<endl
            <<"Data span relative time range ="<<d.t0()<<" to "<<d.endtime()<<endl
            <<"Noise window range of "<<this->noise_window.start<<d.t0()
            << " to " <<this->noise_window.start<<" is not inside data range"<<endl;

      d.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Invalid);
      ++error_count;
    }
  }
  return error_count;
}
void CNR3CDecon::loaddata(Seismogram& d, const int wcomp,const bool loadnoise)
{
  try{
    if(d.dead()) throw MsPASSError("CNR3CDecon::loaddata method received data marked dead",
		    ErrorSeverity::Invalid);
    /* This does everything except load the wavelet from wcomp so we just
     * invoke it here. */
    this->loaddata(d,loadnoise);
    /* We need to pull wcomp now because we alter the decondata matrix with
     * padding next.  We don't want that for the wavelet at this stage as
     * the loadwavelet method handles the padding stuff and we call it after
     * windowing*/
    CoreTimeSeries wtmp(ExtractComponent(decondata,wcomp));
    wtmp=WindowData(wtmp,this->processing_window);
    TimeSeries wtmp2(wtmp,"Invalid");
    this->loadwavelet(wtmp2);
  }catch(...){throw;};
}
void CNR3CDecon::loaddata(Seismogram& d,const bool nload)
{
  if(d.dead()) throw MsPASSError("CNR3CDecon::loaddata method received data marked dead",
		    ErrorSeverity::Invalid);
  try{
    int errcount;
    /* The -9999 is a magic number used to signal the test is
    coming from this variant*/
    errcount=TestSeismogramInput(d,-9999,nload);
    if(errcount>0)
    {
      stringstream ss;
      ss<<"CNR3CDecon::loaddata:  "<<errcount<<" errors were detected in this call"
        <<endl<<"Check error log for input Seismogram has detailed error messages"<<endl
        << "Operator does not contain valid data for processing"<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
    CoreSeismogram dtmp(WindowData3C(d,this->processing_window));
    this->decondata=Seismogram(dtmp,"invalid");
    if(FFTDeconOperator::nfft<(2*this->winlength))
    {
      cerr << "CNR3CDecon:  coding error in loaddata method"<<endl
	      << "fft buffer size="<<FFTDeconOperator::nfft<<endl
	      << "winlength is only "<<this->winlength<<endl
	      << "Expect winlength to be 3 times nfft"<<endl
	      << "Debug exit to avoid seg fault"<<endl;
      exit(-1);
    }
    decondata.u=dmatrix(3,FFTDeconOperator::nfft);
    decondata.u.zero();
    /* Offset by winlength to put zero pad at front of the data.  */
    int k,i,ii;
    for(k=0;k<3;++k)
        for(i=0,ii=(this->winlength);i<(this->winlength);++i,++ii)
        {
            decondata.u(k,ii)=dtmp.u(k,i);
        }
    decondata.set_npts(FFTDeconOperator::nfft);
    double newt0=decondata.t0() - operator_dt*static_cast<double>(winlength);
    decondata.set_t0(newt0);
    //decondata.t0 -= operator_dt*static_cast<double>(winlength);
    if(nload)
    {
      Seismogram ntmp(WindowData3C(d,this->noise_window),"Invalid");
      this->loadnoise_data(ntmp);
    }
  }catch(...){throw;};
}
/* Note we intentionally do not trap nfft size mismatch in this function because
 * we assume loadwavelet would be called within loaddata or after calls to loaddata
 * */
void CNR3CDecon::loadwavelet(const TimeSeries& w)
{
  if(w.dead()) throw MsPASSError("CNR3CDecon::loadwavelet method received data marked dead",
		    ErrorSeverity::Invalid);
  if(w.npts()<=0) throw MsPASSError("CNR3CDecon::loadwavelet method received an empty TimeSeries (number samples <=0)",
		  ErrorSeverity::Invalid);
  try{
    int k,kk;
    int ns_to_copy;
    this->wavelet=w;
    if(w.npts()>(FFTDeconOperator::nfft-this->winlength))
    {
      ns_to_copy=FFTDeconOperator::nfft-2*this->winlength;
      stringstream ss;
      ss<<"loadwavelet method:  size mismatch.  Wavelet received has length="<<w.npts()<<endl
	      << "This is larger than 3x the processing window length of "<<this->winlength<<endl
	      << "Wavelet length must be less than or equal to 3x processing window length"<<endl
	      << "Truncated on the right to processing window length - results may be invalid"
	      <<endl;
      wavelet.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    }
    else
      ns_to_copy=w.npts();
    this->wavelet.s.clear();
    this->wavelet.s.reserve(FFTDeconOperator::nfft);
    for(k=0;k<FFTDeconOperator::nfft;++k)this->wavelet.s.push_back(0.0);
    /* this retains winlength zeros at the front */
    for(k=0,kk=this->winlength;k<ns_to_copy;++k,++kk)this->wavelet.s[kk]=w.s[k];
    //this->wavelet.t0 -= operator_dt*static_cast<double>(winlength);
    //this->wavelet.ns=FFTDeconOperator::nfft;
    double newt0;
    newt0=this->wavelet.t0() - operator_dt*static_cast<double>(winlength);
    this->wavelet.set_t0(newt0);
    this->wavelet.set_npts(FFTDeconOperator::nfft);
    switch(algorithm)
    {
      /* Note all the algorithms here alter wavelet by applying a taper */
      case CNR3C_algorithms::generalized_water_level:
        compute_gwl_inverse();
        break;
      case CNR3C_algorithms::colored_noise_damping:
      default:
        compute_gdamp_inverse();
    };
  }catch(...){throw;};
}
void CNR3CDecon::loadnoise_data(const Seismogram& n)
{
  if(n.dead()) throw MsPASSError("CNR3CDecon::loadnoise_data method received data marked dead",
		    ErrorSeverity::Invalid);
  try{
    /* If the noise data length is larger than the operator we silenetly
    truncate it.  If less we zero pad*/
    CoreSeismogram work(n);
    if(n.npts()>FFTDeconOperator::nfft)
    {
      TimeWindow twork(n.t0(),n.time(FFTDeconOperator::nfft-1));
      work=WindowData3C(n,twork);
    }
    else if(n.npts()<=FFTDeconOperator::nfft)
    {
      work.u=dmatrix(3,FFTDeconOperator::nfft);
      work.u.zero();
      for(int i=0;i<n.npts();++i)
        for(int k=0;k<3;++k) work.u(k,i)=n.u(k,i);
    }
    /* We always compute noise as total of three component power spectra
    normalized by number of components - sum of squares */
    TimeSeries tswork;
    for(int k=0;k<3;++k)
    {
      tswork=TimeSeries(ExtractComponent(work,k),"Invalid");
      if(k==0)
        this->psnoise_data = this->specengine.apply(tswork);
      else
        this->psnoise_data += this->specengine.apply(tswork);
    }
    /* We define total power as the average on all three
    componens */
    double scl=1.0/3.0;
    for(int i=0;i<this->psnoise_data.nf();++i)
         this->psnoise_data.spectrum[i]*=scl;
  }catch(...){throw;};
}
void CNR3CDecon::loadnoise_data(const PowerSpectrum& d)
{
  try{
    psnoise_data=d;
  }catch(...){throw;};
}

void CNR3CDecon::loadnoise_wavelet(const TimeSeries& n)
{
  if(n.dead()) throw MsPASSError("CNR3CDecon::loadnoise_wavelet method received data marked dead",
		    ErrorSeverity::Invalid);
  try{
    /* If the noise data length is larger than the operator we silenetly
    truncate it.  If less we zero pad*/
    CoreTimeSeries work(n);
    if(n.npts()>FFTDeconOperator::nfft)
    {
      TimeWindow twork(n.t0(),n.time(FFTDeconOperator::nfft-1));
      work=WindowData(dynamic_cast<const CoreTimeSeries&>(n),twork);
    }
    else if(n.npts()<=FFTDeconOperator::nfft)
    {
      work.s.reserve(FFTDeconOperator::nfft);
      for(int i=0;i<n.npts();++i)
        work.s.push_back(n.s[i]);
      for(int i=n.npts();i<FFTDeconOperator::nfft;++i) work.s.push_back(0.0);
    }
    psnoise=this->specengine.apply(TimeSeries(work,"INVALID"));
  }catch(...){throw;};
}

void CNR3CDecon::loadnoise_wavelet(const PowerSpectrum& d)
{
  try{
    psnoise=d;
  }catch(...){throw;};
}
/* Note this is intentionally not a reference to assure this is a copy */
void CNR3CDecon::compute_gwl_inverse()
{
  try{
    if(taper_data) wavelet_taper->apply(this->wavelet);
    ComplexArray cwvec(this->wavelet.npts(),this->wavelet.s);
    gsl_fft_complex_forward(cwvec.ptr(),1,FFTDeconOperator::nfft,
          wavetable,workspace);
    /* This computes the (regularized) denominator for the decon operator*/
    double df,fNy;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    fNy=df*static_cast<double>(FFTDeconOperator::nfft/2);
    /* We need largest noise amplitude to establish a relative noise floor.
    We use this std::algorithm to find it in the spectrum vector */
    vector<double>::iterator maxnoise;
    maxnoise=max_element(psnoise.spectrum.begin(),psnoise.spectrum.end());
    //spectrum is power, we need amplitude so sqrt here
    double scaled_noise_floor=noise_floor*sqrt(*maxnoise);
    wavelet_snr.clear();
    int nreg(0);
    for(int j=0;j<FFTDeconOperator::nfft;++j)
    {
      double *z=cwvec.ptr(j);
      double re=(*z);
      double im=(*(z+1));
      double amp=sqrt( re*re +im*im);
      double f;
      f=df*static_cast<double>(j);
      if(f>fNy) f=2.0*fNy-f;  // Fold frequency axis
      double namp=psnoise.amplitude(f);
      /* Avoid divide by zero that could randomly happen with simulation data*/
      double snr;
      if((namp/amp)<DBL_EPSILON)
          snr=10000.0;
      else
          snr=amp/namp;
      wavelet_snr.push_back(snr);
      if(snr<snr_regularization_floor)
      {
        double scale;
        if(namp<scaled_noise_floor)
        {
          scale=snr_regularization_floor*scaled_noise_floor/amp;
        }
        else
        {
          scale=snr_regularization_floor*namp/amp;
        }
        re *= scale;
        im *= scale;
        *z = re;
        *(z+1) = im;
        ++nreg;
      }
    }
    /* This is used in QCMetric */
    regularization_bandwidth_fraction=static_cast<double>(nreg)
                / static_cast<double>(FFTDeconOperator::nfft);
    double *d0=new double[FFTDeconOperator::nfft];
    for(int k=0;k<FFTDeconOperator::nfft;++k) d0[k]=0.0;
    d0[0]=1.0;
    ComplexArray delta0(FFTDeconOperator::nfft,d0);
    delete [] d0;
    gsl_fft_complex_forward(delta0.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
    winv=delta0/cwvec;
  }catch(...){throw;};
}
/* Note this is intentionally not a reference to assure this is a copy */
void CNR3CDecon::compute_gdamp_inverse()
{
  try{
    if(taper_data) wavelet_taper->apply(this->wavelet);
    /* Assume if we got here wavelet.npts() == nfft*/
    ComplexArray b_fft(this->wavelet.npts(),this->wavelet.s);
    gsl_fft_complex_forward(b_fft.ptr(),1,FFTDeconOperator::nfft,
          wavetable,workspace);
    ComplexArray conj_b_fft(b_fft);
    conj_b_fft.conj();
    ComplexArray denom(conj_b_fft*b_fft);
    /* Compute scaling constants for noise based on noise_floor and the
    noise spectrum */
    double df,fNy;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    fNy=df*static_cast<double>(FFTDeconOperator::nfft/2);
    /* We need largest noise amplitude to establish a relative noise floor.
    We use this std::algorithm to find it in the spectrum vector */
    vector<double>::iterator maxnoise;
    maxnoise=max_element(psnoise.spectrum.begin(),psnoise.spectrum.end());
    //Spectrum is power but need amplitude in this context so sqrt here
    double scaled_noise_floor=noise_floor*sqrt(*maxnoise);

    for(int k=0;k<nfft;++k)
    {
      double *ptr;
      ptr=denom.ptr(k);
      double f;
      f=df*static_cast<double>(k);
      if(f>fNy) f=2.0*fNy-f;  // Fold frequency axis
      double namp=psnoise.amplitude(f);
      double theta;
      if(namp>scaled_noise_floor)
      {
        theta=damp*namp;
      }
      else
      {
        theta=damp*scaled_noise_floor;
      }
      /* This uses a normal equation form so theta must be squared to
      be a form of the standard damped least squares inverse */
      theta=theta*theta;
      /* ptr points to the real part - an oddity of this interface */
      *ptr += theta;
    }
    double *d0=new double[FFTDeconOperator::nfft];
    for(int k=0;k<FFTDeconOperator::nfft;++k) d0[k]=0.0;
    d0[0]=1.0;
    ComplexArray delta0(FFTDeconOperator::nfft,d0);
    delete [] d0;
    gsl_fft_complex_forward(delta0.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
    winv=(conj_b_fft*delta0)/denom;
  }catch(...){throw;};
}
Seismogram CNR3CDecon::process()
{
  const string base_error("CNR3CDecon::process method:  ");
  int j,k;
  try{
    Seismogram rfest(decondata);
    /* This is used to apply a shift to the fft outputs to put signals
    at relative time 0 */
    int t0_shift;
    t0_shift= round((-rfest.t0())/rfest.dt());
    vector<double> wvec;
    wvec.reserve(FFTDeconOperator::nfft);
    /* This is the proper mspass way to preserve history */
    rfest.append_chain("process_sequence","CNR3CDecon");
    if(rfest.npts()!=FFTDeconOperator::nfft) rfest.u=dmatrix(3,FFTDeconOperator::nfft);
    int nhighsnr;
    double df;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    for(k=0;k<3;++k)
    {
      TimeSeries work;
      work=TimeSeries(ExtractComponent(decondata,k),"Invalid");
      if(taper_data) data_taper->apply(work);
      wvec.clear();
      int ntocopy=FFTDeconOperator::nfft;
      if(ntocopy>work.npts()) ntocopy=work.npts();
      for(j=0;j<ntocopy;++j) wvec.push_back(work.s[j]);
      for(j=ntocopy;j<FFTDeconOperator::nfft;++j)
                   wvec.push_back(0.0);

      ComplexArray numerator(FFTDeconOperator::nfft,wvec);
      gsl_fft_complex_forward(numerator.ptr(),1,FFTDeconOperator::nfft,
            wavetable,workspace);
      /* This loop computes QCMetrics of bandwidth fraction that
      is above a defined snr floor - not necessarily the same as the
      regularization floor used in computing the inverse */
      double snrmax;
      snrmax=1.0;
      nhighsnr=0;
      for(j=0;j<FFTDeconOperator::nfft/2;++j)
      {
        double f;
        f=df*static_cast<double>(j);
        Complex64 z=numerator[j];
        double sigamp=abs(z);
        double namp=psnoise.amplitude(f);
        double snr=sigamp/namp;
        if(snr>snrmax) snrmax=snr;
        if(snr>band_snr_floor) ++nhighsnr;
      }
      signal_bandwidth_fraction[k]=static_cast<double>(nhighsnr)
                  / static_cast<double>(FFTDeconOperator::nfft/2);
      peak_snr[k]=snrmax;
      ComplexArray rftmp=numerator*winv;
      rftmp=(*shapingwavelet.wavelet())*rftmp;
      gsl_fft_complex_inverse(rftmp.ptr(), 1, FFTDeconOperator::nfft,
          wavetable, workspace);
      wvec.clear();
      for(j=0;j<FFTDeconOperator::nfft;++j) wvec.push_back(rftmp[j].real());
      /* Note we used a time domain shift instead of using a linear phase
      shift in the frequency domain because time domain operator has a lower
      operation count than the frequency domain algorithm and is thus more
      efficient.*/
      if(t0_shift!=0)
        wvec=circular_shift(wvec,t0_shift);
      for(j=0;j<FFTDeconOperator::nfft;++j)rfest.u(k,j)=wvec[j];
    }
    return rfest;
  }catch(...){throw;};
}
TimeSeries CNR3CDecon::ideal_output()
{
  try{
    CoreTimeSeries ideal_tmp=this->shapingwavelet.impulse_response();
    return TimeSeries(ideal_tmp,"Invalid");
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
      result.set_live();
      result.s=ao;
      result.set_t0(operator_dt*(-(double)i0));
      result.set_dt(this->operator_dt);
      result.set_tref(TimeReferenceType::Relative);
      result.set_npts(FFTDeconOperator::nfft);
      return result;
  } catch(...) {
      throw;
  };
}
TimeSeries CNR3CDecon::inverse_wavelet(double tshift)
{
  try {
    /* Using the time shift of wavelet.t0() may be a bad idea here.  Will
    need to sort that out in debugging behaviour*/
    CoreTimeSeries invcore(this->FFTDeconOperator::FourierInverse(this->winv,
        *shapingwavelet.wavelet(),operator_dt,wavelet.t0()));
    TimeSeries result(invcore,"Invalid");
    /* Copy the error log from wavelet and post some information parameters
    to metadata */
    result.elog=wavelet.elog;
    result.put("waveform_type","deconvolution_inverse_wavelet");
    result.put("decon_type","CNR3CDecon");
    return result;
  } catch(...) {
      throw;
  };
}
Metadata CNR3CDecon::QCMetrics()
{
  Metadata result;
  result.put("waveletbf",regularization_bandwidth_fraction);
  result.put("maxsnr0",peak_snr[0]);
  result.put("maxsnr1",peak_snr[1]);
  result.put("maxsnr2",peak_snr[2]);
  result.put("signalbf0",signal_bandwidth_fraction[0]);
  result.put("signalbf1",signal_bandwidth_fraction[1]);
  result.put("signalbf2",signal_bandwidth_fraction[2]);
  return result;
}
} //end namespace
