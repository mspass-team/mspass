#include <string.h>  //needed for memcpy
#include <algorithm>
#include "mspass/utility/MsPASSError.h"
#include "mspass/algorithms/deconvolution/CNR3CDecon.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/algorithms/algorithms.h"

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
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;
using namespace mspass::algorithms;
using namespace mspass::algorithms::amplitudes;

CNR3CDecon::CNR3CDecon() : FFTDeconOperator(),
  signalengine(),waveletengine(),dnoise_engine(),wnoise_engine(),
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
  fhs=2.0;   // appropriate for teleseismic P wave data
  for(int k=0;k<3;++k)
  {
    signal_bandwidth_fraction[k]=0.0;
    peak_snr[k]=0.0;
  }
}
CNR3CDecon::CNR3CDecon(const AntelopePf& pf)
   : FFTDeconOperator(pf),signalengine(),waveletengine(),
      dnoise_engine(),wnoise_engine(),shapingwavelet(pf),psnoise(),
      psnoise_data(),decondata(),wavelet()
{
//DEBUG
//cout << "Entering pf constructor"<<endl;
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
    this->snr_bandwidth=pf.get_double("snr_for_bandwidth_estimator");
    this->operator_dt=pf.get_double("target_sample_interval");
    this->fhs = pf.get_double("high_frequency_search_start");
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
    /* ShapingWavelet has more options than can be accepted in this algorithm
    so this test is needed */
    string swname=this->shapingwavelet.type();
    if( !( (swname == "ricker") || (swname == "butterworth") ) )
    {
      throw MsPASSError(string("CNR3CDecon(AntelopePf constructor):  ")
          + "Cannot use shaping wavelet type="+swname
          + "\nMust be either ricker or butterworth for this algorithm",
          ErrorSeverity::Invalid);
    }
    FFTDeconOperator::change_size(nextPowerOf2(minwinsize));
    ts=pf.get_double("noise_window_start");
    te=pf.get_double("noise_window_end");
    this->noise_window=TimeWindow(ts,te);
    int noise_winlength=round((te-ts)/operator_dt)+1;
    double tbp=pf.get_double("time_bandwidth_product");
    long ntapers=pf.get_long("number_tapers");
    this->dnoise_engine=MTPowerSpectrumEngine(noise_winlength,tbp,ntapers);
    /* Default wavelet noise window to data window length - adjusted dynamically
    if changed*/
    this->wnoise_engine=MTPowerSpectrumEngine(noise_winlength,tbp,ntapers);
    /* Set initial signal and wavelet engine spectrum estimators to length defined
    by data window above */
    this->signalengine=MTPowerSpectrumEngine(this->winlength,tbp,ntapers);
    this->waveletengine=MTPowerSpectrumEngine(this->winlength,tbp,ntapers);
    string sval;
    sval=pf.get_string("taper_type");
    /* New parameter added for dynamic bandwidth adjustment feature implemented
    december 2020 */
    decon_bandwidth_cutoff=pf.get_double("decon_bandwidth_cutoff");
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
  signalengine(parent.signalengine),
  waveletengine(parent.waveletengine),
  dnoise_engine(parent.dnoise_engine),
  wnoise_engine(parent.wnoise_engine),
  shapingwavelet(parent.shapingwavelet),
  psnoise(parent.psnoise),
  psnoise_data(parent.psnoise_data),
  pssignal(parent.pssignal),
  pswavelet(parent.pswavelet),
  decondata(parent.decondata),
  wavelet(parent.wavelet),
  wavelet_taper(parent.wavelet_taper),
  data_taper(parent.data_taper),
  ao_fft(parent.ao_fft),
  wavelet_bwd(parent.wavelet_bwd),
  signal_bwd(parent.signal_bwd),
  wavelet_snr(parent.wavelet_snr)

{
  algorithm=parent.algorithm;
  taper_data=parent.taper_data;
  operator_dt=parent.operator_dt;
  winlength=parent.winlength;
  damp=parent.damp;
  noise_floor=parent.noise_floor;
  snr_regularization_floor=parent.snr_regularization_floor;
  snr_bandwidth=parent.snr_bandwidth;
  band_snr_floor=parent.band_snr_floor;
  regularization_bandwidth_fraction=parent.regularization_bandwidth_fraction;
  decon_bandwidth_cutoff=parent.decon_bandwidth_cutoff;
  fhs=parent.fhs;
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
    signalengine=parent.signalengine;
    waveletengine=parent.waveletengine;
    dnoise_engine=parent.dnoise_engine;
    wnoise_engine=parent.wnoise_engine;
    psnoise=parent.psnoise;
    psnoise_data=parent.psnoise_data;
    pssignal=parent.pssignal;
    pswavelet=parent.pswavelet;
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
    snr_bandwidth=parent.snr_bandwidth;
    band_snr_floor=parent.band_snr_floor;
    regularization_bandwidth_fraction=parent.regularization_bandwidth_fraction;
    decon_bandwidth_cutoff=parent.decon_bandwidth_cutoff;
    fhs=parent.fhs;
    wavelet_bwd=parent.wavelet_bwd;
    signal_bwd=parent.signal_bwd;
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
  if(d.time_is_UTC())
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

/* this method is really just a wrapper for the Seismogram, boolean
overloaded function.   It is appropriate only for conventional rf estimates
where the vertical/longitudinal defines the wavelet.  */
void CNR3CDecon::loaddata(Seismogram& d, const int wcomp,const bool loadnoise)
{
//DEBUG
//cout << "entering loaddata"<<endl;
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
        <<endl<<"Check error log of input Seismogram for detailed error messages"<<endl
        << "Operator does not contain valid data for processing"<<endl;
      throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
    }
    CoreSeismogram dtmp(WindowData(d,this->processing_window));
    this->decondata=Seismogram(dtmp,string("invalid"));
    if(FFTDeconOperator::nfft<(2*this->winlength))
    {
      cerr << "CNR3CDecon:  coding error in loaddata method"<<endl
	      << "fft buffer size="<<FFTDeconOperator::nfft<<endl
	      << "winlength is only "<<this->winlength<<endl
	      << "fft length is required to be at least twice winlength"<<endl
	      << "Debug exit to avoid seg fault"<<endl;
      exit(-1);
    }
    /* In the old api this was necessary - no longer so because set_npts does
    this operation
    decondata.u=dmatrix(3,FFTDeconOperator::nfft);
    decondata.u.zero();
    */
    decondata.set_npts(FFTDeconOperator::nfft);
    double newt0= (-operator_dt*static_cast<double>(winlength));
    decondata.set_t0(newt0);
    int k,i,ii;
    for(i=0;i<dtmp.npts();++i)
    {
      ii=decondata.sample_number(dtmp.time(i));
      if( (ii >=0) && (ii<decondata.npts()) )
      {
        for(k=0;k<3;++k)decondata.u(k,ii)=dtmp.u(k,i);
      }
    }

    /* This was the old code
    int k,i,ii;
    for(k=0;k<3;++k)
        for(i=0,ii=(this->winlength);i<(this->winlength);++i,++ii)
        {
            decondata.u(k,ii)=dtmp.u(k,i);
        }
    double newt0=decondata.t0() - operator_dt*static_cast<double>(winlength);
    //Debug
    cout << "in loaddata; initial t0="<<decondata.t0()<<endl
      << "Changed to "<<newt0<<endl;
    decondata.set_t0(newt0);
    */
    /* We need to compute the power spectrum of the signal here.  It is used
    to compute the bandwidth of the shaping wavelet in process.   This is the
    ONLY place right now this is computed.  Caution in order if there are
    changes in implementation later. */
    this->pssignal=this->ThreeCPower(dtmp);
    if(nload)
    {
      Seismogram ntmp(WindowData(d,this->noise_window),string("Invalid"));
      this->loadnoise_data(ntmp);
    }
    signal_bwd=EstimateBandwidth(FFTDeconOperator::df(this->operator_dt),
        pssignal,psnoise_data,snr_bandwidth,
          signalengine.time_bandwidth_product(),this->fhs);
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
    /* Automatically and silently adjust the window size if input wavelet
    length changes.   We always compute the wavelet spectrum from the full
    signal loaded.   */
    if(this->waveletengine.taper_length()!=(w.npts()))
    {
      this->waveletengine=MTPowerSpectrumEngine(w.npts(),
          this->waveletengine.time_bandwidth_product(),
          this->waveletengine.number_tapers());
    }
    this->pswavelet=this->waveletengine.apply(w);
    /* for now use the same snr floor as regularization - may need to be an
    independent parameter */
    this->wavelet_bwd=EstimateBandwidth(FFTDeconOperator::df(this->operator_dt),pswavelet,
      psnoise,snr_bandwidth,waveletengine.time_bandwidth_product(),this->fhs);
    /* Now load the wavelet into the fft buffer area - there are some tricky
    things to do here to align the data correctly using the relative time
    reference t0 stored with the TimeSeries data */
    int k,kk;
    int ns_to_copy,ntest;
    this->wavelet=w;
    ntest=w.npts()-w.sample_number(0.0);
    if(ntest>(this->winlength))
    {
      ns_to_copy=this->winlength;
      stringstream ss;
      ss<<"loadwavelet method:  size mismatch.  Wavelet received has "<<ntest
            <<" samples with time>0.0"<<endl
	      << "This is larger than the processing window length of "<<this->winlength<<endl
	      << "Wavelet must be contained in the processing window size"<<endl
	      << "Truncated on the right to processing window length - results may be invalid"
	      <<endl;
      wavelet.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Complaint);
    }
    else
      ns_to_copy=w.npts();
    /* These were in the old api but are no longer needed because set_npts
    does this operation
    this->wavelet.s.clear();
    this->wavelet.s.reserve(FFTDeconOperator::nfft);
    for(k=0;k<FFTDeconOperator::nfft;++k)this->wavelet.s.push_back(0.0);
    */
    this->wavelet.set_npts(FFTDeconOperator::nfft);
    this->wavelet.set_t0(-(this->winlength)*(this->operator_dt));
//DEBUG
//cout << "t0 of wavelet stored now wavelet data objec="<<wavelet.t0()<<endl;
    /* We recycle the variable ntest defined above here for convenience only */
    for(k=0,ntest=0;k<w.npts();++k)
    {
      double t=w.time(k);
      if(t>=0.0)++ntest;
      if(ntest>ns_to_copy) break;
      kk=this->wavelet.sample_number(t);
      if( (kk>=0) && (kk<this->wavelet.npts()) )
        this->wavelet.s[kk]=w.s[k];
    }
    /* This was the old code fixed Dec 2020 - remove when revision is know to work
    for(k=0,kk=this->winlength;k<ns_to_copy;++k,++kk)this->wavelet.s[kk]=w.s[k];
    */
    //this->wavelet.t0 -= operator_dt*static_cast<double>(winlength);
    //this->wavelet.ns=FFTDeconOperator::nfft;
    //double newt0;
    //newt0=this->wavelet.t0() - operator_dt*static_cast<double>(winlength);
    //this->wavelet.set_t0(newt0);


    //debug
    /*cout << "Wavelet t, data"<<endl;
    for(auto kw=0;kw<FFTDeconOperator::nfft;++kw)
      cout << wavelet.time(kw)<<" "<<wavelet.s[kw]<<endl;
*/
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
//DEBUG
//cout << "Exiting loadwavelet"<<endl;
  }catch(...){throw;};
}
void CNR3CDecon::loadnoise_data(const Seismogram& n)
{
  if(n.dead()) throw MsPASSError("CNR3CDecon::loadnoise_data method received data marked dead",
		    ErrorSeverity::Invalid);
  try{
    this->psnoise_data=this->ThreeCPower(n);
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
//DEBUG
//cout << "Entering loadnoise_wavelet"<<endl;
  if(n.dead()) throw MsPASSError("CNR3CDecon::loadnoise_wavelet method received data marked dead",
		    ErrorSeverity::Invalid);
  if(n.npts()<=0) throw MsPASSError("CNR3CDecon::loadnoise_wavelet method received an empty data vector.",ErrorSeverity::Invalid);
  try{
    if(n.npts()!=wnoise_engine.taper_length())
    {
      wnoise_engine=MTPowerSpectrumEngine(n.npts(),
        wnoise_engine.time_bandwidth_product(),wnoise_engine.number_tapers());
    }
    psnoise=this->wnoise_engine.apply(n);
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
    if(this->wavelet.npts() != FFTDeconOperator::nfft)
    {
      throw MsPASSError("CNR3CDecon::compute_gwl_inverse():  wavelet size and fft size t0 not match - this should not happen and indicates a bug that needs to be fixed",
         ErrorSeverity::Fatal);
    }
    ComplexArray cwvec(this->wavelet.npts(),&(this->wavelet.s[0]));
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
      double namp=sqrt(psnoise.power(f));
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
    ComplexArray b_fft(this->wavelet.npts(),&(this->wavelet.s[0]));
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
    //debug
    //cout << "Damping values used with f"<<endl;

    for(int k=0;k<nfft;++k)
    {
      double *ptr;
      ptr=denom.ptr(k);
      double f;

      f=df*static_cast<double>(k);
      if(f>fNy) f=2.0*fNy-f;  // Fold frequency axis
      double namp=sqrt(psnoise.power(f));
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
      //Debug test - make pure damping
      *ptr += theta;
      //Debug
      //cout << f<<" "<<theta<<" "<<namp<<endl;
    }
    /*
    double *d0=new double[FFTDeconOperator::nfft];
    for(int k=0;k<FFTDeconOperator::nfft;++k) d0[k]=0.0;
    d0[0]=1.0;
    ComplexArray delta0(FFTDeconOperator::nfft,d0);
    delete [] d0;
    gsl_fft_complex_forward(delta0.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
    winv=(conj_b_fft*delta0)/denom;
    */
    winv=conj_b_fft/denom;
  }catch(...){throw;};
}
/* This is a small helper used in process.  Made a function in case this
required a more elaborate method later - i.e. BandwidthData could change. */
BandwidthData band_overlap(const BandwidthData& b1, const BandwidthData& b2)
{
  BandwidthData overlap;
  if(b1.low_edge_f < b2.low_edge_f)
  {
    overlap.low_edge_f = b2.low_edge_f;
    overlap.low_edge_snr = b2.low_edge_snr;
  }
  else
  {
    overlap.low_edge_f = b1.low_edge_f;
    overlap.low_edge_snr = b1.low_edge_snr;
  }
  if(b1.high_edge_f > b2.high_edge_f)
  {
    overlap.high_edge_f = b2.high_edge_f;
    overlap.high_edge_snr = b2.high_edge_snr;
  }
  else
  {
    overlap.high_edge_f = b1.high_edge_f;
    overlap.high_edge_snr = b1.high_edge_snr;
  }
  if(b2.f_range<=b1.f_range)
    overlap.f_range=b2.f_range;
  else
    overlap.f_range=b1.f_range;
  return overlap;
}
/* another helper used below - posts bandwidth data to Metadata.  Put in
a function to make sure it all is in one place */
void post_bandwidth_data(Seismogram& d,const BandwidthData& bwd)
{
  d.put("CNR3CDecon_low_corner",bwd.low_edge_f);
  d.put("CNR3CDecon_high_corner",bwd.high_edge_f);
  d.put("CNR3CDecon_bandwidth",bwd.bandwidth());
  d.put("CNR3CDecon_low_f_snr",bwd.low_edge_snr);
  d.put("CNR3CDecon_high_f_snr",bwd.high_edge_snr);
}
/*  DEBUG Temporary for debug - remove or comment out for release */
void print_bwdata(const BandwidthData& bwd)
{
  cout <<"low edge frequency="<<bwd.low_edge_f<<endl
   <<"low edge snr="<<bwd.low_edge_snr<< endl
   << "high edge frequency="<<bwd.high_edge_f <<endl
   << "high edge snr="<<bwd.high_edge_snr<<endl
   << "Computed bandwidth (db)="<<bwd.bandwidth()<<endl;
}
Seismogram CNR3CDecon::process()
{
//DEBUG
//cout << "Entering process method"<<endl;
  const string base_error("CNR3CDecon::process method:  ");
  int j,k;
  try{
    /* Immediately determine the data bandwidth for efficiency.
    If the data have insufficient bandwith return a null Seismogram
    with only Metadata and no data.   Do, however, post a mesage to
    elog in that situation assuming it will be saved to database an
    can be queried to tell what data were deleted for this reason.
    Some of the tests here are more cautious than needed as load
    functions should catch useless data before getting this far. */
    BandwidthData bo;
    bo=band_overlap(wavelet_bwd, signal_bwd);
    //DEBUG
    cout << "Bandwidth data from wavelet"<<endl;
    print_bwdata(wavelet_bwd);
    cout << "Bandwidth data from 3D data"<<endl;
    print_bwdata(signal_bwd);
    cout << "Bandwidth data overlap "<<endl;
    print_bwdata(bo);
    /* Note both of the quantities in this test must be in consistent
    untis of dB */
    if(bo.bandwidth()<(this->decon_bandwidth_cutoff))
    {
      /* this is a bit inefficient to create this work space and immediately
      destroy it but will do it this way until this proves to be a performance
      problem. */
      Seismogram no_can_do(decondata);
      no_can_do.u=mspass::utility::dmatrix(1,1);
      /* Be sure this is marked dead */
      no_can_do.kill();
      stringstream ss;
      ss << "Killed because estimated bandwidth="<<bo.bandwidth()
         << " dB is below threshold of "<< this->decon_bandwidth_cutoff<<endl;
      no_can_do.elog.log_error("CNR3CDecon",ss.str(),ErrorSeverity::Invalid);
      return no_can_do;
    }
    this->update_shaping_wavelet(bo);
    Seismogram rfest(decondata);
    post_bandwidth_data(rfest,bo);
    /* This is used to apply a shift to the fft outputs to put signals
    at relative time 0 */
    int t0_shift;
    t0_shift= round((-rfest.t0())/rfest.dt());
    //DEBUG
    //cout << "time shift applied to output signal="<<t0_shift<<endl;
    vector<double> wvec;
    wvec.reserve(FFTDeconOperator::nfft);
    /* This is the proper mspass way to preserve history */
    //rfest.append_chain("process_sequence","CNR3CDecon");
    if(rfest.npts()!=FFTDeconOperator::nfft) rfest.u=dmatrix(3,FFTDeconOperator::nfft);
    int nhighsnr;
    double df;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    for(k=0;k<3;++k)
    {
      TimeSeries work;
      work=TimeSeries(ExtractComponent(decondata,k),"Invalid");
      /* Debug - temporarily remove taper
      if(taper_data) data_taper->apply(work);
      */
      wvec.clear();
      int ntocopy=FFTDeconOperator::nfft;
      if(ntocopy>work.npts()) ntocopy=work.npts();
      for(j=0;j<ntocopy;++j) wvec.push_back(work.s[j]);
      for(j=ntocopy;j<FFTDeconOperator::nfft;++j)
                   wvec.push_back(0.0);

      ComplexArray numerator(FFTDeconOperator::nfft,&(wvec[0]));
      //Debug
      //cout << "numerator data after taper for component="<<k<<endl;
      //for(j=0;j<FFTDeconOperator::nfft;++j)cout<<real(numerator[j])<<endl;
      gsl_fft_complex_forward(numerator.ptr(),1,FFTDeconOperator::nfft,
            wavetable,workspace);
      /* This loop computes QCMetrics of bandwidth fraction that
      is above a defined snr floor - not necessarily the same as the
      regularization floor used in computing the inverse */
      double snrmax;
      snrmax=1.0;
      nhighsnr=0;
      //debug
      //cout << "Spectrum f, signal, noise, snr"<<endl;
      for(j=0;j<FFTDeconOperator::nfft/2;++j)
      {
        double f;
        f=df*static_cast<double>(j);
        Complex64 z=numerator[j];
        double sigamp=abs(z);
        double namp=sqrt(psnoise.power(f));
        double snr=sigamp/namp;
        //Debug
        //cout <<f<<" "<< sigamp<<" "<<namp<<" "<<snr<<endl;

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
      //debug
      //cout << "Raw deconvolved data before time shift (re,im)"<<endl;
      //for(j=0;j<FFTDeconOperator::nfft;++j) cout << wvec[j]<<" "<<rftmp[j].imag()<<endl;
      //cout << "Function output uses time shift="<<t0_shift<<endl;
      /* Note we used a time domain shift instead of using a linear phase
      shift in the frequency domain because time domain operator has a lower
      operation count than the frequency domain algorithm and is thus more
      efficient.*/
      /* Debug - temporarily disable this shift*/
      if(t0_shift!=0)
        wvec=circular_shift(wvec,-t0_shift);
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
      result.set_npts(FFTDeconOperator::nfft);
      /* Force these even though they are likely already defined as
      in the parent wavelet TimeSeries. */
      result.set_live();
      //result.s=ao;
      result.set_t0(operator_dt*(-(double)i0));
      result.set_dt(this->operator_dt);
      result.set_tref(TimeReferenceType::Relative);
      /* set_npts always initializes the s buffer so it is more efficient to
      copy ao elements rather than what was here before:
        result.s=ao;
        */
      for(int k=0;k<FFTDeconOperator::nfft;++k) result.s[k]=ao[k];
      return result;
  } catch(...) {
      throw;
  };
}
TimeSeries CNR3CDecon::inverse_wavelet(double tshift0)
{
  try {
    /* Using the time shift of wavelet.t0() may be a bad idea here.  Will
    need to sort that out in debugging behaviour*/
    double timeshift(tshift0);
    timeshift+=wavelet.t0();
    //cout << "wavelet t0="<<wavelet.t0()<<endl;
    timeshift -= operator_dt*((double)winlength);
    //Debug
    //cout << "inverse_wavelet - applying time shift="<<timeshift<<endl;
    CoreTimeSeries invcore(this->FFTDeconOperator::FourierInverse(this->winv,
        *shapingwavelet.wavelet(),operator_dt,timeshift));
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
void CNR3CDecon::update_shaping_wavelet(const BandwidthData& bwd)
{
  string wtype;
  wtype=shapingwavelet.type();
  if(wtype=="butterworth")
  {
    /* For now always use 2 poles as that produces a decent looking wavelet*/
    shapingwavelet=ShapingWavelet(2,bwd.low_edge_f,2,bwd.high_edge_f,
        this->operator_dt,FFTDeconOperator::nfft);
  }else if(wtype=="ricker")
  {
    double favg=(bwd.high_edge_f-bwd.low_edge_f)/2.0;
    shapingwavelet=ShapingWavelet(favg,operator_dt,FFTDeconOperator::nfft);
  }else
  {
    /* this really shouldn't happen but trap it anyway for completeness.
    Because it shouldn't happen we set the severity fatal*/
    throw MsPASSError(string("CNR3CDecon::update_shaping_wavelet:  ")
           + "shaping wavelet has unsupported type defined="+wtype,
         ErrorSeverity::Fatal);
  }
}
/* Note this private function should only be called for noise data */
PowerSpectrum CNR3CDecon::ThreeCPower(const Seismogram& d)
{
  try{
    PowerSpectrum avg3c;
    TimeSeries tswork;
    if(d.npts()!=dnoise_engine.taper_length())
    {
      dnoise_engine=MTPowerSpectrumEngine(d.npts(),
         dnoise_engine.time_bandwidth_product(),dnoise_engine.number_tapers());
    }
    for(int k=0;k<3;++k)
    {
      tswork=TimeSeries(ExtractComponent(d,k),"Invalid");
      if(k==0)
        avg3c = this->dnoise_engine.apply(tswork);
      else
        avg3c += this->dnoise_engine.apply(tswork);
    }
    /* We define total power as the average on all three
    components */
    double scl=1.0/3.0;
    for(int i=0;i<avg3c.nf();++i)
         avg3c.spectrum[i]*=scl;
    return avg3c;
  }catch(...){throw;};
}


} //end namespace
