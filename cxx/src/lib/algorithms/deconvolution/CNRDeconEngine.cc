#include <string>
#include <sstream>
#include "mspass/utility/MsPASSError.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/algorithms/amplitudes.h"
#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"


namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;
using namespace mspass::algorithms::deconvolution;
using mspass::algorithms::amplitudes::normalize;
CNRDeconEngine::CNRDeconEngine() : FFTDeconOperator()
{
  /* This constructor does not initialize everything.  It initializes
  only the simple types and the values are not necessarily reasonable. */
  algorithm = CNR3C_algorithms::colored_noise_damping;
  damp=1.0;
  noise_floor = 1.5;
  band_snr_floor = 1.5;
  shaping_wavelet_number_poles=3;
  snr_regularization_floor = 2.0;
  /* These are computed private variables - we initialize them all to 0*/
  regularization_bandwidth_fraction = 0.0;
  for(auto i=0;i<3;++i)
  {
    peak_snr[i] = 0.0;
    signal_bandwidth_fraction[i] = 0.0;
  }
}
CNRDeconEngine::CNRDeconEngine(const AntelopePf& pf)
  : FFTDeconOperator(dynamic_cast<const Metadata&>(pf))
{
  try{
    string stmp;
    stmp=pf.get_string("algorithm");
    if(stmp=="generalized_water_level")
    {
      this->algorithm=CNR3C_algorithms::generalized_water_level;
    }
    else if(stmp=="colored_noise_damping")
    {
      this->algorithm=CNR3C_algorithms::colored_noise_damping;
    }
    else
    {
      throw MsPASSError("CNRDeconEngine(constructor):  invalid value for parameter algorithm="
        + stmp,ErrorSeverity::Invalid);
    }
    this->damp=pf.get_double("damping_factor");
    /* Note this paramter is used for both the damping method and the
    generalized_water_level */
    this->noise_floor=pf.get_double("noise_floor");
    this->snr_regularization_floor=pf.get_double("snr_regularization_floor");
    this->band_snr_floor = pf.get_double("snr_data_bandwidth_floor");
    this->operator_dt=pf.get_double("target_sample_interval");
    /* These parameters are not cached to the object directly but
    are used to initialize the multitaper engines.   A window is used
    instead of number of samples as it is less error prone to a user than
    requiring them to compute the number from the sample interval. */
    double ts,te;
    ts=pf.get_double("deconvolution_data_window_start");
    te=pf.get_double("deconvolution_data_window_end");
    this->winlength=round((te-ts)/this->operator_dt)+1;
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
    /* This compication is needed because FFTDeconOperator(pf) is called
    prior to this function and it requires operator_nfft.   We need to
    be sure it size is consistent with window size that we just computed */
    if(nfftneeded!=this->get_size())
    {
      FFTDeconOperator::change_size(nfftneeded);
      Metadata pfcopy(pf);
      pfcopy.put("operator_nfft",nfftneeded);
      this->shapingwavelet=ShapingWavelet(pfcopy);
    }
    else
    {
      this->shapingwavelet=ShapingWavelet(pf);
    }
    /* ShapingWavelet has more options than can be accepted in this algorithm
    so this test is needed */
    string swname=this->shapingwavelet.type();
    if( !( (swname == "ricker") || (swname == "butterworth") ) )
    {
      throw MsPASSError(string("CNRDeconEngine(AntelopePf constructor):  ")
          + "Cannot use shaping wavelet type="+swname
          + "\nMust be either ricker or butterworth for this algorithm",
          ErrorSeverity::Fatal);
    }
    if(swname == "butterworth")
    {
      /* These MUST be consistent with FFTDeconOperator pf constructor
      names.   */
      int npoles_lo,npoles_hi;
      npoles_lo=pf.get_int("npoles_lo");
      npoles_hi=pf.get_int("npoles_hi");
      if(npoles_hi != npoles_lo)
      {
        stringstream ss;
        ss << "CNRDeconEngine(Metadata constructor):  "
           << "Butterworth filter high and low number of poles must be equal for this operator"
           << endl
           << "Found npoles_lo="<<npoles_lo<<" and npoles_hi="<<npoles_hi<<endl
           << "Edit parameter file to make those two parameters equal and rerun"<<endl;
        throw MsPASSError(ss.str(),ErrorSeverity::Fatal);
      }
      this->shaping_wavelet_number_poles = npoles_lo;
    }
    /* As with signal we use this for initializing the noise engine
    rather than the number of points, which is all the engine cares about. */
    ts=pf.get_double("noise_window_start");
    te=pf.get_double("noise_window_end");
    int noise_winlength=round((te-ts)/operator_dt)+1;
    double tbp=pf.get_double("time_bandwidth_product");
    long ntapers=pf.get_long("number_tapers");
    this->noise_engine=MTPowerSpectrumEngine(noise_winlength,tbp,ntapers,noise_winlength,this->operator_dt);
    this->signal_engine=MTPowerSpectrumEngine(this->winlength,tbp,ntapers,this->winlength,this->operator_dt);
  }catch(...){throw;};
}
/* Standard copy constructor */
CNRDeconEngine::CNRDeconEngine(const CNRDeconEngine& parent)
  : FFTDeconOperator(parent),
     shapingwavelet(parent.shapingwavelet),
      signal_engine(parent.signal_engine),
        noise_engine(parent.noise_engine),
          winv(parent.winv)
{
  this->algorithm = parent.algorithm;
  this->damp = parent.damp;
  this->noise_floor = parent.noise_floor;
  this->band_snr_floor = parent.band_snr_floor;
  this->operator_dt = parent.operator_dt;
  this->winlength = parent.winlength;
  this->shaping_wavelet_number_poles = parent.shaping_wavelet_number_poles;
  this->snr_regularization_floor = parent.snr_regularization_floor;
  this->regularization_bandwidth_fraction = parent.regularization_bandwidth_fraction;
  for(int i=0;i<3;++i)
  {
    this->peak_snr[i] = parent.peak_snr[i];
    this->signal_bandwidth_fraction[i] = parent.signal_bandwidth_fraction[i];
  }
  winv_t0_lag = parent.winv_t0_lag;
}
CNRDeconEngine& CNRDeconEngine::operator=(const CNRDeconEngine& parent)
{
  if(&parent != this)
  {
    this->shapingwavelet=parent.shapingwavelet;
    this->signal_engine=parent.signal_engine;
    this->noise_engine=parent.noise_engine;
    this->winv=parent.winv;
    this->winv_t0_lag = parent.winv_t0_lag;
    this->algorithm = parent.algorithm;
    this->damp = parent.damp;
    this->noise_floor = parent.noise_floor;
    this->band_snr_floor = parent.band_snr_floor;
    this->operator_dt = parent.operator_dt;
    this->shaping_wavelet_number_poles = parent.shaping_wavelet_number_poles;
    this->snr_regularization_floor = parent.snr_regularization_floor;
    this->regularization_bandwidth_fraction = parent.regularization_bandwidth_fraction;
    for(int i=0;i<3;++i)
    {
      this->peak_snr[i] = parent.peak_snr[i];
      this->signal_bandwidth_fraction[i] = parent.signal_bandwidth_fraction[i];
    }
  }
  return *this;
}
void CNRDeconEngine::initialize_inverse_operator(const TimeSeries& wavelet,
        const TimeSeries& noise_data)
{
  const string alg("CNRDeconEngine::initialize_inverse_operator");
  if(wavelet.dead() || noise_data.dead())
  {
    string message;
    message = alg + string(":  Received TimeSeries inputs marked dead\n");
    if(wavelet.dead()) message += "wavelet signal input was marked dead\n";
    if(noise_data.dead()) message += "noise data segment was marked dead\n";
    throw MsPASSError(message,ErrorSeverity::Invalid);
  }
  try{
    /* Assume wavelet and noise_data have the correct sample rate.
    * python wrapper should guarantee that */
    PowerSpectrum psnoise(this->compute_noise_spectrum(noise_data));
    if(psnoise.dead())
    {
      string message;
      message = alg + string("compute_noise_spectrum method failed - cannot compute inverse opeator");
      throw MsPASSError(message,ErrorSeverity::Invalid);
    }
    this->initialize_inverse_operator(wavelet,psnoise);
  }catch(...){throw;};
}
void CNRDeconEngine::initialize_inverse_operator(const TimeSeries& wavelet,
        const PowerSpectrum& noise_spectrum)
{
  string alg("CNRDeconEngine::initialize_inverse_operator");
  if(wavelet.dead())
  {
    string message;
    message = alg + string("Received wavelet signal marked dead");
    throw MsPASSError(message,ErrorSeverity::Invalid);
  }
  if(noise_spectrum.dead())
  {
    string message;
    message = alg + string("Received a PowerSpectrum object marked dead");
    throw MsPASSError(message,ErrorSeverity::Invalid);
  }
  try{
    this->compute_winv(wavelet,noise_spectrum);
  }catch(...){throw;};
}
PowerSpectrum CNRDeconEngine::compute_noise_spectrum(const TimeSeries& n)
{
  if(n.dead())
  {
    PowerSpectrum badout;
    badout.elog.log_error("CNRDeconEngine:compute_noise",
        "Received noise data segment marked dead",
        ErrorSeverity::Invalid);
    return badout;
  }
  try{
    if(n.npts()!=noise_engine.taper_length())
    {
      /* use this varaint of the construtor too allow the fft size to
       * be automatically changed if necessary.  */
      this->noise_engine=MTPowerSpectrumEngine(n.npts(),
        noise_engine.time_bandwidth_product(),noise_engine.number_tapers());
      /* with auto fft we also need this to set the df and dt correctly */
      this->noise_engine.set_df(this->operator_dt);
    }
    return this->noise_engine.apply(n);
  }catch(...){throw;};
}
PowerSpectrum CNRDeconEngine::compute_noise_spectrum(const Seismogram& n)
{
  if(n.dead())
  {
    PowerSpectrum badout;
    badout.elog.log_error("CNRDeconEngine:compute_noise",
        "Received noise data segment marked dead",
        ErrorSeverity::Invalid);
    return badout;
  }
  try{
    PowerSpectrum avg3c;
    TimeSeries tswork;
    if(n.npts()!=noise_engine.taper_length())
    {
      /* this may change fft size and will set dt wrong so we need the
       * call to set_df afterward to correct that. */
      noise_engine=MTPowerSpectrumEngine(n.npts(),
         noise_engine.time_bandwidth_product(),noise_engine.number_tapers());
      noise_engine.set_df(this->operator_dt);
    }
    for(int k=0;k<3;++k)
    {
      tswork=TimeSeries(ExtractComponent(n,k),"Invalid");
      PowerSpectrum psnoise = this->noise_engine.apply(tswork);
      if(psnoise.dead())
      {
        return psnoise;
      }
      if(k==0)
        avg3c = psnoise;
      else
        avg3c += psnoise;
    }
    /* We define total power as the average on all three
    components */
    double scl=1.0/3.0;
    for(int i=0;i<avg3c.nf();++i)
         avg3c.spectrum[i]*=scl;
    return avg3c;
  }catch(...){throw;};
}

/* Small helper used by process method to validate the sample
   rate of wavelet and data loaded for processing.   Uses a
   frozen fractional value test to allow for some data with slippery
   clocks.  Returns true of the d.dt and operator_dt are significantly
   different and false if they are approximately equal.
*/
bool sample_interval_invalid(const mspass::seismic::BasicTimeSeries& d,
        const double operator_dt)
{
  /* Fractional error allowed in sample interval */
  const double DTSKEW(0.0001);
  double frac;
  frac = abs(d.dt() - operator_dt)/operator_dt;
  if(frac<DTSKEW)
  {
      return true;
  }
  else
  {
      return false;
  }
}
/*! private method of this class that computes the internal
  variable "winv" - a ComplexArray containing the spectrum of the
  inverse wavelet.   It assumes the content of the internally
  cached "wavelet" TimeSeries object contains the data to be
  used to compute winv.   Which inverse operator to use is
  controlled by this->algorithm.

  Do not use this method outside of the internal use.   Its
  intrinsic state dependence on loading the wavelet data before
  calling it is very error prone.  It is done here largely for
  convenience to modularize the algorithm.
  */
void CNRDeconEngine::compute_winv(const TimeSeries& wavelet, const PowerSpectrum& psnoise)
{
  /* Because this is a private method we don't test if wavelet and psnoise
  are marked dead.  Methods that call this one should always do so though.*/
  try{
    /* Need to always create a local copy to allow taper option to work corectly.
       Also wavelet is passed const so taper would not work anyway */
    TimeSeries w(wavelet);
    this->winv_t0_lag = w.sample_number(0.0);
    switch(algorithm)
    {
      case CNR3C_algorithms::generalized_water_level:
        compute_gwl_inverse(w,psnoise);
        break;
      case CNR3C_algorithms::colored_noise_damping:
      default:
        compute_gdamp_inverse(w,psnoise);
    };

  }catch(...){throw;};
}

void CNRDeconEngine::compute_gwl_inverse(const TimeSeries& wavelet, const PowerSpectrum& psnoise)
{
  try{
    if(wavelet.npts() != FFTDeconOperator::nfft)
    {
      throw MsPASSError("CNRDeconEngine::compute_gwl_inverse():  wavelet size and fft size t0 not match - this should not happen and indicates a bug that needs to be fixed",
         ErrorSeverity::Fatal);
    }
    ComplexArray cwvec(wavelet.npts(),wavelet.s[0]);
    gsl_fft_complex_forward(cwvec.ptr(),1,FFTDeconOperator::nfft,
          wavetable,workspace);
    /* This computes the (regularized) denominator for the decon operator*/
    double df,fNy;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    fNy=df*static_cast<double>(FFTDeconOperator::nfft/2);
    /* We need largest noise amplitude to establish a relative noise floor.
    We use this std::algorithm to find it in the spectrum vector */
    vector<double>::iterator maxnoise;
    /* Copy needed because max_element alters content of work vector */
    vector<double> work(psnoise.spectrum);
    maxnoise=std::max_element(work.begin(),work.end());
    //spectrum is power, we need amplitude so sqrt here
    double scaled_noise_floor=noise_floor*sqrt(*maxnoise);
    vector<double> wavelet_snr;
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
    this->regularization_bandwidth_fraction=static_cast<double>(nreg)
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
void CNRDeconEngine::compute_gdamp_inverse(const TimeSeries& wavelet, const PowerSpectrum& psnoise)
{
  try{
    /* Assume if we got here wavelet.npts() == nfft*/
    ComplexArray b_fft;
    if(wavelet.npts()==FFTDeconOperator::nfft)
    {
      b_fft = ComplexArray(wavelet.npts(),wavelet.s[0]);
    }
    else if(wavelet.npts() < FFTDeconOperator::nfft)
    {
      /* In this case we zero pad*/
      std::vector<double> btmp;
      btmp.reserve(FFTDeconOperator::nfft);
      for(auto i=0;i<wavelet.npts();++i) btmp.push_back(wavelet.s[i]);
      for(auto i=wavelet.npts();i<FFTDeconOperator::nfft;++i) btmp.push_back(0.0);
      b_fft = ComplexArray(FFTDeconOperator::nfft,btmp);
    }
    else
    {
      /* land here if we need to change the fft size - if this happens don't force power of 2*/
      this->change_size(wavelet.npts());
    }
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
    /* Copy needed because max_element alters content of work vector */
    vector<double> work(psnoise.spectrum);
    maxnoise=std::max_element(work.begin(),work.end());
    //Spectrum is power but need amplitude in this context so sqrt here
    double scaled_noise_floor=noise_floor*sqrt(*maxnoise);\

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
      *ptr += theta;
    }
    winv=conj_b_fft/denom;
  }catch(...){throw;};
}
/* Computes deconvolution of data in d using inverse operator that was assumed
to be previously loaded via the initialize_wavelet method.   Note a potential
confusion is that because this opeation is done in the frequency domain
we do NOT apply the shaping wavelet filter to either the data (numerator) or
the denominator (the inverse wavelet) as required by convolutional quelling
(an obscure name for this regularization from an old Backus paper).   The reason
is the form is (f)(d)/(f)(w)  where f is the shaping wavelet filter, d is
the numerator fft, and w is the wavelet fft.   The f terms cancel so we
don't apply them to either the numerator or denominator.  We do need to
post filter with the shaping wavelet, which is done here, or the output
will almost always be junk.
*/
Seismogram CNRDeconEngine::process(const Seismogram& d, const PowerSpectrum& psnoise,
    const double fl, const double fh)
{
  const string alg("CNRDeconEngine::process");
  if(d.dead() || psnoise.dead())
  {
    Seismogram dout(d);
    dout.set_npts(0);
    if(d.dead())
    {
      dout.elog.log_error(alg,
        "received Seismogram input segment marked dead - cannot process",
        ErrorSeverity::Invalid);
    }
    if(psnoise.dead())
    {
      dout.elog.log_error(alg,
        "received PowerSpectrum object marked dead - cannot process",
        ErrorSeverity::Invalid);
    }
    dout.kill();
    return dout;
  }
  try{
    string base_error("CNRDeconEngine::process:  ");
    this->update_shaping_wavelet(fl,fh);
    Seismogram rfest(d);
    /* This is used to apply a shift to the fft outputs to put signals
    at relative time 0 */
    int t0_shift;
    t0_shift= round((-rfest.t0())/rfest.dt());
    vector<double> wvec;
    wvec.reserve(FFTDeconOperator::nfft);
    if(rfest.npts()!=FFTDeconOperator::nfft) rfest.u=dmatrix(3,FFTDeconOperator::nfft);
    int nhighsnr;
    double df;
    df=1.0/(operator_dt*static_cast<double>(FFTDeconOperator::nfft));
    for(int k=0;k<3;++k)
    {
      TimeSeries work;
      work=TimeSeries(ExtractComponent(d,k),"Invalid");
      wvec.clear();
      int ntocopy=FFTDeconOperator::nfft;
      if(ntocopy>work.npts()) ntocopy=work.npts();
      for(int j=0;j<ntocopy;++j) wvec.push_back(work.s[j]);
      for(int j=ntocopy;j<FFTDeconOperator::nfft;++j)
                   wvec.push_back(0.0);

      ComplexArray numerator(FFTDeconOperator::nfft,&(wvec[0]));
      gsl_fft_complex_forward(numerator.ptr(),1,FFTDeconOperator::nfft,
            wavetable,workspace);
      /* This loop computes QCMetrics of bandwidth fraction that
      is above a defined snr floor - not necessarily the same as the
      regularization floor used in computing the inverse */
      double snrmax;
      snrmax=1.0;
      nhighsnr=0;
      for(int j=0;j<FFTDeconOperator::nfft/2;++j)
      {
        double f;
        f=df*static_cast<double>(j);
        Complex64 z=numerator[j];
        double sigamp=abs(z);
        double namp=sqrt(psnoise.power(f));
        double snr=sigamp/namp;

        if(snr > snrmax) snrmax=snr;
        if(snr > this->band_snr_floor) ++nhighsnr;
      }
      signal_bandwidth_fraction[k]=static_cast<double>(nhighsnr)
                  / static_cast<double>(FFTDeconOperator::nfft/2);
      peak_snr[k]=snrmax;
      ComplexArray rftmp=numerator*winv;
      rftmp=(*this->shapingwavelet.wavelet())*rftmp;
      gsl_fft_complex_inverse(rftmp.ptr(), 1, FFTDeconOperator::nfft,
          wavetable, workspace);
      wvec.clear();
      for(int j=0;j<FFTDeconOperator::nfft;++j) wvec.push_back(rftmp[j].real());
      /* Note we used a time domain shift instead of using a linear phase
      shift in the frequency domain because the time domain operator has a lower
      operation count than the frequency domain algorithm and is thus more
      efficient.*/
      if(t0_shift!=0)
        wvec=circular_shift(wvec,-t0_shift);
      for(int j=0;j<FFTDeconOperator::nfft;++j)rfest.u(k,j)=wvec[j];
    }
    return rfest;
  }catch(...){throw;};
}

void CNRDeconEngine::update_shaping_wavelet(const double fl,const double fh)
{
  string wtype;
  wtype=shapingwavelet.type();
  if(wtype=="butterworth")
  {
    /* shaping_wavelet_number_poles is a private attribute of the class*/
    shapingwavelet=ShapingWavelet(this->shaping_wavelet_number_poles,fl,
      this->shaping_wavelet_number_poles,fh,
        this->operator_dt,FFTDeconOperator::nfft);
  }else if(wtype=="ricker")
  {
    double favg=(fh-fl)/2.0;
    shapingwavelet=ShapingWavelet(favg,operator_dt,FFTDeconOperator::nfft);
  }else
  {
    /* this really shouldn't happen but trap it anyway for completeness.
    Because it shouldn't happen we set the severity fatal*/
    throw MsPASSError(string("CNRDeconEngine::update_shaping_wavelet:  ")
           + "shaping wavelet has unsupported type defined="+wtype,
         ErrorSeverity::Fatal);
  }
}
TimeSeries CNRDeconEngine::ideal_output()
{
  try{
    CoreTimeSeries ideal_tmp=this->shapingwavelet.impulse_response();
    return TimeSeries(ideal_tmp,"Invalid");
  }catch(...){throw;};
}
TimeSeries CNRDeconEngine::actual_output(const TimeSeries& wavelet)
{
  if(wavelet.dead())
  {
    TimeSeries badout(wavelet);
    badout.kill();
    badout.set_npts(0);
    badout.elog.log_error("CRFDeconEngine::actual_output",
       "received wavelet data via arg0 marked dead - cannot procede",
       ErrorSeverity::Invalid);
    return badout;
  }
  TimeSeries result(wavelet);  // Use this to clone metadata and elog from wavelet
  result.set_npts(FFTDeconOperator::nfft);
  /* Force these even though they are likely already defined as
  in the parent wavelet TimeSeries. */
  result.set_live();
  /* We always shift this wavelet to the center of the data vector.
  We handle the time through the CoreTimeSeries object. */
  int i0=FFTDeconOperator::nfft/2;
  result.set_t0(operator_dt*(-(double)i0));
  result.set_dt(this->operator_dt);
  result.set_tref(TimeReferenceType::Relative);
  /* We need to require that wavelet time range is consistent with
   * operator.   We assume relative time so we demand wavelet t0 be less
   * than or equal to nff2/2 to assure a wavelet signal is in the
   * the range -nfft/2 to nff2/2.  */
  int w_t0_lag;
  w_t0_lag = wavelet.sample_number(0.0);
  /* We correct the relative phase of the input wavelet to that
  saved when winv was created. */
  w_t0_lag -= this->winv_t0_lag;
  /* note we handle two extremes differently*/
  if(w_t0_lag>=FFTDeconOperator::nfft)
  {
    stringstream ss;
    ss << "actual_output method received wavelet with t0="
       << wavelet.t0() << " that resolves to offset of "<<w_t0_lag<<" samples"
       <<endl
       <<"That exceeds buffer for frequency domain calculation of size="
       <<FFTDeconOperator::nfft
       <<endl
       <<"Cannot compute actual_output because we assume signal is in range t>0"
       <<endl;
    result.elog.log_error("CNRDeconEngine::actual_output",
         ss.str(), ErrorSeverity::Invalid);
    result.kill();
    result.set_npts(0);
    return result;
  }
  else if(w_t0_lag>(FFTDeconOperator::nfft)/2)
  {
    stringstream ss;
    ss << "Warning: actual output method received wavelet with t0="
       << wavelet.t0() << " that resolves to offset of "<<w_t0_lag<<" samples"
       <<endl
       <<"That exceeds the midpoint of the frequency domain buffer used by this method="
       <<FFTDeconOperator::nfft/2
       <<endl
       <<"Result may be incorrect as the function assumes the signal is after time 0"
       <<endl;
    result.elog.log_error("CNRDeconEngine::actual_output",
         ss.str(), ErrorSeverity::Complaint);
  }

  try {
      std::vector<double> work;
      if(wavelet.npts() == FFTDeconOperator::nfft)
      {
        work = wavelet.s;
      }
      else
      {
        work.reserve(FFTDeconOperator::nfft);
        int i,nend;
        for(i=0;i<FFTDeconOperator::nfft;++i) work.push_back(0.0);
        if(wavelet.npts()>FFTDeconOperator::nfft)
          nend = FFTDeconOperator::nfft;
        else
          nend = wavelet.npts();
        for(i=0;i<nend;++i) work[i] = wavelet.s[i];
      }
      /* This converts wavelet to zero phase - needed to preserve timing.*/
      work = circular_shift(work,w_t0_lag);
      ComplexArray W(FFTDeconOperator::nfft,&(work[0]));
      gsl_fft_complex_forward(W.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
      ComplexArray ao_fft;
      ao_fft=this->winv*W;
      ComplexArray *stmp = this->shapingwavelet.wavelet();
      /* We always apply the shaping wavelet - this perhaps should be optional
      but probably better done with a none option for the shaping wavelet */
      ao_fft=(*stmp)*ao_fft;
      gsl_fft_complex_inverse(ao_fft.ptr(),1,FFTDeconOperator::nfft,wavetable,workspace);
      vector<double> ao;
      ao.reserve(FFTDeconOperator::nfft);
      for(int k=0; k<ao_fft.size(); ++k) ao.push_back(ao_fft[k].real());
      ao=circular_shift(ao,i0);
      ao = normalize<double>(ao);
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
TimeSeries CNRDeconEngine::inverse_wavelet(const TimeSeries& wavelet, const double tshift0)
{
  try {
    /* Using the time shift of wavelet.t0() may be a bad idea here.  Will
    need to sort that out in debugging behaviour*/
    double timeshift(tshift0);
    timeshift+=wavelet.t0();
    timeshift -= operator_dt*((double)this->winlength);
    CoreTimeSeries invcore(this->FFTDeconOperator::FourierInverse(this->winv,
        *this->shapingwavelet.wavelet(),operator_dt,timeshift));
    TimeSeries result(invcore,"Invalid");
    /* Copy the error log from wavelet and post some information parameters
    to metadata */
    result.elog=wavelet.elog;
    result.put("waveform_type","deconvolution_inverse_wavelet");
    result.put("decon_type","CNRDeconEngine");
    return result;
  } catch(...) {
      throw;
  };
}

Metadata CNRDeconEngine::QCMetrics()
{
  Metadata result;
  result.put("waveletbf",this->regularization_bandwidth_fraction);
  result.put("maxsnr0",peak_snr[0]);
  result.put("maxsnr1",peak_snr[1]);
  result.put("maxsnr2",peak_snr[2]);
  result.put("signalbf0",signal_bandwidth_fraction[0]);
  result.put("signalbf1",signal_bandwidth_fraction[1]);
  result.put("signalbf2",signal_bandwidth_fraction[2]);
  return result;
}
}  // end namespace enscapsulation
