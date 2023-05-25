#include <sstream>
#include "mspass/utility/utility.h"
#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/algorithms/deconvolution/dpss.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
namespace mspass::algorithms::deconvolution
{
using namespace std;
using namespace mspass::seismic;
using namespace mspass::utility;

MTPowerSpectrumEngine::MTPowerSpectrumEngine()
{
  taperlen=0;
  ntapers=0;
  nfft=0;
  tbp=0.0;
  deltaf=1.0;
  operator_dt=1.0;
  wavetable=NULL;
  workspace=NULL;
}
MTPowerSpectrumEngine::MTPowerSpectrumEngine(const int winsize,
  const double tbpin,
      const int ntpin,
          const int nfftin,
              const double dtin)
{
  taperlen=winsize;
  tbp=tbpin;
  ntapers=ntpin;
  if(nfftin<winsize)
      nfft = 2*winsize + 1;
  else
      nfft = nfftin;
  /* The call to set_df as implemented makes the initializations below unnecessary
  deltaf=1.0;
  operator_dt=dtin;
  */
  this->set_df(dtin);
  int nseq=static_cast<int>(2.0*tbp);
  if(ntapers>nseq)
  {
    cerr << "MTPowerSpectrumEngine (WARNING):  requested number of tapers="<<ntpin
      << endl
      << "is inconsistent with requested time time bandwidth product ="<<tbp
      << endl
      << "Automatically reset number tapers to max allowed="<<nseq<<endl;
    ntapers=nseq;
  }
  int seql(0);
  int sequ=ntapers-1;
  double *work=new double[ntapers*taperlen];
  dpss_calc(taperlen, tbp, seql, sequ, work);
  tapers=dmatrix(ntapers,taperlen);
  int i,ii,j;
  for(i=0,ii=0; i<ntapers; ++i)
  {
      for(j=0; j<taperlen; ++j)
      {
          tapers(i,j)=work[ii];
          ++ii;
      }
  }
  delete [] work;
  wavetable=gsl_fft_complex_wavetable_alloc (nfft);
  workspace=gsl_fft_complex_workspace_alloc (nfft);
}
MTPowerSpectrumEngine::MTPowerSpectrumEngine(const MTPowerSpectrumEngine& parent) : tapers(parent.tapers)
{
  taperlen=parent.taperlen;
  ntapers=parent.ntapers;
  nfft=parent.nfft;
  tbp=parent.tbp;
  operator_dt=parent.operator_dt;
  deltaf=parent.deltaf;
  wavetable=gsl_fft_complex_wavetable_alloc (nfft);
  workspace=gsl_fft_complex_workspace_alloc (nfft);
}

MTPowerSpectrumEngine::~MTPowerSpectrumEngine()
{
    if(wavetable!=NULL) gsl_fft_complex_wavetable_free (wavetable);
    if(workspace!=NULL) gsl_fft_complex_workspace_free (workspace);
}
MTPowerSpectrumEngine& MTPowerSpectrumEngine::operator=(const MTPowerSpectrumEngine& parent)
{
  if(&parent!=this)
  {
    taperlen=parent.taperlen;
    ntapers=parent.ntapers;
    nfft=parent.nfft;
    tbp=parent.tbp;
    operator_dt=parent.operator_dt;
    deltaf=parent.deltaf;
    tapers=parent.tapers;
    wavetable = gsl_fft_complex_wavetable_alloc (nfft);
    workspace = gsl_fft_complex_workspace_alloc (nfft);
  }
  return *this;
}
PowerSpectrum MTPowerSpectrumEngine::apply(const TimeSeries& d)
{
  try{
    int k;
    /* Used to test for operator sample interval against data sample interval.
    We don't use a epsilon comparison as slippery clock data sometime shave sample
    rates small percentage difference from nominal.*/
    const double DT_FRACTION_TOLERANCE(0.001);
    const string algorithm("MTPowerSpectrumEngine");
    /* We need to define this here to allow posting problems to elog.*/
    PowerSpectrum result;
    int dsize=d.npts();
    vector<double> work;
    work.reserve(this->nfft);
    double dtfrac=fabs(d.dt()-this->operator_dt)/this->operator_dt;
    if(dtfrac>DT_FRACTION_TOLERANCE)
    {
      stringstream ss;
      ss << "Date sample interval="<<d.dt()
         << " does not match operator sample interval="
         << this->operator_dt <<endl
         << "Cannot proceed.  Returning a null result";
      result.elog.log_error("MTPowerSpectrumEngine::apply",
            ss.str(), ErrorSeverity::Invalid);
      return result;
    }

    if(dsize<taperlen)
    {
      stringstream ss;
      ss<<"Received data window of length="<<d.npts()<<" samples"<<endl
         << "Operator length="<<taperlen<<endl
         << "Results may be unreliable"<<endl;
      result.elog.log_error(algorithm,string(ss.str()),ErrorSeverity::Suspect);
      for(k=0;k<taperlen;++k)work.push_back(0.0);
      for(k=0;k<dsize;++k)work[k]=d.s[k];
    }
    else
    {
      if(dsize>taperlen)
      {
        stringstream ss;
        ss<<"Received data window of length="<<d.npts()<<" samples"<<endl
           << "Operator length="<<taperlen<<endl
           << "Results may be unreliable because data will be truncated to taper length"<<endl;
        result.elog.log_error(algorithm,ss.str(),ErrorSeverity::Suspect);
      }
      for(k=0;k<taperlen;++k)work.push_back(d.s[k]);
    }
    /* intentionally omit try catch here because the above logic assures Sizes
    must match here. This overloaded method will throw an exception in that case.*/
    vector<double> spec(this->apply(work));
    /* This scaling is necessary to turn the result into power spectral density
    in units of 1/Hz. Multiply by dt, of course,  is division by the sampling
    frequency that many sources use. */
    for(auto sptr=spec.begin();sptr!=spec.end();++sptr) *sptr *= d.dt();
    result=PowerSpectrum(dynamic_cast<const Metadata&>(d),
       spec,deltaf,string("Multitaper"),0.0,d.dt(),d.npts());
    /* We post these to metadata for the generic PowerSpectrum object. */
    result.put<double>("time_bandwidth_product",tbp);
    result.put<long>("number_tapers",ntapers);
    return result;
  }catch(...){throw;};
}
vector<double> MTPowerSpectrumEngine::apply(const vector<double>& d)
{
  /* This function must be dogmatic about d size = taperlen*/
  if(d.size() != this->taperlen)
  {
    stringstream ss;
    ss<<"MTPowerSpectrumEngine::apply method:  input data vector length of "
       << d.size()<<endl
       << "does not match operator taper length length="<<this->taperlen<<endl
       << "Sizes must match to use this implementation of this algorithm"<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
  double var(0.0);   // sum of squares of data - used for psd scaling below
  for(auto ptr=d.begin();ptr!=d.end();++ptr)var += (*ptr)*(*ptr);
  /* This is the only function in this entire object that does anything
  but housework.   Computes the power spectrum by average DFT of d^*d where
  the average is over the tapes. First taper data and store tapered data in
  tdata container*/
  int i,j;
  vector<ComplexArray> tdata;
  tdata.reserve(ntapers);
  vector<double> work;
  work.reserve(nfft);
  for(i=0; i<ntapers; ++i)
  {
    work.clear();
    /* This will assure part of vector between end of
       * data and nfft is zero padded */
    for(j=0; j<taperlen; ++j) work.push_back(tapers(i,j)*d[j]);
    for(j=taperlen;j<nfft;++j) work.push_back(0.0);
    ComplexArray cwork(nfft,&(work[0]));
    tdata.push_back(cwork);
  }
  /* Now apply DFT to each of tapered arrays */
  for(i=0; i<ntapers; ++i)
  {
      gsl_fft_complex_forward(tdata[i].ptr(),1,nfft,wavetable,workspace);
  }
  /* New version - delete this comment if it works*/
  vector<double> result;
  result.reserve(this->nf());
  for(j=0;j<this->nf();++j) result.push_back(0.0);
  for(i=0;i<ntapers;++i)
  {
    for(j=0;j<this->nf();++j)
    {
      mspass::algorithms::deconvolution::Complex64 z;
      double rp,ip;
      z = tdata[i][j];
      rp = z.real();
      ip = z.imag();
      result[j] += rp*rp + ip*ip;
    }
  }
  /* Scale using Parseval's theorem - this is adapted from Prieto's
  multitaper python implementation.   No need with this to correct for
  summing eigenspecra as sum is aborbed with parseval's theoren scaling*/
  double specssq(0.0),scale;
  for(auto p=result.begin();p!=result.end();++p) specssq += (*p);
  scale = var/(specssq*this->df());
  for(j=0;j<this->nf();++j) result[j] *= scale;
  return result;
}
vector<double> MTPowerSpectrumEngine::frequencies()
{
  vector<double> f;
  /* If taperlen is odd this still works according to gsl documentation.*/
  for(int i=0;i<this->nf();++i)
  {
    /* Here we assume i=0 frequency is 0 */
    f.push_back(deltaf*((double)i));
  }
  return f;
}
}  //end namespace
