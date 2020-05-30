#include <sstream>
#include "mspass/utility/utility.h"
#include "mspass/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/deconvolution/dpss.h"
#include "mspass/deconvolution/ComplexArray.h"
using namespace std;
using namespace mspass;
namespace mspass {
MTPowerSpectrumEngine::MTPowerSpectrumEngine()
{
  taperlen=0;
  ntapers=0;
  tbp=0.0;
  deltaf=1.0;
}
MTPowerSpectrumEngine::MTPowerSpectrumEngine(const int winsize, const double tbpin, const int ntpin)
{
  taperlen=winsize;
  tbp=tbpin;
  ntapers=ntpin;
  deltaf=1.0;
  int nseq=static_cast<int>(2.0*tbp);
  if(ntapers>nseq)
  {
    cerr << "MTPowerSpectrumEngine (WARNING):  requested number of tapers="<<ntpin
      << endl
      << "is inconsistent with requested time time bandwidth product ="<<tbp
      << endl
      << "Rest number tapers to max allowed="<<nseq<<endl;
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
  wavetable=shared_ptr<gsl_fft_complex_wavetable>(gsl_fft_complex_wavetable_alloc (taperlen));
  workspace=shared_ptr<gsl_fft_complex_workspace>(gsl_fft_complex_workspace_alloc (taperlen));
}
MTPowerSpectrumEngine::MTPowerSpectrumEngine(const MTPowerSpectrumEngine& parent) : tapers(parent.tapers)
{
  taperlen=parent.taperlen;
  ntapers=parent.ntapers;
  tbp=parent.tbp;
  deltaf=parent.deltaf;
  wavetable=parent.wavetable;
  workspace=parent.workspace;
}
/*
MTPowerSpectrumEngine::MTPowerSpectrumEngine::~MTPowerSpectrumEngine()
{
    if(wavetable!=NULL) gsl_fft_complex_wavetable_free (wavetable);
    if(workspace!=NULL) gsl_fft_complex_workspace_free (workspace);
}
*/
MTPowerSpectrumEngine& MTPowerSpectrumEngine::operator=(const MTPowerSpectrumEngine& parent)
{
  if(&parent!=this)
  {
    taperlen=parent.taperlen;
    ntapers=parent.ntapers;
    tbp=parent.tbp;
    deltaf=parent.deltaf;
    tapers=parent.tapers;
    /*
    wavetable = gsl_fft_complex_wavetable_alloc (taperlen);
    workspace = gsl_fft_complex_workspace_alloc (taperlen);
    */
    wavetable=parent.wavetable;
    workspace=parent.workspace;
  }
  return *this;
}
PowerSpectrum MTPowerSpectrumEngine::apply(const mspass::TimeSeries& d)
{
  try{
    const string algorithm("MTPowerSpectrumEngine");
    /* We need to define this here to allow posting problems to elog.*/
    PowerSpectrum result;
    int dsize=d.ns;
    vector<double> work;
    deltaf=this->set_df(d.dt);
    if(dsize<taperlen)
    {
      stringstream ss;
      ss<<"Received data window of length="<<d.ns<<" samples"<<endl
         << "Operator length="<<taperlen<<endl
         << "Results may be unreliable"<<endl;
      result.elog.log_error(algorithm,string(ss.str()),ErrorSeverity::Suspect);
      int k;
      for(k=0;k<taperlen;++k)work.push_back(0.0);
      for(k=0;k<dsize;++k)work[k]=d.s[k];
    }
    else if(dsize>taperlen)
    {
      stringstream ss;
      ss<<"Received data window of length="<<d.ns<<" samples"<<endl
         << "Operator length="<<taperlen<<endl
         << "Results may be unreliable because data will be truncated to taper length"<<endl;
      result.elog.log_error(algorithm,ss.str(),ErrorSeverity::Suspect);
      int k;
      for(k=0;k<taperlen;++k)work.push_back(d.s[k]);
    }
    else
    {
      work=d.s;
    }
    /* intentionally omit try catch here because the above logic assures Sizes
    must match here. This overloaded method will throw an exception in that case.*/
    vector<double> spec(this->apply(work));
    result=PowerSpectrum(dynamic_cast<const Metadata&>(d),spec,deltaf,string("Multitaper"));
    /* We post these to metadata for the generic PowerSpectrum object. */
    result.put<double>("time_bandwidth_product",tbp);
    result.put<long>("number_tapers",ntapers);
    return result;
  }catch(...){throw;};
}
vector<double> MTPowerSpectrumEngine::apply(const vector<double>& d)
{
  /* This function must be dogmatic about d size = taperlen*/
  if(d.size() != taperlen)
  {
    stringstream ss;
    ss<<"MTPowerSpectrumEngine::apply method:  input data vector length of "
       << d.size()<<endl
       << "does not match operator taper length="<<taperlen<<endl
       << "Sizes must match to use this implementation of this algorithm"<<endl;
    throw MsPASSError(ss.str(),ErrorSeverity::Invalid);
  }
  /* This is the only function in this entire object that does anything
  but housework.   Computes the power spectrum by average DFT of d^*d where
  the average is over the tapes. First taper data and store tapered data in
  tdata container*/
  int i,j;
  vector<ComplexArray> tdata;
  cerr << "calling reserve"<<endl;
  tdata.reserve(ntapers);
  cerr << "Creating work vector"<<endl;
  vector<double> work;
  cerr << "Calling reserve for second work vector"<<endl;
  work.reserve(taperlen);
  cerr << "Entering loop over tapers"<<endl;
  for(i=0; i<ntapers; ++i)
  {
    work.clear();
    /* This will assure part of vector between end of
       * data and nfft is zero padded */
    for(j=0; j<d.size(); ++j)
    {
      work.push_back(tapers(i,j)*d[j]);
    }
    ComplexArray cwork(taperlen,&(work[0]));
    tdata.push_back(cwork);
  }
  /* Now apply DFT to each of tapered arrays */
  for(i=0; i<ntapers; ++i)
  {
      gsl_fft_complex_forward(tdata[i].ptr(),1,taperlen,
              wavetable.get(),workspace.get());
  }
  /* could bundle this into the previous loop, but clearer here.  We
  accumulate power spectra here - created by A.conj * A . */
  i=0;
  ComplexArray power;
  do{
    ComplexArray work(tdata[i]);
    work.conj();
    if(i==0)
    {
      power=work*tdata[i];
    }
    else
    {
      power+=work*tdata[i];
    }
    ++i;
  }while(i<ntapers);
  vector<double> result;
  /* Documentation for gsl_complex_forward indicates if taperlen is odd
  integer truncated divide by 2 like this (i.e. taperlen/2 below) will
  correctly extract the nyquist frequency sample at the end of the array*/
  result.reserve(taperlen/2);
  double scale=1.0/static_cast<double>(ntapers);
  for(j=0;j<taperlen/2;++j)
  {
    double pval;
    pval=abs(power[j]);
    pval*=scale;
    result.push_back(pval);
  }
  return result;
}
vector<double> MTPowerSpectrumEngine::frequencies()
{
  vector<double> f;
  /* If taperlen is odd this still works according to gsl documentation.*/
  for(int i=0;i<taperlen/2;++i)
  {
    /* Here we assume i=0 frequency is 0 */
    f.push_back(deltaf*((double)i));
  }
  return f;
}
}  //end namespace
