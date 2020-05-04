#include <vector>
#include "mspass/deconvolution/wavelet.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/deconvolution/MTPowerSpectrumEngine.h"
using namespace std;
using namespace mspass;
int main(int argc, char **argv)
{
  cout << "mspass spectrum test program starting"<<endl
    << "Trying default constructor for MTPowerSpectrumEngine"<<endl;
  MTPowerSpectrumEngine mtemp;
  cout << "Trying main constructor for 3 TBP values"<<endl;
  MTPowerSpectrumEngine mtpse2(512,2.5,4);
  MTPowerSpectrumEngine mtpse4(512,4,8);
  MTPowerSpectrumEngine mtpse5(512,5,10);
  double *gtmp,*rtmp;
  gtmp=mspass::gaussian(10.0,1.0,512);
  rtmp=mspass::rickerwavelet(5.0,0.01,512);
  vector<double> g,r;
  int i,nfft(512);
  for(i=0;i<nfft;++i)
  {
    g.push_back(gtmp[i]);
    r.push_back(rtmp[i]);
  }
  cout << "Trying apply method for raw vector of doubles"<<endl;
  vector<double> G,f;
  G=mtpse5.apply(g);
  f=mtpse5.frequencies();
  cout << "Results (f,power)"<<endl;
  for(i=0;i<G.size();++i)
  {
    cout << f[i]<<" "<<G[i]<<endl;
  }
  cout << "Trying TimeSeries apply method"<<endl;
  TimeSeries ts;
  ts.t0=0;
  ts.dt=0.01;
  ts.tref=TimeReferenceType::Relative;
  ts.ns=200;
  ts.live=true;
  ts.s=g;
  PowerSpectrum ps2,ps4,ps5;
  ps2=mtpse2.apply(ts);
  ps4=mtpse4.apply(ts);
  ps5=mtpse5.apply(ts);
  f=mtpse2.frequencies();
  cout << "Results f,tbp25,tbp4,tbp5"<<endl;
  for(i=0;i<f.size();++i)
  {
    cout << ps2.f0+i*ps2.df<<" "<<ps2.spectrum[i]
      << " "<<ps4.spectrum[i]
      << " "<<ps5.spectrum[i]<<endl;
  }
  cout << "Testing interpolation for overloaded amplitude method"<<endl;
  int nfreq2=2*f.size();
  double df2=ps2.df/2.0;
  for(i=0;i<nfreq2;++i)
  {
      double f2=ps2.f0+i*df2;
      double amp=ps2.amplitude(f2);
      cout << f2 << " "<<amp<<endl;
  }
}
